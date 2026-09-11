# route: supertable.streaming.jobs
"""Streaming query jobs: state in Redis, data spilled to object storage.

WHY A JOB AT ALL

``DataReader.stream`` already streams Arrow batches without materialising the
result, and for a consumer in the same process that is the whole answer — no
job, no spill, no extra copy. Use it when you can.

A job exists for the case that one does not cover: the producer and the consumer
are in DIFFERENT containers. The instance that runs the query is not necessarily
the instance the next HTTP request lands on, so the result cannot live in the
producer's memory. It has to go somewhere both can reach.

WHERE THE DATA LIVES

Chunks are Arrow IPC streams written to the lake's own object storage under a
per-job prefix; Redis holds only the job record and an ordered manifest of chunk
references. Redis is deliberately not the data path — a "larger export without
materializing the entire result" is exactly the payload you must not put in
memory on a shared server.

Because the manifest is an append-only LIST, a consumer can read chunk N while
the producer is still writing chunk N+1. That is what makes this streaming
rather than poll-until-done, and it is why the manifest is a separate key from
the job document rather than a field inside it.

BACKPRESSURE

The producer stops when it is ``SUPERTABLE_STREAM_MAX_AHEAD_CHUNKS`` ahead of
the slowest acknowledged read. Without that, "stream a huge export" degrades
into "write the entire result to storage as fast as possible", which is the
memory problem moved rather than solved.

CANCELLATION AND DEADLINES

Both are checked between batches, and both act through the same mechanism: the
cursor is interrupted, which raises inside the in-flight read. Checking between
batches means the granularity is one batch, so batch size is also the
cancellation latency knob.

A job that dies with its container leaves keys behind; ``reap`` exists for that,
and every key carries a TTL as a backstop.
"""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from supertable import redis_keys as RK
from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.redis_catalog import RedisCatalog


class JobState:
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"

    TERMINAL = frozenset({DONE, FAILED, CANCELLED, EXPIRED})


def new_job_id() -> str:
    """Lowercase hex: the Redis key validator rejects anything else."""
    return uuid.uuid4().hex[:16]


def instance_id() -> str:
    """Identifies the container that owns a job, for diagnosis and reaping."""
    return f"{socket.gethostname()}-{os.getpid()}"


@dataclass
class ChunkRef:
    index: int
    path: str
    rows: int
    bytes: int


@dataclass
class JobRecord:
    job_id: str
    organization: str
    super_name: str
    sql: str
    role_name: str
    state: str = JobState.PENDING
    created_ts: float = 0.0
    deadline_ts: float = 0.0
    owner: str = ""
    rows: int = 0
    chunks: int = 0
    bytes: int = 0
    # Chunks the consumer has finished with; drives backpressure.
    acked: int = 0
    error: str = ""
    schema_json: str = ""
    batch_rows: int = 0
    fullscan: bool = False

    def to_redis(self) -> Dict[str, str]:
        return {k: ("1" if v is True else "0" if v is False else str(v))
                for k, v in asdict(self).items()}

    @staticmethod
    def from_redis(raw: Dict[Any, Any]) -> "JobRecord":
        def s(k, d=""):
            v = raw.get(k, raw.get(k.encode() if isinstance(k, str) else k, d))
            return v.decode() if isinstance(v, (bytes, bytearray)) else v
        return JobRecord(
            job_id=s("job_id"), organization=s("organization"),
            super_name=s("super_name"), sql=s("sql"), role_name=s("role_name"),
            state=s("state", JobState.PENDING),
            created_ts=float(s("created_ts", "0") or 0),
            deadline_ts=float(s("deadline_ts", "0") or 0),
            owner=s("owner"), rows=int(s("rows", "0") or 0),
            chunks=int(s("chunks", "0") or 0), bytes=int(s("bytes", "0") or 0),
            error=s("error"), schema_json=s("schema_json"),
            acked=int(s("acked", "0") or 0),
            batch_rows=int(s("batch_rows", "0") or 0),
            fullscan=s("fullscan", "0") == "1",
        )


def chunk_prefix(organization: str, super_name: str, job_id: str) -> str:
    """Storage prefix for one job's spilled chunks.

    Leading underscore keeps it out of the way of table directories; the job id
    scopes it so cleanup is a prefix delete.
    """
    return f"{organization}/{super_name}/_query_jobs/{job_id}"


class JobStore:
    """Redis-side job state. Holds no data — only references to it."""

    def __init__(self, catalog: Optional[RedisCatalog] = None):
        self.catalog = catalog or RedisCatalog()

    # -- redis handle ----------------------------------------------------

    @property
    def _r(self):
        """The catalog's Redis client.

        Reached through RedisCatalog rather than opened separately so sentinel
        discovery, failover and retry behaviour stay defined in exactly one
        place — a second client here would drift from it silently.
        """
        client = getattr(self.catalog, "r", None)
        if client is None:
            raise RuntimeError("RedisCatalog has no client (.r is None)")
        return client

    # -- lifecycle -------------------------------------------------------

    def create(self, organization: str, super_name: str, sql: str,
               role_name: str, *, deadline_sec: Optional[int] = None,
               batch_rows: int = 0, fullscan: bool = False) -> JobRecord:
        now = time.time()
        budget = (settings.SUPERTABLE_STREAM_DEADLINE_SEC
                  if deadline_sec is None else deadline_sec)
        rec = JobRecord(
            job_id=new_job_id(), organization=organization,
            super_name=super_name, sql=sql, role_name=role_name,
            state=JobState.PENDING, created_ts=now,
            # 0 means "no deadline" — an export may legitimately outlive any
            # fixed budget, so this must be expressible.
            deadline_ts=(now + budget) if budget else 0.0,
            owner="", batch_rows=batch_rows, fullscan=fullscan,
        )
        r = self._r
        doc = RK.query_job_doc(organization, super_name, rec.job_id)
        ttl = settings.SUPERTABLE_STREAM_JOB_TTL_SEC
        pipe = r.pipeline()
        pipe.hset(doc, mapping=rec.to_redis())
        pipe.expire(doc, ttl)
        pipe.sadd(RK.query_job_index(organization, super_name), rec.job_id)
        pipe.execute()
        logger.info(f"[stream.job] created {rec.job_id} deadline={rec.deadline_ts:.0f}")
        return rec

    def get(self, organization: str, super_name: str,
            job_id: str) -> Optional[JobRecord]:
        raw = self._r.hgetall(RK.query_job_doc(organization, super_name, job_id))
        if not raw:
            return None
        return JobRecord.from_redis(raw)

    def update(self, rec: JobRecord, **fields) -> None:
        for k, v in fields.items():
            setattr(rec, k, v)
        key = RK.query_job_doc(rec.organization, rec.super_name, rec.job_id)
        mapping = {k: ("1" if v is True else "0" if v is False else str(v))
                   for k, v in fields.items()}
        self._r.hset(key, mapping=mapping)

    def set_state(self, rec: JobRecord, state: str, error: str = "") -> None:
        self.update(rec, state=state, error=error[:2000])
        if state in JobState.TERMINAL:
            # Terminal jobs keep their chunks for the TTL so a consumer that
            # disconnected can still collect them.
            ttl = settings.SUPERTABLE_STREAM_JOB_TTL_SEC
            org, sup, jid = rec.organization, rec.super_name, rec.job_id
            for key in (RK.query_job_doc(org, sup, jid),
                        RK.query_job_chunks(org, sup, jid)):
                try:
                    self._r.expire(key, ttl)
                except Exception:
                    pass

    # -- manifest --------------------------------------------------------

    def append_chunk(self, rec: JobRecord, ref: ChunkRef) -> None:
        """Publish a chunk. Consumers may read it the moment this returns."""
        key = RK.query_job_chunks(rec.organization, rec.super_name, rec.job_id)
        pipe = self._r.pipeline()
        pipe.rpush(key, json.dumps(asdict(ref)))
        pipe.expire(key, settings.SUPERTABLE_STREAM_JOB_TTL_SEC)
        pipe.execute()

    def chunks(self, organization: str, super_name: str, job_id: str,
               start: int = 0) -> List[ChunkRef]:
        key = RK.query_job_chunks(organization, super_name, job_id)
        out = []
        for raw in self._r.lrange(key, start, -1):
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            out.append(ChunkRef(**json.loads(raw)))
        return out

    def chunk_count(self, organization: str, super_name: str,
                    job_id: str) -> int:
        return int(self._r.llen(
            RK.query_job_chunks(organization, super_name, job_id)) or 0)

    # -- cancellation ----------------------------------------------------

    def cancel(self, organization: str, super_name: str, job_id: str) -> bool:
        """Request cancellation. Any instance may call this, not just the owner.

        A separate key rather than a field on the job document: cancelling is
        then a single SET that cannot race with the producer's counter updates,
        and the producer's between-batch check is one cheap GET.
        """
        key = RK.query_job_cancel(organization, super_name, job_id)
        self._r.set(key, "1", ex=settings.SUPERTABLE_STREAM_JOB_TTL_SEC)
        logger.info(f"[stream.job] cancel requested for {job_id}")
        return True

    def is_cancelled(self, organization: str, super_name: str,
                     job_id: str) -> bool:
        return bool(self._r.exists(
            RK.query_job_cancel(organization, super_name, job_id)))

    # -- cleanup ---------------------------------------------------------

    def delete(self, organization: str, super_name: str, job_id: str,
               storage=None) -> None:
        """Drop a job's Redis keys and its spilled chunks."""
        if storage is not None:
            for ref in self.chunks(organization, super_name, job_id):
                try:
                    storage.delete(ref.path)
                except Exception:
                    pass
        r = self._r
        pipe = r.pipeline()
        for key in (RK.query_job_doc(organization, super_name, job_id),
                    RK.query_job_chunks(organization, super_name, job_id),
                    RK.query_job_cancel(organization, super_name, job_id)):
            pipe.delete(key)
        pipe.srem(RK.query_job_index(organization, super_name), job_id)
        pipe.execute()

    def list_jobs(self, organization: str, super_name: str) -> List[str]:
        vals = self._r.smembers(RK.query_job_index(organization, super_name))
        return sorted(v.decode() if isinstance(v, (bytes, bytearray)) else v
                      for v in (vals or []))

    def reap(self, organization: str, super_name: str, storage=None) -> int:
        """Remove jobs whose document has expired out from under the index.

        A container killed mid-export leaves the index entry behind; the doc TTL
        expires but the set member does not. Without this the index grows
        without bound and every listing gets slower.
        """
        removed = 0
        for job_id in self.list_jobs(organization, super_name):
            if self.get(organization, super_name, job_id) is None:
                self.delete(organization, super_name, job_id, storage=storage)
                removed += 1
        if removed:
            logger.info(f"[stream.job] reaped {removed} orphaned job(s)")
        return removed


def write_chunk(storage, path: str, batches: List[pa.RecordBatch],
                schema: pa.Schema) -> int:
    """Serialise batches as one Arrow IPC stream and store it.

    Arrow IPC rather than Parquet: this is a transient handoff, so the cost that
    matters is encode plus decode, not long-term size. IPC keeps the exact
    schema, including types Parquet would round-trip differently.
    """
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        for b in batches:
            writer.write_batch(b)
    buf = sink.getvalue()
    data = buf.to_pybytes()
    storage.write_bytes(path, data)
    return len(data)


def read_chunk(storage, path: str) -> Iterator[pa.RecordBatch]:
    """Read one spilled chunk back as record batches."""
    raw = storage.read_bytes(path)
    with pa.ipc.open_stream(pa.BufferReader(pa.py_buffer(raw))) as reader:
        for batch in reader:
            yield batch
