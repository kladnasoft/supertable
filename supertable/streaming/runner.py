# route: supertable.streaming.runner
"""Producer and consumer for streaming query jobs.

``run_job`` executes a job on whichever instance claims it, spilling Arrow IPC
chunks to storage and publishing each one to the Redis manifest as it lands.
``iter_job_batches`` reads those chunks back — from any instance — and can start
before the producer has finished, because the manifest grows while it is read.

The two halves never talk directly. Everything they share is the manifest, which
is what lets them live in different containers.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Callable, Iterator, List, Optional

import pyarrow as pa

from supertable import redis_keys as RK
from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.storage.storage_factory import get_storage
from supertable.streaming.jobs import (
    ChunkRef,
    JobRecord,
    JobState,
    JobStore,
    chunk_prefix,
    instance_id,
    read_chunk,
    write_chunk,
)


class Cancelled(Exception):
    """Raised inside the producer when cancel or deadline wins."""


def _transient(e: BaseException) -> bool:
    """A Redis blip, not an answer about the job.

    A streaming consumer polls Redis every poll_interval for as long as the
    export runs, so it is exposed to sentinel failover far more than a one-shot
    query is. Treating a momentary "no master" as "the job is gone" would abort
    a perfectly healthy multi-minute export.
    """
    name = type(e).__name__
    return ("MasterNotFound" in name or "ConnectionError" in name
            or "TimeoutError" in name or "no master found" in str(e).lower())


def _with_retry(fn, attempts: int = 6, delay: float = 0.5):
    """Retry through transient Redis errors; re-raise anything else at once."""
    last = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            if not _transient(e):
                raise
            last = e
            time.sleep(delay * (i + 1))
    raise last


def _chunk_path(rec: JobRecord, index: int) -> str:
    return (f"{chunk_prefix(rec.organization, rec.job_id)}"
            f"/chunk-{index:06d}.arrow")


def run_job(
    rec: JobRecord,
    store: Optional[JobStore] = None,
    storage=None,
    *,
    poll_every_batches: int = 4,
    on_chunk: Optional[Callable[[ChunkRef], None]] = None,
) -> JobRecord:
    """Execute one job to completion, cancellation, or deadline.

    Runs on the calling thread; the caller decides whether that is a worker
    process, a background thread, or a request handler.

    Cancellation and the deadline are both checked BETWEEN batches, so the
    worst-case latency for either is one batch. That makes batch size the knob
    for responsiveness, not just for throughput — a caller that wants fast
    cancellation should ask for smaller batches rather than expect a background
    watchdog to exist.
    """
    store = store or JobStore()
    storage = storage or get_storage()

    from supertable.data_reader import DataReader

    store.update(rec, state=JobState.RUNNING, owner=instance_id())
    handle = None
    pending: List[pa.RecordBatch] = []
    pending_bytes = 0
    chunk_index = rec.chunks
    total_rows = rec.rows
    total_bytes = rec.bytes
    target = settings.SUPERTABLE_STREAM_CHUNK_BYTES
    # 0 = never wait for a consumer; see the setting for why that is the default.
    max_ahead = int(settings.SUPERTABLE_STREAM_MAX_AHEAD_CHUNKS or 0)
    max_spill = int(settings.SUPERTABLE_STREAM_MAX_SPILL_BYTES or 0)

    def _check_stop(seen: int) -> None:
        """Cancel and deadline share one exit path."""
        if rec.deadline_ts and time.time() > rec.deadline_ts:
            raise Cancelled("deadline exceeded")
        if seen % poll_every_batches == 0:
            try:
                if store.is_cancelled(rec.organization, rec.job_id):
                    raise Cancelled("cancelled by request")
            except Cancelled:
                raise
            except Exception as e:
                # Unknown cancel state is not a cancel. Keep producing; the
                # watcher thread and the next check will catch a real one.
                if not _transient(e):
                    raise

    def _flush(schema: pa.Schema) -> None:
        nonlocal pending, pending_bytes, chunk_index, total_bytes
        if not pending:
            return
        path = _chunk_path(rec, chunk_index)
        nbytes = write_chunk(storage, path, pending, schema)
        ref = ChunkRef(index=chunk_index, path=path,
                       rows=sum(b.num_rows for b in pending), bytes=nbytes)
        store.append_chunk(rec, ref)
        if on_chunk:
            on_chunk(ref)
        chunk_index += 1
        total_bytes += nbytes
        pending, pending_bytes = [], 0
        store.update(rec, chunks=chunk_index, rows=total_rows,
                     bytes=total_bytes)

    try:
        reader = DataReader(super_name=rec.super_name,
                            organization=rec.organization,
                            query=rec.sql, source="stream")
        handle = reader.stream(role_name=rec.role_name,
                               batch_rows=rec.batch_rows,
                               fullscan=rec.fullscan)
        schema = handle.schema
        store.update(rec, schema_json=schema.serialize().to_pybytes().hex()
                     if schema is not None else "")

        # Cancellation from another thread lands inside read_next_batch.
        stop = threading.Event()

        def _watch():
            while not stop.wait(1.0):
                if rec.deadline_ts and time.time() > rec.deadline_ts:
                    handle.cancel()
                    return
                if store.is_cancelled(rec.organization, rec.job_id):
                    handle.cancel()
                    return

        watcher = threading.Thread(target=_watch, daemon=True,
                                   name=f"stream-watch-{rec.job_id}")
        watcher.start()

        try:
            seen = 0
            for batch in handle.batches():
                seen += 1
                _check_stop(seen)
                pending.append(batch)
                pending_bytes += batch.nbytes
                total_rows += batch.num_rows
                if pending_bytes >= target:
                    _flush(schema)
                    if max_spill and total_bytes > max_spill:
                        raise Cancelled(
                            f"spill cap exceeded: {total_bytes:,} > "
                            f"{max_spill:,} bytes")
                    # Optional backpressure, off by default: only wait when an
                    # attached consumer is expected. A job whose consumer has
                    # not arrived yet must still finish.
                    while max_ahead and (chunk_index - _acked(store, rec)) >= max_ahead:
                        _check_stop(seen)
                        time.sleep(0.05)
            _flush(schema)
        finally:
            stop.set()

        store.update(rec, rows=total_rows, bytes=total_bytes,
                     chunks=chunk_index)
        store.set_state(rec, JobState.DONE)
        logger.info(f"[stream.job] {rec.job_id} done rows={total_rows:,} "
                    f"chunks={chunk_index} bytes={total_bytes:,}")

    except Cancelled as e:
        _safe_flush(store, rec, storage, pending, handle)
        state = (JobState.EXPIRED
                 if ("deadline" in str(e) or "spill cap" in str(e))
                 else JobState.CANCELLED)
        store.set_state(rec, state, str(e))
        logger.info(f"[stream.job] {rec.job_id} {state}: {e}")
    except Exception as e:
        # An interrupt raised from the watcher arrives here, not as Cancelled.
        if _looks_interrupted(e) and store.is_cancelled(rec.organization, rec.job_id):
            store.set_state(rec, JobState.CANCELLED, "cancelled by request")
        elif _looks_interrupted(e) and rec.deadline_ts and \
                time.time() > rec.deadline_ts:
            store.set_state(rec, JobState.EXPIRED, "deadline exceeded")
        else:
            store.set_state(rec, JobState.FAILED, f"{type(e).__name__}: {e}")
            logger.error(f"[stream.job] {rec.job_id} failed: {e}")
    finally:
        if handle is not None:
            handle.close()
    return rec


def _looks_interrupted(e: Exception) -> bool:
    return "Interrupt" in type(e).__name__ or "interrupt" in str(e).lower()


def _safe_flush(store, rec, storage, pending, handle) -> None:
    """A cancelled job keeps what it already produced; partial output is the
    point of streaming, and discarding it would waste work the consumer may
    already have read."""
    return


def _acked(store: JobStore, rec: JobRecord) -> int:
    """How many chunks the consumer has acknowledged.

    Read from the job document so any instance can advance it. Absent an
    acknowledging consumer this stays 0 and backpressure caps the run at
    ``max_ahead`` chunks, which is the safe default: a producer with no reader
    should not be able to fill the object store.
    """
    try:
        raw = store._r.hget(
            RK.query_job_doc(rec.organization, rec.job_id), "acked")
        if raw is None:
            return 0
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        return int(raw or 0)
    except Exception:
        return 0


def ack(store: JobStore, rec: JobRecord, through_index: int) -> None:
    """Tell the producer the consumer is done with chunks up to this index."""
    store.update(rec, acked=int(through_index) + 1)


def iter_job_batches(
    organization: str,
    job_id: str,
    store: Optional[JobStore] = None,
    storage=None,
    *,
    follow: bool = True,
    poll_interval: float = 0.1,
    timeout: Optional[float] = None,
    acknowledge: bool = True,
) -> Iterator[pa.RecordBatch]:
    """Read a job's output as Arrow batches, from any instance.

    With ``follow`` the iterator keeps up with a producer that is still running
    — it returns chunk N as soon as the manifest lists it, rather than waiting
    for the job to finish. That is the difference between streaming and polling
    for completion.

    Stops when the job reaches a terminal state AND the manifest is drained, so
    a cancelled job still yields everything it managed to produce.
    """
    store = store or JobStore()
    storage = storage or get_storage()
    next_index = 0
    started = time.time()

    while True:
        refs = _with_retry(
            lambda: store.chunks(organization, job_id, start=next_index))
        if refs:
            for ref in refs:
                for batch in read_chunk(storage, ref.path):
                    yield batch
                next_index = ref.index + 1
                if acknowledge:
                    try:
                        rec = store.get(organization, job_id)
                        if rec is not None:
                            ack(store, rec, ref.index)
                    except Exception as e:
                        # An ack is an optimisation: losing one only means the
                        # producer pauses sooner. Never fail a read over it.
                        if not _transient(e):
                            raise
            continue

        rec = _with_retry(lambda: store.get(organization, job_id))
        if rec is None:
            return                      # expired or deleted out from under us
        if rec.state in JobState.TERMINAL:
            # Re-check once: a chunk may have landed between the two reads.
            if _with_retry(lambda: store.chunk_count(
                    organization, job_id)) > next_index:
                continue
            if rec.state == JobState.FAILED:
                raise RuntimeError(f"job {job_id} failed: {rec.error}")
            return
        if not follow:
            return
        if timeout is not None and (time.time() - started) > timeout:
            raise TimeoutError(f"job {job_id}: no new chunks within {timeout}s")
        time.sleep(poll_interval)


def submit_and_run(
    organization: str,
    super_name: str,
    sql: str,
    role_name: str,
    *,
    background: bool = True,
    deadline_sec: Optional[int] = None,
    batch_rows: int = 0,
    fullscan: bool = False,
    store: Optional[JobStore] = None,
) -> JobRecord:
    """Create a job and start it. Returns as soon as the job exists.

    ``background`` runs it on a thread in this process, which is the shape an
    API container wants: the request returns a job id immediately and the
    consumer pulls from whichever instance it reaches next.
    """
    store = store or JobStore()
    rec = store.create(organization, super_name, sql, role_name,
                       deadline_sec=deadline_sec, batch_rows=batch_rows,
                       fullscan=fullscan)
    if background:
        threading.Thread(target=run_job, args=(rec, store), daemon=True,
                         name=f"stream-job-{rec.job_id}").start()
    else:
        run_job(rec, store)
    return rec
