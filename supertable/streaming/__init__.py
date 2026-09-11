# route: supertable.streaming
"""Streaming query execution and exports.

Two entry points, for two different situations:

``DataReader.stream(...)``
    In-process Arrow batches, no job, no spill, no LIMIT. The right choice
    whenever the consumer lives in the same process as the query.

``submit_and_run(...)`` / ``iter_job_batches(...)``
    A job whose output is spilled to object storage and indexed in Redis, so a
    consumer in a DIFFERENT container can pull it. Use when the producer and
    consumer are not the same process.
"""

from supertable.streaming.jobs import (
    ChunkRef,
    JobRecord,
    JobState,
    JobStore,
    chunk_prefix,
)
from supertable.streaming.runner import (
    Cancelled,
    ack,
    iter_job_batches,
    run_job,
    submit_and_run,
)

__all__ = [
    "ChunkRef", "JobRecord", "JobState", "JobStore", "chunk_prefix",
    "Cancelled", "ack", "iter_job_batches", "run_job", "submit_and_run",
]
