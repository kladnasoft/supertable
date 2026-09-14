# route: test_suite.runtime
"""Hermetic environment for the correctness suite.

Every run gets a throwaway Redis container and a throwaway workspace on local
disk, so the suite never touches a developer's or a deployment's state and two
runs cannot interfere. It needs Docker and nothing else.

WHY THE ENVIRONMENT IS PINNED BEFORE ``supertable`` IS IMPORTED

``supertable.config.settings`` is a singleton built at import time, so the
storage backend and Redis endpoint are decided by whatever is in the
environment at that moment. Pinning them afterwards does not take effect, and
re-pinning them mid-process is not reliable. The launcher therefore sets the
environment first and only then hands control to pytest, which is why this is
an entry point rather than a fixture.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

#: Container label so a stray container from an interrupted run is easy to find:
#: ``docker ps -a --filter label=supertable.test-suite=true``
CONTAINER_LABEL = "supertable.test-suite=true"

REDIS_IMAGE = "redis:7-alpine"


@contextlib.contextmanager
def isolated_redis():
    """Run a disposable Redis and yield its port.

    Bound to 127.0.0.1 on a kernel-assigned port so concurrent runs — and a
    developer's own Redis on 6379 — cannot collide. ``--rm`` plus the
    ``finally`` removal means an interrupted run does not leak a container.
    """
    if not shutil.which("docker"):
        raise RuntimeError(
            "The correctness suite needs Docker to run an isolated Redis. "
            "Install/start Docker, or point SUPERTABLE_TEST_REDIS_PORT at a "
            "Redis you are willing to have flushed."
        )
    name = "st-test-suite-" + uuid.uuid4().hex[:12]
    container = None
    try:
        container = subprocess.check_output([
            "docker", "run", "--detach", "--rm", "--name", name,
            "--label", CONTAINER_LABEL, "-p", "127.0.0.1::6379",
            REDIS_IMAGE, "redis-server", "--save", "", "--appendonly", "no",
        ], text=True).strip()
        mapping = subprocess.check_output(
            ["docker", "port", container, "6379/tcp"], text=True,
        ).strip()
        port = int(mapping.rsplit(":", 1)[1])

        import redis
        client = redis.Redis(host="127.0.0.1", port=port,
                             socket_connect_timeout=1, socket_timeout=2)
        for _ in range(100):
            try:
                if client.ping():
                    break
            except redis.RedisError:
                time.sleep(0.1)
        else:
            raise RuntimeError("Isolated Redis never became ready")
        try:
            yield port
        finally:
            client.close()
    finally:
        if container:
            subprocess.run(["docker", "rm", "--force", container],
                           capture_output=True, text=True)


def configure_environment(workspace: Path, redis_port: int) -> None:
    """Pin storage and Redis for this process. Call before importing supertable.

    Every ``SUPERTABLE_*`` / cloud-credential variable is cleared first, so a
    developer's ``.env`` or exported cloud settings cannot silently redirect the
    suite at real storage — the reason this suite can be trusted to be talking
    to its own workspace.
    """
    prefixes = ("SUPERTABLE_", "STORAGE_", "AWS_", "AZURE_", "GCS_", "GCP_")
    extra = {
        "GOOGLE_APPLICATION_CREDENTIALS", "MAX_MEMORY_CHUNK_SIZE",
        "MAX_OVERLAPPING_FILES", "MAX_TOMBSTONE_ROWS", "DEFAULT_TIMEOUT_SEC",
        "DEFAULT_LOCK_DURATION_SEC",
    }
    for key in list(os.environ):
        if key.startswith(prefixes) or key in extra:
            os.environ.pop(key)

    workspace.mkdir(parents=True, exist_ok=True)
    os.environ.update({
        "SUPERTABLE_HOME": str(workspace),
        "STORAGE_TYPE": "LOCAL",
        "SUPERTABLE_REDIS_HOST": "127.0.0.1",
        "SUPERTABLE_REDIS_PORT": str(redis_port),
        "SUPERTABLE_REDIS_DB": "0",
        "SUPERTABLE_REDIS_SENTINEL": "false",
        "SUPERTABLE_REDIS_SSL": "false",
        # Monitoring and audit are write-path side effects, not the subject
        # under test; off keeps each transaction to the work being measured.
        "SUPERTABLE_MONITORING_ENABLED": "false",
        "SUPERTABLE_AUDIT_ENABLED": "false",
        "SUPERTABLE_LOG_LEVEL": "CRITICAL",
        "SUPERTABLE_DUCKDB_MEMORY_LIMIT": "512MB",
        "SUPERTABLE_DUCKDB_THREADS": "2",
        "SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD": "false",
        # Pruning ON: a pruning bug that drops a live row is exactly the kind
        # of defect this suite exists to catch.
        "SUPERTABLE_READ_PRUNING_ENABLED": "true",
        # Deliberately small so multi-batch streaming is exercised rather than
        # every result fitting in one batch.
        "SUPERTABLE_STREAM_BATCH_ROWS": "17",
        # UTC by default; override to re-run the suite under another zone.
        "TZ": os.environ.get("TZ_OVERRIDE", "UTC"),
    })
    if hasattr(time, "tzset"):
        time.tzset()


SELECTIONS = {
    "write": ["test_write_correctness.py"],
    "read": ["test_read_correctness.py"],
}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="test_suite",
        description="SuperTable write/read correctness suite (hermetic).",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all", action="store_true", help="run both halves (default)")
    group.add_argument("--write", action="store_true", help="write + tombstone correctness only")
    group.add_argument("--read", action="store_true", help="SQL read correctness only")
    parser.add_argument("--seed", type=int, default=None,
                        help="seed for the randomized write workload "
                             "(default: fixed, so runs are reproducible)")
    parser.add_argument("--transactions", type=int, default=None,
                        help="override the number of write transactions")
    parser.add_argument("--keep", action="store_true",
                        help="keep the workspace on disk for inspection")
    parser.add_argument("pytest_args", nargs="*",
                        help="extra arguments forwarded to pytest (e.g. -k, -x)")
    args = parser.parse_args(argv)

    if args.write:
        selected = SELECTIONS["write"]
    elif args.read:
        selected = SELECTIONS["read"]
    else:
        selected = SELECTIONS["write"] + SELECTIONS["read"]

    here = Path(__file__).resolve().parent
    targets = [str(here / name) for name in selected]

    workspace_root = Path(tempfile.mkdtemp(prefix="supertable-test-suite-"))
    if args.seed is not None:
        os.environ["SUPERTABLE_TEST_SEED"] = str(args.seed)
    if args.transactions is not None:
        os.environ["SUPERTABLE_TEST_TRANSACTIONS"] = str(args.transactions)

    print(f"workspace: {workspace_root}")
    try:
        with isolated_redis() as port:
            configure_environment(workspace_root / "lake", port)
            # Imported here, not at module scope: pytest pulls in the test
            # modules (and through them supertable) only once the environment
            # above is already pinned.
            import pytest
            code = pytest.main([*targets, "-q", "-p", "no:randomly", *args.pytest_args])
    finally:
        if args.keep:
            print(f"workspace kept: {workspace_root}")
        else:
            shutil.rmtree(workspace_root, ignore_errors=True)
    return int(code)


if __name__ == "__main__":
    raise SystemExit(main())
