"""Isolated subprocess probes for the tests that require real ``fork()``.

The probes deliberately run outside pytest's already multi-threaded process.
Two of them first create the thread whose inherited state is under test.  On
CPython 3.12 and newer, those two forks must emit the interpreter's precise
``DeprecationWarning``; the warning is captured and checked at the call site.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import signal
import subprocess
import sys
import warnings
from pathlib import Path
from typing import Any


_CPYTHON_FORK_WARNING = re.compile(
    r"This process \(pid=[0-9]+\) is multi-threaded, use of fork\(\) may "
    r"lead to deadlocks in the child\."
)
_PROBE_TIMEOUT_SECONDS = 30.0


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _expected_fork_warning_count(*, threaded: bool) -> int:
    return int(
        threaded
        and sys.implementation.name == "cpython"
        and sys.version_info >= (3, 12)
    )


def _fork_with_warning_contract(*, threaded: bool) -> tuple[int, int]:
    """Fork once and validate the complete warning stream in the parent."""

    with warnings.catch_warnings(record=True) as caught:
        # This narrow override lets the probe inspect the warning even when its
        # fresh interpreter was launched with DeprecationWarning-as-error.
        warnings.simplefilter("always")
        child_pid = os.fork()

    if child_pid == 0:
        return child_pid, 0

    expected_count = _expected_fork_warning_count(threaded=threaded)
    _require(
        len(caught) == expected_count,
        "fork warning count mismatch: "
        f"expected={expected_count}, actual={len(caught)}, "
        f"warnings={[str(item.message) for item in caught]!r}",
    )
    for item in caught:
        _require(
            item.category is DeprecationWarning
            and _CPYTHON_FORK_WARNING.fullmatch(str(item.message)) is not None,
            "unexpected warning around fork: "
            f"category={item.category.__name__}, message={item.message!s}",
        )
    return child_pid, len(caught)


def _audit_identity_probe() -> dict[str, Any]:
    # Loading ``supertable.audit`` executes its broad package facade, whose
    # PyArrow dependencies create unrelated native worker threads. Execute the
    # exact leaf module under an isolated test name so this probe measures the
    # event module's own at-fork registration in a genuinely single-threaded
    # process.
    module_name = "_supertable_audit_events_fork_probe"
    events_path = Path(__file__).resolve().parents[1] / "audit" / "events.py"
    spec = importlib.util.spec_from_file_location(module_name, events_path)
    _require(spec is not None and spec.loader is not None, "cannot load events")
    events = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = events
    spec.loader.exec_module(events)

    parent_identity = events.current_instance_id()
    read_fd, write_fd = os.pipe()
    child_pid, warning_count = _fork_with_warning_contract(threaded=False)
    if child_pid == 0:  # pragma: no cover - assertions execute in the parent
        try:
            os.close(read_fd)
            child_identity = events.current_instance_id().encode("ascii")
            os.write(write_fd, child_identity)
        except BaseException:
            os._exit(1)
        finally:
            os.close(write_fd)
        os._exit(0)

    os.close(write_fd)
    try:
        child_identity = os.read(read_fd, 128).decode("ascii")
    finally:
        os.close(read_fd)
        waited, status = os.waitpid(child_pid, 0)

    child_exitcode = os.waitstatus_to_exitcode(status)
    _require(waited == child_pid, "waitpid returned the wrong audit child")
    _require(child_exitcode == 0, "audit child did not exit cleanly")
    _require(
        child_identity != parent_identity,
        "forked audit child retained its parent's process identity",
    )
    _require(
        re.fullmatch(
            r"audit-[0-9a-f]{12}-[0-9]{1,20}-[0-9a-f]{16}",
            child_identity,
        )
        is not None,
        "forked audit child returned a malformed process identity",
    )
    return {
        "child_exitcode": child_exitcode,
        "child_identity": child_identity,
        "fork_warning_count": warning_count,
        "parent_identity": parent_identity,
        "probe": "audit_identity",
    }


def _file_lock_probe(root: str) -> dict[str, Any]:
    from supertable.locking.file_lock import FileLocking

    owner = FileLocking(working_dir=root, retry_interval=0.01)
    contender = FileLocking(working_dir=root, retry_interval=0.01)
    token = owner.acquire("parent-owned", ttl_s=5, timeout_s=1)
    _require(token is not None, "parent failed to acquire the probe lock")

    try:
        child_pid, warning_count = _fork_with_warning_contract(threaded=True)
        if child_pid == 0:  # pragma: no cover - assertions execute in parent
            try:
                owner._on_exit()
            except BaseException:
                os._exit(1)
            os._exit(0)

        waited, status = os.waitpid(child_pid, 0)
        child_exitcode = os.waitstatus_to_exitcode(status)
        parent_token_survived = owner.who("parent-owned") == token
        contender_blocked = (
            contender.acquire(
                "parent-owned",
                ttl_s=2,
                timeout_s=1,
                retry_interval=0.01,
            )
            is None
        )
        _require(waited == child_pid, "waitpid returned the wrong lock child")
        _require(child_exitcode == 0, "file-lock child did not exit cleanly")
        _require(
            parent_token_survived,
            "child cleanup released the live parent lock",
        )
        _require(contender_blocked, "a contender acquired the live parent lock")
        return {
            "child_exitcode": child_exitcode,
            "contender_blocked": contender_blocked,
            "fork_warning_count": warning_count,
            "parent_token_survived": parent_token_survived,
            "probe": "file_lock",
        }
    finally:
        owner._on_exit()
        contender._on_exit()


def _durability_batch_probe(root: str) -> dict[str, Any]:
    from supertable.storage.local_storage import LocalStorage

    storage = LocalStorage(root)
    # Materialize the process-global sync pool before fork. The child must
    # discard the inherited bookkeeping for those vanished parent threads.
    with storage.durability_batch() as seed_batch:
        storage.write_bytes("seed-object", b"seed")
        seed_batch.barrier()
        seed_batch.catalog_commit_started()
        seed_batch.catalog_commit_succeeded()

    with storage.durability_batch() as batch:
        storage.write_bytes("parent-before-fork", b"parent-before")
        child_pid, warning_count = _fork_with_warning_contract(threaded=True)
        if child_pid == 0:  # pragma: no cover - assertions execute in parent
            try:
                # The inherited batch may neither wait on vanished workers nor
                # unlink a path belonging to the parent process.
                batch.abort()
                batch.close()
                with storage.durability_batch() as child_batch:
                    storage.write_bytes("child-object", b"child")
                    child_batch.barrier()
                    child_batch.catalog_commit_started()
                    child_batch.catalog_commit_succeeded()
            except BaseException:
                os._exit(1)
            os._exit(0)

        waited, status = os.waitpid(child_pid, 0)
        child_exitcode = os.waitstatus_to_exitcode(status)
        parent_before = storage.read_bytes("parent-before-fork")
        parent_publication_count = len(batch._publications)
        _require(
            waited == child_pid,
            "waitpid returned the wrong durability child",
        )
        _require(child_exitcode == 0, "durability child did not exit cleanly")
        _require(
            parent_before == b"parent-before",
            "child cleanup changed the parent's pre-fork publication",
        )
        _require(
            parent_publication_count == 1,
            "the parent durability batch changed across fork",
        )
        storage.write_bytes("parent-object", b"parent")
        batch.barrier()
        batch.catalog_commit_started()
        batch.catalog_commit_succeeded()

    child_object = storage.read_bytes("child-object")
    parent_before = storage.read_bytes("parent-before-fork")
    parent_object = storage.read_bytes("parent-object")
    _require(child_object == b"child", "child durability publication is missing")
    _require(
        parent_before == b"parent-before",
        "parent's pre-fork durability publication is missing",
    )
    _require(
        parent_object == b"parent",
        "parent's post-fork durability publication is missing",
    )
    return {
        "child_exitcode": child_exitcode,
        "child_object": child_object.decode("ascii"),
        "fork_warning_count": warning_count,
        "parent_before_fork": parent_before.decode("ascii"),
        "parent_object": parent_object.decode("ascii"),
        "parent_publication_count": parent_publication_count,
        "probe": "durability_batch",
    }


def run_fork_probe(
    probe: str,
    *,
    root: str | os.PathLike[str] | None = None,
    timeout: float = _PROBE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Run one probe in a fresh interpreter and return its JSON result."""

    command = [sys.executable, str(Path(__file__).resolve()), probe]
    if root is not None:
        command.append(os.fspath(root))
    env = os.environ.copy()
    repo_root = str(Path(__file__).resolve().parents[2])
    current_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        repo_root
        if not current_pythonpath
        else os.pathsep.join((repo_root, current_pythonpath))
    )
    warning_options = env.get("PYTHONWARNINGS")
    env["PYTHONWARNINGS"] = (
        "error::DeprecationWarning"
        if not warning_options
        else f"{warning_options},error::DeprecationWarning"
    )
    process = subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        stdout, stderr = process.communicate()
        raise AssertionError(
            f"fork probe timed out after {timeout:g}s: {probe}; "
            f"stdout={stdout!r}; stderr={stderr!r}"
        ) from exc

    _require(
        process.returncode == 0,
        f"fork probe exited with {process.returncode}: {probe}; "
        f"stdout={stdout!r}; stderr={stderr!r}",
    )
    _require(stderr == "", f"fork probe wrote unexpected stderr: {stderr!r}")
    lines = stdout.splitlines()
    _require(
        len(lines) == 1,
        f"fork probe must emit exactly one JSON line; stdout={stdout!r}",
    )
    try:
        result = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise AssertionError(f"fork probe emitted invalid JSON: {stdout!r}") from exc
    _require(isinstance(result, dict), "fork probe result is not a JSON object")
    _require(result.get("probe") == probe, "fork probe returned the wrong result")
    return result


def _main() -> int:
    _require(len(sys.argv) in {2, 3}, "usage: fork_semantics_probe.py PROBE [ROOT]")
    probe = sys.argv[1]
    root = sys.argv[2] if len(sys.argv) == 3 else None
    _require(hasattr(os, "fork"), "fork probes require POSIX fork semantics")
    if probe == "audit_identity":
        _require(root is None, "audit identity probe does not accept a root")
        result = _audit_identity_probe()
    elif probe == "file_lock":
        _require(root is not None, "file-lock probe requires a root")
        result = _file_lock_probe(root)
    elif probe == "durability_batch":
        _require(root is not None, "durability probe requires a root")
        result = _durability_batch_probe(root)
    else:
        raise AssertionError(f"unknown fork probe: {probe}")
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        exit_code = _main()
    except BaseException:
        # The caller treats any stderr as a contract failure. A traceback is
        # still useful evidence when the isolated helper itself fails.
        import traceback

        traceback.print_exc(file=sys.stderr)
        raise
    raise SystemExit(exit_code)
