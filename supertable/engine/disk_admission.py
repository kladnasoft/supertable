"""Cross-process disk admission for caches and IslandDB spill files."""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import time
import uuid
from pathlib import Path

from supertable.config.homedir import get_app_home


_STALE_SECONDS = 24 * 60 * 60


class DiskAdmissionUnavailable(RuntimeError):
    """The shared filesystem reservation cannot be admitted."""


def _existing_ancestor(path: str | os.PathLike[str]) -> Path:
    candidate = Path(path).expanduser().resolve()
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def _reservation_root(path: str | os.PathLike[str]) -> Path:
    probe = _existing_ancestor(path)
    try:
        device = os.stat(probe).st_dev
    except OSError as exc:
        raise DiskAdmissionUnavailable("cannot identify cache filesystem") from exc
    base = Path(get_app_home()) / ".supertable-disk-admission"
    try:
        if os.stat(_existing_ancestor(base)).st_dev != device:
            base = probe / ".supertable-disk-admission"
    except OSError:
        base = probe / ".supertable-disk-admission"
    return base / str(device)


def _read_size(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle).get("size", 0)
        return max(0, int(value))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return 0


def _clean_stale(root: Path) -> None:
    now = time.time()
    try:
        entries = tuple(root.iterdir())
    except OSError:
        return
    for entry in entries:
        if not entry.name.endswith(".json"):
            continue
        try:
            if now - entry.stat().st_mtime > _STALE_SECONDS:
                entry.unlink(missing_ok=True)
                continue
            with entry.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            pid = payload.get("pid")
            if isinstance(pid, int) and pid > 0 and pid != os.getpid():
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    entry.unlink(missing_ok=True)
                except PermissionError:
                    pass
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            try:
                entry.unlink(missing_ok=True)
            except OSError:
                pass


class DiskReservation:
    """An idempotent shared filesystem reservation."""

    def __init__(self, path: Path):
        self.path = path
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            self.path.unlink(missing_ok=True)
        except OSError:
            pass

    def __enter__(self) -> "DiskReservation":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def reserve_disk(
    path: str | os.PathLike[str],
    size: int,
    *,
    min_free_bytes: int = 0,
) -> DiskReservation | None:
    """Atomically reserve bytes shared by cache and spill users."""
    size = max(0, int(size))
    if size == 0:
        return None
    root = _reservation_root(path)
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        _clean_stale(root)
        try:
            free = max(0, int(shutil.disk_usage(_existing_ancestor(path)).free))
        except (OSError, ValueError, TypeError):
            raise DiskAdmissionUnavailable("cannot inspect free disk space") from None
        reserved = sum(
            _read_size(entry)
            for entry in root.iterdir()
            if entry.name.endswith(".json")
        )
        if free - reserved - size < max(0, int(min_free_bytes)):
            raise DiskAdmissionUnavailable(
                "shared filesystem reservation would exhaust the disk reserve"
            )
        token = root / f"{os.getpid()}-{uuid.uuid4().hex}.json"
        temporary = token.with_suffix(".tmp")
        payload = {
            "size": size,
            "pid": os.getpid(),
            "created_ns": time.time_ns(),
        }
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, token)
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return DiskReservation(token)


def release_local_reservation(local_path: str | os.PathLike[str]) -> None:
    """Release a shared token embedded in a cache reservation record."""
    try:
        with Path(local_path).open("r", encoding="utf-8") as handle:
            token = json.load(handle).get("shared_token")
        if token:
            DiskReservation(Path(str(token))).release()
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        pass


__all__ = [
    "DiskAdmissionUnavailable",
    "DiskReservation",
    "release_local_reservation",
    "reserve_disk",
]
