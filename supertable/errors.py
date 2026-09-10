# route: supertable.errors
"""
Public exception hierarchy for SuperTable.

The exceptions defined here are part of the SDK's stable public surface
and are raised at well-known boundaries (read-path pre-flight checks,
constructor opt-outs, etc.). API layers wrap them into HTTP responses;
CLI layers print their messages directly.

Hierarchy
---------

    LookupError                                 (stdlib)
      └── SupertableLookupError                 (this module)
            ├── SuperTableNotFoundError
            └── TableNotFoundError

    RuntimeError
      ├── SnapshotCommitConflictError
      ├── LockLostError
      └── TombstoneIntegrityError

Inheriting from the stdlib ``LookupError`` means existing
``except LookupError`` / ``except KeyError`` callers keep working — every
SuperTable lookup failure is a "key not found" at heart.
"""
from __future__ import annotations


class SupertableLookupError(LookupError):
    """Base for catalog lookup failures on read paths.

    Carries the ``organization`` it was raised against so API/CLI layers
    can format the error without re-parsing the message.
    """

    def __init__(self, message: str, organization: str):
        super().__init__(message)
        self.organization = organization


class SuperTableNotFoundError(SupertableLookupError):
    """Raised when a SuperTable name is referenced but no Redis ``meta:root``
    pointer exists for it.

    Read-side code (``DataReader``, ``MetaReader``, ``DataEstimator``)
    raises this instead of silently bootstrapping a new supertable as a
    side effect of constructing the Python object.
    """

    def __init__(self, organization: str, super_name: str):
        super().__init__(
            f"SuperTable not found: {organization}/{super_name}",
            organization=organization,
        )
        self.super_name = super_name


class TableNotFoundError(SupertableLookupError):
    """Raised when a SimpleTable (``super.simple``) is referenced but no
    Redis ``meta:leaf:doc:{simple}`` pointer exists for it.

    Read-side code raises this instead of silently bootstrapping a new
    empty table as a side effect of constructing the Python object.
    """

    def __init__(self, organization: str, super_name: str, simple_name: str):
        super().__init__(
            f"Table not found: {organization}/{super_name}/{simple_name}",
            organization=organization,
        )
        self.super_name = super_name
        self.simple_name = simple_name


class RetryableCommitError(RuntimeError):
    """Base for definite publication rejections that a caller should redo.

    Both subclasses mean the same thing operationally: this mutation was
    rejected before it changed anything, and the correct response is to redo
    the whole write against freshly read state -- never to blindly re-publish
    the same payload, whose immutable artifacts were derived from a base that
    is no longer current.

    They exist as siblings because the *cause* differs (stale base vs. lost
    lease), but callers that only caught ``SnapshotCommitConflictError``
    silently missed lease takeover.  Catch this base to cover both.
    """


class SnapshotCommitConflictError(RetryableCommitError):
    """Raised when a writer tries to publish from a stale base snapshot.

    Snapshot data files are immutable, so a rejected writer may leave
    unreferenced objects for the retention-aware garbage collector, but it
    must never overwrite a newer catalog pointer.
    """


class LockLostError(RetryableCommitError):
    """Raised when a mutation no longer owns its table fencing lock."""


class TombstoneIntegrityError(RuntimeError):
    """Raised when a referenced deletion vector cannot be trusted.

    Tombstones are required snapshot state.  Treating a missing, malformed,
    truncated, or foreign vector as empty would resurrect rows; coercing an
    invalid vector could delete live rows.  Both conditions therefore fail
    closed with this exception.
    """


__all__ = [
    "SupertableLookupError",
    "SuperTableNotFoundError",
    "TableNotFoundError",
    "RetryableCommitError",
    "SnapshotCommitConflictError",
    "LockLostError",
    "TombstoneIntegrityError",
]
