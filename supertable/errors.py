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


__all__ = [
    "SupertableLookupError",
    "SuperTableNotFoundError",
    "TableNotFoundError",
]
