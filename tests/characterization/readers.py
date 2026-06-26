"""Pluggable table readers for the forward-compatibility contract.

A :class:`TableReader` turns a sealed scenario fixture into a canonical
:class:`TableResult`.  The golden suite seals the CURRENT reader's output; this
abstraction lets a FUTURE reader (the deletion-vector / ``__rowid``
implementation) be held to the *same* sealed bytes without ever touching the
goldens.

To add an implementation, write a new ``TableReader`` and append an instance to
:data:`READERS`.  ``test_compatibility`` then exercises it against every
non-error sealed scenario automatically.  A reader that is not implemented yet
advertises ``available() is False`` and is skipped — visible as a pending
conformance gate, never silently passed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from tests.characterization.table_result import TableResult


@runtime_checkable
class TableReader(Protocol):
    """Structural contract every read implementation must satisfy."""

    #: Stable identifier used in pytest ids and skip messages.
    name: str

    def available(self) -> bool:
        """Whether this reader is implemented and can run right now."""

    def read(self, scenario, golden_root: str | Path) -> TableResult:
        """Read ``scenario`` from its sealed input dir into a canonical result."""


class CurrentViewReader:
    """The CURRENT production read path (tombstone + dedup views over parquet).

    This is the implementation that SEALED the goldens, exposed through the
    reader Protocol so the compatibility test and the golden test share a single
    oracle path (no second reimplementation to drift).
    """

    name = "current_view"

    def available(self) -> bool:
        return True

    def read(self, scenario, golden_root: str | Path) -> TableResult:
        from tests.characterization.current_reader import read_scenario

        return read_scenario(scenario, golden_root, engine="duckdb")


class DeletionVectorReader:
    """PLACEHOLDER for the future internal-``__rowid`` + deletion-vector reader.

    When the deletion logic migrates to a stable row identifier + hybrid
    deletion vectors + compaction, implement :meth:`read` to read the SAME
    sealed inputs — which already contain only public columns plus the existing
    internal ``__timestamp__`` — and return the SAME canonical
    :class:`TableResult`.  Flip :meth:`available` to ``True`` and the
    compatibility suite immediately holds the new reader to every sealed byte.
    """

    name = "deletion_vector"

    def available(self) -> bool:
        return False

    def read(self, scenario, golden_root: str | Path) -> TableResult:  # pragma: no cover - placeholder
        raise NotImplementedError(
            "DeletionVectorReader is a placeholder for the post-migration read "
            "path.  Implement it to read the sealed inputs via __rowid + "
            "deletion vectors and return the same canonical TableResult, then "
            "set available() -> True."
        )


# Registry the compatibility suite parameterizes over.  Order is stable so test
# ids stay deterministic.
READERS = [CurrentViewReader(), DeletionVectorReader()]
