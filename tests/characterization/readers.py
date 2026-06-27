"""Pluggable table readers for the reader-compatibility contract.

A :class:`TableReader` turns a sealed scenario fixture into a canonical
:class:`TableResult`.  The golden suite seals the production deletion-vector
reader's output; this abstraction lets an ALTERNATIVE reader (a new engine or a
reimplemented executor) be held to the *same* sealed bytes without ever touching
the goldens.

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
    """The production deletion-vector read path (tombstone anti-join over parquet).

    Removes rows solely by anti-joining ``__rowid__`` against the per-table
    tombstone deletion-vector; there is no read-time key-collapse dedup.  This is
    the implementation that SEALED the goldens, exposed through the reader
    Protocol so the compatibility test and the golden test share a single oracle
    path (no second reimplementation to drift).
    """

    name = "current_view"

    def available(self) -> bool:
        return True

    def read(self, scenario, golden_root: str | Path) -> TableResult:
        from tests.characterization.current_reader import read_scenario

        return read_scenario(scenario, golden_root, engine="duckdb")


class AlternativeReader:
    """PLACEHOLDER slot for an ALTERNATIVE read implementation.

    The production deletion-vector reader (:class:`CurrentViewReader`) already
    seals the goldens.  Use this slot to hold a *second* implementation — a new
    engine, or a reimplemented executor — to the SAME sealed inputs (public
    columns + internal ``__rowid__``/``__timestamp__`` + the per-table tombstone
    deletion-vector parquet) and the SAME canonical :class:`TableResult`.  Flip
    :meth:`available` to ``True`` and the compatibility suite immediately holds
    it to every sealed byte.
    """

    name = "alternative"

    def available(self) -> bool:
        return False

    def read(self, scenario, golden_root: str | Path) -> TableResult:  # pragma: no cover - placeholder
        raise NotImplementedError(
            "AlternativeReader is a placeholder slot.  Implement it to read the "
            "sealed inputs (public cols + __rowid__/__timestamp__ + tombstone "
            "deletion-vector) and return the same canonical TableResult, then "
            "set available() -> True."
        )


# Registry the compatibility suite parameterizes over.  Order is stable so test
# ids stay deterministic.
READERS = [CurrentViewReader(), AlternativeReader()]
