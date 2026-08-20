from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Optional, Set, Tuple


class PredInterval(NamedTuple):
    """A closed/open range of values a column is *allowed* to take, derived from
    a SQL WHERE predicate, used for read-path file pruning.

    ``lane`` identifies the literal's comparison domain (``"numeric"``,
    internal ``"numeric_cast"``, ``"string"``, ``"date"``, ``"timestamp"``
    or ``"timestamptz"``).  ``numeric_cast`` retains cast provenance so an
    executor whose overflow rules differ can decline the constraint.  A
    pruning consumer compares it only to a footer lane whose ordering and
    executor coercion semantics are known to be identical; otherwise it keeps
    the file.  ``lo``/``hi`` are the bounds with
    ``None`` meaning unbounded (-inf / +inf); ``lo_incl``/``hi_incl`` mark
    whether each bound is inclusive.  Examples: ``col = 5`` →
    ``(lane, 5, True, 5, True)``; ``col > 5`` → ``(lane, 5, False, None, True)``.
    """
    lane: str
    lo: object
    lo_incl: bool
    hi: object
    hi_incl: bool


class JoinEdge(NamedTuple):
    """One equi-join link ``left_table.left_col = right_table.right_col`` between
    two *different* physical tables, used for join-aware (cross-table) file
    pruning.

    A table is identified by its ``(super_name.lower(), simple_name.lower())``
    key — the same key :meth:`SQLParser.get_predicate_constraints` uses — so an
    edge can be matched directly against the per-table file/stats maps.  Column
    names are lowercased.

    ``prune_left`` / ``prune_right`` say which endpoints may be *pruned* by the
    range the partner exports.  Ranges always flow both ways, but a preserved
    outer-join side (LEFT's left, RIGHT's right, both sides of FULL, ANTI's
    preserved side) must keep every file — its non-matching rows appear in the
    result — so :meth:`SQLParser.get_join_edges` clears its flag.  The flag is
    also cleared for any table the query binds more than once (the executor
    scans ONE shared file list per physical table, so pruning it to what a
    single occurrence needs would starve the others).  ``True``/``True`` (the
    default) is plain inner-join semantics.
    """
    left_table: Tuple[str, str]
    left_col: str
    right_table: Tuple[str, str]
    right_col: str
    prune_left: bool = True
    prune_right: bool = True


@dataclass(frozen=True)
class RowGroupSelection:
    """A validated, conservative row-group scan hint for one Parquet object.

    The hint is useful only when the executor proves that the current footer's
    SHA-256 matches ``footer_sha256`` and it still has
    ``expected_row_group_count`` groups. ``selected_ids`` is always a non-empty,
    strictly increasing subset of that range. Scanning *all* row groups is
    represented by the **absence** of a mapping entry rather than by a selection
    containing every id; this makes legacy snapshots and every validation
    failure fail open naturally.
    """

    expected_row_group_count: int
    selected_ids: Tuple[int, ...]
    # SHA-256 of PyArrow's serialized Parquet FileMetaData.  Row-group IDs are
    # safe only when the executor proves they were derived from this exact
    # footer; a matching group count or file size is not an identity seal.
    footer_sha256: str

    def __post_init__(self) -> None:
        count = self.expected_row_group_count
        ids = self.selected_ids
        if (
            not isinstance(count, int)
            or isinstance(count, bool)
            or count <= 0
        ):
            raise ValueError("expected_row_group_count must be a positive integer")
        if not isinstance(ids, tuple) or not ids:
            raise ValueError("selected_ids must be a non-empty tuple")
        if any(
            not isinstance(group_id, int)
            or isinstance(group_id, bool)
            or group_id < 0
            or group_id >= count
            for group_id in ids
        ):
            raise ValueError("selected_ids contains an invalid row-group id")
        if ids != tuple(sorted(set(ids))):
            raise ValueError("selected_ids must be unique and strictly increasing")
        if (
            not isinstance(self.footer_sha256, str)
            or len(self.footer_sha256) != 64
            or any(ch not in "0123456789abcdef" for ch in self.footer_sha256)
        ):
            raise ValueError("footer_sha256 must be a lowercase SHA-256 digest")


@dataclass(frozen=True)
class ResourceStatsSeal:
    """Snapshot-pinned identity for one resource's footer-statistics rows.

    ``footer_sha256`` binds the rows to the exact Parquet footer from which they
    were extracted. ``stats_rows`` prevents truncation/extension, while
    ``stats_digest`` seals the canonical ``STATS_SCHEMA`` row stream.  All three
    values are required before statistics may prove a resource absent.  Legacy
    resources simply have no seal and therefore scan conservatively.
    """

    footer_sha256: str
    stats_rows: int
    stats_digest: str

    def __post_init__(self) -> None:
        for name, value in (
            ("footer_sha256", self.footer_sha256),
            ("stats_digest", self.stats_digest),
        ):
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(ch not in "0123456789abcdef" for ch in value)
            ):
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")
        if (
            not isinstance(self.stats_rows, int)
            or isinstance(self.stats_rows, bool)
            or self.stats_rows < 0
        ):
            raise ValueError("stats_rows must be a non-negative integer")


@dataclass(frozen=True)
class ResourceObjectSeal:
    """Snapshot-pinned identity for one immutable storage object.

    ``size`` is a bounds check, not an identity by itself.  At least one
    provider identity attribute must also be present so a range reader can
    address and validate the exact object observed immediately after upload.
    Legacy resources have no seal and retain the normal storage ``stat`` path.
    """

    size: int
    version: str = ""
    etag: str = ""
    last_modified_ns: int = 0
    checksum_sha256: str = ""

    def __post_init__(self) -> None:
        if (
            not isinstance(self.size, int)
            or isinstance(self.size, bool)
            or self.size < 0
        ):
            raise ValueError("size must be a non-negative integer")
        if (
            not isinstance(self.last_modified_ns, int)
            or isinstance(self.last_modified_ns, bool)
            or self.last_modified_ns < 0
        ):
            raise ValueError("last_modified_ns must be a non-negative integer")
        for name, value in (("version", self.version), ("etag", self.etag)):
            if (
                not isinstance(value, str)
                or len(value) > 1024
                or "\x00" in value
            ):
                raise ValueError(f"{name} must be a bounded string")
        checksum = self.checksum_sha256
        if not isinstance(checksum, str) or (
            checksum
            and (
                len(checksum) != 64
                or any(ch not in "0123456789abcdef" for ch in checksum)
            )
        ):
            raise ValueError(
                "checksum_sha256 must be an empty string or lowercase SHA-256"
            )
        if not (
            self.version
            or self.etag
            or self.last_modified_ns
            or self.checksum_sha256
        ):
            raise ValueError("an object identity beyond size is required")


@dataclass(frozen=True)
class IntegerDomainBound:
    """Complete integer domain bound for one snapshot/query selection.

    The estimator creates this only from every selected row group's
    snapshot-sealed footer row.  ``minimum``/``maximum`` therefore bound every
    non-NULL value that can reach execution, while ``has_null`` accounts for
    SQL's one additional NULL grouping key.  Both extrema are ``None`` only
    for an empty/all-NULL selection.

    This is deliberately a narrow resource-planning proof, not an NDV
    estimate: holes in the integer range are retained, so
    :attr:`cardinality_upper_bound` can overstate but never understate the
    number of GROUP BY keys.
    """

    minimum: Optional[int]
    maximum: Optional[int]
    has_null: bool = False

    def __post_init__(self) -> None:
        minimum = self.minimum
        maximum = self.maximum
        if (minimum is None) != (maximum is None):
            raise ValueError("integer domain extrema must both be present or absent")
        if minimum is not None:
            if (
                not isinstance(minimum, int)
                or isinstance(minimum, bool)
                or not isinstance(maximum, int)
                or isinstance(maximum, bool)
                or minimum > maximum
            ):
                raise ValueError("integer domain extrema must be ordered integers")
        if not isinstance(self.has_null, bool):
            raise ValueError("integer domain has_null must be boolean")

    @property
    def cardinality_upper_bound(self) -> int:
        non_null = (
            0
            if self.minimum is None
            else int(self.maximum) - int(self.minimum) + 1
        )
        return non_null + int(self.has_null)


@dataclass
class TableDefinition:
    super_name: str
    simple_name: str
    alias: str
    columns: List[str] = field(default_factory=list)

@dataclass
class SuperSnapshot:
    super_name: str
    simple_name: str
    simple_version: int
    files: List[str] = field(default_factory=list)
    columns: Set[str] = field(default_factory=set)
    # Stable catalog object keys corresponding one-for-one with ``files``.
    # Executors use these to prove that every deletion-vector entry belongs to
    # a resource in the pinned snapshot; resolved/presigned scan URLs are not a
    # stable identity boundary.
    resource_keys: List[str] = field(default_factory=list)
    # Snapshot schema types keyed by column name.  Needed to construct a typed
    # zero-row reflection after a metadata-only delete-all sunsets every file.
    column_types: Dict[str, str] = field(default_factory=dict)
    # Complete raw resource set from the pinned snapshot, before WHERE/join
    # pruning.  DV file binding is checked against this set because a valid DV
    # may contain deletes for files irrelevant to the current query.
    # ``None`` means legacy/unavailable; an empty list is authoritative.
    snapshot_resource_keys: Optional[List[str]] = None
    # Metadata below is pinned from the *same* immutable snapshot document as
    # ``files``.  Keeping it on the reflection prevents DataReader from doing a
    # second Redis leaf lookup and accidentally combining one version's files
    # with a newer version's deletion-vector.
    snapshot_path: Optional[str] = None
    # Bare/stable object key as recorded in the snapshot.  DataReader resolves
    # it for the selected engine/storage and uses it as the DV cache key.
    tombstone_key: Optional[str] = None
    # Exact row count recorded beside ``tombstone_key``.  ``None`` supports
    # snapshots written before the count was introduced.
    tombstone_rows: Optional[int] = None
    # SHA-256 of the canonical deletion-vector row stream pinned beside the
    # pointer/count. Required for active modern deletion vectors.
    tombstone_digest: Optional[str] = None
    # Linked-share policy is leaf payload metadata and must be pinned alongside
    # the data snapshot for the same split-read reason.
    share_row_filter: Optional[str] = None
    # Declared immutable object sizes, corresponding one-for-one with files and
    # resource_keys.  This new field follows the pre-existing positional API.
    # The shared cache uses sizes for admission and truncation detection without
    # re-estimating or reverse-parsing resolved URLs.
    resource_sizes: List[int] = field(default_factory=list)
    # Stable raw resource key -> conservative literal-WHERE row-group hint.
    # A missing key always means ALL row groups.  This is deliberately the last
    # trailing extension so the pre-existing positional constructor remains
    # compatible.
    row_group_selections: Dict[str, RowGroupSelection] = field(default_factory=dict)
    # Conservative physical-row upper bound for the selected row groups. It is
    # exact when trusted row-group stats are present and may be the full
    # snapshot manifest count otherwise. Zero with
    # ``candidate_rows_complete=False`` means unknown, never an empty relation.
    # Used for bounded result/join/response-LIMIT planning.
    candidate_rows: int = 0
    candidate_rows_complete: bool = False
    # Stable raw resource key -> independently snapshot-pinned statistics seal.
    # Absence is the explicit legacy/fail-open representation: the file remains
    # readable, but no min/max row may be used to exclude it or one of its row
    # groups from the scan.
    resource_stats_seals: Dict[str, ResourceStatsSeal] = field(default_factory=dict)
    # Stable raw resource key -> exact provider identity observed immediately
    # after the unique immutable object was uploaded.  IslandDB may pass this
    # directly to its conditional range reader and avoid one remote HEAD per
    # file/query. Missing/malformed/ambiguous entries fail open to the existing
    # stat path.
    resource_object_seals: Dict[str, ResourceObjectSeal] = field(default_factory=dict)
    # Exact maximum logical value width for selected variable-width columns,
    # keyed by their canonical schema spelling.  This is optional because old
    # snapshots did not persist the write-time measurement.  IslandDB may use a
    # bound only for resource admission; absence or malformed values route a
    # variable-width native reduction away rather than inventing a small value.
    # The bound must describe every immutable resource represented by this
    # SuperSnapshot (the maximum across them), not merely the current survivor.
    column_max_value_bytes: Dict[str, int] = field(default_factory=dict)
    # Canonical column name -> complete selected-row-group integer domain.
    # These optional resource-planning proofs are derived only after every
    # contributing stats row has passed its snapshot footer/count/digest seal.
    # Absence is conservative and preserves the general spill plan.
    integer_domain_bounds: Dict[str, IntegerDomainBound] = field(default_factory=dict)
    # Exact number of estimator-candidate row groups across ``resource_keys``.
    # This is planning provenance, not a runtime scanner counter: a native
    # reader may conservatively open a wider set and apply its own footer/page
    # pruning.  ``candidate_row_groups_complete=False`` means unknown, never
    # zero.  Trailing fields preserve the positional constructor contract.
    candidate_row_groups: int = 0
    candidate_row_groups_complete: bool = False
    # Deletion-vector representation pinned by this snapshot.  ``None`` and
    # ``1`` are the legacy single-Parquet artifact; explicit ``2`` means an
    # active pointer names a standalone, content-sealed segment manifest.  A
    # drained v2 table may retain format 2 with the exact empty state.
    # Kept last to preserve every existing positional constructor.
    tombstone_format: Optional[int] = None


@dataclass
class RbacViewDef:
    """RBAC view definition for a single table alias.

    Produced by restrict_read_access(), consumed by executors to create
    a filtered view on top of the reflection table.

    - allowed_columns: columns the role can see, or ["*"] for unrestricted.
    - where_clause: SQL WHERE predicate from role filters, or "" if none.
    - excluded_columns: case-insensitive deny list applied after
      ``allowed_columns``.  This trailing field keeps older positional
      construction compatible while allowing a wildcard role to expose every
      current/future column except an explicit sensitive set.
    - filter_spec: validated structured filter retained so each executor can
      render identifiers using its own SQL dialect. ``where_clause`` remains
      the backwards-compatible DuckDB representation.
    """
    allowed_columns: List[str] = field(default_factory=lambda: ["*"])
    where_clause: str = ""
    excluded_columns: List[str] = field(default_factory=list)
    filter_spec: object = None


@dataclass
class TombstoneDef:
    """Tombstone (deletion-vector) definition for a single table alias.

    Produced by DataReader from the snapshot's ``tombstone`` pointer,
    consumed by executors to create a view that anti-joins the data on
    ``__rowid__`` against the deletion-vector parquet, hiding rows that
    have been logically deleted but not yet physically compacted.

    - tombstone_path: storage path of the deletion-vector parquet
      (columns ``__file__`` + ``__rowid__``).  ``None`` means no
      tombstone exists, so no anti-join is applied.  This may be a
      presigned/object-store URL that rotates per request, so it is
      *not* stable enough to use as a cache key.
    - cache_key: the bare, stable storage key of the same deletion-vector
      parquet (no presign).  Stable across pure appends (carry-forward
      returns the previous tombstone), so DuckDB engines use it to key the
      materialised deletion-vector table cache.  ``None`` disables caching
      for this alias (falls back to inline ``read_parquet``).
    - expected_rows: snapshot-pinned deletion-vector cardinality, when the
      writer recorded it.  Executors can use this to validate the immutable
      artifact before applying it; ``None`` is the legacy/unknown case.
    """
    tombstone_path: Optional[str] = None
    cache_key: Optional[str] = None
    expected_rows: Optional[int] = None
    tombstone_digest: Optional[str] = None
    # Stable raw resource keys from the exact pinned snapshot.  A deletion
    # vector naming a foreign/sunset file is rejected before it can hide a live
    # row with a coincidentally equal row id.
    resource_keys: Tuple[str, ...] = field(default_factory=tuple)
    snapshot_resource_keys: Optional[Tuple[str, ...]] = None
    # Trailing for positional compatibility; see SuperSnapshot above.
    tombstone_format: Optional[int] = None


@dataclass
class Reflection:
    storage_type: str
    reflection_bytes: int
    total_reflections: int
    supers: List[SuperSnapshot]
    # Max last_updated_ms across all snapshots.  Used by the engine
    # auto-picker to distinguish fresh (still-churning) data from stable
    # data where caching pays off.  0 means unknown.
    freshness_ms: int = 0
    # alias -> RbacViewDef.  Empty dict means no RBAC filtering.
    rbac_views: Dict[str, RbacViewDef] = field(default_factory=dict)
    # alias -> TombstoneDef.  Empty dict means no tombstone filtering.
    tombstone_views: Dict[str, TombstoneDef] = field(default_factory=dict)
    # Whole-file physical bytes for the estimator-selected survivors.  New
    # fields follow the pre-existing positional API (freshness/RBAC/tombstone).
    # This is intentionally separate from reflection_bytes, which is the
    # projected compressed scan estimate used by the existing router.
    source_bytes: int = 0
    # False when at least one survivor had no trustworthy catalog size and the
    # storage backend could not recover it. AUTO then avoids Spark/Island size
    # decisions instead of treating an unknown multi-GB scan as zero bytes.
    source_bytes_complete: bool = True
    # Compressed bytes in the literal-WHERE candidate row groups and decoded
    # (uncompressed) bytes for their required columns.  These estimates are
    # Island-native planning hints only: ``reflection_bytes`` and
    # ``source_bytes`` retain their historical file-level meanings for engines
    # that ignore row-group selections.  Completeness flags prevent unknown
    # legacy/corrupt stats from being mistaken for a zero-byte scan.
    row_group_scan_bytes: int = 0
    row_group_scan_bytes_complete: bool = False
    decoded_bytes: int = 0
    decoded_bytes_complete: bool = False
    # Decoded bytes belonging to the estimator-selected query row groups only.
    # ``decoded_bytes`` above remains the total work bound and may additionally
    # include a whole-file system-column integrity pass for an active deletion
    # vector. Keeping the two separate prevents a large streaming proof from
    # being divided by a tiny candidate-row count when sizing source batches.
    selected_decoded_bytes: int = 0
    selected_decoded_bytes_complete: bool = False
    # First-use full-file integrity work included in ``decoded_bytes`` but not
    # in a normal selected-row batch. This is retained separately for routing,
    # profiling, and future proof-cache-aware planning; it is never subtracted
    # from the conservative total decoded work bound.
    proof_decoded_bytes: int = 0
    proof_decoded_bytes_complete: bool = False
