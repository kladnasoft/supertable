"""IslandDB: a conservative, specialised Parquet SQL engine.

IslandDB is intentionally *not* a second general-purpose SQL implementation.
It accepts a statically checked subset of SELECT, registers the estimator's
exact surviving Parquet files as Polars lazy scans, and lets Polars push
projections and predicates into Parquet row groups.  Anything whose semantics
are not proven equivalent to SuperTable's DuckDB contract is rejected before
I/O with :class:`IslandUnsupportedError`.

Local and whole-object-cache paths use the native multi-file scanner.  Remote
objects use a version-sealed seekable range cache, so Arrow fetches only the
footer and compressed column ranges needed by the selected row groups.  Stable
``resource_keys`` remain attached to rows for the composite deletion-vector
anti join in both cases.
"""

from __future__ import annotations

import json
import hashlib
import os
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.fs as pafs
import pyarrow.parquet as papq
import sqlglot
from sqlglot import exp

from supertable.config.defaults import logger
from supertable.config.homedir import get_app_home
from supertable.config.settings import settings
from supertable.data_classes import Reflection, SuperSnapshot
from supertable.engine.engine_common import (
    ROWID_COL,
    SOURCE_FILE_COL,
    TIMESTAMP_COL,
    rewrite_query_with_hashed_tables,
)
from supertable.processing import (
    TOMBSTONE_FILE_COL,
    parquet_footer_sha256,
    validate_tombstone_frame,
)
from supertable.engine.island_resources import (
    ArrowBatchStream,
    ContainerResources,
    ExecutionAdvice,
    QueryResourceEstimate,
    QueryResourcePlan,
    ResourceGovernor,
    ResourcePlanner,
    ResourcePolicy,
    ResultMemoryLimitExceeded,
)
from supertable.engine.island_spill import (
    AggregateSpec,
    IslandSpillError,
    SpillSession,
    external_group_aggregate,
    external_sort,
)


class IslandUnsupportedError(RuntimeError):
    """The query is outside IslandDB's proven-equivalent native subset."""


class IslandIntegrityError(RuntimeError):
    """Pinned data or deletion metadata failed a correctness boundary."""


@dataclass(frozen=True)
class IslandCapability:
    supported: bool
    reasons: Tuple[str, ...] = ()

    def require(self) -> None:
        if not self.supported:
            raise IslandUnsupportedError("; ".join(self.reasons))


@dataclass
class IslandProfile:
    native: bool = True
    source_bytes: int = 0
    estimated_scan_bytes: int = 0
    files: int = 0
    elapsed_ms: float = 0.0
    optimized_plan: str = ""
    cache: Dict[str, object] = field(default_factory=dict)
    resources: Dict[str, object] = field(default_factory=dict)
    spill: Dict[str, object] = field(default_factory=dict)
    selected_row_groups: int = 0
    cpu_time_ms: float = 0.0
    logical_scan_bytes: int = 0
    logical_scan_bytes_complete: bool = False
    physical_read_bytes: int = 0
    physical_read_bytes_measured: bool = False
    decoded_bytes: int = 0
    decoded_bytes_complete: bool = False
    rows_scanned: int = 0
    rows_scanned_measured: bool = False
    result_rows: int = 0
    result_bytes: int = 0
    peak_memory_bytes: int = 0
    peak_memory_scope: str = "unknown"
    spill_bytes: int = 0
    spill_bytes_measured: bool = False

    def as_dict(self) -> Dict[str, object]:
        return {
            "engine": "islanddb",
            "native": self.native,
            "source_bytes": int(self.source_bytes),
            "estimated_scan_bytes": int(self.estimated_scan_bytes),
            "files": int(self.files),
            "elapsed_ms": round(float(self.elapsed_ms), 3),
            "optimized_plan": self.optimized_plan,
            "cache": dict(self.cache),
            "resources": dict(self.resources),
            "spill": dict(self.spill),
            "selected_row_groups": int(self.selected_row_groups),
            "cpu_time_ms": round(float(self.cpu_time_ms), 3),
            "logical_scan_bytes": int(self.logical_scan_bytes),
            "logical_scan_bytes_complete": bool(self.logical_scan_bytes_complete),
            "physical_read_bytes": int(self.physical_read_bytes),
            "physical_read_bytes_measured": bool(self.physical_read_bytes_measured),
            "decoded_bytes": int(self.decoded_bytes),
            "decoded_bytes_complete": bool(self.decoded_bytes_complete),
            "rows_scanned": int(self.rows_scanned),
            "rows_scanned_measured": bool(self.rows_scanned_measured),
            "result_rows": int(self.result_rows),
            "result_bytes": int(self.result_bytes),
            "peak_memory_bytes": int(self.peak_memory_bytes),
            "peak_memory_scope": self.peak_memory_scope,
            "spill_bytes": int(self.spill_bytes),
            "spill_bytes_measured": bool(self.spill_bytes_measured),
        }


def _proc_counter(name: str) -> Optional[int]:
    """Read one Linux /proc/self/io counter; unavailable off Linux."""
    try:
        with open("/proc/self/io", "r", encoding="ascii") as handle:
            for line in handle:
                key, _, value = line.partition(":")
                if key == name:
                    return max(0, int(value.strip()))
    except (OSError, ValueError):
        return None
    return None


def _process_rss_bytes() -> Optional[int]:
    try:
        with open("/proc/self/status", "r", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return max(0, int(line.split()[1]) * 1024)
    except (OSError, ValueError, IndexError):
        return None
    return None


class _IslandTelemetry:
    """Low-overhead, explicitly process-scoped execution measurements."""

    def __init__(self) -> None:
        self.cpu_started = time.process_time()
        self.read_started = _proc_counter("read_bytes")
        self.rss_started = _process_rss_bytes()
        self.rss_peak = self.rss_started
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._sample_rss, name="islanddb-telemetry", daemon=True,
        )
        self._thread.start()

    def _sample_rss(self) -> None:
        while not self._stop.wait(0.01):
            value = _process_rss_bytes()
            if value is not None:
                self.rss_peak = max(self.rss_peak or 0, value)

    def finish(self) -> Dict[str, object]:
        self._stop.set()
        self._thread.join(timeout=0.1)
        rss = _process_rss_bytes()
        if rss is not None:
            self.rss_peak = max(self.rss_peak or 0, rss)
        read_finished = _proc_counter("read_bytes")
        read_measured = self.read_started is not None and read_finished is not None
        return {
            "cpu_time_ms": max(0.0, (time.process_time() - self.cpu_started) * 1000.0),
            "physical_read_bytes": (
                max(0, int(read_finished) - int(self.read_started))
                if read_measured else 0
            ),
            "physical_read_bytes_measured": read_measured,
            # RSS is process-wide. The delta is useful and honest under a
            # dedicated worker; concurrent queries may overlap this sample.
            "peak_memory_bytes": (
                max(0, int(self.rss_peak or 0) - int(self.rss_started or 0))
                if self.rss_started is not None and self.rss_peak is not None else 0
            ),
            "peak_memory_scope": "process_rss_delta" if self.rss_started is not None else "unknown",
        }


_GOVERNOR_LOCK = threading.Lock()
_GOVERNORS: Dict[Tuple[object, ...], ResourceGovernor] = {}
_ARROW_POOL_LOCK = threading.Lock()
_ROWID_PROOF_LOCK = threading.Lock()
_ROWID_PROOFS: "OrderedDict[Tuple[object, ...], float]" = OrderedDict()
_ROWID_PROOF_MAX_ENTRIES = 4096
_ROWID_PROOF_TTL_SEC = 24 * 60 * 60


class _IslandFileSystemHandler(pafs.FileSystemHandler):
    """Read-only Arrow filesystem spanning local files and sealed ranges."""

    def __init__(self, entries: Dict[str, Dict[str, object]]):
        self.entries = entries

    def get_type_name(self):
        return "islanddb"

    def normalize_path(self, path):
        return str(path)

    def get_file_info(self, paths):
        result = []
        for path in paths:
            entry = self.entries.get(str(path))
            result.append(
                pafs.FileInfo(
                    str(path),
                    type=(pafs.FileType.File if entry else pafs.FileType.NotFound),
                    size=(int(entry["size"]) if entry else None),
                )
            )
        return result

    def get_file_info_selector(self, selector):
        return []

    def open_input_file(self, path):
        entry = self.entries.get(str(path))
        if entry is None:
            raise FileNotFoundError(str(path))
        local = entry.get("local")
        if local:
            return pa.OSFile(str(local), "r")
        cache = entry.get("cache")
        if cache is None:
            raise FileNotFoundError(str(path))
        reader = cache.open(
            str(entry["raw_key"]), expected=entry.get("metadata"),
        )
        return pa.PythonFile(reader, mode="r")

    def open_input_stream(self, path):
        return self.open_input_file(path)

    @staticmethod
    def _readonly(*args, **kwargs):
        raise NotImplementedError("IslandDB Arrow filesystem is read-only")

    create_dir = _readonly
    delete_dir = _readonly
    delete_dir_contents = _readonly
    delete_root_dir_contents = _readonly
    delete_file = _readonly
    move = _readonly
    copy_file = _readonly
    open_output_stream = _readonly
    open_append_stream = _readonly


# Deliberately small.  Adding syntax here is a correctness change and should be
# accompanied by a DuckDB differential test, not merely a Polars unit test.
_ALLOWED_NODES = {
    exp.Select, exp.From, exp.Table, exp.TableAlias, exp.Column,
    exp.Identifier, exp.Alias, exp.Star,
    exp.Where, exp.Group, exp.Order, exp.Ordered, exp.Limit,
    exp.Join,
    exp.Literal, exp.Boolean, exp.Null,
    exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
    exp.And, exp.Or, exp.Not, exp.Paren, exp.Between, exp.In, exp.Is,
    exp.Count, exp.Sum, exp.Min, exp.Max, exp.Avg,
    exp.Neg,
}

_NUMERIC_TYPE_NAMES = frozenset({
    "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32",
    "uint64", "byte", "short", "integer", "int", "long", "bigint",
    "hugeint", "float32", "float64", "float", "real", "double",
})
_OUTPUT_SAFE_TYPE_NAMES = _NUMERIC_TYPE_NAMES | frozenset({
    "bool", "boolean", "string", "utf8",
})


def _local_path(path: str) -> str:
    raw = str(path or "")
    if raw.startswith("file://"):
        parsed = urlparse(raw)
        if parsed.netloc not in ("", "localhost"):
            raise IslandUnsupportedError(
                f"IslandDB cannot scan non-local file URL {raw!r}"
            )
        raw = unquote(parsed.path)
    if "://" in raw:
        raise IslandUnsupportedError(
            "IslandDB requires a localised Reflection; remote path remained"
        )
    resolved = os.path.realpath(raw)
    if not os.path.isfile(resolved):
        raise FileNotFoundError(f"IslandDB local Parquet file is missing: {raw}")
    return resolved


def _normalized_type(type_name: object) -> str:
    value = str(type_name or "").strip().casefold()
    # Persisted Polars parameterised values (Datetime/Decimal/Duration) remain
    # visibly outside the numeric whitelist.
    return value


def _is_numeric_type(type_name: object) -> bool:
    return _normalized_type(type_name) in _NUMERIC_TYPE_NAMES


def _numeric_family(type_name: object) -> str:
    normalized = _normalized_type(type_name)
    if normalized in {"float32", "float64", "float", "real", "double"}:
        return "float"
    return "integer" if normalized in _NUMERIC_TYPE_NAMES else "other"


def _is_output_safe_type(type_name: object) -> bool:
    return _normalized_type(type_name) in _OUTPUT_SAFE_TYPE_NAMES


def _canonical_physical_type(type_name: object) -> str:
    """Canonicalise only type spellings known to describe one physical type."""
    normalized = _normalized_type(type_name)
    aliases = {
        "byte": "int8", "short": "int16", "integer": "int32",
        "int": "int32", "long": "int64", "bigint": "int64",
        "real": "float32", "double": "float64",
        "boolean": "bool", "utf8": "string",
    }
    return aliases.get(normalized, normalized)


class IslandDB:
    """Native lazy-Parquet executor for IslandDB's conservative SQL subset."""

    def __init__(
        self,
        storage: Optional[object] = None,
        *,
        organization: str = "",
        range_cache: Optional[object] = None,
    ):
        self.storage = storage
        self.organization = str(organization or "")
        try:
            storage_namespace = (
                storage.cache_namespace() if storage is not None else {"provider": "none"}
            )
            encoded_namespace = json.dumps(
                storage_namespace,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        except Exception:
            encoded_namespace = (
                f"{type(storage).__module__}.{type(storage).__qualname__}:"
                f"{id(storage)}"
            ).encode("utf-8")
        self._proof_namespace = hashlib.sha256(encoded_namespace).hexdigest()
        self.range_cache = range_cache
        self.last_profile = IslandProfile()
        self._resources = self._detect_resources()
        self._policy = self._resource_policy()
        # Arrow owns a process-global CPU/I/O pool. Bound it to the container's
        # cgroup/affinity capacity once; individual plans then reserve a subset
        # as an admission weight while fragmented scans can use every available
        # core when the planner decides the work is large enough.
        with _ARROW_POOL_LOCK:
            pa.set_cpu_count(max(1, self._resources.cpu_count))
            pa.set_io_thread_count(max(
                1,
                min(
                    self._resources.cpu_count * 2,
                    self._policy.max_io_workers,
                ),
            ))
        self._spill_root = Path(
            settings.SUPERTABLE_ISLAND_SPILL_DIR
            or os.path.join(get_app_home(), "island_spill")
        )
        self._planner = ResourcePlanner(
            self._resources,
            spill_root=self._spill_root,
            policy=self._policy,
        )
        # CPU quota, cgroup memory limit, and current memory availability are
        # live resource signals. They must refresh one process-wide admission
        # domain, not create a second governor that can reserve concurrently
        # with queries admitted under the previous limits.
        governor_key = (
            str(self._spill_root.resolve()),
            self._policy,
        )
        with _GOVERNOR_LOCK:
            self._governor = _GOVERNORS.get(governor_key)
            if self._governor is None:
                self._governor = ResourceGovernor(
                    self._resources,
                    spill_root=self._spill_root,
                    policy=self._policy,
                )
                _GOVERNORS[governor_key] = self._governor
            else:
                # ``memory_available`` is a live pressure signal and is not
                # part of the stable sharing key.  Never retain the first
                # process sample as the governor's capacity indefinitely.
                self._governor.refresh_resources(self._resources)
        self._query_range_start: Optional[object] = None

    @staticmethod
    def _detect_resources() -> ContainerResources:
        resources = ContainerResources.detect()
        cpu_limit = int(settings.SUPERTABLE_ISLAND_CPU_MAX or 0)
        memory_limit = int(settings.SUPERTABLE_ISLAND_MAX_MEMORY_BYTES or 0)
        if cpu_limit > 0:
            resources = replace(
                resources,
                cpu_count=max(1, min(resources.cpu_count, cpu_limit)),
                cpu_capacity=min(resources.cpu_capacity, float(cpu_limit)),
            )
        if memory_limit > 0:
            resources = replace(
                resources,
                memory_limit_bytes=min(
                    resources.memory_limit_bytes, memory_limit,
                ),
                memory_available_bytes=min(
                    resources.memory_available_bytes, memory_limit,
                ),
            )
        return resources

    @staticmethod
    def _resource_policy() -> ResourcePolicy:
        return ResourcePolicy(
            query_memory_fraction=min(
                1.0, max(0.05, settings.SUPERTABLE_ISLAND_MEMORY_FRACTION),
            ),
            global_memory_fraction=min(
                1.0,
                max(0.05, settings.SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION),
            ),
            max_query_memory_bytes=max(
                0, int(settings.SUPERTABLE_ISLAND_MAX_MEMORY_BYTES or 0),
            ),
            max_result_memory_bytes=max(
                1, int(settings.SUPERTABLE_ISLAND_MAX_RESULT_BYTES),
            ),
            max_spill_bytes=(
                max(0, int(settings.SUPERTABLE_ISLAND_SPILL_MAX_BYTES))
                if settings.SUPERTABLE_ISLAND_SPILL_ENABLED else 0
            ),
            min_spill_free_bytes=max(
                0, int(settings.SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES),
            ),
            max_io_workers=max(
                1, int(settings.SUPERTABLE_ISLAND_IO_WORKERS_MAX),
            ),
        )

    def _get_range_cache(self):
        if self.range_cache is False:
            return None
        if self.range_cache is None:
            if (
                not settings.SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED
                or self.storage is None
            ):
                self.range_cache = False
                return None
            from supertable.engine.range_cache import RangeCache
            self.range_cache = RangeCache(
                self.storage,
                self.organization,
                root=(settings.SUPERTABLE_ISLAND_RANGE_CACHE_DIR or None),
                max_bytes=settings.SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES,
                ttl=settings.SUPERTABLE_ISLAND_RANGE_CACHE_TTL_SEC,
            )
        return self.range_cache

    # ------------------------------------------------------------------
    # Capability boundary
    # ------------------------------------------------------------------

    @staticmethod
    def _snapshots(reflection: Reflection) -> Dict[Tuple[str, str], SuperSnapshot]:
        return {
            (s.super_name.casefold(), s.simple_name.casefold()): s
            for s in reflection.supers
        }

    @staticmethod
    def _table_maps(parser, reflection: Reflection):
        snapshots = IslandDB._snapshots(reflection)
        aliases: Dict[str, SuperSnapshot] = {}
        for td in parser.get_table_tuples():
            snap = snapshots.get((td.super_name.casefold(), td.simple_name.casefold()))
            if snap is not None:
                aliases[td.alias.casefold()] = snap
                aliases.setdefault(td.simple_name.casefold(), snap)
        return aliases

    @staticmethod
    def _required_columns_by_snapshot(
        parser,
        reflection: Reflection,
    ) -> Dict[int, Optional[set[str]]]:
        """Return exact physical columns needed by every table snapshot.

        ``None`` means a star occurrence requires all public columns.  The SQL
        parser already includes predicate/join/group/order columns in each
        ``TableDefinition``; merging occurrence sets here lets the footer
        planner bound only the buffers the query can actually decode instead
        of rejecting a numeric projection merely because an unrelated string
        payload column exists in the same Parquet file.
        """
        snapshots = IslandDB._snapshots(reflection)
        required: Dict[int, Optional[set[str]]] = {}
        for definition in parser.get_table_tuples():
            snapshot = snapshots.get((
                definition.super_name.casefold(),
                definition.simple_name.casefold(),
            ))
            if snapshot is None:
                continue
            marker = id(snapshot)
            columns = list(definition.columns or [])
            if not columns:
                required[marker] = None
                continue
            if marker not in required:
                required[marker] = set()
            if required[marker] is not None:
                required[marker].update(str(column).casefold() for column in columns)
        return required

    @staticmethod
    def _resolve_column_type(
        column: exp.Column,
        aliases: Dict[str, SuperSnapshot],
    ) -> Optional[str]:
        wanted = column.name.casefold()
        if column.table:
            snap = aliases.get(column.table.casefold())
            if snap is None:
                return None
            matches = [
                value for name, value in (snap.column_types or {}).items()
                if str(name).casefold() == wanted
            ]
            return str(matches[0]) if len(matches) == 1 else None

        matches: List[str] = []
        seen = set()
        for snap in aliases.values():
            marker = id(snap)
            if marker in seen:
                continue
            seen.add(marker)
            for name, value in (snap.column_types or {}).items():
                if str(name).casefold() == wanted:
                    matches.append(str(value))
                    break
        return matches[0] if len(matches) == 1 else None

    @staticmethod
    def _column_spelling_is_exact(
        column: exp.Column,
        aliases: Dict[str, SuperSnapshot],
    ) -> bool:
        """Return whether Polars can bind the identifier without case folding.

        DuckDB identifiers are case-insensitive while Polars SQL column lookup
        is case-sensitive.  Falling through on a differently-cased spelling is
        an availability bug in explicit mode and could become a wrong binding
        after schema evolution, so native execution requires the pinned schema
        spelling exactly.
        """
        if column.is_star:
            return True
        wanted = column.name.casefold()
        if column.table:
            snap = aliases.get(column.table.casefold())
            if snap is None:
                return False
            matches = [
                str(name) for name in (snap.column_types or {})
                if str(name).casefold() == wanted
            ]
            return len(matches) == 1 and matches[0] == column.name

        matches: List[str] = []
        seen = set()
        for snap in aliases.values():
            marker = id(snap)
            if marker in seen:
                continue
            seen.add(marker)
            matches.extend(
                str(name) for name in (snap.column_types or {})
                if str(name).casefold() == wanted
            )
        return len(matches) == 1 and matches[0] == column.name

    def can_execute(self, reflection: Reflection, parser) -> IslandCapability:
        reasons: List[str] = []
        if getattr(reflection, "rbac_views", None):
            reasons.append("RBAC views are not yet native in IslandDB")
        if any(not s.files for s in reflection.supers):
            reasons.append("typed empty snapshots use DuckDB until native type parity is complete")

        try:
            root = sqlglot.parse_one(parser.original_query, read="duckdb")
        except Exception as exc:
            return IslandCapability(False, (f"SQL parse failed: {exc}",))
        if not isinstance(root, exp.Select):
            reasons.append("only one top-level SELECT is native")

        unsupported = sorted({
            type(node).__name__ for node in root.walk()
            if type(node) not in _ALLOWED_NODES
        })
        if unsupported:
            reasons.append("unsupported SQL nodes: " + ", ".join(unsupported))

        for join in root.find_all(exp.Join):
            side = str(join.args.get("side") or "").upper()
            kind = str(join.args.get("kind") or "").upper()
            method = str(join.args.get("method") or "").upper()
            if (
                side not in ("", "LEFT")
                or kind not in ("", "INNER", "CROSS")
                or method
                or join.args.get("using")
            ):
                reasons.append(
                    f"join form {method or side or kind or 'USING'} is not native"
                )

        # ``query_sql`` appends a response-safety LIMIT to every SELECT.  It is
        # harmless when sealed candidate-row counts prove that it cannot
        # truncate the relation.  A genuinely truncating LIMIT remains outside
        # the native subset: even ORDER BY does not identify a deterministic
        # DuckDB-equivalent subset when the final key contains ties.
        limit = root.args.get("limit")
        if limit is not None:
            redundant_limit = False
            try:
                expression = limit.expression
                limit_value = int(expression.this)
                if (
                    isinstance(expression, exp.Literal)
                    and not expression.is_string
                    and str(limit_value) == str(expression.this)
                    and limit_value >= 0
                    and root.args.get("offset") is None
                ):
                    snapshots = self._snapshots(reflection)
                    occurrence_rows: List[int] = []
                    for definition in parser.get_table_tuples():
                        snapshot = snapshots.get((
                            definition.super_name.casefold(),
                            definition.simple_name.casefold(),
                        ))
                        if snapshot is None or not bool(getattr(
                            snapshot, "candidate_rows_complete", False,
                        )):
                            break
                        occurrence_rows.append(max(
                            0, int(getattr(snapshot, "candidate_rows", 0) or 0),
                        ))
                    else:
                        upper_bound = 1
                        for rows in occurrence_rows:
                            upper_bound *= rows
                        if not occurrence_rows:
                            upper_bound = 0
                        scalar = bool(root.expressions) and all(
                            next(
                                selected.find_all(
                                    exp.Count, exp.Sum, exp.Min, exp.Max,
                                ),
                                None,
                            ) is not None
                            for selected in root.expressions
                        ) and root.args.get("group") is None
                        redundant_limit = limit_value >= (
                            1 if scalar else upper_bound
                        )
            except Exception:
                redundant_limit = False
            if not redundant_limit:
                reasons.append("LIMIT/OFFSET needs a proven unique total order")

        aliases = self._table_maps(parser, reflection)
        declared_aliases = {
            definition.alias.casefold(): definition.alias
            for definition in parser.get_table_tuples()
        }
        # Output naming is per SQL table occurrence, not per physical snapshot.
        # Two aliases of the same snapshot still produce duplicate columns.
        table_count = len(parser.get_table_tuples())

        def is_proven_boolean(node: object) -> bool:
            if not isinstance(node, exp.Expression):
                return False
            if isinstance(node, exp.Paren):
                return is_proven_boolean(node.this)
            if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
                return True
            if isinstance(node, (exp.Between, exp.In, exp.Boolean)):
                return True
            if isinstance(node, (exp.And, exp.Or)):
                return is_proven_boolean(node.this) and is_proven_boolean(
                    node.expression
                )
            if isinstance(node, exp.Not):
                return is_proven_boolean(node.this)
            return False

        for column in root.find_all(exp.Column):
            if column.table:
                declared = declared_aliases.get(column.table.casefold())
                if declared is None or declared != column.table:
                    reasons.append(
                        f"table qualifier {column.table!r} does not exactly match "
                        "its declared alias"
                    )
            if not self._column_spelling_is_exact(column, aliases):
                reasons.append(
                    f"column spelling {column.sql()} does not exactly match "
                    "the pinned Parquet schema"
                )

        where = root.args.get("where")
        if isinstance(where, exp.Where) and not is_proven_boolean(where.this):
            reasons.append("WHERE expression has unproven Boolean coercion")
        for join in root.find_all(exp.Join):
            on = join.args.get("on")
            if isinstance(on, exp.Expression) and not is_proven_boolean(on):
                reasons.append("JOIN condition has unproven Boolean coercion")
        for logical in root.find_all(exp.And, exp.Or, exp.Not):
            if not is_proven_boolean(logical):
                reasons.append(
                    f"logical expression {logical.sql()} has unproven Boolean coercion"
                )
        if next(root.find_all(exp.Is), None) is not None:
            reasons.append("IS TRUE/FALSE/NULL semantics are not yet native")

        # DuckDB fetchdf and Polars-to-pandas intentionally differ for several
        # rich physical types (for example DECIMAL is float64 vs Decimal
        # objects). Raw projection is native only for the closed set whose
        # pandas representation has differential coverage. Aggregate outputs
        # are normalized separately below.
        unique_snapshots = tuple(
            {id(snap): snap for snap in aliases.values()}.values()
        )
        output_names: set[str] = set()
        for selected in root.expressions:
            core = selected.this if isinstance(selected, exp.Alias) else selected
            if not isinstance(selected, exp.Alias) and not isinstance(
                core, (exp.Column, exp.Star)
            ):
                reasons.append(
                    f"computed projection {selected.sql()} requires an explicit alias"
                )
            if isinstance(core, (exp.Literal, exp.Boolean, exp.Null)):
                reasons.append(
                    f"constant projection {selected.sql()} has unproven output typing"
                )
            output_name = selected.alias_or_name
            if output_name and not isinstance(core, exp.Star) and not (
                isinstance(core, exp.Column) and core.is_star
            ):
                folded_output = str(output_name).casefold()
                if folded_output in output_names:
                    reasons.append(
                        f"duplicate output name {output_name!r} needs explicit aliases"
                    )
                output_names.add(folded_output)
            if isinstance(core, exp.Star):
                if table_count > 1:
                    reasons.append("joined SELECT * output naming is not native")
                for snap in unique_snapshots:
                    for name, type_name in (snap.column_types or {}).items():
                        if str(name).casefold() in {
                            ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                        }:
                            continue
                        if not _is_output_safe_type(type_name):
                            reasons.append(
                                f"projected column {name} type {type_name} "
                                "has unproven pandas parity"
                            )
            elif isinstance(core, exp.Column) and core.is_star:
                if table_count > 1:
                    reasons.append("joined table-star output naming is not native")
                snap = aliases.get(core.table.casefold()) if core.table else None
                if snap is None:
                    reasons.append(f"cannot resolve projected star {core.sql()}")
                else:
                    for name, type_name in (snap.column_types or {}).items():
                        if str(name).casefold() in {
                            ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                        }:
                            continue
                        if not _is_output_safe_type(type_name):
                            reasons.append(
                                f"projected column {name} type {type_name} "
                                "has unproven pandas parity"
                            )
            elif isinstance(core, exp.Column):
                type_name = self._resolve_column_type(core, aliases)
                if type_name is None:
                    reasons.append(f"cannot prove type of projected column {core.sql()}")
                elif not _is_output_safe_type(type_name):
                    reasons.append(
                        f"projected column {core.sql()} type {type_name} "
                        "has unproven pandas parity"
                    )

        semantic_roots: List[exp.Expression] = []
        for key in ("where", "group", "order"):
            value = root.args.get(key)
            if isinstance(value, exp.Expression):
                semantic_roots.append(value)
        for join in root.find_all(exp.Join):
            on = join.args.get("on")
            if isinstance(on, exp.Expression):
                semantic_roots.append(on)
        semantic_roots.extend(
            list(root.find_all(exp.Sum, exp.Avg, exp.Min, exp.Max))
        )

        dangerous_columns: List[exp.Column] = []
        for node in semantic_roots:
            dangerous_columns.extend(node.find_all(exp.Column))
        for column in dangerous_columns:
            type_name = self._resolve_column_type(column, aliases)
            if type_name is None:
                reasons.append(f"cannot prove type of semantic column {column.sql()}")
            elif not _is_numeric_type(type_name):
                reasons.append(
                    f"column {column.sql()} type {type_name} has unproven DuckDB semantics"
                )

        # Cross-type coercion is an engine semantic, not a Parquet semantic.
        # In particular BIGINT/DOUBLE can compare equal in DuckDB after lossy
        # rounding while Polars retains a different expression type.  Native
        # IslandDB accepts only same stored types for column-column comparison,
        # and literals from the same integer/float family as their column.
        comparison_types = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)

        def check_column_literal(column, literal) -> None:
            if not isinstance(column, exp.Column) or not isinstance(literal, exp.Literal):
                return
            column_type = self._resolve_column_type(column, aliases)
            if column_type is not None and literal.is_number:
                literal_family = "integer" if literal.is_int else "float"
                if _numeric_family(column_type) != literal_family:
                    reasons.append(
                        f"mixed literal comparison for {column.sql()} is not native"
                    )

        for comparison in root.find_all(*comparison_types):
            left = comparison.this
            right = comparison.expression
            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                left_type = self._resolve_column_type(left, aliases)
                right_type = self._resolve_column_type(right, aliases)
                if not _is_numeric_type(left_type) or not _is_numeric_type(right_type):
                    reasons.append(
                        f"comparison {comparison.sql()} uses non-numeric semantics"
                    )
                if (
                    left_type is not None and right_type is not None
                    and _normalized_type(left_type) != _normalized_type(right_type)
                ):
                    reasons.append(
                        f"mixed comparison types {left_type}/{right_type} are not native"
                    )
            elif isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                if not _is_numeric_type(self._resolve_column_type(left, aliases)):
                    reasons.append(
                        f"comparison {comparison.sql()} uses non-numeric semantics"
                    )
                check_column_literal(left, right)
            elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
                if not _is_numeric_type(self._resolve_column_type(right, aliases)):
                    reasons.append(
                        f"comparison {comparison.sql()} uses non-numeric semantics"
                    )
                check_column_literal(right, left)
            elif isinstance(left, exp.Column) or isinstance(right, exp.Column):
                reasons.append(
                    f"comparison {comparison.sql()} has an unproven operand"
                )
        for between in root.find_all(exp.Between):
            low = between.args.get("low")
            high = between.args.get("high")
            if not (
                isinstance(between.this, exp.Column)
                and isinstance(low, exp.Literal) and low.is_number
                and isinstance(high, exp.Literal) and high.is_number
            ):
                reasons.append(
                    f"BETWEEN expression {between.sql()} has mixed/unproven types"
                )
            check_column_literal(between.this, low)
            check_column_literal(between.this, high)
        for membership in root.find_all(exp.In):
            if not (
                isinstance(membership.this, exp.Column)
                and membership.expressions
                and all(
                    isinstance(value, exp.Literal) and value.is_number
                    for value in membership.expressions
                )
            ):
                reasons.append(
                    f"IN expression {membership.sql()} has mixed/unproven types"
                )
            for value in membership.expressions:
                check_column_literal(membership.this, value)

        # DuckDB connections use NOCASE by contract.  Polars string comparison
        # is binary, so even an unqualified string literal makes the predicate
        # ineligible until normalized footer/data lanes exist.
        for literal in root.find_all(exp.Literal):
            if literal.is_string:
                reasons.append("string predicate/ordering semantics differ from DuckDB NOCASE")
                break

        if next(root.find_all(exp.Neg), None) is not None:
            reasons.append("signed expression semantics are not yet native")
        for ordered in root.find_all(exp.Ordered):
            if ordered.args.get("desc") or ordered.args.get("nulls_first"):
                reasons.append(
                    "DESC/NULLS FIRST ordering differs from DuckDB NULL placement"
                )

        # Floating reductions are order-sensitive and the two vector engines
        # are free to reduce row groups in a different order. Integer AVG also
        # enters an engine-specific floating accumulator. Keep exact integer
        # SUM (widened to Int128 below), COUNT, MIN and MAX native for now.
        if next(root.find_all(exp.Avg), None) is not None:
            reasons.append("AVG reduction parity is not yet proven")
        aggregates = tuple(root.find_all(exp.Count, exp.Sum, exp.Min, exp.Max))
        if root.args.get("group") is not None and not aggregates:
            reasons.append("GROUP BY without an aggregate uses DuckDB's distinct path")
        for aggregate in aggregates:
            argument = aggregate.this
            is_count_star = isinstance(aggregate, exp.Count) and isinstance(
                argument, exp.Star
            )
            if not isinstance(argument, exp.Column) and not is_count_star:
                reasons.append(
                    f"aggregate argument {aggregate.sql()} must be one direct column"
                )
                continue
            type_name = (
                self._resolve_column_type(argument, aliases)
                if isinstance(argument, exp.Column)
                else None
            )
            if isinstance(aggregate, exp.Sum):
                if type_name is not None and _numeric_family(type_name) == "float":
                    reasons.append("floating SUM reduction parity is not yet proven")
            elif isinstance(aggregate, (exp.Min, exp.Max)):
                if type_name is not None and _numeric_family(type_name) == "float":
                    reasons.append("floating MIN/MAX NaN semantics are not native")

        # Native joins are deliberately in-memory only for now.  The resource
        # planner routes a decoded hash table that exceeds its budget to
        # DuckDB/Spark; no hidden Polars materialization is allowed.  Sort/group
        # spill has the smaller sealed SQL subset implemented below.

        return IslandCapability(not reasons, tuple(dict.fromkeys(reasons)))

    @staticmethod
    def _query_shape(parser) -> Tuple[exp.Select, bool, bool, bool]:
        root = sqlglot.parse_one(parser.original_query, read="duckdb")
        if not isinstance(root, exp.Select):
            raise IslandUnsupportedError("only one top-level SELECT is native")
        has_join = next(root.find_all(exp.Join), None) is not None
        has_group = root.args.get("group") is not None
        has_sort = root.args.get("order") is not None
        return root, has_join, has_group, has_sort

    @staticmethod
    def _selected_row_group_count(reflection: Reflection) -> int:
        total = 0
        for snapshot in reflection.supers:
            try:
                selections = (
                    getattr(snapshot, "row_group_selections", None) or {}
                )
                values = selections.values()
                for selection in values:
                    ids = selection.selected_ids
                    if isinstance(ids, tuple):
                        total += len(ids)
            except Exception:
                # Observability must follow the scan hint's fail-open contract.
                # A malformed optional mapping cannot fail an otherwise valid
                # SELECT after execution has already chosen to scan all groups.
                continue
        return total

    @staticmethod
    def _declared_row_group_units(snapshot: SuperSnapshot) -> int:
        """Best-effort parallelism units; malformed hints mean one/file."""
        try:
            selections = getattr(snapshot, "row_group_selections", None) or {}
            selected = sum(
                len(selection.selected_ids)
                for selection in selections.values()
                if isinstance(selection.selected_ids, tuple)
            )
            return selected + max(0, len(snapshot.files) - len(selections))
        except Exception:
            return max(1, len(snapshot.files))

    def resource_plan(
        self,
        reflection: Reflection,
        parser,
        *,
        streaming_result: bool,
    ) -> QueryResourcePlan:
        """Return the bounded execution decision used by routing and execute.

        Missing decoded statistics never become a zero-byte estimate.  The
        planner routes such a query to the mature external-memory engines.
        Operator-state multipliers are deliberately conservative until the
        native spill subset proves a tighter bound.
        """
        root, has_join, has_group, has_sort = self._query_shape(parser)
        decoded = max(0, int(getattr(reflection, "decoded_bytes", 0) or 0))
        selected_decoded = decoded
        selected_decoded_complete = bool(
            getattr(reflection, "selected_decoded_bytes_complete", False)
        )
        if selected_decoded_complete:
            selected_decoded = max(
                0,
                int(getattr(reflection, "selected_decoded_bytes", 0) or 0),
            )
        tombstones = tuple(
            (getattr(reflection, "tombstone_views", None) or {}).values()
        )
        has_active_tombstone = any(
            getattr(tombstone, "tombstone_path", None)
            for tombstone in tombstones
        )
        compressed = max(
            0,
            int(
                getattr(reflection, "row_group_scan_bytes", 0)
                or reflection.reflection_bytes
                or 0
            ),
        )
        complete = bool(
            getattr(reflection, "decoded_bytes_complete", False)
            and getattr(reflection, "row_group_scan_bytes_complete", False)
        )
        selected_groups = 0
        if not complete and not has_active_tombstone:
            footer_estimate = self._footer_working_set(
                reflection,
                required_columns=self._required_columns_by_snapshot(
                    parser, reflection,
                ),
            )
            if footer_estimate is not None:
                compressed, decoded, selected_groups = footer_estimate
                selected_decoded = decoded
                selected_decoded_complete = True
                complete = True
        candidate_rows_complete = all(
            bool(getattr(snapshot, "candidate_rows_complete", False))
            for snapshot in reflection.supers
        )
        candidate_rows = sum(
            max(0, int(getattr(snapshot, "candidate_rows", 0) or 0))
            for snapshot in reflection.supers
        )
        scalar_aggregate = bool(root.expressions) and all(
            next(expression.find_all(exp.Count, exp.Sum, exp.Min, exp.Max), None)
            is not None
            for expression in root.expressions
        ) and not has_group
        # A join needs build/probe hash state; ordering needs runs/merge state;
        # grouped aggregation needs a key-state table.  Spill integration below
        # supports only the sealed direct-column GROUP/ORDER subset; joins route.
        if has_join:
            # Polars hash joins carry keys, row indices and allocator/hash-table
            # overhead beyond decoded buffers. Unknown output cardinality never
            # becomes a small result estimate below.
            state = decoded * 4
        elif has_sort:
            state = decoded
        elif has_group:
            state = max(decoded * 2, compressed)
        elif scalar_aggregate:
            # COUNT/SUM/MIN/MAX reduce incrementally. Scan buffers are budgeted
            # separately; the aggregate itself has O(1) state.
            state = 4096 * max(1, len(root.expressions))
        else:
            # A projection/filter stream has no blocking operator state. Its
            # source and result batches are governed by scan/result budgets;
            # charging the full decoded input here would incorrectly route a
            # bounded 100-GB stream away merely because all batches together do
            # not fit memory.
            state = 0
        if selected_groups == 0:
            for snapshot in reflection.supers:
                selected_groups += self._declared_row_group_units(snapshot)
        # No cardinality estimator currently seals result width.  A SELECT with
        # no reduction can return the decoded input; grouped/sorted results may
        # do the same, while scalar aggregates are tiny.
        if scalar_aggregate:
            result_bytes = 4096
            estimated_result_rows = 1
        else:
            projected_fields = 0
            for selected in root.expressions:
                core = selected.this if isinstance(selected, exp.Alias) else selected
                if isinstance(core, exp.Star) or (
                    isinstance(core, exp.Column) and core.is_star
                ):
                    projected_fields += sum(
                        1 for snapshot in reflection.supers
                        for name in (snapshot.column_types or {})
                        if str(name).casefold() not in {
                            ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                        }
                    )
                else:
                    projected_fields += 1
            projected_fields = max(1, projected_fields)
            if has_join:
                occurrence_rows: List[int] = []
                snapshots = self._snapshots(reflection)
                for definition in parser.get_table_tuples():
                    snapshot = snapshots.get((
                        definition.super_name.casefold(),
                        definition.simple_name.casefold(),
                    ))
                    if snapshot is None or not getattr(
                        snapshot, "candidate_rows_complete", False,
                    ):
                        candidate_rows_complete = False
                        break
                    occurrence_rows.append(max(
                        0, int(getattr(snapshot, "candidate_rows", 0) or 0),
                    ))
                worst_rows = 1
                for rows in occurrence_rows:
                    worst_rows *= rows
                result_bytes = worst_rows * projected_fields * 24
                estimated_result_rows = worst_rows
            else:
                result_bytes = candidate_rows * projected_fields * 24
                estimated_result_rows = candidate_rows
            if not candidate_rows_complete:
                complete = False

        # A sealed deletion vector is eager metadata and anti-join hash state.
        # Bound it from its exact row count and the longest permitted resource
        # key. This also covers targeted-rowid integrity proof state.
        tombstone_state = 0
        for tombstone in tombstones:
            rows = getattr(tombstone, "expected_rows", None)
            digest = getattr(tombstone, "tombstone_digest", None)
            if (
                not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0
                or not isinstance(digest, str) or len(digest) != 64
            ):
                complete = False
                continue
            keys = getattr(tombstone, "snapshot_resource_keys", None)
            if keys is None:
                keys = getattr(tombstone, "resource_keys", None) or ()
            max_key = max((len(str(key).encode("utf-8")) for key in keys), default=0)
            # UTF-8 bytes + offsets/rowid/validity + conservative hash/object
            # overhead for validation and anti-join tables. Count each SQL
            # table occurrence: a self-join currently builds one eager DV frame
            # and one anti-join hash state per alias even when their immutable
            # tombstone seal is identical. Only the source-rowid proof is shared.
            tombstone_state += rows * (max_key + 8 + 8 + 1 + 128)
        state += tombstone_state
        decoded += tombstone_state
        spillable = bool((has_sort or has_group) and not has_join)
        estimate = QueryResourceEstimate(
            compressed_scan_bytes=compressed,
            decoded_scan_bytes=decoded,
            result_bytes=result_bytes,
            operator_state_bytes=state,
            selected_files=max(1, int(reflection.total_reflections or 0)),
            selected_row_groups=max(1, selected_groups),
            estimated_rows=(candidate_rows if candidate_rows_complete else 0),
            estimated_result_rows=(
                estimated_result_rows if candidate_rows_complete else 0
            ),
            selected_decoded_bytes=selected_decoded,
            selected_decoded_bytes_complete=selected_decoded_complete,
            spillable=spillable,
            has_sort=has_sort,
            has_group_by=has_group,
            has_join=has_join,
            estimates_complete=complete,
        )
        return self._planner.plan(estimate, streaming_result=streaming_result)

    def _footer_working_set(
        self,
        reflection: Reflection,
        *,
        required_columns: Optional[Dict[int, Optional[set[str]]]] = None,
    ) -> Optional[Tuple[int, int, int]]:
        """Seal a conservative working set from local Parquet footers.

        This compatibility path covers directly constructed/local reflections
        that predate estimator byte fields.  It sums every physical column in
        selected row groups, so it may route away earlier but cannot understate
        decoded memory.  Remote reflections use the estimator/range metadata
        path and never trigger a whole-object download here.
        """
        compressed = decoded = groups = 0
        try:
            for snapshot in reflection.supers:
                requested = (
                    required_columns.get(id(snapshot))
                    if required_columns is not None
                    and id(snapshot) in required_columns
                    else None
                )
                for path, resource_key in zip(
                    snapshot.files, snapshot.resource_keys,
                ):
                    local = _local_path(path)
                    parquet_file = papq.ParquetFile(local)
                    metadata = parquet_file.metadata
                    selected_indexes: List[int] = []
                    widths: List[int] = []
                    for field_index, field in enumerate(parquet_file.schema_arrow):
                        if (
                            requested is not None
                            and field.name.casefold() not in requested
                        ):
                            continue
                        width = __import__(
                            "supertable.engine.data_estimator",
                            fromlist=["DataEstimator"],
                        ).DataEstimator._decoded_fixed_width(str(field.type))
                        if width is None:
                            return None
                        selected_indexes.append(field_index)
                        widths.append(width)
                    if not selected_indexes:
                        return None
                    selected = self._validated_row_group_ids(
                        snapshot,
                        str(resource_key),
                        metadata.num_row_groups,
                        parquet_footer_sha256(metadata),
                    )
                    ids = selected or tuple(range(metadata.num_row_groups))
                    groups += len(ids)
                    for group_id in ids:
                        group = metadata.row_group(group_id)
                        for index in selected_indexes:
                            column = group.column(index)
                            compressed += max(0, int(column.total_compressed_size))
                        decoded += int(group.num_rows) * sum(
                            width + 1 for width in widths
                        )
            return compressed, decoded, groups
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Physical relations
    # ------------------------------------------------------------------

    @staticmethod
    def _reserved_schema_guard(schema: pl.Schema, *, source: str) -> None:
        reserved = {
            ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
            SOURCE_FILE_COL.casefold(), TOMBSTONE_FILE_COL.casefold(),
        }
        seen: Dict[str, str] = {}
        for name in schema.names():
            folded = str(name).casefold()
            if folded in seen:
                raise IslandIntegrityError(
                    f"{source} has case-insensitive duplicate columns "
                    f"{seen[folded]!r}/{name!r}"
                )
            seen[folded] = str(name)
        # Source identity is internal and must never be supplied by a data file.
        if SOURCE_FILE_COL.casefold() in seen:
            raise IslandIntegrityError(
                f"{source} contains reserved internal column {SOURCE_FILE_COL!r}"
            )
        # Case variants of the two persisted system columns are ambiguous.
        for canonical in (ROWID_COL, TIMESTAMP_COL):
            actual = seen.get(canonical.casefold())
            if actual is not None and actual != canonical:
                raise IslandIntegrityError(
                    f"{source} contains non-canonical system column {actual!r}"
                )

    @staticmethod
    def _validated_row_group_ids(
        snapshot: SuperSnapshot,
        resource_key: str,
        actual_count: int,
        actual_footer_sha256: object,
    ) -> Optional[Tuple[int, ...]]:
        """Return a sound explicit RG subset, or ``None`` for scan-all.

        Estimator hints are an optimisation, never an authority boundary. A
        stale/corrupt footer hash or count, or a malformed subset, therefore
        disables the hint for this object instead of clamping IDs or excluding
        data. Callers must pass the hash of the metadata backing their scan so
        planning and execution cannot accidentally apply different rules.
        """
        try:
            selections = getattr(snapshot, "row_group_selections", None) or {}
            selection = selections.get(resource_key)
            if selection is None:
                return None
            expected = selection.expected_row_group_count
            raw_ids = selection.selected_ids
            footer_sha256 = selection.footer_sha256
        except Exception:
            return None
        if (
            not isinstance(expected, int)
            or isinstance(expected, bool)
            or not isinstance(raw_ids, tuple)
            or not isinstance(footer_sha256, str)
            or len(footer_sha256) != 64
            or any(ch not in "0123456789abcdef" for ch in footer_sha256)
            or not isinstance(actual_footer_sha256, str)
            or len(actual_footer_sha256) != 64
            or any(
                ch not in "0123456789abcdef"
                for ch in actual_footer_sha256
            )
            or any(
                not isinstance(value, int) or isinstance(value, bool)
                for value in raw_ids
            )
        ):
            return None
        ids = raw_ids
        if (
            expected != actual_count
            or expected <= 0
            or not ids
            or ids != tuple(sorted(set(ids)))
            or any(value < 0 or value >= actual_count for value in ids)
            or footer_sha256 != actual_footer_sha256
        ):
            return None
        # Encoding the complete set is equivalent to scan-all and needlessly
        # couples execution to an estimator artifact.
        return None if len(ids) == actual_count else ids

    def _base_relation(
        self,
        snapshot: SuperSnapshot,
        *,
        row_group_hints: bool = True,
        batch_rows: int = 65_536,
        object_metadata_out: Optional[Dict[str, object]] = None,
        expected_object_metadata: Optional[Dict[str, object]] = None,
    ) -> pl.LazyFrame:
        if not (
            len(snapshot.files) == len(snapshot.resource_keys)
            and len(snapshot.files) > 0
        ):
            raise IslandIntegrityError(
                f"{snapshot.super_name}.{snapshot.simple_name} has a non-bijective "
                "file/resource mapping"
            )
        if len(set(snapshot.resource_keys)) != len(snapshot.resource_keys):
            raise IslandIntegrityError(
                f"{snapshot.super_name}.{snapshot.simple_name} has duplicate "
                "canonical resource keys"
            )
        resolved_to_key: Dict[str, str] = {}
        fragments: List[pads.ParquetFileFragment] = []
        file_entries: Dict[str, Dict[str, object]] = {}
        has_remote = any(
            "://" in str(path or "")
            and not str(path or "").startswith("file://")
            for path in snapshot.files
        )
        filesystem = (
            pafs.PyFileSystem(_IslandFileSystemHandler(file_entries))
            if has_remote else pafs.LocalFileSystem()
        )
        common_schema: Optional[Dict[str, Tuple[str, pl.DataType]]] = None
        ordered_fields: List[pa.Field] = []
        seen_fields: set[str] = set()
        local_paths: List[str] = []
        local_to_resource: Dict[str, str] = {}
        pinned_schema: Dict[str, Tuple[str, str]] = {}
        for name, type_name in (snapshot.column_types or {}).items():
            folded = str(name).casefold()
            if folded in {
                ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
            }:
                continue
            if folded in pinned_schema:
                raise IslandIntegrityError(
                    "snapshot schema contains case-insensitive duplicate columns"
                )
            pinned_schema[folded] = (str(name), str(type_name))
        physical_union: set[str] = set()
        declared_sizes = list(getattr(snapshot, "resource_sizes", None) or [])
        if declared_sizes and len(declared_sizes) != len(snapshot.files):
            raise IslandIntegrityError("snapshot resource sizes are non-bijective")
        for file_index, (path, resource_key) in enumerate(zip(
            snapshot.files, snapshot.resource_keys,
        )):
            virtual_path = f"{file_index:016x}.parquet"
            raw_path = str(path or "")
            is_remote = "://" in raw_path and not raw_path.startswith("file://")
            if is_remote:
                cache = self._get_range_cache()
                if cache is None:
                    raise IslandUnsupportedError(
                        "remote IslandDB scan requires the sealed range reader"
                    )
                expected_metadata = (
                    (expected_object_metadata or {}).get(str(resource_key))
                )
                remote_reader = cache.open(
                    str(resource_key), expected=expected_metadata,
                )
                metadata = remote_reader.metadata
                file_size = int(remote_reader.size)
                remote_reader.close()
                if object_metadata_out is not None:
                    object_metadata_out[str(resource_key)] = metadata
                declared = int(declared_sizes[file_index]) if declared_sizes else 0
                if declared > 0 and declared != file_size:
                    raise IslandIntegrityError(
                        f"snapshot size for {resource_key!r} does not match object seal"
                    )
                file_entries[virtual_path] = {
                    "raw_key": str(resource_key),
                    "cache": cache,
                    "metadata": metadata,
                    "size": file_size,
                }
            else:
                local = _local_path(path)
                prior_key = resolved_to_key.setdefault(local, str(resource_key))
                if prior_key != str(resource_key):
                    raise IslandIntegrityError(
                        "multiple canonical resource keys resolve to one local Parquet file"
                    )
                file_size = os.path.getsize(local)
                declared = int(declared_sizes[file_index]) if declared_sizes else 0
                if declared > 0 and declared != file_size:
                    raise IslandIntegrityError(
                        f"snapshot size for {resource_key!r} does not match local object"
                    )
                local_paths.append(local)
                local_to_resource[local] = str(resource_key)
                if has_remote:
                    file_entries[virtual_path] = {
                        "local": local,
                        "size": file_size,
                    }
            fragment_path = virtual_path if has_remote else local
            file_format = pads.ParquetFileFormat()
            base_fragment = file_format.make_fragment(
                fragment_path, filesystem=filesystem, file_size=file_size,
            )
            physical_arrow_schema = base_fragment.physical_schema
            for field in physical_arrow_schema:
                if field.name not in seen_fields:
                    ordered_fields.append(field)
                    seen_fields.add(field.name)
            physical_schema = pl.Schema(physical_arrow_schema)
            self._reserved_schema_guard(physical_schema, source=resource_key)
            comparable_schema = {
                str(name).casefold(): (str(name), dtype)
                for name, dtype in physical_schema.items()
                if str(name).casefold() not in {
                    ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                }
            }
            physical_union.update(comparable_schema)
            metadata_mismatches: List[str] = []
            for folded, (physical_name, physical_type) in comparable_schema.items():
                pinned = pinned_schema.get(folded)
                if pinned is None:
                    metadata_mismatches.append(f"{physical_name} (not pinned)")
                    continue
                pinned_name, pinned_type = pinned
                if (
                    pinned_name != physical_name
                    or _canonical_physical_type(pinned_type)
                    != _canonical_physical_type(physical_type)
                ):
                    metadata_mismatches.append(
                        f"{physical_name}:{physical_type} != "
                        f"{pinned_name}:{pinned_type}"
                    )
            if metadata_mismatches:
                raise IslandUnsupportedError(
                    "IslandDB cannot prove pinned/physical schema equivalence: "
                    + ", ".join(metadata_mismatches)
                )
            if common_schema is None:
                common_schema = comparable_schema
            else:
                overlap = set(common_schema).intersection(comparable_schema)
                incompatible = sorted(
                    name for name in overlap
                    if common_schema[name] != comparable_schema[name]
                )
                if incompatible:
                    raise IslandUnsupportedError(
                        "IslandDB native schema evolution requires identical "
                        "physical names and types for shared columns: "
                        + ", ".join(incompatible)
                    )
                common_schema.update(comparable_schema)
            selected_ids = None
            # Local scans intentionally use Polars' conservative native footer
            # pruning below. Computing a SHA-256 metadata seal and constructing
            # a second exact Arrow fragment for every local file is therefore
            # dead work (severe at 10k+ files). Remote range reads still require
            # the exact sealed subset because expanding it changes physical I/O.
            if row_group_hints and has_remote:
                try:
                    live_footer_sha256 = parquet_footer_sha256(
                        base_fragment.metadata,
                    )
                except Exception:
                    live_footer_sha256 = ""
                selected_ids = self._validated_row_group_ids(
                    snapshot,
                    str(resource_key),
                    int(base_fragment.num_row_groups),
                    live_footer_sha256,
                )
            if has_remote:
                fragments.append(
                    file_format.make_fragment(
                        fragment_path,
                        filesystem=filesystem,
                        file_size=file_size,
                        row_groups=(list(selected_ids) if selected_ids else None),
                        partition_expression=(
                            pads.field(SOURCE_FILE_COL) == str(resource_key)
                        ),
                    )
                )
        missing_pinned = sorted(set(pinned_schema).difference(physical_union))
        if missing_pinned:
            raise IslandUnsupportedError(
                "IslandDB pinned columns are absent from every selected file: "
                + ", ".join(pinned_schema[name][0] for name in missing_pinned)
            )
        # Arrow's dataset scanner treats the canonical object key as a virtual
        # partition column.  This replaces one LazyFrame + one literal column
        # per file and lets 100+ resources participate in one parallel scan.
        # Shared columns were already proven exact above; missing evolution
        # columns are represented as NULL by the dataset schema.
        ordered_fields.append(pa.field(SOURCE_FILE_COL, pa.string()))
        dataset_schema = pa.schema(ordered_fields)
        if not has_remote:
            # Polars' Rust-native multi-file scanner is materially faster for a
            # full-row-group local/whole-object-cache scan.  It still performs
            # projection and predicate pushdown and attaches an exact path
            # identity, which is immediately translated to the pinned raw key.
            # For local immutable files the Rust scanner's own conservative
            # footer-statistics pruning is faster than crossing the Python
            # Arrow Dataset bridge.  An estimator row-group subset is only a
            # performance hint: scanning its safe superset preserves results.
            # Remote objects still require exact fragments below so range I/O
            # never expands from selected chunks to whole objects.
            physical_schema = pl.Schema(pa.schema(ordered_fields[:-1]))
            return (
                pl.scan_parquet(
                    local_paths,
                    schema=physical_schema,
                    include_file_paths=SOURCE_FILE_COL,
                    hive_partitioning=False,
                    use_statistics=True,
                    missing_columns="insert",
                    extra_columns="raise",
                )
                .with_columns(
                    pl.col(SOURCE_FILE_COL).replace_strict(
                        local_to_resource,
                        return_dtype=pl.String,
                    )
                )
            )
        dataset = pads.FileSystemDataset(
            fragments,
            dataset_schema,
            pads.ParquetFileFormat(),
            filesystem=filesystem,
        )
        return pl.scan_pyarrow_dataset(
            dataset,
            allow_pyarrow_filter=True,
            batch_size=max(1, int(batch_rows)),
        )

    def _load_tombstone(self, tomb_def) -> pl.DataFrame:
        path = str(getattr(tomb_def, "tombstone_path", "") or "")
        cache_key = str(getattr(tomb_def, "cache_key", "") or "")
        if not path:
            raise IslandIntegrityError("active deletion-vector definition has no path")
        if "://" not in path and os.path.isfile(path):
            frame = pl.read_parquet(path, hive_partitioning=False)
        elif self.storage is not None and cache_key:
            # DVs are bounded correctness metadata (normally <= threshold), so
            # the storage SDK path is acceptable here.  Data Parquet files never
            # take this whole-object fallback.
            frame = pl.from_arrow(self.storage.read_parquet(cache_key))
        else:
            raise IslandIntegrityError("required deletion vector cannot be read safely")
        allowed = getattr(tomb_def, "snapshot_resource_keys", None)
        if allowed is None:
            allowed = getattr(tomb_def, "resource_keys", ())
        return validate_tombstone_frame(
            frame,
            expected_rows=getattr(tomb_def, "expected_rows", None),
            expected_digest=getattr(tomb_def, "tombstone_digest", None),
            allowed_files=set(allowed),
            source=f"IslandDB deletion-vector {cache_key or path}",
        )

    def _source_rowid_proof_key(
        self,
        resource_key: str,
        metadata: object,
        rowids: Iterable[int],
    ) -> Optional[Tuple[object, ...]]:
        """Return a cache key only for a conditionally readable remote object."""
        identity = getattr(metadata, "identity_token", None)
        try:
            token = identity() if callable(identity) else None
        except Exception:
            token = None
        if not isinstance(token, str) or not token:
            return None
        digest = hashlib.sha256(b"st-island-rowid-proof-v1\0")
        try:
            values = sorted(int(value) for value in rowids)
        except Exception:
            return None
        for value in values:
            if value <= 0 or value > 2**63 - 1:
                return None
            digest.update(value.to_bytes(8, "big", signed=False))
        return (
            self.organization,
            self._proof_namespace,
            str(resource_key),
            token,
            digest.hexdigest(),
        )

    @staticmethod
    def _source_rowid_proof_hit(key: Optional[Tuple[object, ...]]) -> bool:
        if key is None:
            return False
        now = time.monotonic()
        with _ROWID_PROOF_LOCK:
            completed = _ROWID_PROOFS.get(key)
            if completed is None:
                return False
            if now - completed > _ROWID_PROOF_TTL_SEC:
                _ROWID_PROOFS.pop(key, None)
                return False
            _ROWID_PROOFS.move_to_end(key)
            return True

    @staticmethod
    def _store_source_rowid_proofs(
        keys: Iterable[Optional[Tuple[object, ...]]],
    ) -> None:
        now = time.monotonic()
        with _ROWID_PROOF_LOCK:
            for key in keys:
                if key is None:
                    continue
                _ROWID_PROOFS[key] = now
                _ROWID_PROOFS.move_to_end(key)
            while len(_ROWID_PROOFS) > _ROWID_PROOF_MAX_ENTRIES:
                _ROWID_PROOFS.popitem(last=False)

    def _validate_source_rowids(
        self,
        snapshot: SuperSnapshot,
        deletion_vector: pl.DataFrame,
        *,
        object_metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        targets = deletion_vector.filter(
            pl.col(TOMBSTONE_FILE_COL).is_in(snapshot.resource_keys)
        ).select([TOMBSTONE_FILE_COL, ROWID_COL])
        if targets.height == 0:
            return
        target_keys = set(
            targets.get_column(TOMBSTONE_FILE_COL).unique().to_list()
        )
        proof_keys: Dict[str, Optional[Tuple[object, ...]]] = {}
        missed_keys: set[str] = set()
        for resource_key in target_keys:
            per_file_rowids = (
                targets
                .filter(pl.col(TOMBSTONE_FILE_COL) == resource_key)
                .get_column(ROWID_COL)
                .to_list()
            )
            proof_key = self._source_rowid_proof_key(
                resource_key,
                (object_metadata or {}).get(resource_key),
                per_file_rowids,
            )
            proof_keys[resource_key] = proof_key
            if not self._source_rowid_proof_hit(proof_key):
                missed_keys.add(resource_key)
        if not missed_keys:
            return
        targets = targets.filter(
            pl.col(TOMBSTONE_FILE_COL).is_in(list(missed_keys))
        )
        indexes = [
            index for index, key in enumerate(snapshot.resource_keys)
            if key in missed_keys
        ]
        if len(indexes) != len(missed_keys):
            raise IslandIntegrityError(
                "deletion-vector references a source outside the selected snapshot"
            )
        declared_sizes = list(getattr(snapshot, "resource_sizes", None) or [])
        proof_snapshot = replace(
            snapshot,
            files=[snapshot.files[index] for index in indexes],
            resource_keys=[snapshot.resource_keys[index] for index in indexes],
            resource_sizes=(
                [declared_sizes[index] for index in indexes]
                if declared_sizes else []
            ),
            row_group_selections={},
        )
        # One Arrow dataset covers 100+ resources and partition-prunes to the
        # exact DV-referenced object keys; unrelated survivor files are not
        # opened for this integrity pass. Row-group hints are intentionally
        # disabled: proving source rowid uniqueness is a whole-file integrity
        # boundary, not a query optimization.
        scan = self._base_relation(
            proof_snapshot,
            row_group_hints=False,
            expected_object_metadata={
                key: metadata
                for key, metadata in (object_metadata or {}).items()
                if key in missed_keys
            },
        )
        schema = scan.collect_schema()
        if ROWID_COL not in schema or schema[ROWID_COL] != pl.Int64:
            raise IslandIntegrityError(
                f"deletion-vector source lacks canonical Int64 {ROWID_COL}"
            )
        # Prove exactly the identity the anti join will consume. A duplicate
        # source row for an unrelated id cannot over-delete this query; every
        # DV-targeted (file,rowid) must resolve to exactly one physical row.
        # This bounds hash state by the sealed DV rather than all source rows.
        matches = (
            scan
            .select([SOURCE_FILE_COL, ROWID_COL])
            .join(
                targets.lazy(),
                left_on=[SOURCE_FILE_COL, ROWID_COL],
                right_on=[TOMBSTONE_FILE_COL, ROWID_COL],
                how="inner",
            )
            .group_by([SOURCE_FILE_COL, ROWID_COL])
            .agg(pl.len().alias("matches"))
            .collect(engine="streaming")
        )
        if (
            matches.height != targets.height
            or matches.filter(pl.col("matches") != 1).height
        ):
            raise IslandIntegrityError(
                "a deletion-vector (source file, rowid) does not identify "
                "exactly one physical row"
            )
        self._store_source_rowid_proofs(
            proof_keys.get(resource_key) for resource_key in missed_keys
        )

    def _apply_tombstone(
        self,
        relation: pl.LazyFrame,
        snapshot: SuperSnapshot,
        tomb_def,
        *,
        object_metadata: Optional[Dict[str, object]] = None,
        query_proofs: Optional[set[Tuple[object, ...]]] = None,
    ) -> pl.LazyFrame:
        if tomb_def is not None and getattr(tomb_def, "tombstone_path", None):
            dv = self._load_tombstone(tomb_def)
            query_marker = (
                id(snapshot),
                getattr(tomb_def, "cache_key", None),
                getattr(tomb_def, "expected_rows", None),
                getattr(tomb_def, "tombstone_digest", None),
            )
            if query_proofs is None or query_marker not in query_proofs:
                self._validate_source_rowids(
                    snapshot,
                    dv,
                    object_metadata=object_metadata,
                )
                if query_proofs is not None:
                    query_proofs.add(query_marker)
            relation = relation.join(
                dv.lazy(),
                left_on=[SOURCE_FILE_COL, ROWID_COL],
                right_on=[TOMBSTONE_FILE_COL, ROWID_COL],
                how="anti",
                coalesce=True,
            )
        schema = relation.collect_schema()
        public = [
            name for name in schema.names()
            if str(name).casefold() not in {
                ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
                SOURCE_FILE_COL.casefold(), TOMBSTONE_FILE_COL.casefold(),
            }
        ]
        return relation.select(public)

    @staticmethod
    def _normalize_aggregate_dtypes(
        result: pl.DataFrame,
        root: exp.Select,
    ) -> pl.DataFrame:
        casts: List[pl.Expr] = []
        for selected in root.expressions:
            core = selected.this if isinstance(selected, exp.Alias) else selected
            name = selected.alias_or_name
            if not name or name not in result.columns:
                continue
            if isinstance(core, exp.Count):
                casts.append(pl.col(name).cast(pl.Int64))
            elif isinstance(core, exp.Sum) and (
                result.schema[name].is_integer()
                or result.schema[name].is_decimal()
            ):
                # DuckDB SUM(integer) is HUGEINT; fetchdf exposes it as float64.
                casts.append(pl.col(name).cast(pl.Float64))
        return result.with_columns(casts) if casts else result

    def _rewrite_native_aggregates(
        self,
        sql: str,
        parser,
        reflection: Reflection,
    ) -> Tuple[str, exp.Select]:
        """Bridge documented aggregate edge semantics before Polars parses SQL.

        Polars returns integer zero for ``SUM`` over an empty/all-NULL input;
        SQL and DuckDB require NULL.  Rewriting each native SUM to a guarded
        aggregate preserves row-group/projection pushdown and the exact result.
        """
        root = sqlglot.parse_one(sql, read="duckdb")
        if not isinstance(root, exp.Select):  # already fenced by can_execute
            raise IslandUnsupportedError("only one top-level SELECT is native")
        original = root.copy()
        aliases = self._table_maps(parser, reflection)
        for aggregate in list(root.find_all(exp.Sum)):
            argument = aggregate.this.copy()
            widened = aggregate.copy()
            argument_type = (
                self._resolve_column_type(aggregate.this, aliases)
                if isinstance(aggregate.this, exp.Column)
                else None
            )
            if (
                _numeric_family(argument_type) == "integer"
                or isinstance(aggregate.this, exp.Literal)
                and aggregate.this.is_int
            ):
                widened.set(
                    "this",
                    exp.Cast(
                        this=argument.copy(),
                        to=exp.DataType.build("INT128"),
                    ),
                )
            condition = exp.EQ(
                this=exp.Count(this=argument.copy()),
                expression=exp.Literal.number(0),
            )
            guarded = exp.Case(
                ifs=[exp.If(this=condition, true=exp.Null())],
                default=widened,
            )
            aggregate.replace(guarded)
        return root.sql(dialect="duckdb"), original

    @staticmethod
    def _to_duckdb_pandas(result: pl.DataFrame) -> pd.DataFrame:
        """Match ``DuckDBPyRelation.fetchdf`` nullable scalar dtypes."""
        frame = result.to_pandas(use_pyarrow_extension_array=False)
        nullable_integer = {
            pl.Int8: "Int8", pl.Int16: "Int16", pl.Int32: "Int32",
            pl.Int64: "Int64", pl.UInt8: "UInt8", pl.UInt16: "UInt16",
            pl.UInt32: "UInt32", pl.UInt64: "UInt64",
        }
        for name, dtype in result.schema.items():
            series = result.get_column(name)
            if series.null_count() == 0:
                continue
            pandas_dtype = nullable_integer.get(dtype)
            if pandas_dtype:
                frame[name] = pd.array(series.to_list(), dtype=pandas_dtype)
            elif dtype == pl.Boolean:
                frame[name] = pd.array(series.to_list(), dtype="boolean")
        return frame

    @staticmethod
    def _write_profile(path: str, profile: IslandProfile) -> None:
        try:
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(profile.as_dict(), handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, target)
        except Exception as exc:  # profiling must never break a read
            logger.debug("[islanddb] profile write skipped: %s", exc)

    # ------------------------------------------------------------------
    # Public execution API
    # ------------------------------------------------------------------

    def _prepare_lazy_query(
        self,
        reflection: Reflection,
        parser,
        timer_capture,
        log_prefix: str,
        *,
        sql_override: Optional[str] = None,
        batch_rows: int = 65_536,
    ) -> Tuple[pl.LazyFrame, exp.Select, str, str]:
        snapshots = self._snapshots(reflection)
        table_defs = parser.get_table_tuples()
        context = pl.SQLContext(eager=False)
        alias_to_physical: Dict[str, str] = {}
        query_rowid_proofs: set[Tuple[object, ...]] = set()
        tombstone_views = {
            str(alias).casefold(): tombstone
            for alias, tombstone in (
                getattr(reflection, "tombstone_views", None) or {}
            ).items()
        }
        for index, td in enumerate(table_defs):
            snapshot = snapshots.get(
                (td.super_name.casefold(), td.simple_name.casefold())
            )
            if snapshot is None:
                raise IslandIntegrityError(
                    f"missing pinned snapshot for {td.super_name}.{td.simple_name}"
                )
            physical = f"island_{index}_{uuid.uuid4().hex}"
            object_metadata: Dict[str, object] = {}
            relation = self._base_relation(
                snapshot,
                batch_rows=batch_rows,
                object_metadata_out=object_metadata,
            )
            tomb_def = tombstone_views.get(str(td.alias).casefold())
            relation = self._apply_tombstone(
                relation,
                snapshot,
                tomb_def,
                object_metadata=object_metadata,
                query_proofs=query_rowid_proofs,
            )
            context.register(physical, relation)
            alias_to_physical[td.alias] = physical

        timer_capture("CREATING_REFLECTION")
        native_sql, original_root = self._rewrite_native_aggregates(
            sql_override or parser.original_query, parser, reflection,
        )
        query = rewrite_query_with_hashed_tables(native_sql, alias_to_physical)
        parser.executing_query = query
        lazy_result = context.execute(query, eager=False)
        if not isinstance(lazy_result, pl.LazyFrame):
            lazy_result = lazy_result.lazy()

        # Normalize the native Arrow stream itself, not only its materialized
        # pandas facade, so streaming and non-streaming callers share one type
        # contract.
        schema = lazy_result.collect_schema()
        casts: List[pl.Expr] = []
        for selected in original_root.expressions:
            core = selected.this if isinstance(selected, exp.Alias) else selected
            name = selected.alias_or_name
            if not name or name not in schema:
                continue
            if isinstance(core, exp.Count):
                casts.append(pl.col(name).cast(pl.Int64))
            elif isinstance(core, exp.Sum) and schema[name].is_integer():
                # Polars widens integer SUM to Int128, but the PyArrow build
                # supported by SuperTable cannot import Polars' private
                # ``_pli128`` C data-interface format. Decimal128(38, 0) is the
                # exact public Arrow representation DuckDB itself uses for a
                # HUGEINT result. The legacy pandas facade converts it to
                # DuckDB fetchdf's float64 only *after* bounded collection;
                # Arrow streaming therefore never loses integers above 2**53.
                casts.append(
                    pl.col(name).cast(pl.Decimal(precision=38, scale=0))
                )
        if casts:
            lazy_result = lazy_result.with_columns(casts)
        optimized_plan = lazy_result.explain(optimized=True)
        logger.debug("%s[islanddb] executing native query: %s", log_prefix, query)
        return lazy_result, original_root, query, optimized_plan

    @staticmethod
    def _lazy_batches(
        lazy_result: pl.LazyFrame,
        *,
        batch_rows: int,
    ) -> Tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        frames = lazy_result.collect_batches(
            chunk_size=max(1, int(batch_rows)),
            maintain_order=True,
            engine="streaming",
        )
        try:
            first_frame = next(frames)
        except StopIteration:
            first_frame = None
        # Polars' logical String maps to Arrow string_view while materialized
        # batches currently expose large_string.  Anchor a non-empty stream to
        # its first physical batch; for an empty stream the logical schema is
        # the only schema available and no batch can violate it.
        schema = (
            first_frame.to_arrow().schema
            if first_frame is not None
            else lazy_result.collect_schema().to_arrow()
        )

        def produce() -> Iterable[pa.RecordBatch]:
            try:
                if first_frame is not None:
                    yield from first_frame.to_arrow().to_batches(
                        max_chunksize=max(1, int(batch_rows)),
                    )
                for frame in frames:
                    yield from frame.to_arrow().to_batches(
                        max_chunksize=max(1, int(batch_rows)),
                    )
            finally:
                close = getattr(frames, "close", None)
                if callable(close):
                    close()

        return schema, produce()

    def _prepare_spilled_stream(
        self,
        reflection: Reflection,
        parser,
        timer_capture,
        log_prefix: str,
        plan: QueryResourcePlan,
        session: SpillSession,
    ) -> Tuple[ArrowBatchStream, str, str]:
        """Compile the sealed direct-column GROUP/ORDER subset to spill ops."""
        root = sqlglot.parse_one(parser.original_query, read="duckdb")
        if (
            not isinstance(root, exp.Select)
            or len(parser.get_table_tuples()) != 1
            or next(root.find_all(exp.Join), None) is not None
        ):
            raise IslandUnsupportedError(
                "bounded native spill currently requires one physical table"
            )

        group = root.args.get("group")
        order = root.args.get("order")
        pre_root = root.copy()
        pre_root.set("order", None)
        aggregate_specs: List[AggregateSpec] = []
        group_names: List[str] = []

        if isinstance(group, exp.Group):
            group_columns = list(group.expressions)
            if not group_columns or not all(
                isinstance(column, exp.Column) for column in group_columns
            ):
                raise IslandUnsupportedError(
                    "spill GROUP BY requires direct columns"
                )
            group_names = [column.name for column in group_columns]
            pre_columns: Dict[str, exp.Column] = {
                column.name: column.copy() for column in group_columns
            }
            seen_group_outputs: set[str] = set()
            for selected in root.expressions:
                core = selected.this if isinstance(selected, exp.Alias) else selected
                output_name = selected.alias_or_name
                if isinstance(core, exp.Column):
                    if core.name not in group_names or output_name != core.name:
                        raise IslandUnsupportedError(
                            "spill GROUP BY requires unaliased grouping columns"
                        )
                    seen_group_outputs.add(core.name)
                    continue
                if not isinstance(core, (exp.Count, exp.Sum, exp.Min, exp.Max)):
                    raise IslandUnsupportedError(
                        "spill GROUP BY projection must be a group column or sealed aggregate"
                    )
                if not isinstance(selected, exp.Alias) or not output_name:
                    raise IslandUnsupportedError(
                        "spill aggregate outputs require explicit aliases"
                    )
                argument = core.this
                if isinstance(core, exp.Count) and isinstance(argument, exp.Star):
                    aggregate_specs.append(AggregateSpec(output_name, "count_star"))
                elif isinstance(argument, exp.Column):
                    pre_columns.setdefault(argument.name, argument.copy())
                    function = {
                        exp.Count: "count", exp.Sum: "sum",
                        exp.Min: "min", exp.Max: "max",
                    }[type(core)]
                    output_type = (
                        pa.decimal128(38, 0)
                        if isinstance(core, exp.Sum) else None
                    )
                    aggregate_specs.append(AggregateSpec(
                        output_name,
                        function,
                        argument.name,
                        output_type,
                    ))
                else:
                    raise IslandUnsupportedError(
                        "spill aggregate arguments must be direct columns"
                    )
            if seen_group_outputs != set(group_names):
                raise IslandUnsupportedError(
                    "every GROUP BY column must be projected by the spill subset"
                )
            pre_root.set("expressions", list(pre_columns.values()))
            pre_root.set("group", None)
        elif next(root.find_all(exp.Count, exp.Sum, exp.Min, exp.Max), None):
            raise IslandUnsupportedError(
                "global aggregate state is bounded in memory and is not a spill shape"
            )

        pre_sql = pre_root.sql(dialect="duckdb")
        lazy_input, _, _, input_plan = self._prepare_lazy_query(
            reflection,
            parser,
            timer_capture,
            log_prefix,
            sql_override=pre_sql,
            batch_rows=plan.batch_rows,
        )
        input_schema, input_batches = self._lazy_batches(
            lazy_input, batch_rows=plan.batch_rows,
        )
        stream: ArrowBatchStream
        if group_names:
            stream = external_group_aggregate(
                input_batches,
                schema=input_schema,
                group_keys=group_names,
                aggregates=aggregate_specs,
                session=session,
                memory_budget_bytes=plan.operator_memory_bytes,
                output_batch_rows=max(1, plan.batch_rows),
            )
        else:
            stream = ArrowBatchStream(input_schema, input_batches)

        if isinstance(order, exp.Order):
            sort_keys: List[Tuple[str, str]] = []
            for ordered in order.expressions:
                target = ordered.this if isinstance(ordered, exp.Ordered) else ordered
                if not isinstance(target, exp.Column):
                    stream.close()
                    raise IslandUnsupportedError(
                        "spill ORDER BY requires direct projected columns"
                    )
                if stream.schema.get_field_index(target.name) < 0:
                    stream.close()
                    raise IslandUnsupportedError(
                        f"spill ORDER BY key {target.name!r} is not projected"
                    )
                direction = (
                    "descending"
                    if isinstance(ordered, exp.Ordered) and ordered.args.get("desc")
                    else "ascending"
                )
                sort_keys.append((target.name, direction))
            try:
                stream = external_sort(
                    stream,
                    schema=stream.schema,
                    sort_keys=sort_keys,
                    session=session,
                    memory_budget_bytes=plan.operator_memory_bytes,
                    output_batch_rows=max(1, plan.batch_rows),
                    null_placement="at_end",
                )
            except BaseException:
                stream.close()
                raise
        desired_names = [selected.alias_or_name for selected in root.expressions]
        if (
            any(not name for name in desired_names)
            or len(set(desired_names)) != len(desired_names)
            or any(stream.schema.get_field_index(name) < 0 for name in desired_names)
        ):
            stream.close()
            raise IslandUnsupportedError(
                "spill output cannot prove the SQL projection order"
            )
        if desired_names != stream.schema.names:
            source_stream = stream
            output_schema = pa.schema([
                source_stream.schema.field(
                    source_stream.schema.get_field_index(name),
                )
                for name in desired_names
            ])

            def reorder_batches():
                try:
                    for batch in source_stream:
                        yield batch.select(desired_names)
                finally:
                    source_stream.close()

            stream = ArrowBatchStream(
                output_schema,
                reorder_batches(),
                cancel_event=source_stream.cancel_event,
            )
        return stream, parser.original_query, input_plan + "\nEXTERNAL SPILL"

    def execute_stream(
        self,
        reflection: Reflection,
        parser,
        query_manager,
        timer_capture,
        log_prefix: str = "",
        engine_config=None,
        cache_metrics=None,
        _defer_reservation_release: bool = False,
    ) -> ArrowBatchStream:
        """Execute natively and yield bounded Arrow batches.

        The resource reservation and any cache/spill leases are held until the
        one-shot stream is exhausted or explicitly closed.  This is the safe
        interface for a result larger than the configured collection budget.
        """
        self.can_execute(reflection, parser).require()
        plan = self.resource_plan(reflection, parser, streaming_result=True)
        if plan.advice in {ExecutionAdvice.ROUTE_DUCKDB, ExecutionAdvice.ROUTE_SPARK}:
            raise IslandUnsupportedError(
                f"bounded IslandDB plan routes to {plan.advice.value}: {plan.reason}"
            )
        query_id = uuid.uuid4().hex
        reservation = self._governor.reserve(
            plan,
            query_id=query_id,
            timeout=float(max(1, settings.DEFAULT_TIMEOUT_SEC)),
        )
        started = time.perf_counter()
        telemetry = _IslandTelemetry()
        range_cache = None
        range_start = None
        session: Optional[SpillSession] = None
        inner_stream: Optional[ArrowBatchStream] = None
        try:
            # Cache construction can touch directories/provider metadata. Keep
            # it inside the reservation/telemetry cleanup boundary so a bad
            # deployment cannot leak a governor slot or sampler thread.
            range_cache = self._get_range_cache()
            range_start = range_cache.metrics() if range_cache is not None else None
            timer_capture("CONNECTING")
            if plan.advice == ExecutionAdvice.ISLAND_SPILL:
                if not settings.SUPERTABLE_ISLAND_SPILL_ENABLED:
                    raise IslandUnsupportedError("IslandDB spill is disabled")
                session = SpillSession(
                    self._spill_root,
                    budget_bytes=plan.spill_budget_bytes,
                    min_free_bytes=self._policy.min_spill_free_bytes,
                    query_id=query_id,
                )
                session.__enter__()
                inner_stream, query, optimized_plan = self._prepare_spilled_stream(
                    reflection,
                    parser,
                    timer_capture,
                    log_prefix,
                    plan,
                    session,
                )
                schema, batches = inner_stream.schema, inner_stream
            else:
                lazy_result, _, query, optimized_plan = self._prepare_lazy_query(
                    reflection,
                    parser,
                    timer_capture,
                    log_prefix,
                    batch_rows=plan.batch_rows,
                )
                schema, batches = self._lazy_batches(
                    lazy_result, batch_rows=plan.batch_rows,
                )
        except IslandSpillError as exc:
            telemetry.finish()
            try:
                if inner_stream is not None:
                    inner_stream.close()
                if session is not None:
                    session.close()
            finally:
                reservation.release()
            raise IslandUnsupportedError(
                f"bounded spill could not be honored: {exc}"
            ) from exc
        except BaseException:
            telemetry.finish()
            try:
                if inner_stream is not None:
                    inner_stream.close()
                if session is not None:
                    session.close()
            finally:
                reservation.release()
            raise

        result_rows = 0
        result_bytes = 0

        def measured_batches():
            nonlocal result_rows, result_bytes
            for batch in batches:
                result_rows += int(batch.num_rows)
                result_bytes += int(batch.nbytes)
                yield batch

        def finish() -> None:
            range_metrics: Dict[str, object] = {}
            if range_cache is not None and range_start is not None:
                current = range_cache.metrics()
                range_metrics = {
                    f"range_{name}": max(
                        0, int(value) - int(getattr(range_start, name, 0)),
                    )
                    for name, value in current.as_dict().items()
                }
            combined_cache = (
                cache_metrics.as_dict()
                if cache_metrics is not None else {}
            )
            combined_cache.update(range_metrics)
            execution_metrics = telemetry.finish()
            scan_complete = bool(
                getattr(reflection, "row_group_scan_bytes_complete", False)
            )
            snapshots = tuple(getattr(reflection, "supers", ()) or ())
            rows_complete = bool(snapshots) and all(
                bool(getattr(snapshot, "candidate_rows_complete", False))
                for snapshot in snapshots
            )
            rows_scanned = (
                sum(
                    max(0, int(getattr(snapshot, "candidate_rows", 0) or 0))
                    for snapshot in snapshots
                )
                if rows_complete else 0
            )
            decoded_complete = bool(
                getattr(reflection, "decoded_bytes_complete", False)
            )
            spill_peak = int(session.peak_used_bytes) if session is not None else 0
            self.last_profile = IslandProfile(
                source_bytes=int(getattr(reflection, "source_bytes", 0) or 0),
                estimated_scan_bytes=int(
                    getattr(reflection, "row_group_scan_bytes", 0)
                    or reflection.reflection_bytes
                ),
                files=int(reflection.total_reflections),
                elapsed_ms=(time.perf_counter() - started) * 1000.0,
                optimized_plan=optimized_plan,
                cache=combined_cache,
                resources={
                    **asdict(plan),
                    "advice": plan.advice.value,
                    "container_cpus": self._resources.cpu_count,
                    "container_memory_bytes": self._resources.memory_limit_bytes,
                },
                spill=(
                    {
                        "triggered": True,
                        "budget_bytes": plan.spill_budget_bytes,
                        "estimated_bytes": plan.estimated_spill_bytes,
                        "directory": str(self._spill_root),
                    }
                    if plan.advice == ExecutionAdvice.ISLAND_SPILL else
                    {"triggered": False}
                ),
                selected_row_groups=self._selected_row_group_count(reflection),
                cpu_time_ms=float(execution_metrics["cpu_time_ms"]),
                logical_scan_bytes=int(
                    getattr(reflection, "row_group_scan_bytes", 0) or 0
                ),
                logical_scan_bytes_complete=scan_complete,
                physical_read_bytes=int(execution_metrics["physical_read_bytes"]),
                physical_read_bytes_measured=bool(
                    execution_metrics["physical_read_bytes_measured"]
                ),
                decoded_bytes=int(getattr(reflection, "decoded_bytes", 0) or 0),
                decoded_bytes_complete=decoded_complete,
                rows_scanned=rows_scanned,
                rows_scanned_measured=rows_complete,
                result_rows=result_rows,
                result_bytes=result_bytes,
                peak_memory_bytes=int(execution_metrics["peak_memory_bytes"]),
                peak_memory_scope=str(execution_metrics["peak_memory_scope"]),
                spill_bytes=spill_peak,
                spill_bytes_measured=session is not None,
            )
            try:
                self._write_profile(query_manager.query_plan_path, self.last_profile)
                if session is not None:
                    session.close()
            finally:
                if not _defer_reservation_release:
                    reservation.release()

        stream = ArrowBatchStream(
            schema,
            measured_batches(),
            close_callback=finish,
        )
        if _defer_reservation_release:
            # Private handoff used only by the pandas facade so Arrow -> Polars
            # -> pandas conversion remains inside the same governor reservation.
            stream._island_release_reservation = reservation.release
        return stream

    def execute(
        self,
        reflection: Reflection,
        parser,
        query_manager,
        timer_capture,
        log_prefix: str = "",
        engine_config=None,
        cache_metrics=None,
    ) -> pd.DataFrame:
        plan = self.resource_plan(reflection, parser, streaming_result=False)
        if plan.advice == ExecutionAdvice.STREAM_RESULT:
            raise ResultMemoryLimitExceeded(
                f"{plan.reason}; call IslandDB.execute_stream()"
            )
        stream = self.execute_stream(
            reflection=reflection,
            parser=parser,
            query_manager=query_manager,
            timer_capture=timer_capture,
            log_prefix=log_prefix,
            engine_config=engine_config,
            cache_metrics=cache_metrics,
            _defer_reservation_release=True,
        )
        release = getattr(stream, "_island_release_reservation", lambda: None)
        try:
            with stream:
                table = stream.collect_table(max_bytes=plan.result_memory_bytes)
            result = pl.from_arrow(table)
            root = sqlglot.parse_one(parser.original_query, read="duckdb")
            if isinstance(root, exp.Select):
                result = self._normalize_aggregate_dtypes(result, root)
            return self._to_duckdb_pandas(result)
        finally:
            release()


__all__ = [
    "IslandDB", "IslandCapability", "IslandProfile",
    "IslandUnsupportedError", "IslandIntegrityError", "ArrowBatchStream",
]
