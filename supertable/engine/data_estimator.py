# supertable/engine/data_estimator.py

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from typing import Any, cast, Iterable, Set, List, Dict, Optional, Tuple
from urllib.parse import urlparse

import polars

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.data_classes import (
    Reflection,
    ResourceObjectSeal,
    ResourceStatsSeal,
    SuperSnapshot,
    RowGroupSelection,
)
from supertable.super_table import SuperTable
from supertable.utils.helper import dict_keys_to_lowercase
from supertable.utils.snapshot import (
    combined_share_row_filter,
    complete_snapshot_payload,
)
from supertable.engine.plan_stats import PlanStats
from supertable.utils.timer import Timer
from supertable.utils.profiler import Profiler
from supertable.utils.diagnostic_redaction import (
    local_path_metadata,
    safe_exception_type,
    safe_storage_path_for_diagnostic,
)
from supertable.redis_catalog import (
    RedisCatalog,
    _LINKED_TABLE_INDEX_DOMAIN,
)  # Redis leaf pointers for snapshots

from supertable.data_classes import JoinEdge
from supertable.utils.sql_parser import TableDefinition
from supertable.engine.join_pruner import prune_files_across_joins
from supertable.processing import (
    integer_domains_from_complete_stats,
    load_bounded_stats_for_planning,
    load_stats,
    prune_files_by_predicates,
    resource_object_seal,
    select_row_groups_by_predicates,
    resource_stats_seal,
    stats_for_complete_files,
    stats_cache_identity,
    ROWID_COL,
    TIMESTAMP_COL,
)
from supertable.tombstone_manifest_v2 import (
    normalize_snapshot_tombstone_state,
)
from supertable.row_identity import (
    ResourceRowIdIntegritySeal,
    resource_rowid_integrity_seal,
    snapshot_proves_stable_rowids,
)


def _safe_path_for_log(value: object) -> str:
    """Return non-secret local metadata or a redacted remote authority."""

    try:
        text = str(value or "")
    except Exception:
        return "<path-unavailable>"
    if "://" in text:
        return safe_storage_path_for_diagnostic(text)
    return local_path_metadata(text)


def _trusted_storage_type(storage: object) -> str:
    """Classify shipped storage adapters without reflecting custom class text."""

    known_adapters = (
        ("supertable.storage.local_storage", "LocalStorage", "local"),
        ("supertable.storage.s3_storage", "S3Storage", "s3"),
        ("supertable.storage.minio_storage", "MinioStorage", "minio"),
        ("supertable.storage.azure_storage", "AzureBlobStorage", "azure"),
        ("supertable.storage.gcp_storage", "GCSStorage", "gcp"),
    )
    for module_name, class_name, label in known_adapters:
        module = sys.modules.get(module_name)
        adapter_type = vars(module).get(class_name) if module is not None else None
        if isinstance(adapter_type, type) and isinstance(storage, adapter_type):
            return label
    return "custom"


def _linked_share_policy_state(
    *documents: object,
    schema: Dict[str, str],
) -> Tuple[Optional[str], Optional[List[str]]]:
    """Pin linked-share provenance and column policy without credentials.

    The row predicate is deliberately handled separately: DataReader parses it
    into canonical DuckDB SQL before constructing the final effective-policy
    fingerprint. This seal covers every other share authorization component
    that is present in the immutable/cached snapshot payload.
    """
    max_name_bytes = 1024
    max_type_bytes = 64 * 1024
    max_columns = 16 * 1024
    max_total_text_bytes = 4 * 1024 * 1024
    total_text_bytes = 0

    def bounded_text(value: object, *, label: str, max_bytes: int) -> str:
        nonlocal total_text_bytes
        if not isinstance(value, str) or not value.strip() or "\x00" in value:
            raise RuntimeError(f"{label} is invalid")
        # Reject obviously oversized Unicode before allocating its UTF-8 form.
        if len(value) > max_bytes:
            raise RuntimeError(f"{label} exceeds the size limit")
        encoded = value.encode("utf-8")
        if len(encoded) > max_bytes:
            raise RuntimeError(f"{label} exceeds the size limit")
        total_text_bytes += len(encoded)
        if total_text_bytes > max_total_text_bytes:
            raise RuntimeError("Linked-share policy metadata exceeds the size limit")
        return value

    candidates: List[dict] = []
    seen_candidates = set()
    for document in documents:
        if not isinstance(document, dict):
            continue
        pending = [document]
        for wrapper_name in ("payload", "data", "snapshot"):
            wrapped = document.get(wrapper_name)
            if isinstance(wrapped, dict):
                pending.append(wrapped)
                nested = wrapped.get("snapshot")
                if isinstance(nested, dict):
                    pending.append(nested)
        for candidate in pending:
            marker = id(candidate)
            if marker not in seen_candidates:
                seen_candidates.add(marker)
                candidates.append(candidate)

    link_values = []
    provider_values = []
    allowed_policies: List[Tuple[str, ...]] = []
    policy_marker_present = False
    for candidate in candidates:
        if "_linked_share" in candidate:
            policy_marker_present = True
            raw_link = bounded_text(
                candidate.get("_linked_share"),
                label="Linked-share identity metadata",
                max_bytes=max_name_bytes,
            )
            if raw_link not in link_values:
                link_values.append(raw_link)
        if "_provider_org" in candidate:
            policy_marker_present = True
            raw_provider = bounded_text(
                candidate.get("_provider_org"),
                label="Linked-share provider metadata",
                max_bytes=max_name_bytes,
            )
            if raw_provider not in provider_values:
                provider_values.append(raw_provider)
        if "_allowed_columns" in candidate:
            policy_marker_present = True
            raw_allowed = candidate.get("_allowed_columns")
            if (
                not isinstance(raw_allowed, list)
                or not raw_allowed
                or len(raw_allowed) > max_columns
            ):
                raise RuntimeError("Linked-share column policy is invalid")
            folded: Dict[str, str] = {}
            for raw_column in raw_allowed:
                raw_column = bounded_text(
                    raw_column,
                    label="Linked-share column policy",
                    max_bytes=max_name_bytes,
                )
                key = raw_column.casefold()
                if key in folded:
                    raise RuntimeError("Linked-share column policy is ambiguous")
                folded[key] = raw_column
            normalized = tuple(sorted(folded))
            if "*" in normalized and normalized != ("*",):
                raise RuntimeError("Linked-share column policy is invalid")
            if normalized not in allowed_policies:
                allowed_policies.append(normalized)

    if not link_values:
        if policy_marker_present:
            raise RuntimeError("Linked-share policy has no authoritative identity")
        return None, None
    if (
        len(link_values) != 1
        or len(provider_values) != 1
        or len(allowed_policies) > 1
    ):
        raise RuntimeError("Conflicting linked-share policy metadata")

    if len(schema) > max_columns:
        raise RuntimeError("Linked-share schema projection exceeds the size limit")
    schema_projection: Dict[str, str] = {}
    for raw_name, raw_type in schema.items():
        name = bounded_text(
            raw_name,
            label="Linked-share schema column",
            max_bytes=max_name_bytes,
        )
        type_text = bounded_text(
            raw_type,
            label="Linked-share schema type",
            max_bytes=max_type_bytes,
        )
        schema_folded = name.casefold()
        if schema_folded in schema_projection:
            raise RuntimeError("Linked-share schema projection is ambiguous")
        schema_projection[schema_folded] = type_text
    if not schema_projection:
        raise RuntimeError("Linked-share schema projection is empty")

    # Every linked leaf must carry the publisher's explicit column policy.
    # Treating an absent policy as ``*`` would reopen malformed/legacy payloads
    # to every column present in their projected schema.
    if not allowed_policies:
        raise RuntimeError("Linked-share column policy is unavailable")
    explicit_allowed = allowed_policies[0]
    if explicit_allowed == ("*",):
        effective_allowed = sorted(schema_projection)
    else:
        missing = set(explicit_allowed).difference(schema_projection)
        if missing:
            raise RuntimeError(
                "Linked-share column policy is not represented by its schema"
            )
        effective_allowed = list(explicit_allowed)

    identity = {
        "version": 1,
        "link_id": link_values[0],
        "provider_org": provider_values[0],
        "allowed_columns": list(explicit_allowed),
        "schema_projection": [
            {"name": name, "type": schema_projection[name]}
            for name in sorted(schema_projection)
        ],
    }
    fingerprint = hashlib.sha256(json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")).hexdigest()
    return fingerprint, effective_allowed


_SHARE_CACHE_ID_RE = re.compile(r"share-cache-v1:[0-9a-f]{64}\Z")
_LINKED_MANIFEST_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")
_MAX_SAFE_LINKED_INTEGER = (1 << 53) - 1
_MAX_LINKED_AUTHORITIES_PER_ESTIMATE = 256


def _linked_table_names_digest(names: Iterable[str]) -> str:
    digest = hashlib.sha256(_LINKED_TABLE_INDEX_DOMAIN)
    for name in sorted(names):
        encoded = name.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _linked_share_authority_fields(
    *documents: object,
) -> Optional[Tuple[str, int, int, str]]:
    """Extract one unambiguous linked publication identity from wrappers."""
    link_ids: Set[str] = set()
    local_generations: Set[int] = set()
    provider_generations: Set[int] = set()
    manifest_digests: Set[str] = set()
    authority_marker_present = False
    seen = set()
    pending = [document for document in documents if isinstance(document, dict)]
    while pending:
        candidate = pending.pop()
        marker = id(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        if "_linked_share" in candidate:
            authority_marker_present = True
            value = candidate.get("_linked_share")
            if (
                not isinstance(value, str)
                or not value
                or len(value.encode("utf-8")) > 1024
                or "\x00" in value
            ):
                raise RuntimeError("Linked-share identity metadata is invalid")
            link_ids.add(value)
        for field_name, destination in (
            ("_linked_generation", local_generations),
            ("_linked_provider_generated_ms", provider_generations),
        ):
            if field_name in candidate:
                authority_marker_present = True
                value = candidate.get(field_name)
                if (
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or value <= 0
                    or value > _MAX_SAFE_LINKED_INTEGER
                ):
                    raise RuntimeError(
                        "Linked-share publication authority is invalid"
                    )
                destination.add(value)
        if "_linked_provider_manifest_digest" in candidate:
            authority_marker_present = True
            value = candidate.get("_linked_provider_manifest_digest")
            if (
                not isinstance(value, str)
                or _LINKED_MANIFEST_DIGEST_RE.fullmatch(value) is None
            ):
                raise RuntimeError(
                    "Linked-share manifest authority is invalid"
                )
            manifest_digests.add(value)
        for wrapper_name in ("payload", "data", "snapshot"):
            wrapped = candidate.get(wrapper_name)
            if isinstance(wrapped, dict):
                pending.append(wrapped)
    if not authority_marker_present:
        return None
    if (
        len(link_ids) != 1
        or len(local_generations) != 1
        or len(provider_generations) != 1
        or len(manifest_digests) != 1
    ):
        raise RuntimeError("Linked-share publication authority is ambiguous")
    return (
        next(iter(link_ids)),
        next(iter(local_generations)),
        next(iter(provider_generations)),
        next(iter(manifest_digests)),
    )


def _linked_share_credential_expiry(
    *documents: object,
    linked: bool,
) -> Optional[int]:
    """Pin one unambiguous provider credential expiry from leaf wrappers."""
    values = set()
    marker_present = False
    seen = set()
    pending = [document for document in documents if isinstance(document, dict)]
    while pending:
        candidate = pending.pop()
        marker = id(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        if "_credential_expires_ms" in candidate:
            marker_present = True
            value = candidate.get("_credential_expires_ms")
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
            ):
                raise RuntimeError("Linked-share credential expiry is invalid")
            values.add(value)
        for wrapper_name in ("payload", "data", "snapshot"):
            wrapped = candidate.get(wrapper_name)
            if isinstance(wrapped, dict):
                pending.append(wrapped)
    if not linked:
        if marker_present:
            raise RuntimeError(
                "Credential expiry metadata has no linked-share identity"
            )
        return None
    if len(values) != 1:
        raise RuntimeError("Linked-share credential expiry is unavailable")
    return next(iter(values))


def _linked_share_publication_generation(
    *documents: object,
    linked: bool,
) -> Optional[int]:
    """Pin one unambiguous provider manifest order from leaf wrappers."""
    values = set()
    marker_present = False
    seen = set()
    pending = [document for document in documents if isinstance(document, dict)]
    while pending:
        candidate = pending.pop()
        marker = id(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        if "_linked_provider_generated_ms" in candidate:
            marker_present = True
            value = candidate.get("_linked_provider_generated_ms")
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
            ):
                raise RuntimeError(
                    "Linked-share publication generation is invalid"
                )
            values.add(value)
        for wrapper_name in ("payload", "data", "snapshot"):
            wrapped = candidate.get(wrapper_name)
            if isinstance(wrapped, dict):
                pending.append(wrapped)
    if not linked:
        if marker_present:
            raise RuntimeError(
                "Publication generation metadata has no linked-share identity"
            )
        return None
    if len(values) != 1:
        raise RuntimeError(
            "Linked-share publication generation is unavailable"
        )
    return next(iter(values))


def _linked_resource_cache_identity(resource: object, *, linked: bool) -> Optional[str]:
    if not isinstance(resource, dict):
        if linked:
            raise RuntimeError("Linked-share resource metadata is invalid")
        return None
    value = resource.get("_cache_identity")
    if not linked:
        return None
    if (
        not isinstance(value, str)
        or _SHARE_CACHE_ID_RE.fullmatch(value) is None
    ):
        raise RuntimeError("Linked-share resource cache identity is invalid")
    return value


def get_missing_columns(
        tables: List[TableDefinition],
        selected: List[SuperSnapshot],
) -> List[Tuple[str, str, Set[str]]]:
    """
    Returns list of (super_name, simple_name, missing_columns),
    but only for tables where at least one requested column is missing.

    Semantics (updated):

      - `tables` (TableDefinition):
          * Represents what the query requests (from SQLParser).
          * Match key: (super_name, simple_name), case-insensitive.
          * columns == []  => SELECT * / t.*  => all columns requested,
                                but we DO NOT validate -> skip missing-check.
          * columns != [] => explicit requested columns that MUST exist.

      - `selected` (SuperSnapshot):
          * Represents what is actually available for that table/version.
          * columns: Set[str] of available columns.
          * Multiple snapshots for same table:
                union their columns.

      - Missing logic:
          * If TableDefinition.columns == []:
                - No validation (treated as "don't check SELECT *").
          * Else:
                - If there is no matching SuperSnapshot:
                      all requested columns are missing.
                - If there is a match:
                      missing = requested - available (case-insensitive).
          * Only tables with non-empty missing set are returned.
    """

    # Build availability index from selected snapshots:
    #   (super_name.lower(), simple_name.lower()) -> set(lowercase columns)
    available_index: Dict[Tuple[str, str], Set[str]] = {}

    for s in selected:
        key = (s.super_name.lower(), s.simple_name.lower())
        if key not in available_index:
            available_index[key] = set()
        # s.columns is a Set[str]; guard if it's empty/None
        for c in (s.columns or []):
            available_index[key].add(c.lower())

    results: List[Tuple[str, str, Set[str]]] = []

    # Check each requested table definition
    for t in tables:
        key = (t.super_name.lower(), t.simple_name.lower())

        # [] means SELECT * (or t.*) -> all columns requested,
        # but as per requirement: do NOT validate in this function.
        if not t.columns:
            continue

        requested_lower = {c.lower() for c in t.columns}
        available_lower = available_index.get(key)

        if available_lower is None:
            # No snapshot for this table -> everything requested is missing
            missing_lower = requested_lower
        else:
            # Only columns that are requested but not present
            missing_lower = requested_lower - available_lower

        if missing_lower:
            # Preserve original casing for reporting
            missing_original = {c for c in t.columns if c.lower() in missing_lower}
            if missing_original:
                results.append((t.super_name, t.simple_name, missing_original))

    return results


class DataEstimator:
    """
    Estimates which files will be read for a query and validates read access.
    Returns:
      {
        "STORAGE_TYPE": "<storage backend class name or identifier>",
        "BYTES_AFFECTED": <int>",
        "FILE_LIST": [<resolved_url_or_path>, ...]
      }
    """

    def __init__(
        self,
        organization: str,
        storage,
        tables: List[TableDefinition],
        predicate_constraints: Optional[Dict] = None,
        join_edges: Optional[List[JoinEdge]] = None,
        plan_stats: Optional[PlanStats] = None,
        join_pruning_lanes: Optional[Set[str]] = None,
        aggregate_children: Optional[
            Dict[Tuple[str, str], Iterable[str]]
        ] = None,
        require_odata_identity: bool = False,
        require_bounded_resource_estimates: bool = False,
    ):
        self.organization = organization
        self.storage = storage
        self.tables = tables
        # (super_lower, simple_lower) -> List[Dict[col, PredInterval]] from the
        # query's WHERE clauses; used to prune files by the stats artifact.
        # None / empty ⇒ no read-path pruning.
        self.predicate_constraints = predicate_constraints or {}
        # Equi-join links between the query's tables.  After each table is
        # pruned by its own WHERE, a filtered table's surviving join-key range
        # is propagated across these edges to prune its partners (cross-table
        # file pruning).  Empty ⇒ no propagation (single-table or un-joined).
        self.join_edges: List[JoinEdge] = join_edges or []
        # Optional executor-specific whitelist passed to the join kernel.
        # Disabled lanes become unknown and therefore retain files.  ``None``
        # preserves the kernel's full safe-lane set for standalone callers.
        self.join_pruning_lanes = join_pruning_lanes
        if type(require_odata_identity) is not bool:
            raise TypeError("require_odata_identity must be a bool")
        self.require_odata_identity = require_odata_identity
        if type(require_bounded_resource_estimates) is not bool:
            raise TypeError(
                "require_bounded_resource_estimates must be a bool"
            )
        self.require_bounded_resource_estimates = (
            require_bounded_resource_estimates
        )
        # Aggregate relations (``super_name == simple_name``) are resolved and
        # authorized by DataReader before estimation.  Pin that exact child set
        # here so a table created between authorization and Redis SCAN cannot be
        # pulled into the union without a policy decision.
        self.aggregate_children: Dict[Tuple[str, str], Set[str]] = {
            (str(key[0]).casefold(), str(key[1]).casefold()): {
                str(child).casefold() for child in children
            }
            for key, children in (aggregate_children or {}).items()
        }
        self.timer: Optional[Timer] = None
        # When the caller (DataReader) injects its PlanStats, estimator stats —
        # REFLECTIONS, REFLECTION_SIZE and the read-pruning counters — land on
        # the same object that flows to extend_execution_plan, so they reach the
        # read monitoring payload. Standalone callers get a fresh PlanStats.
        self.plan_stats: Optional[PlanStats] = plan_stats
        self.catalog = RedisCatalog()
        # Lazily populated only when storage helpers cannot resolve keys.  These
        # settings/storage attributes are invariant for one estimator run; do
        # not rediscover them for every file in a large scan.
        self._fallback_url_config: Optional[Tuple[Optional[str], Optional[str], bool, bool]] = None

    def _schema_to_dict(self, schema_obj) -> Dict[str, str]:
        """Normalize schema representations into a {name: type} dict."""
        if isinstance(schema_obj, dict):
            return schema_obj
        if isinstance(schema_obj, list):
            out: Dict[str, str] = {}
            for item in schema_obj:
                if isinstance(item, dict):
                    name = item.get("name")
                    if name is not None:
                        out[str(name)] = str(item.get("type", ""))
            return out
        return {}

    # ----------------------- storage helpers (matching original) -----------------------

    def _get_env(self, *names: str) -> Optional[str]:
        """Deprecated: retained for backward compatibility. Prefer os.getenv('STORAGE_*') directly."""
        for n in names:
            v = os.getenv(n)
            if v:
                return v
        return None

    def _storage_attr(self, *names: str) -> Optional[str]:
        for n in names:
            if hasattr(self.storage, n):
                v = getattr(self.storage, n)
                if v not in (None, "", False):
                    return str(v)
        return None

    def _normalize_endpoint_for_s3(self, ep: str) -> str:
        if not ep:
            return ep
        u = urlparse(ep if "://" in ep else f"//{ep}")
        host = u.hostname or ep
        port = f":{u.port}" if u.port else ("" if ":" in ep else "")
        return f"{host}{port}"

    def _detect_endpoint(self) -> Optional[str]:
        # 1) storage object attributes (e.g. endpoint_url, endpoint)
        candidates = [
            "endpoint_url", "endpoint", "url", "api_url", "base_url",
            "s3_endpoint", "minio_endpoint", "public_endpoint",
        ]
        for name in candidates:
            val = self._storage_attr(name)
            if val:
                logger.debug(
                    f"[estimate.env] storage.{name}="
                    f"'{_safe_path_for_log(val)}'"
                )
                return self._normalize_endpoint_for_s3(val)

        host = self._storage_attr("host", "hostname")
        port = self._storage_attr("port")
        if host:
            composed = f"{host}{':' + port if port else ''}"
            return self._normalize_endpoint_for_s3(composed)

        # 2) Environment variable
        if settings.STORAGE_ENDPOINT_URL:
            return self._normalize_endpoint_for_s3(settings.STORAGE_ENDPOINT_URL)

        return None

    def _detect_bucket(self) -> Optional[str]:
        for name in ("bucket", "bucket_name", "default_bucket"):
            v = self._storage_attr(name)
            if v:
                return v
        return settings.STORAGE_BUCKET or None

    def _detect_ssl(self) -> bool:
        val = (
                (str(getattr(self.storage, "secure", "")).lower() if hasattr(self.storage, "secure") else "")
                or str(settings.STORAGE_USE_SSL)
        ).lower()
        return val in ("1", "true", "yes", "on")

    def _to_duckdb_path(self, key: str) -> str:
        return self._to_duckdb_path_with_credential_generation(key)[0]

    def _to_duckdb_path_with_credential_generation(
        self, key: str,
    ) -> Tuple[str, Optional[int]]:
        """
        Resolve a stable storage key without minting a bearer credential.

        Estimation runs before an execution engine is selected and before the
        engine owns the request deadline.  Presigning here would therefore do
        unnecessary work for IslandDB/Spark and could block beyond the query
        deadline.  The selected DuckDB executor performs the sole bounded,
        deadline-aware presign immediately before engine setup when requested.
        """
        if not key:
            return key, None

        # Existing URLs may be provider-issued linked-share bearer paths. A
        # consumer storage adapter has no authority to renew them and must
        # never receive them as object keys, even when explicit presigning is
        # enabled for locally owned resources.
        if "://" in key:
            logger.debug(f"[estimate.resolve] already URL: {_safe_path_for_log(key)}")
            return key, None

        # Storage helpers return canonical, non-expiring paths.  A custom
        # helper that returns a bearer URL remains supported, but DuckDB will
        # replace consumer-owned bearer paths at its bounded setup boundary.
        for attr in (
            "to_duckdb_path",
            "make_duckdb_url",
            "make_url",
            "canonical_uri",
        ):
            fn = getattr(self.storage, attr, None)
            if callable(fn):
                try:
                    url = fn(key)  # key in, URL out (not presigned)
                    if isinstance(url, str) and url:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"[estimate.resolve] storage.{attr} → "
                                f"{_safe_path_for_log(url)}"
                            )
                        return url, None
                except Exception as e:
                    logger.debug(
                        f"[estimate.resolve] storage.{attr} failed; "
                        f"error_type={safe_exception_type(e)}"
                    )

        # 3) Construct URL from endpoint/bucket
        fallback = getattr(self, "_fallback_url_config", None)
        if fallback is None:
            fallback = (
                self._detect_endpoint(),
                self._detect_bucket(),
                settings.SUPERTABLE_DUCKDB_USE_HTTPFS,
                self._detect_ssl(),
            )
            self._fallback_url_config = fallback
        endpoint_raw, bucket, use_http, use_ssl = fallback
        scheme = "https" if use_ssl else "http"
        key_norm = key.lstrip("/")

        if endpoint_raw and bucket:
            if use_http:
                return (
                    f"{scheme}://{endpoint_raw.rstrip('/')}/{bucket}/{key_norm}",
                    None,
                )
            else:
                return f"s3://{bucket}/{key_norm}", None

        # 4) Fallback
        return key, None

    # ----------------------- snapshot discovery & filtering -----------------------

    def _authoritative_linked_control(
        self,
        organization: str,
        super_name: str,
        link_id: str,
    ) -> Dict[str, object]:
        """Return one normalized, atomically indexed authority record.

        Reflection construction may encounter thousands of leaves from a small
        number of shares. Resolve each distinct link exactly once rather than
        enumerating and decoding every cached manifest in the namespace.
        """
        cache = getattr(self, "_linked_authority_cache", None)
        if cache is None:
            cache = {}
            self._linked_authority_cache = cache
        cache_key = (
            str(organization),
            str(super_name),
            link_id,
        )
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        if len(cache) >= _MAX_LINKED_AUTHORITIES_PER_ESTIMATE:
            raise RuntimeError(
                "Linked-share authority lookup exceeds its safety limit"
            )
        try:
            control = self.catalog.get_authoritative_linked_share(
                organization, super_name, link_id,
            )
        except FileNotFoundError:
            raise RuntimeError(
                "Linked-share leaf has no authoritative control"
            ) from None
        if not isinstance(control, dict):
            raise RuntimeError("Linked-share leaf has no authoritative control")
        if control.get("link_id") != link_id:
            raise RuntimeError("Linked-share control metadata is invalid")

        local_generation = control.get("publication_generation")
        provider_generation = control.get("_linked_provider_generated_ms")
        manifest_digest = control.get("_linked_provider_manifest_digest")
        if (
            not isinstance(local_generation, int)
            or isinstance(local_generation, bool)
            or local_generation <= 0
            or local_generation > _MAX_SAFE_LINKED_INTEGER
            or not isinstance(provider_generation, int)
            or isinstance(provider_generation, bool)
            or provider_generation <= 0
            or provider_generation > _MAX_SAFE_LINKED_INTEGER
            or not isinstance(manifest_digest, str)
            or _LINKED_MANIFEST_DIGEST_RE.fullmatch(manifest_digest) is None
        ):
            raise RuntimeError("Linked-share control metadata is invalid")

        alias_prefix = control.get("alias_prefix", "")
        manifest = control.get("cached_manifest")
        if (
            not isinstance(alias_prefix, str)
            or len(alias_prefix.encode("utf-8")) > 1024
            or "\x00" in alias_prefix
            or not isinstance(manifest, dict)
        ):
            raise RuntimeError("Linked-share control metadata is invalid")
        tables = manifest.get("tables")
        if not isinstance(tables, list) or len(tables) > 10_000:
            raise RuntimeError("Linked-share control metadata is invalid")
        expected_names: Set[str] = set()
        for table in tables:
            provider_name = table.get("table") if isinstance(table, dict) else None
            if (
                not isinstance(provider_name, str)
                or not provider_name
                or len(provider_name.encode("utf-8")) > 1024
                or "\x00" in provider_name
            ):
                raise RuntimeError("Linked-share control metadata is invalid")
            folded = f"{alias_prefix}{provider_name}".casefold()
            if folded in expected_names:
                raise RuntimeError("Linked-share control metadata is ambiguous")
            expected_names.add(folded)

        normalized: Dict[str, object] = {
            "publication_generation": local_generation,
            "provider_generated_ms": provider_generation,
            "manifest_digest": manifest_digest,
            "table_names": frozenset(expected_names),
            "table_count": len(expected_names),
            "table_names_digest": _linked_table_names_digest(expected_names),
        }
        cache[cache_key] = normalized
        return normalized

    def _validate_linked_snapshot_authority(
        self,
        organization: str,
        super_name: str,
        simple_name: str,
        *documents: object,
    ) -> None:
        """Validate a linked leaf against its indexed control at acquisition.

        This check deliberately lives inside Reflection construction. MCP may
        validate the same state before database admission, but a partial share
        refresh can race between that check and this Redis snapshot scan.
        """
        authority = _linked_share_authority_fields(*documents)
        if authority is None:
            return
        link_id, local_generation, provider_generation, manifest_digest = authority
        control = self._authoritative_linked_control(
            organization, super_name, link_id,
        )
        if (
            control["publication_generation"] != local_generation
            or control["provider_generated_ms"] != provider_generation
            or control["manifest_digest"] != manifest_digest
        ):
            raise RuntimeError(
                "Linked-share leaf does not match its authoritative control"
            )
        table_names = control.get("table_names")
        if (
            table_names is not None
            and (
                not isinstance(table_names, (set, frozenset))
                or str(simple_name).casefold() not in table_names
            )
        ):
            raise RuntimeError(
                "Linked-share leaf is outside its authoritative manifest"
            )

    def _collect_snapshots_from_redis(self, organization, super_name) -> List[Dict]:
        # Keep one root-generation fence open through SCAN, compact authority
        # acquisition, and exact completeness validation. Linked leaf/control
        # Lua mutations advance that same root, so a concurrent publication is
        # either wholly before/after this view or rejected for retry.
        authority_cache = getattr(self, "_linked_authority_cache", None)
        if authority_cache is None:
            authority_cache = {}
            self._linked_authority_cache = authority_cache
        pin_snapshot = getattr(self.catalog, "pin_leaf_authority_snapshot", None)
        pin = (
            pin_snapshot(organization, super_name)
            if callable(pin_snapshot) else None
        )
        items = list(
            self.catalog.scan_leaf_items(organization, super_name, count=512)
        )
        seen_names: Dict[str, Set[str]] = defaultdict(set)
        seen_authority: Dict[str, Tuple[int, int, str]] = {}
        for item in items:
            authority = _linked_share_authority_fields(item)
            if authority is None:
                continue
            link_id, local_generation, provider_generation, manifest_digest = authority
            simple_name = str(item.get("simple") or "")
            if not simple_name:
                raise RuntimeError("Linked-share leaf name is invalid")
            identity = (local_generation, provider_generation, manifest_digest)
            prior = seen_authority.setdefault(link_id, identity)
            if prior != identity:
                raise RuntimeError(
                    "Linked-share leaves span multiple authority generations"
                )
            folded = simple_name.casefold()
            if folded in seen_names[link_id]:
                raise RuntimeError("Linked-share leaf names are ambiguous")
            seen_names[link_id].add(folded)

        list_indexes = getattr(
            self.catalog, "list_linked_share_table_indexes", None,
        )
        indexes = (
            list_indexes(
                organization,
                super_name,
                limit=_MAX_LINKED_AUTHORITIES_PER_ESTIMATE,
            )
            if callable(list_indexes)
            else {link_id: None for link_id in seen_names}
        )
        for link_id, compact in indexes.items():
            cache_key = (str(organization), str(super_name), link_id)
            if compact is None:
                control = self._authoritative_linked_control(
                    organization, super_name, link_id,
                )
            else:
                control = {
                    "publication_generation": compact["publication_generation"],
                    "provider_generated_ms": compact["provider_generated_ms"],
                    "manifest_digest": compact["manifest_digest"],
                    "table_names": None,
                    "table_count": compact["table_count"],
                    "table_names_digest": compact["table_names_digest"],
                }
                authority_cache[cache_key] = control
            observed_identity = seen_authority.get(link_id)
            expected_identity = (
                control["publication_generation"],
                control["provider_generated_ms"],
                control["manifest_digest"],
            )
            if (
                observed_identity is not None
                and observed_identity != expected_identity
            ):
                raise RuntimeError(
                    "Linked-share leaves do not match authoritative control"
                )
            observed_names = seen_names.get(link_id, set())
            expected_names = control.get("table_names")
            if expected_names is not None:
                complete = observed_names == expected_names
            else:
                complete = (
                    len(observed_names) == control["table_count"]
                    and _linked_table_names_digest(observed_names)
                    == control["table_names_digest"]
                )
            if not complete:
                raise RuntimeError(
                    "Linked-share leaf set is incomplete or non-authoritative"
                )
        unknown_links = set(seen_names).difference(indexes)
        if unknown_links:
            raise RuntimeError("Linked-share leaf has no authoritative control")
        verify_snapshot = getattr(
            self.catalog, "verify_leaf_authority_snapshot", None,
        )
        if pin is not None and callable(verify_snapshot):
            verify_snapshot(organization, super_name, pin)
        snapshots = []
        for it in items:
            if not it.get("path"):
                continue
            snapshots.append(
                {
                    "table_name": it["simple"],
                    "last_updated_ms": int(it.get("ts", 0)),
                    "path": it["path"],
                    "version": it['version'],
                    "payload": it.get("payload"),
                    "_row_filter": it.get("_row_filter"),
                }
            )
        return snapshots

    def _filter_snapshots(self, super_name, simple_name, snapshots: List[Dict]) -> List[Dict]:
        if super_name.lower() == simple_name.lower():
            authorized = self.aggregate_children.get(
                (str(super_name).casefold(), str(simple_name).casefold())
            )
            # Standalone DataEstimator callers retain the historical aggregate
            # behavior. DataReader always supplies a pinned set after RBAC.
            return [
                s for s in snapshots
                if not (
                    s["table_name"].startswith("__")
                    and s["table_name"].endswith("__")
                )
                and (
                    authorized is None
                    or str(s["table_name"]).casefold() in authorized
                )
            ]
        return [s for s in snapshots if s["table_name"].lower() == simple_name.lower()]

    def _get_supertable_map(self) -> List[Tuple[str, List[str]]]:
        grouped = defaultdict(list)

        for t in self.tables:  # t: TableDefinition
            grouped[t.super_name].append(t.simple_name)

        # optional: sort simple_names per supertable
        return [
            (super_name, sorted(simple_names))
            for super_name, simple_names in grouped.items()
        ]

    def _prune_files(
        self,
        super_name: str,
        simple_name: str,
        raw_keys: List[str],
        stats_df: Optional["polars.DataFrame"],
        profiler: Optional[Profiler] = None,
    ) -> List[str]:
        """Narrow *raw_keys* to those that could satisfy the query predicates.

        Takes the already-loaded *stats_df* (loaded once per table by
        :meth:`estimate` and shared with projection sizing) rather than a path,
        so the artifact is read at most once.  Returns *raw_keys* unchanged
        whenever pruning is disabled, there's no stats artifact, or the query
        carries no usable constraint for this table — and never raises (a
        pruning failure must not break a read).

        *profiler*, when supplied, accumulates the same IO/pruning counters the
        write path emits (``read_pruned_files``) so the read monitoring payload
        can surface them.
        """
        if not settings.SUPERTABLE_READ_PRUNING_ENABLED:
            return raw_keys
        if stats_df is None or not raw_keys:
            return raw_keys
        occurrences = self.predicate_constraints.get(
            (super_name.lower(), simple_name.lower())
        )
        if not self._has_potential_select_predicate(occurrences):
            return raw_keys
        try:
            return prune_files_by_predicates(
                raw_keys,
                stats_df,
                cast(List[Dict[str, Any]], occurrences),
                profiler=profiler,
            )
        except Exception as e:
            logger.warning(
                f"[estimate.prune] pruning skipped for "
                f"{super_name}.{simple_name}; error_type={safe_exception_type(e)}"
            )
            return raw_keys

    @staticmethod
    def _has_potential_select_predicate(occurrences) -> bool:
        """Whether every shared scan occurrence has a usable SELECT lane.

        ``prune_files_by_predicates`` unions the files needed by physical-table
        occurrences.  One empty occurrence, or one occurrence containing only
        comparison lanes SELECT deliberately distrusts (string/double), makes
        file elimination impossible.  Detect that before loading or scanning a
        stats artifact.  This mirrors the processing layer's conservative lane
        gates; returning ``False`` can only skip a guaranteed no-op.
        """
        if not occurrences:
            return False
        try:
            for occurrence in occurrences:
                if not occurrence:
                    return False
                occurrence_can_prune = False
                for predicate in occurrence.values():
                    lane = predicate.lane
                    if lane in ("date", "timestamp", "timestamptz"):
                        occurrence_can_prune = True
                        break
                    if lane in ("numeric", "numeric_cast") and not any(
                        isinstance(bound, float)
                        for bound in (predicate.lo, predicate.hi)
                        if bound is not None
                    ):
                        occurrence_can_prune = True
                        break
                if not occurrence_can_prune:
                    return False
        except Exception:
            return False
        return True

    @staticmethod
    def _validated_file_subset(
        original: Iterable[str], candidate: Iterable[str],
    ) -> Optional[List[str]]:
        """Return the candidate list only when it is a multiset subset.

        File pruning is allowed to remove input occurrences, never to introduce
        or duplicate them.  Treating the lists as multisets also covers corrupt
        snapshots that happen to repeat a resource key.  ``None`` means the
        pruning result violated that contract and must be discarded wholesale.
        """
        try:
            # Every fail-open/no-op path intentionally returns the exact input
            # list.  Avoid allocating two Counters on the overwhelmingly common
            # unfiltered-table path.
            if candidate is original and isinstance(original, list):
                return original
            original_list = list(original)
            candidate_list = list(candidate)
            if Counter(candidate_list) - Counter(original_list):
                return None
        except Exception:
            # File keys are strings in valid snapshots.  Any unhashable or
            # otherwise malformed value is not safe to apply as a pruning plan.
            return None
        return candidate_list

    @staticmethod
    def _stats_for_complete_files(
        stats_df: Optional["polars.DataFrame"],
        resource_rows: Dict[str, Optional[int]],
        resource_seals: Optional[
            Dict[str, Optional[ResourceStatsSeal]]
        ] = None,
        *,
        stats_path: Optional[str] = None,
    ) -> Optional["polars.DataFrame"]:
        """Apply the shared per-resource cardinality/footer/digest boundary."""
        return stats_for_complete_files(
            stats_df,
            resource_rows,
            resource_seals,
            stats_path=stats_path,
        )

    # ----------------------- projection-aware sizing -----------------------

    # Rough per-value byte widths for the type-width *fallback* (used only when
    # per-column ``compressed_bytes`` are unavailable — e.g. a stats file that
    # predates the column, or no stats artifact at all).  Substring-matched
    # against the stored (DuckDB/polars) type name, most-specific first.
    _STRING_AVG_WIDTH = 16

    def _type_width(self, type_name: Optional[str]) -> int:
        """Estimated on-disk bytes-per-value for a column type (fallback only)."""
        t = (type_name or "").strip().lower()
        if not t:
            return 8
        if "bool" in t:
            return 1
        if "timestamp" in t or "datetime" in t:
            return 8
        if "date" in t:
            return 4
        if any(s in t for s in ("varchar", "char", "utf8", "string", "text",
                                "json", "blob", "binary", "bytea")):
            return self._STRING_AVG_WIDTH
        if "double" in t or "float64" in t:
            return 8
        if "float" in t or "real" in t:
            return 4
        if "decimal" in t or "numeric" in t:
            return 8
        if "bigint" in t or "int64" in t or "long" in t:
            return 8
        if "smallint" in t or "int16" in t:
            return 2
        if "tinyint" in t or "int8" in t:
            return 1
        if "int" in t:
            return 4
        return 8

    def _selected_columns(self, super_name: str, simple_name: str) -> Optional[Set[str]]:
        """Lowercased set of user columns this query projects from the table.

        Returns ``None`` to mean "every column" — i.e. ``SELECT *`` / ``t.*``
        (an empty ``TableDefinition.columns``), an unrecognised table, or a
        projection that resolves to only system columns.  In every ``None`` case
        the caller uses the whole-file size (no projection savings), which is
        the safe over-estimate.  System columns requested by user SQL are
        stripped here; Pass 1 adds ``__rowid__`` back when an active composite
        deletion-vector makes it a real physical scan dependency.
        """
        key = (super_name.lower(), simple_name.lower())
        selected: Set[str] = set()
        matched = False
        for t in self.tables:
            if (t.super_name.lower(), t.simple_name.lower()) != key:
                continue
            matched = True
            if not t.columns:          # [] => SELECT * / t.* => whole table
                return None
            for c in t.columns:
                cl = c.lower()
                if cl not in (ROWID_COL, TIMESTAMP_COL):
                    selected.add(cl)
        if not matched or not selected:
            return None
        return selected

    def _projected_bytes_index(
        self,
        stats_df: Optional["polars.DataFrame"],
        selected_cols: Set[str],
        file_keys: Optional[Iterable[str]] = None,
    ) -> Tuple[Set[str], Dict[str, int]]:
        """Sum per-column ``compressed_bytes`` for the selected columns per file.

        Returns ``(tier3_files, proj)`` where *proj* maps a file path to the
        summed on-disk bytes of its selected columns, and *tier3_files* is the
        subset of files for which that sum is *trustworthy* — every matched row
        carried a non-NULL ``compressed_bytes``.  A file with any NULL (an older
        carried-forward row) is omitted so the caller falls back to a whole-file
        ratio rather than under-counting it as zero.  When *file_keys* is given,
        rows for files already removed by predicate/join pruning are discarded
        before aggregation.
        """
        if (
            stats_df is None
            or stats_df.height == 0
            or "compressed_bytes" not in stats_df.columns
        ):
            return set(), {}
        requested_cols = {
            str(column).casefold()
            for column in selected_cols
            if str(column).casefold() not in {
                ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
            }
        }
        if not requested_cols:
            return set(), {}
        projected = stats_df.select(
            ["file_path", "row_group_id", "column_name", "compressed_bytes"]
        )
        if file_keys is not None:
            survivor_keys = list(file_keys)
            if not survivor_keys:
                return set(), {}
            projected = projected.filter(
                polars.col("file_path").is_in(survivor_keys)
            )
        sel = (
            projected
            .with_columns(polars.col("column_name").str.to_lowercase().alias("__cn"))
            .filter(polars.col("__cn").is_in(sorted(requested_cols)))
        )
        if sel.height == 0:
            return set(), {}
        all_group_counts = (
            projected.select(["file_path", "row_group_id"])
            .unique()
            .group_by("file_path")
            .agg(polars.len().alias("__expected_groups"))
        )
        agg = (
            sel.group_by(["file_path", "row_group_id"])
            .agg([
                polars.len().alias("__slots"),
                polars.col("__cn").n_unique().alias("__unique_slots"),
                polars.col("compressed_bytes").sum().alias("__b"),
                (
                    polars.col("compressed_bytes").is_null()
                    | (polars.col("compressed_bytes") < 0)
                ).sum().alias("__invalid"),
            ])
            .group_by("file_path")
            .agg([
                polars.len().alias("__matched_groups"),
                polars.col("__b").sum().alias("__b"),
                polars.col("__invalid").sum().alias("__invalid"),
                (
                    (polars.col("__slots") == len(requested_cols))
                    & (polars.col("__unique_slots") == len(requested_cols))
                ).all().alias("__slots_complete"),
            ])
            .join(all_group_counts, on="file_path", how="inner")
        )
        tier3_files: Set[str] = set()
        proj: Dict[str, int] = {}
        for r in agg.iter_rows(named=True):
            projected_bytes = r["__b"]
            if (
                int(r["__invalid"] or 0) == 0
                and bool(r["__slots_complete"])
                and int(r["__matched_groups"] or 0)
                == int(r["__expected_groups"] or -1)
                and isinstance(projected_bytes, int)
                and not isinstance(projected_bytes, bool)
                and projected_bytes >= 0
            ):
                fp = r["file_path"]
                tier3_files.add(fp)
                proj[fp] = projected_bytes
        return tier3_files, proj

    @staticmethod
    def _row_group_byte_estimate(
        stats_df: Optional["polars.DataFrame"],
        file_keys: Iterable[str],
        selected_cols: Optional[Set[str]],
        selections: Dict[str, RowGroupSelection],
        byte_column: str,
    ) -> Tuple[int, bool]:
        """Sum one footer byte metric over the exact candidate chunks.

        ``selected_cols is None`` means every physical column.  A missing
        selection means every group for that resource.  The result is usable
        only when ``complete`` is true; malformed/legacy stats return a partial
        value with ``False`` and callers must not use it for a routing limit.
        The implementation stays columnar so hundreds of files do not cause a
        Python loop over millions of stats rows.
        """
        keys = list(file_keys)
        if not keys:
            return 0, True
        if (
            stats_df is None
            or not isinstance(stats_df, polars.DataFrame)
            or stats_df.height == 0
            or byte_column not in stats_df.columns
            or not {"file_path", "row_group_id", "column_name"}.issubset(
                stats_df.columns
            )
        ):
            return 0, False
        try:
            scoped = stats_df.filter(polars.col("file_path").is_in(keys))
            present_files = set(
                scoped.get_column("file_path").drop_nulls().unique().to_list()
            )
            if present_files != set(keys):
                return 0, False

            groups = scoped.select(["file_path", "row_group_id"]).unique()
            narrowed_keys = set(selections).intersection(keys)
            all_keys = [key for key in keys if key not in narrowed_keys]
            selected_group_frames: List[polars.DataFrame] = []
            if all_keys:
                selected_group_frames.append(
                    groups.filter(polars.col("file_path").is_in(all_keys))
                )
            if narrowed_keys:
                pairs = [
                    (key, group_id)
                    for key in keys
                    if key in narrowed_keys
                    for group_id in selections[key].selected_ids
                ]
                selected_group_frames.append(polars.DataFrame(
                    pairs,
                    schema={"file_path": polars.Utf8, "row_group_id": polars.Int64},
                    orient="row",
                ))
            if not selected_group_frames:
                return 0, False
            selected_groups = polars.concat(selected_group_frames).unique()
            selected_stats = scoped.join(
                selected_groups,
                on=["file_path", "row_group_id"],
                how="inner",
            )
            # Every requested group must exist in the validated stats manifest.
            if selected_stats.select(
                ["file_path", "row_group_id"]
            ).unique().height != selected_groups.height:
                return 0, False

            if selected_cols is not None:
                requested_cols = sorted(
                    str(column).casefold() for column in selected_cols
                )
                if not requested_cols:
                    return 0, False
                selected_stats = selected_stats.filter(
                    polars.col("column_name").str.to_lowercase().is_in(
                        requested_cols
                    )
                )
                if selected_stats.height == 0:
                    return 0, False
                slot_health = (
                    selected_stats
                    .with_columns(
                        polars.col("column_name").str.to_lowercase().alias("__cn")
                    )
                    .group_by(["file_path", "row_group_id"])
                    .agg([
                        polars.len().alias("__slots"),
                        polars.col("__cn").n_unique().alias("__unique_slots"),
                    ])
                )
                if (
                    slot_health.height != selected_groups.height
                    or slot_health.filter(
                        (polars.col("__slots") != len(requested_cols))
                        | (
                            polars.col("__unique_slots")
                            != len(requested_cols)
                        )
                    ).height
                ):
                    return 0, False
            metric = selected_stats.get_column(byte_column)
            invalid = metric.is_null() | (metric < 0)
            if invalid.any():
                return 0, False
            value = metric.sum()
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                return 0, False
            return int(value), True
        except Exception:
            return 0, False

    @staticmethod
    def _row_group_row_count_estimate(
        stats_df: Optional["polars.DataFrame"],
        file_keys: Iterable[str],
        selections: Dict[str, RowGroupSelection],
    ) -> Tuple[int, bool]:
        """Return physical rows in the selected groups when fully manifested.

        The same ``row_group_rows`` value is repeated for every column chunk.
        Collapse it once per group and require exact agreement before using it
        for system-column memory/I/O estimates such as tombstone ``__rowid__``.
        """
        keys = list(file_keys)
        if not keys:
            return 0, True
        if (
            stats_df is None
            or not isinstance(stats_df, polars.DataFrame)
            or stats_df.height == 0
            or not {
                "file_path", "row_group_id", "row_group_rows",
            }.issubset(stats_df.columns)
        ):
            return 0, False
        try:
            groups = (
                stats_df
                .filter(polars.col("file_path").is_in(keys))
                .group_by(["file_path", "row_group_id"])
                .agg([
                    polars.col("row_group_rows").n_unique().alias("__counts"),
                    polars.col("row_group_rows").first().alias("__rows"),
                    (
                        polars.col("row_group_rows").is_not_null()
                        & (polars.col("row_group_rows") >= 0)
                    ).all().alias("__valid"),
                ])
            )
            if set(groups.get_column("file_path").to_list()) != set(keys):
                return 0, False
            if groups.filter(
                (polars.col("__counts") != 1) | ~polars.col("__valid")
            ).height:
                return 0, False

            narrowed = set(selections).intersection(keys)
            frames: List[polars.DataFrame] = []
            all_keys = [key for key in keys if key not in narrowed]
            if all_keys:
                frames.append(groups.filter(polars.col("file_path").is_in(all_keys)))
            if narrowed:
                pairs = polars.DataFrame(
                    [
                        (key, group_id)
                        for key in keys
                        if key in narrowed
                        for group_id in selections[key].selected_ids
                    ],
                    schema={"file_path": polars.Utf8, "row_group_id": polars.Int64},
                    orient="row",
                )
                frames.append(groups.join(
                    pairs,
                    on=["file_path", "row_group_id"],
                    how="inner",
                ))
                expected_pairs = sum(
                    len(selections[key].selected_ids) for key in narrowed
                )
                if frames[-1].height != expected_pairs:
                    return 0, False
            selected_groups = polars.concat(frames) if len(frames) > 1 else frames[0]
            value = selected_groups.get_column("__rows").sum()
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                return 0, False
            return int(value), True
        except Exception:
            return 0, False

    @staticmethod
    def _row_group_count_estimate(
        stats_df: Optional["polars.DataFrame"],
        file_keys: Iterable[str],
        selections: Dict[str, RowGroupSelection],
    ) -> Tuple[int, bool]:
        """Return the exact estimator-candidate row-group count when sealed.

        A missing selection means every manifested group for that resource;
        an explicit selection contributes only its validated IDs.  This value
        is observability for the estimator plan, not evidence that a downstream
        native scanner physically opened exactly that many groups.
        """
        keys = list(file_keys)
        if not keys:
            return 0, True
        if (
            stats_df is None
            or not isinstance(stats_df, polars.DataFrame)
            or stats_df.height == 0
            or not {"file_path", "row_group_id"}.issubset(stats_df.columns)
            or len(set(keys)) != len(keys)
        ):
            return 0, False
        try:
            key_set = set(keys)
            groups = stats_df.filter(
                polars.col("file_path").is_in(keys)
            ).select(["file_path", "row_group_id"]).unique()
            if set(groups.get_column("file_path").to_list()) != key_set:
                return 0, False

            counts: Dict[str, int] = {}
            available: Dict[str, Set[int]] = {}
            for file_key, raw_group_id in groups.iter_rows():
                if (
                    file_key not in key_set
                    or not isinstance(raw_group_id, int)
                    or isinstance(raw_group_id, bool)
                    or raw_group_id < 0
                ):
                    return 0, False
                available.setdefault(file_key, set()).add(raw_group_id)
            for file_key in keys:
                group_ids = available.get(file_key)
                if not group_ids:
                    return 0, False
                # Parquet group IDs are a dense zero-based sequence.  A hole
                # means the stats artifact is incomplete and must not produce
                # a deceptively small telemetry value.
                if min(group_ids) != 0 or max(group_ids) != len(group_ids) - 1:
                    return 0, False
                selection = selections.get(file_key)
                if selection is None:
                    counts[file_key] = len(group_ids)
                    continue
                if (
                    not isinstance(selection, RowGroupSelection)
                    or selection.expected_row_group_count != len(group_ids)
                    or not set(selection.selected_ids).issubset(group_ids)
                ):
                    return 0, False
                counts[file_key] = len(selection.selected_ids)
            return sum(counts.values()), True
        except Exception:
            return 0, False

    @staticmethod
    def _decoded_fixed_width(type_name: object) -> Optional[int]:
        """Return a conservative fixed-width Arrow value size, or unknown.

        Variable-width/nested values cannot be bounded by Parquet's dictionary
        or RLE page sizes.  They require a write-time logical-byte seal; until
        one exists their decoded estimate is incomplete and native routing is
        disabled. The caller adds one byte/value, which safely covers Arrow's
        optional validity bitmap and alignment slack for primitive arrays.
        """
        value = str(type_name or "").strip().casefold()
        if not value:
            return None
        if any(token in value for token in (
            "string", "utf8", "varchar", "char", "text", "json", "binary",
            "blob", "list", "array", "struct", "map", "object", "categorical",
            "enum",
        )):
            return None
        if "bool" in value:
            return 1
        if "timestamp" in value or "datetime" in value or "duration" in value:
            return 8
        if value == "date" or "date32" in value:
            return 4
        if "decimal" in value or "hugeint" in value or "int128" in value:
            return 16
        if any(token in value for token in ("int64", "uint64", "bigint", "long", "float64", "double")):
            return 8
        if any(token in value for token in ("int32", "uint32", "integer", "float32", "float", "real")):
            return 4
        if any(token in value for token in ("int16", "uint16", "smallint")):
            return 2
        if any(token in value for token in ("int8", "uint8", "tinyint", "byte")):
            return 1
        return None

    @staticmethod
    def _integer_domain_type(type_name: object) -> bool:
        """Whether a schema spelling can use the signed footer integer lane."""
        return str(type_name or "").strip().casefold() in {
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "byte", "short", "integer", "int", "long", "bigint",
        }

    def _decoded_row_group_estimate(
        self,
        stats_df: Optional["polars.DataFrame"],
        file_keys: Iterable[str],
        selected_cols: Optional[Set[str]],
        selections: Dict[str, RowGroupSelection],
        schema_types: Dict[str, str],
        max_value_bytes: Optional[Dict[str, int]] = None,
    ) -> Tuple[int, bool]:
        """Bound decoded primitive buffers for the exact candidate groups.

        Parquet ``total_uncompressed_size`` is still encoded (dictionary/RLE)
        and is not a memory bound.  Fixed-width columns are instead charged by
        selected physical rows times logical width, plus validity slack.  The
        encoded-page total remains a lower bound for decoder input/work. Any
        variable/unknown requested type makes the estimate explicitly
        incomplete so IslandDB routes to a mature bounded engine.
        """
        normalized_types = {
            str(name).casefold(): type_name
            for name, type_name in (schema_types or {}).items()
            if str(name).casefold() not in {
                ROWID_COL.casefold(), TIMESTAMP_COL.casefold(),
            }
        }
        requested = (
            set(normalized_types)
            if selected_cols is None
            else {str(name).casefold() for name in selected_cols}
        )
        if not requested:
            return 0, False
        widths: List[int] = []
        for name in requested:
            normalized_type = str(
                normalized_types.get(name) or ""
            ).strip().casefold()
            width = self._decoded_fixed_width(normalized_type)
            if (
                width is None
                and normalized_type in {"binary", "string", "utf8"}
            ):
                bound = (max_value_bytes or {}).get(name)
                if (
                    isinstance(bound, int)
                    and not isinstance(bound, bool)
                    and bound >= 0
                ):
                    # Binary owns a 32-bit offset; Polars' native UTF-8 value
                    # representation can retain a 16-byte string view.  The
                    # caller adds validity/alignment slack below.
                    width = int(bound) + (
                        4 if normalized_type == "binary" else 16
                    )
            if width is None:
                return 0, False
            widths.append(width)
        rows, rows_complete = self._row_group_row_count_estimate(
            stats_df, file_keys, selections,
        )
        if not rows_complete:
            return 0, False
        logical_bytes = rows * sum(width + 1 for width in widths)
        encoded_bytes, encoded_complete = self._row_group_byte_estimate(
            stats_df, file_keys, requested, selections, "uncompressed_bytes",
        )
        return max(logical_bytes, encoded_bytes if encoded_complete else 0), True

    def _ratio_bytes(
        self,
        file_key: str,
        key_size: Dict[str, int],
        selected_cols: Set[str],
        schema_types: Dict[str, str],
    ) -> int:
        """Fallback size: scale the whole-file bytes by the selected columns'
        type-width share of the table schema.  Used only when precise
        per-column ``compressed_bytes`` are unavailable for *file_key*."""
        full = int(key_size.get(file_key, 0))
        return self._ratio_bytes_with_widths(
            full,
            self._projection_widths(selected_cols, schema_types),
        )

    def _projection_widths(
        self,
        selected_cols: Set[str],
        schema_types: Dict[str, str],
    ) -> Optional[Tuple[int, int]]:
        """Return ``(selected_width, total_width)`` for fallback sizing.

        ``None`` means the schema cannot establish a useful projection share,
        so callers conservatively charge each file's full size.  The result is
        table-constant and should be computed once for a batch of files.
        """
        if not schema_types:
            return None
        all_cols = {
            c: ty for c, ty in schema_types.items()
            if c not in (ROWID_COL, TIMESTAMP_COL) or c in selected_cols
        }
        total_w = sum(self._type_width(ty) for ty in all_cols.values())
        if total_w <= 0:
            return None
        sel_w = sum(self._type_width(all_cols[c]) for c in selected_cols if c in all_cols)
        if sel_w <= 0:
            return None
        return sel_w, total_w

    @staticmethod
    def _ratio_bytes_with_widths(
        full: int,
        widths: Optional[Tuple[int, int]],
    ) -> int:
        """Apply precomputed projection *widths* to one whole-file size."""
        if full <= 0 or widths is None:
            return full
        sel_w, total_w = widths
        return int(full * sel_w / total_w)

    # ----------------------- main API -----------------------
    def estimate(self) -> Reflection:
        """
        Returns a dict with keys: STORAGE_TYPE, BYTES_AFFECTED, FILE_LIST.
        Performs RBAC check and column validation.
        """
        self.timer = Timer()
        self._linked_authority_cache = {}
        # Preserve compatibility with embedders/test harnesses that construct
        # the estimator through ``__new__`` and populate its historical fields
        # directly. Only an explicit True enables the OData identity work.
        require_odata_identity = (
            getattr(self, "require_odata_identity", False) is True
        )
        if self.plan_stats is None:
            self.plan_stats = PlanStats()

        # One profiler for the whole estimate; threaded into _prune_files so the
        # stats-cache and pruned-file counters mirror the write-path convention.
        prune_profiler = Profiler()

        supers: List[SuperSnapshot] = []
        reflection_file_size = 0        # projected (selected-column) bytes — routing
        reflection_file_size_raw = 0    # whole-file bytes — physical footprint
        reflection_sizes_complete = True
        row_group_scan_bytes = 0
        row_group_scan_bytes_complete = True
        decoded_bytes = 0
        decoded_bytes_complete = True
        selected_decoded_bytes = 0
        selected_decoded_bytes_complete = True
        proof_decoded_bytes = 0
        proof_decoded_bytes_complete = True
        candidate_row_groups = 0
        candidate_row_groups_complete = True
        max_freshness_ms = 0
        files_before_prune = 0
        files_pruned = 0
        files_kept = 0

        super_map = self._get_supertable_map()

        # Tables that participate in a *usable* join edge need their stats loaded
        # even for a bare SELECT * with no WHERE: their surviving files still
        # bound the join keys they export to partners during cross-table pruning.
        # Edges that cannot prune either endpoint (for example FULL OUTER JOIN)
        # consume no stats and never need to enter the kernel.  A physical table
        # mentioned more than once is also ambiguous here: records and join maps
        # use one normalized key, so collapsing occurrences could export the
        # wrong filtered range.  Disable every edge touching such a key.
        table_key_counts = Counter(
            (t.super_name.lower(), t.simple_name.lower()) for t in self.tables
        )
        duplicate_table_keys = {
            key for key, count in table_key_counts.items() if count > 1
        }
        candidate_join_edges: List[JoinEdge] = []
        if settings.SUPERTABLE_READ_PRUNING_ENABLED:
            candidate_join_edges = [
                edge for edge in self.join_edges
                if (edge.prune_left or edge.prune_right)
                and edge.left_table not in duplicate_table_keys
                and edge.right_table not in duplicate_table_keys
            ]

        join_table_keys: Set[Tuple[str, str]] = set()
        if candidate_join_edges:
            for _edge in candidate_join_edges:
                join_table_keys.add(_edge.left_table)
                join_table_keys.add(_edge.right_table)

        # ---- Pass 1: discover snapshots + per-table (own-WHERE) pruning --------
        # Each record carries everything Pass 2 needs to size files and build the
        # SuperSnapshot, plus the own-WHERE survivors + loaded stats that feed the
        # cross-table join propagation that runs between the two passes.
        # Records intentionally carry heterogeneous estimator state. ``Any``
        # is scoped to this private staging dictionary; each value is built in
        # Pass 1 and consumed under the same key contract in Pass 2.
        records: List[Dict[str, Any]] = []

        for super_name, tables in super_map:
            # Collect snapshots ONCE per super_name (avoid redundant SCAN per simple table)
            all_snapshots = self._collect_snapshots_from_redis(organization=self.organization, super_name=super_name)

            for simple_name in tables:
                snapshots = self._filter_snapshots(super_name, simple_name, all_snapshots)
                # Defence in depth: the read path must never bootstrap a
                # missing supertable. ``DataReader._assert_targets_exist``
                # is the primary guard at the entry point; this kwarg
                # ensures any other caller of ``DataEstimator`` (or any
                # future code path) cannot accidentally side-effect a
                # creation through the SuperTable constructor.
                super_table = SuperTable(
                    super_name, self.organization, create_if_missing=False,
                )

                schema: Set[str] = set()
                schema_types: Dict[str, str] = {}
                raw_keys: List[str] = []
                key_size: Dict[str, int] = {}
                resource_rows: Dict[str, Optional[int]] = {}
                resource_seals: Dict[str, Optional[ResourceStatsSeal]] = {}
                resource_object_seals: Dict[
                    str, Optional[ResourceObjectSeal]
                ] = {}
                resource_rowid_integrity_seals: Dict[
                    str, Optional[ResourceRowIdIntegritySeal]
                ] = {}
                resource_cache_identities: Dict[str, Optional[str]] = {}
                resource_value_bounds: Dict[
                    str, Optional[Dict[str, int]]
                ] = {}
                stats_file: Optional[str] = None
                expected_stats_rows: Optional[int] = None
                pinned_snapshot_metadata: List[Dict[str, object]] = []
                # A zero-resource manifest is a valid, committed table state
                # after metadata-only delete-all.  Keep this proof separate
                # from ``not raw_keys``: a corrupt/legacy snapshot that omits
                # ``resources`` must not be silently reinterpreted as empty.
                authoritative_empty = bool(snapshots)

                current_version = 0
                for snapshot in snapshots:
                    ts = int(snapshot.get("last_updated_ms", 0))
                    if ts > max_freshness_ms:
                        max_freshness_ms = ts
                    current_snapshot_path = snapshot["path"]
                    leaf_payload = snapshot.get("payload")
                    current_snapshot_data = complete_snapshot_payload(
                        leaf_payload,
                        expected_version=snapshot.get("version"),
                        require_policy_marker=True,
                    )
                    if current_snapshot_data is None:
                        current_snapshot_data = super_table.read_simple_table_snapshot(current_snapshot_path)
                    self._validate_linked_snapshot_authority(
                        self.organization,
                        super_name,
                        str(snapshot.get("table_name") or simple_name),
                        snapshot,
                        current_snapshot_data,
                    )

                    # Pin deletion metadata from the exact snapshot document
                    # whose resources are accumulated below.  In particular,
                    # path-only Redis leaves take this branch and therefore do
                    # not lose tombstones that live only in the heavy JSON.
                    raw_tombstone = current_snapshot_data.get("tombstone")
                    raw_tombstone_rows = current_snapshot_data.get(
                        "tombstone_rows"
                    )
                    raw_tombstone_digest = current_snapshot_data.get(
                        "tombstone_digest"
                    )
                    try:
                        tombstone_state = normalize_snapshot_tombstone_state(
                            current_snapshot_data,
                        )
                    except (TypeError, ValueError):
                        if raw_tombstone is not None and not (
                            isinstance(raw_tombstone_rows, int)
                            and not isinstance(raw_tombstone_rows, bool)
                            and raw_tombstone_rows > 0
                        ):
                            raise RuntimeError(
                                f"Snapshot for {super_name}.{simple_name} "
                                "references a deletion vector without a "
                                "positive row count"
                            ) from None
                        if raw_tombstone is None and not (
                            isinstance(raw_tombstone_rows, int)
                            and not isinstance(raw_tombstone_rows, bool)
                            and raw_tombstone_rows == 0
                        ):
                            raise RuntimeError(
                                f"Invalid tombstone row count for "
                                f"{super_name}.{simple_name}"
                            ) from None
                        raise RuntimeError(
                            f"Invalid deletion-vector state for "
                            f"{super_name}.{simple_name}"
                        ) from None

                    tombstone_key = (
                        str(tombstone_state.pointer)
                        if tombstone_state.pointer is not None
                        else None
                    )
                    tombstone_rows = tombstone_state.rows
                    tombstone_digest = (
                        str(tombstone_state.digest)
                        if tombstone_state.digest is not None
                        else None
                    )
                    # Preserve the legacy absence spelling after validation so
                    # v1 Reflection/TombstoneDef payloads stay byte-for-byte
                    # compatible. Explicit 1 and 2 remain pinned as written.
                    tombstone_format = (
                        current_snapshot_data.get("tombstone_format")
                        if "tombstone_format" in current_snapshot_data
                        else None
                    )

                    # Policy overlays have existed in all three wrappers
                    # across catalog versions.  Never let a newer/outer marker
                    # silently replace a different inner restriction: metadata
                    # disagreement is safest when every valid predicate is
                    # enforced.  Exact duplicates are collapsed to avoid
                    # needlessly growing the expression passed to the strict
                    # row-filter AST validator at the read boundary.
                    share_row_filter = combined_share_row_filter(
                        snapshot,
                        current_snapshot_data,
                    )

                    current_version = current_snapshot_data.get("snapshot_version", 0)
                    current_schema = self._schema_to_dict(current_snapshot_data.get("schema", {}))
                    (
                        share_policy_fingerprint,
                        share_allowed_columns,
                    ) = _linked_share_policy_state(
                        snapshot,
                        current_snapshot_data,
                        schema=current_schema,
                    )
                    share_credential_expires_ms = (
                        _linked_share_credential_expiry(
                            snapshot,
                            current_snapshot_data,
                            linked=share_policy_fingerprint is not None,
                        )
                    )
                    share_publication_generation = (
                        _linked_share_publication_generation(
                            snapshot,
                            current_snapshot_data,
                            linked=share_policy_fingerprint is not None,
                        )
                    )
                    stable_rowid_contract = bool(
                        require_odata_identity
                        and snapshot_proves_stable_rowids(
                            current_snapshot_data,
                            snapshot,
                        )
                    )
                    pinned_snapshot_metadata.append({
                        "path": current_snapshot_path,
                        "table_name": snapshot.get("table_name"),
                        "tombstone_key": tombstone_key,
                        "tombstone_rows": tombstone_rows,
                        "tombstone_digest": tombstone_digest,
                        "tombstone_format": tombstone_format,
                        "share_row_filter": share_row_filter,
                        "share_policy_fingerprint": share_policy_fingerprint,
                        "share_allowed_columns": share_allowed_columns,
                        "share_credential_expires_ms": (
                            share_credential_expires_ms
                        ),
                        "share_publication_generation": (
                            share_publication_generation
                        ),
                        "stable_rowid_contract": stable_rowid_contract,
                        "rowid_high_watermark": (
                            current_snapshot_data.get("rowid_high_watermark")
                            if stable_rowid_contract else None
                        ),
                    })

                    lowered_schema = dict_keys_to_lowercase(current_schema)
                    schema.update(lowered_schema.keys())
                    # Retain name->type for the projection ratio fallback. First
                    # writer wins for a given column (schemas are stable per table).
                    for _cname, _ctype in lowered_schema.items():
                        schema_types.setdefault(_cname, _ctype)
                    sf = current_snapshot_data.get("stats_file")
                    if sf:
                        stats_file = sf
                        recorded_stats_rows = current_snapshot_data.get(
                            "stats_rows"
                        )
                        expected_stats_rows = (
                            recorded_stats_rows
                            if isinstance(recorded_stats_rows, int)
                            and not isinstance(recorded_stats_rows, bool)
                            and recorded_stats_rows >= 0
                            else None
                        )

                    resources_value = current_snapshot_data.get("resources")
                    manifest_is_authoritative_empty = (
                        isinstance(resources_value, list)
                        and not resources_value
                        and bool(current_schema)
                        and not tombstone_key
                    )
                    authoritative_empty = (
                        authoritative_empty and manifest_is_authoritative_empty
                    )
                    resources = resources_value if isinstance(resources_value, list) else []
                    for resource in resources:
                        file_key = resource.get("file")
                        if not file_key:
                            continue
                        raw_keys.append(file_key)
                        raw_size = resource.get("file_size")
                        valid_size = (
                            isinstance(raw_size, int)
                            and not isinstance(raw_size, bool)
                            and raw_size > 0
                        )
                        if valid_size:
                            key_size[file_key] = int(raw_size)
                        else:
                            if share_policy_fingerprint is not None:
                                raise RuntimeError(
                                    "Linked-share resource size is unavailable"
                                )
                            # Legacy snapshots may lack file_size. Recover it
                            # through the storage SDK rather than silently route
                            # a genuinely large scan as zero bytes.
                            try:
                                measured = int(self.storage.size(file_key))
                            except Exception:
                                measured = 0
                            if measured > 0:
                                key_size[file_key] = measured
                            else:
                                key_size[file_key] = 0
                                reflection_sizes_complete = False
                        rows_value = resource.get("rows")
                        valid_rows = (
                            rows_value
                            if isinstance(rows_value, int)
                            and not isinstance(rows_value, bool)
                            and rows_value >= 0
                            else None
                        )
                        if file_key in resource_rows:
                            # A repeated resource key with conflicting or
                            # missing cardinality has no unambiguous manifest.
                            if resource_rows[file_key] != valid_rows:
                                resource_rows[file_key] = None
                        else:
                            resource_rows[file_key] = valid_rows
                        parsed_seal = resource_stats_seal(resource)
                        if file_key in resource_seals:
                            # Duplicate keys are never allowed to borrow one
                            # occurrence's valid seal. Any disagreement, including
                            # sealed vs legacy, makes the identity ambiguous.
                            if resource_seals[file_key] != parsed_seal:
                                resource_seals[file_key] = None
                        else:
                            resource_seals[file_key] = parsed_seal
                        parsed_object_seal = resource_object_seal(resource)
                        if file_key in resource_object_seals:
                            # Never let one duplicate occurrence lend identity
                            # to another. Sealed-vs-legacy and any field mismatch
                            # both revert this key to the normal provider stat.
                            if (
                                resource_object_seals[file_key]
                                != parsed_object_seal
                            ):
                                resource_object_seals[file_key] = None
                        else:
                            resource_object_seals[file_key] = parsed_object_seal
                        if require_odata_identity:
                            parsed_rowid_seal = resource_rowid_integrity_seal(
                                resource,
                            )
                            if file_key in resource_rowid_integrity_seals:
                                # Duplicate resource keys cannot borrow one
                                # occurrence's identity attestation.
                                resource_rowid_integrity_seals[file_key] = None
                            else:
                                resource_rowid_integrity_seals[file_key] = (
                                    parsed_rowid_seal
                                )
                        parsed_cache_identity = _linked_resource_cache_identity(
                            resource,
                            linked=share_policy_fingerprint is not None,
                        )
                        if file_key in resource_cache_identities:
                            if (
                                resource_cache_identities[file_key]
                                != parsed_cache_identity
                            ):
                                raise RuntimeError(
                                    "Linked-share resource cache identity is ambiguous"
                                )
                        else:
                            resource_cache_identities[file_key] = (
                                parsed_cache_identity
                            )
                        raw_bounds = resource.get("column_max_value_bytes")
                        parsed_bounds: Optional[Dict[str, int]] = None
                        if isinstance(raw_bounds, dict):
                            candidate_bounds: Dict[str, int] = {}
                            valid_bounds = True
                            for column_name, bound in raw_bounds.items():
                                folded = str(column_name).casefold()
                                if (
                                    not str(column_name)
                                    or folded in candidate_bounds
                                    or not isinstance(bound, int)
                                    or isinstance(bound, bool)
                                    or bound < 0
                                ):
                                    valid_bounds = False
                                    break
                                candidate_bounds[folded] = int(bound)
                            if valid_bounds:
                                parsed_bounds = candidate_bounds
                        if file_key in resource_value_bounds:
                            if resource_value_bounds[file_key] != parsed_bounds:
                                resource_value_bounds[file_key] = None
                        else:
                            resource_value_bounds[file_key] = parsed_bounds

                key = (super_name.lower(), simple_name.lower())

                # Which columns does the query actually read? None => SELECT *
                # (whole table, no projection savings).
                selected_cols = self._selected_columns(super_name, simple_name)
                if (
                    selected_cols is not None
                    and any(
                        meta.get("tombstone_key")
                        for meta in pinned_snapshot_metadata
                    )
                ):
                    # The visible query may project one tiny user column, but a
                    # deletion-vector view also reads __rowid__ for the exact
                    # composite anti join.  Include its compressed chunks in
                    # the routed scan estimate instead of systematically
                    # underestimating active-DV queries.
                    selected_cols = set(selected_cols)
                    selected_cols.add(ROWID_COL)
                need_projection = (
                    selected_cols is not None
                    and settings.SUPERTABLE_READ_PROJECTION_SIZING_ENABLED
                )
                has_predicate = self._has_potential_select_predicate(
                    self.predicate_constraints.get(key)
                )

                # Load the stats artifact ONCE per table and reuse it for
                # predicate pruning, projection sizing, cross-table join
                # propagation, and bounded engine resource planning. The
                # AUTO/IslandDB lane performs pre-decode admission; a normal
                # cache miss would otherwise whole-decode the artifact before
                # the cache byte cap can reject it.
                stats_df: Optional["polars.DataFrame"] = None
                stats_identity = (
                    stats_cache_identity(
                        stats_file,
                        organization=self.organization,
                        storage=self.storage,
                    )
                    if stats_file else None
                )
                if stats_file and (
                    need_projection
                    or has_predicate
                    or key in join_table_keys
                    or bool(getattr(
                        self, "require_bounded_resource_estimates", False,
                    ))
                ):
                    try:
                        if bool(getattr(
                            self, "require_bounded_resource_estimates", False,
                        )):
                            if expected_stats_rows is None:
                                raise ValueError(
                                    "Statistics row count is unavailable"
                                )
                            stats_df = load_bounded_stats_for_planning(
                                stats_file,
                                expected_rows=expected_stats_rows,
                                cache_identity=stats_identity,
                                profiler=prune_profiler,
                                storage=self.storage,
                            )
                        else:
                            stats_df = load_stats(
                                stats_file,
                                allow_cache=True,
                                cache_identity=stats_identity,
                                profiler=prune_profiler,
                                storage=self.storage,
                            )
                    except Exception as stats_err:
                        # Stats are an optional optimisation artifact.  A stale,
                        # corrupt, unavailable, or malformed pointer must never
                        # turn a valid SELECT into an error or a narrower scan.
                        logger.warning(
                            f"[estimate.stats] stats unavailable for "
                            f"{super_name}.{simple_name}; pruning and precise "
                            f"projection sizing skipped; "
                            f"error_type={safe_exception_type(stats_err)}"
                        )
                        stats_df = None

                if stats_df is not None and expected_stats_rows is not None:
                    try:
                        actual_stats_rows = int(stats_df.height)
                    except Exception:
                        actual_stats_rows = -1
                    if actual_stats_rows != expected_stats_rows:
                        # A stats parquet may still be syntactically readable
                        # after a bad copy/truncation.  Its snapshot records the
                        # immutable artifact's exact row count; disagreement
                        # means it is incomplete/foreign and therefore cannot
                        # prove a data file absent.  Projection sizing also
                        # falls back rather than under-counting from it.
                        logger.warning(
                            f"[estimate.stats] stats row-count mismatch for "
                            f"{super_name}.{simple_name}: expected "
                            f"{expected_stats_rows}, got {actual_stats_rows}; "
                            f"pruning and precise projection sizing skipped"
                        )
                        stats_df = None

                if stats_df is not None:
                    # A table-level row count cannot identify a missing slot
                    # replaced by a duplicate elsewhere.  Remove every file
                    # whose stats do not form a complete per-resource manifest;
                    # absent stats always retain the data file.
                    stats_df = self._stats_for_complete_files(
                        stats_df,
                        resource_rows,
                        resource_seals,
                        stats_path=stats_identity,
                    )

                # Read-path pruning: drop raw keys whose stats prove they cannot
                # satisfy this table's own WHERE before any cross-table step.
                # The span accumulates the wall-clock of the per-table pruning
                # (predicate eval) across every table in the query.
                literal_count_before = prune_profiler.counts.get(
                    "read_pruned_files", 0,
                )
                with prune_profiler.span("read.prune"):
                    literal_survivors = self._prune_files(
                        super_name, simple_name, raw_keys, stats_df,
                        profiler=prune_profiler,
                    )
                # Validate again at the estimator boundary so even an overridden
                # or future _prune_files implementation cannot inject files or
                # make the aggregate counters negative.
                validated_literal = self._validated_file_subset(
                    raw_keys, literal_survivors,
                )
                invalid_literal = (
                    validated_literal is None
                    or (raw_keys and not validated_literal)
                )
                if invalid_literal:
                    logger.warning(
                        f"[estimate.prune] invalid survivor set for "
                        f"{super_name}.{simple_name}; keeping all candidate files"
                    )
                    literal_survivors = list(raw_keys)
                    literal_row_groups: Dict[str, RowGroupSelection] = {}
                else:
                    literal_survivors = cast(List[str], validated_literal)
                    # Row-group hints come only from this table's literal WHERE.
                    # Join propagation below may remove whole files, but must not
                    # manufacture tighter group ids from cross-table ranges.
                    if not settings.SUPERTABLE_READ_PRUNING_ENABLED:
                        literal_row_groups = {}
                    else:
                        try:
                            literal_row_groups = select_row_groups_by_predicates(
                                raw_keys,
                                stats_df,
                                self.predicate_constraints.get(key) or [],
                            )
                        except Exception as row_group_err:
                            logger.warning(
                                f"[estimate.row_groups] selection skipped for "
                                f"{super_name}.{simple_name}; "
                                f"error_type={safe_exception_type(row_group_err)}"
                            )
                            literal_row_groups = {}
                if require_odata_identity:
                    # OData continuation identity is table-global. A WHERE
                    # predicate may prove that one resource cannot contribute
                    # rows to this page, but it cannot prove that the pruned
                    # resource has no row ID duplicated by a survivor or by a
                    # later page. Keep the exact pinned manifest for the
                    # backend proof relation; DuckDB still applies the user
                    # predicate to the final query normally.
                    literal_survivors = list(raw_keys)
                    literal_row_groups = {}
                # Reconcile observability from the validated boundary result,
                # rather than trusting an inner implementation to update the
                # profiler exactly once.  This also keeps custom/no-op pruners
                # from reporting removals that were never applied.
                literal_count_after = literal_count_before + (
                    len(raw_keys) - len(literal_survivors)
                )
                if literal_count_after:
                    prune_profiler.counts["read_pruned_files"] = literal_count_after
                else:
                    prune_profiler.counts.pop("read_pruned_files", None)
                files_before_prune += len(raw_keys)

                records.append({
                    "super_name": super_name,
                    "simple_name": simple_name,
                    "key": key,
                    "schema": schema,
                    "schema_types": schema_types,
                    "key_size": key_size,
                    "resource_rows": resource_rows,
                    "resource_seals": resource_seals,
                    "resource_object_seals": resource_object_seals,
                    "resource_rowid_integrity_seals": (
                        resource_rowid_integrity_seals
                    ),
                    "resource_cache_identities": resource_cache_identities,
                    "resource_value_bounds": resource_value_bounds,
                    "current_version": current_version,
                    "has_snapshots": bool(snapshots),
                    "pinned_snapshot_metadata": pinned_snapshot_metadata,
                    "stats_df": stats_df,
                    "selected_cols": selected_cols,
                    "need_projection": need_projection,
                    "files_before": len(raw_keys),
                    "snapshot_resource_keys": list(raw_keys),
                    "authoritative_empty": authoritative_empty and not raw_keys,
                    "survivors": literal_survivors,
                    "row_group_selections": literal_row_groups,
                })

        # ---- Cross-table join pruning (ON by default under the read switch) ----
        # Propagate every surviving file-set's join-key min/max across the query's
        # equi-join edges to a fixpoint, so a table narrowed by its own WHERE also
        # narrows its join partners. ``allow_empty=False`` keeps the estimator's
        # "never empty a table" guard — a genuinely empty join surfaces at
        # execution time rather than as a zero-file estimate error.
        join_files_removed = 0
        join_iterations = 0
        # Re-check uniqueness on the concrete records before constructing maps:
        # dictionary comprehensions must never silently collapse two records.
        records_by_key: Dict[
            Tuple[str, str], List[Dict[str, Any]]
        ] = defaultdict(list)
        for record in records:
            records_by_key[record["key"]].append(record)
        runnable_join_edges = [
            edge for edge in candidate_join_edges
            if len(records_by_key.get(edge.left_table, [])) == 1
            and len(records_by_key.get(edge.right_table, [])) == 1
        ]
        if runnable_join_edges and not require_odata_identity:
            # Same contract as _prune_files: a pruning failure must degrade to
            # "no pruning" (keep the Pass-1 survivors), never break the read.
            committed_join_plan = False
            join_count_before = prune_profiler.counts.get(
                "read_join_pruned_files", 0,
            )
            try:
                joined_keys = {
                    key
                    for edge in runnable_join_edges
                    for key in (edge.left_table, edge.right_table)
                }
                joined_records = {
                    key: records_by_key[key][0] for key in joined_keys
                }
                table_files = {
                    key: list(record["survivors"])
                    for key, record in joined_records.items()
                }
                table_stats = {
                    key: record["stats_df"]
                    for key, record in joined_records.items()
                }
                with prune_profiler.span("read.join_prune"):
                    join_plan = prune_files_across_joins(
                        runnable_join_edges,
                        {},  # own-WHERE pruning already applied per-table above
                        table_files,
                        table_stats,
                        allow_empty=False,
                        allowed_lanes=getattr(
                            self, "join_pruning_lanes", None,
                        ),
                    )

                # Stage and validate every table before mutating any record.
                # If one endpoint violates the subset contract, reject the whole
                # propagation plan: applying a partial fixpoint can be unsound.
                if set(join_plan.survivors) != set(table_files):
                    raise ValueError(
                        "join pruner returned an incomplete or foreign table set"
                    )
                staged_survivors: Dict[Tuple[str, str], List[str]] = {}
                staged_removed = 0
                for key, record in joined_records.items():
                    original = table_files[key]
                    proposed = join_plan.survivors[key]
                    validated = self._validated_file_subset(original, proposed)
                    if validated is None or (original and not validated):
                        raise ValueError(
                            f"join pruner returned an invalid survivor set for "
                            f"{key[0]}.{key[1]}"
                        )
                    staged_survivors[key] = validated
                    staged_removed += len(original) - len(validated)

                staged_iterations = max(0, int(join_plan.iterations))
                staged_summary = (
                    join_plan.summary()
                    if join_plan.steps and logger.isEnabledFor(logging.DEBUG)
                    else None
                )

                # Atomic commit after all endpoints have passed validation.
                for key, survivors in staged_survivors.items():
                    joined_records[key]["survivors"] = survivors
                committed_join_plan = True
                join_files_removed = staged_removed
                join_iterations = staged_iterations
                prune_profiler.add("read_join_pruned_files", join_files_removed)
                if staged_summary:
                    logger.debug("[estimate.join_prune]\n" + staged_summary)
            except Exception as jp_err:
                if committed_join_plan:
                    # Even instrumentation/logging failures after assignment
                    # must roll the records back to their Pass-1 survivors.
                    for key, original in table_files.items():
                        joined_records[key]["survivors"] = original
                    if join_count_before:
                        prune_profiler.counts["read_join_pruned_files"] = join_count_before
                    else:
                        prune_profiler.counts.pop("read_join_pruned_files", None)
                join_files_removed = 0
                join_iterations = 0
                logger.warning(
                    "[estimate.join_prune] cross-table pruning skipped; "
                    f"error_type={safe_exception_type(jp_err)}"
                )

        # ---- Pass 2: resolve survivors to scan URLs, size, build snapshots -----
        for r in records:
            super_name = r["super_name"]
            simple_name = r["simple_name"]
            survivors = r["survivors"]
            key_size = r["key_size"]
            resource_rows = r["resource_rows"]
            schema = r["schema"]
            schema_types = r["schema_types"]
            resource_value_bounds = r["resource_value_bounds"]
            selected_cols = r["selected_cols"]
            need_projection = r["need_projection"]
            stats_df = r["stats_df"]
            survivor_set = set(survivors)
            column_value_bounds = {
                column_name: max(
                    cast(
                        Dict[str, int], resource_value_bounds[file_key]
                    )[column_name.casefold()]
                    for file_key in survivors
                )
                for column_name, type_name in schema_types.items()
                if str(type_name).strip().casefold() in {
                    "binary", "string", "utf8",
                }
                and survivors
                and all(
                    isinstance(resource_value_bounds.get(file_key), dict)
                    and column_name.casefold() in cast(
                        Dict[str, int], resource_value_bounds[file_key]
                    )
                    for file_key in survivors
                )
            }
            literal_row_groups = {
                file_key: selection
                for file_key, selection in r["row_group_selections"].items()
                if file_key in survivor_set
            }
            # Optional GROUP BY resource proof. The helper accepts only a
            # complete slot for every selected row group of every survivor;
            # any legacy/unsealed/malformed resource returns no bounds and the
            # engine retains its conservative external plan.
            integer_domain_bounds = integer_domains_from_complete_stats(
                stats_df,
                survivors,
                literal_row_groups,
                {
                    str(column_name).casefold()
                    for column_name, type_name in schema_types.items()
                    if self._integer_domain_type(type_name)
                    and (
                        selected_cols is None
                        or str(column_name).casefold() in selected_cols
                    )
                },
            )

            # Projection-aware size: a query selecting specific columns scans only
            # those columns' on-disk (compressed) chunks, not the whole
            # multi-column file. Precise path sums per-column compressed_bytes from
            # the stats artifact; files predating that column (or with no stats)
            # fall back to a type-width ratio of the whole-file size. SELECT *
            # keeps every column (full file).
            tier3_files: Set[str] = set()
            proj: Dict[str, int] = {}
            if need_projection:
                try:
                    tier3_files, proj = self._projected_bytes_index(
                        stats_df,
                        selected_cols,
                        file_keys=(
                            survivors
                            if len(survivors) < r["files_before"]
                            else None
                        ),
                    )
                except Exception as projection_err:
                    # Projection stats only improve routing precision.  Fall
                    # back to schema-width/whole-file sizing if a legacy or
                    # malformed artifact cannot provide a trustworthy index.
                    logger.warning(
                        f"[estimate.projection] precise sizing skipped for "
                        f"{super_name}.{simple_name}; "
                        f"error_type={safe_exception_type(projection_err)}"
                    )
                    tier3_files, proj = set(), {}

            # The fallback width share depends only on this table's schema and
            # selected columns, not on a file.  Hoist it out of the survivor
            # loop; wide schemas with thousands of files otherwise repeat the
            # same dictionary/filter/sum work for every file lacking tier-3
            # compressed-byte stats.
            projection_widths = (
                self._projection_widths(selected_cols, schema_types)
                if need_projection
                else None
            )

            parquet_files: List[str] = []
            resource_credential_generations: List[Optional[int]] = []
            resource_credential_expires_ms: List[Optional[int]] = []
            table_reflection_bytes = 0
            for file_key in survivors:
                # Estimation deliberately never mints credentials. Keep this
                # compatibility seam as the canonical path resolver used by
                # existing embedders/tests; generation is assigned only by
                # Executor's bounded DuckDB presign boundary.
                resolved_path = self._to_duckdb_path(file_key)
                parquet_files.append(resolved_path)
                resource_credential_generations.append(None)
                resource_credential_expires_ms.append(None)
                full = int(key_size.get(file_key, 0))
                reflection_file_size_raw += full
                if not need_projection:
                    reflection_file_size += full
                    table_reflection_bytes += full
                elif file_key in tier3_files:
                    projected = proj.get(file_key, 0)
                    reflection_file_size += projected
                    table_reflection_bytes += projected
                else:
                    projected = self._ratio_bytes_with_widths(
                        full, projection_widths,
                    )
                    reflection_file_size += projected
                    table_reflection_bytes += projected
            files_kept += len(survivors)

            # Exact candidate-chunk estimates are separate from the historical
            # file-level routing estimate.  Include predicate columns even if a
            # future parser narrows TableDefinition.columns to output-only.
            scan_columns = None if selected_cols is None else set(selected_cols)
            if scan_columns is not None:
                for occurrence in self.predicate_constraints.get(r["key"], []) or []:
                    scan_columns.update(
                        str(column).lower() for column in occurrence
                        if isinstance(column, str)
                    )
            stats_scan_columns = (
                None
                if scan_columns is None
                else {
                    column for column in scan_columns
                    if column not in (ROWID_COL, TIMESTAMP_COL)
                }
            )
            table_rg_bytes, table_rg_complete = self._row_group_byte_estimate(
                stats_df,
                survivors,
                stats_scan_columns,
                literal_row_groups,
                "compressed_bytes",
            )
            table_decoded_bytes, table_decoded_complete = (
                self._decoded_row_group_estimate(
                    stats_df,
                    survivors,
                    stats_scan_columns,
                    literal_row_groups,
                    schema_types,
                    column_value_bounds,
                )
            )
            # Keep selected-query buffers distinct from first-use DV proof work.
            # The total below remains their sum and retains its conservative
            # routing meaning.
            table_selected_decoded_bytes = table_decoded_bytes
            table_selected_decoded_complete = table_decoded_complete
            table_proof_decoded_bytes = 0
            table_proof_decoded_complete = True
            selected_rows, selected_rows_complete = (
                self._row_group_row_count_estimate(
                    stats_df, survivors, literal_row_groups,
                )
            )
            table_candidate_row_groups, table_candidate_row_groups_complete = (
                self._row_group_count_estimate(
                    stats_df, survivors, literal_row_groups,
                )
            )
            if table_candidate_row_groups_complete:
                candidate_row_groups += table_candidate_row_groups
            else:
                candidate_row_groups_complete = False
            if not selected_rows_complete and not literal_row_groups:
                # Resource cardinalities are snapshot-pinned and therefore a
                # safe upper bound even when optional row-group stats are
                # absent. Predicates may reduce the actual result, but they can
                # never make this bound too small. This keeps response-limit
                # and streaming memory decisions bounded for legacy manifests
                # without inventing row-group precision.
                manifest_rows_complete = all(
                    isinstance(resource_rows.get(file_key), int)
                    and not isinstance(resource_rows.get(file_key), bool)
                    and cast(int, resource_rows[file_key]) >= 0
                    for file_key in survivors
                )
                if manifest_rows_complete:
                    selected_rows = sum(
                        cast(int, resource_rows[file_key])
                        for file_key in survivors
                    )
                    selected_rows_complete = True
            has_active_tombstone = any(
                meta.get("tombstone_key")
                for meta in r["pinned_snapshot_metadata"]
            )
            if has_active_tombstone:
                # Applying a DV requires two source-rowid consumers: the normal
                # selected-RG anti join and a full-file uniqueness proof for
                # every referenced immutable resource. Stats deliberately omit
                # system columns, so use manifest row counts for decoded Int64
                # buffers. Conservatively charge one entire file for each rowid
                # consumer as compressed work: the candidate anti join and the
                # full identity proof. This is deliberately looser than the
                # actual range reads, but complete even when rowid chunks
                # dominate tiny user columns.
                full_rows_complete = all(
                    isinstance(resource_rows.get(file_key), int)
                    and not isinstance(resource_rows.get(file_key), bool)
                    and cast(int, resource_rows[file_key]) >= 0
                    for file_key in survivors
                )
                full_sizes_complete = all(
                    isinstance(key_size.get(file_key), int)
                    and not isinstance(key_size.get(file_key), bool)
                    and key_size[file_key] > 0
                    for file_key in survivors
                )
                if selected_rows_complete:
                    # The actual candidate-row anti join decodes __rowid__ in
                    # every selected group; it belongs to normal batch width.
                    table_selected_decoded_bytes += selected_rows * 9
                else:
                    table_selected_decoded_complete = False
                if full_rows_complete:
                    full_rows = sum(
                        cast(int, resource_rows[file_key])
                        for file_key in survivors
                    )
                    # Eight value bytes plus one full byte/value of validity and
                    # alignment slack for the whole-file source-rowid proof.
                    table_proof_decoded_bytes += full_rows * 9
                else:
                    table_proof_decoded_complete = False
                if full_sizes_complete:
                    table_rg_bytes += 2 * sum(
                        key_size[file_key] for file_key in survivors
                    )
                else:
                    table_rg_complete = False
            table_decoded_bytes = (
                table_selected_decoded_bytes + table_proof_decoded_bytes
            )
            table_decoded_complete = (
                table_selected_decoded_complete
                and table_proof_decoded_complete
            )
            if table_rg_complete:
                row_group_scan_bytes += table_rg_bytes
            else:
                # Preserve a conservative, useful compressed fallback while the
                # completeness bit prevents it being described as row-group exact.
                row_group_scan_bytes += table_reflection_bytes
                row_group_scan_bytes_complete = False
            if table_decoded_complete:
                decoded_bytes += table_decoded_bytes
            else:
                decoded_bytes_complete = False
            if table_selected_decoded_complete:
                selected_decoded_bytes += table_selected_decoded_bytes
            else:
                selected_decoded_bytes_complete = False
            if table_proof_decoded_complete:
                proof_decoded_bytes += table_proof_decoded_bytes
            else:
                proof_decoded_bytes_complete = False

            # SuperSnapshot is created ONCE per (super_name, simple_name) after all
            # snapshot iterations have accumulated their files and schema. Creating
            # it inside the loop caused duplicate SuperSnapshot entries with
            # cumulatively growing file lists, inflating total_reflections and
            # confusing the executor's snapshots_by_key lookup.
            if r["has_snapshots"]:
                pinned_metadata = r["pinned_snapshot_metadata"]
                if len(pinned_metadata) == 1:
                    pinned = pinned_metadata[0]
                else:
                    # ``super == simple`` is the legacy all-simple-tables scan
                    # and can accumulate multiple independently-versioned
                    # snapshots into one reflection.  One rowid-only DV cannot
                    # safely represent that set (rowids are table-local), so a
                    # tombstone or share filter on any member must fail closed.
                    if any(
                        meta.get("tombstone_key")
                        or meta.get("share_row_filter")
                        or meta.get("share_policy_fingerprint")
                        for meta in pinned_metadata
                    ):
                        raise RuntimeError(
                            f"Cannot safely combine independently filtered "
                            f"snapshots for {super_name}.{simple_name}"
                        )
                    pinned = {}

                super_snapshot = SuperSnapshot(
                    super_name=super_name,
                    simple_name=simple_name,
                    simple_version=r["current_version"],
                    files=parquet_files,
                    columns=schema,
                    column_types=dict(schema_types),
                    resource_keys=list(survivors),
                    resource_sizes=[int(key_size.get(k, 0)) for k in survivors],
                    snapshot_resource_keys=list(r["snapshot_resource_keys"]),
                    snapshot_path=pinned.get("path"),
                    tombstone_key=pinned.get("tombstone_key"),
                    tombstone_rows=pinned.get("tombstone_rows"),
                    tombstone_digest=pinned.get("tombstone_digest"),
                    tombstone_format=pinned.get("tombstone_format"),
                    share_row_filter=pinned.get("share_row_filter"),
                    share_policy_fingerprint=pinned.get(
                        "share_policy_fingerprint"
                    ),
                    share_allowed_columns=pinned.get("share_allowed_columns"),
                    share_credential_expires_ms=pinned.get(
                        "share_credential_expires_ms"
                    ),
                    resource_cache_identities=[
                        r["resource_cache_identities"].get(key)
                        for key in survivors
                    ],
                    resource_credential_generations=(
                        resource_credential_generations
                    ),
                    share_publication_generation=pinned.get(
                        "share_publication_generation"
                    ),
                    resource_credential_expires_ms=(
                        resource_credential_expires_ms
                    ),
                    stable_rowid_contract=(
                        pinned.get("stable_rowid_contract") is True
                    ),
                    rowid_high_watermark=(
                        pinned.get("rowid_high_watermark")
                        if pinned.get("stable_rowid_contract") is True
                        else None
                    ),
                    resource_rowid_integrity_seals={
                        key: seal
                        for key in survivors
                        for seal in [
                            r["resource_rowid_integrity_seals"].get(key)
                        ]
                        if (
                            pinned.get("stable_rowid_contract") is True
                            and isinstance(seal, ResourceRowIdIntegritySeal)
                        )
                    },
                    resource_row_counts={
                        key: int(cast(int, resource_rows[key]))
                        for key in survivors
                        if (
                            require_odata_identity
                            and isinstance(resource_rows.get(key), int)
                            and not isinstance(resource_rows.get(key), bool)
                            and cast(int, resource_rows[key]) >= 0
                        )
                    },
                    row_group_selections=literal_row_groups,
                    candidate_rows=(selected_rows if selected_rows_complete else 0),
                    candidate_rows_complete=selected_rows_complete,
                    candidate_row_groups=(
                        table_candidate_row_groups
                        if table_candidate_row_groups_complete else 0
                    ),
                    candidate_row_groups_complete=(
                        table_candidate_row_groups_complete
                    ),
                    resource_stats_seals={
                        key: seal
                        for key in survivors
                        for seal in [r["resource_seals"].get(key)]
                        if isinstance(seal, ResourceStatsSeal)
                    },
                    resource_object_seals={
                        key: seal
                        for key in survivors
                        for seal in [r["resource_object_seals"].get(key)]
                        if isinstance(seal, ResourceObjectSeal)
                    },
                    column_max_value_bytes=column_value_bounds,
                    integer_domain_bounds=integer_domain_bounds,
                )
                supers.append(super_snapshot)

        # files_before_prune counts raw candidates; files_kept the final survivors
        # after BOTH own-WHERE and cross-table join pruning. The difference is the
        # total number of files eliminated.
        files_pruned = files_before_prune - files_kept
        if files_pruned < 0:  # defensive; subset validation above makes this unreachable
            logger.warning(
                "[estimate.prune] kept-file count exceeded candidates; "
                "reporting zero files pruned"
            )
            files_pruned = 0

        # Validate requested columns
        missing_info = get_missing_columns(self.tables, supers)

        # Total parquet files across all selected snapshots
        total_reflections = sum(len(s.files) for s in supers)

        # A committed zero-resource manifest with a preserved schema is a
        # valid table, not a missing-data error.  Only records that proved the
        # resources field was explicitly an empty list may take this path.
        authoritative_empty_keys = {
            record["key"] for record in records
            if record.get("authoritative_empty")
        }
        all_have_files = all(
            bool(s.files)
            or (s.super_name.lower(), s.simple_name.lower())
            in authoritative_empty_keys
            for s in supers
        )

        if not supers or missing_info or not all_have_files:
            if not supers:
                msg = "No snapshots selected."
            elif missing_info:
                # missing_info: List[(super_name, table_name, Set[missing_cols])]
                details = []
                for super_name, table_name, cols in missing_info:
                    cols_str = ", ".join(sorted(cols))
                    details.append(f"{super_name}.{table_name}: {cols_str}")
                msg = "Missing required column(s): " + " | ".join(details)
            else:  # not all_have_files
                msg = "No parquet files found for one or more selected tables."

            logger.warning(msg)
            raise RuntimeError(msg)

        self.timer.capture_and_reset_timing(event="ESTIMATE")

        self.plan_stats.add_stat({"REFLECTIONS": total_reflections})
        # REFLECTION_SIZE is the projected (selected-column) size that drives
        # engine routing; REFLECTION_SIZE_RAW is the whole-file footprint, kept
        # for observability so the two are comparable in the plans payload.
        self.plan_stats.add_stat({"REFLECTION_SIZE": reflection_file_size})
        self.plan_stats.add_stat({"REFLECTION_SIZE_RAW": reflection_file_size_raw})
        self.plan_stats.add_stat({"ROW_GROUP_SCAN_SIZE": row_group_scan_bytes})
        self.plan_stats.add_stat({
            "ROW_GROUP_SCAN_SIZE_COMPLETE": row_group_scan_bytes_complete,
        })
        self.plan_stats.add_stat({"DECODED_SIZE": decoded_bytes})
        self.plan_stats.add_stat({
            "DECODED_SIZE_COMPLETE": decoded_bytes_complete,
        })
        self.plan_stats.add_stat({
            "SELECTED_DECODED_SIZE": selected_decoded_bytes,
        })
        self.plan_stats.add_stat({
            "SELECTED_DECODED_SIZE_COMPLETE": selected_decoded_bytes_complete,
        })
        self.plan_stats.add_stat({"PROOF_DECODED_SIZE": proof_decoded_bytes})
        self.plan_stats.add_stat({
            "PROOF_DECODED_SIZE_COMPLETE": proof_decoded_bytes_complete,
        })
        self.plan_stats.add_stat({
            "CANDIDATE_ROW_GROUPS": (
                candidate_row_groups if candidate_row_groups_complete else 0
            ),
        })
        self.plan_stats.add_stat({
            "CANDIDATE_ROW_GROUPS_COMPLETE": candidate_row_groups_complete,
        })

        # Read-path pruning observability — only when pruning is engaged, so a
        # disabled-pruning read doesn't litter the payload with noise. Mirrors
        # the write path: surface the count effect plus the profiler's IO/cache
        # counters (stats_cache_hit/miss, read_pruned_files).
        if settings.SUPERTABLE_READ_PRUNING_ENABLED:
            self.plan_stats.add_stat({"FILES_BEFORE_PRUNE": files_before_prune})
            self.plan_stats.add_stat({"FILES_PRUNED": files_pruned})
            self.plan_stats.add_stat({"FILES_KEPT": files_kept})
            self.plan_stats.add_stat({
                "PRUNE_DURATION_MS": round(
                    prune_profiler.timings.get("read.prune", 0.0) * 1000, 3
                )
            })
            # Cross-table join pruning observability — only when the query
            # carried join edges, so single-table reads don't emit empty stats.
            if runnable_join_edges:
                self.plan_stats.add_stat({"JOIN_EDGES": len(runnable_join_edges)})
                self.plan_stats.add_stat({"JOIN_FILES_PRUNED": join_files_removed})
                self.plan_stats.add_stat({"JOIN_PRUNE_ITERATIONS": join_iterations})
                self.plan_stats.add_stat({
                    "JOIN_PRUNE_DURATION_MS": round(
                        prune_profiler.timings.get("read.join_prune", 0.0) * 1000, 3
                    )
                })
            prune_counts = prune_profiler.emit_counts()
            if prune_counts:
                self.plan_stats.add_stat({"PRUNE_COUNTS": prune_counts})

        return Reflection(
            storage_type=_trusted_storage_type(self.storage),
            reflection_bytes=int(reflection_file_size),
            total_reflections=total_reflections,
            supers=supers,
            freshness_ms=max_freshness_ms,
            source_bytes=int(reflection_file_size_raw),
            source_bytes_complete=reflection_sizes_complete,
            row_group_scan_bytes=int(row_group_scan_bytes),
            row_group_scan_bytes_complete=row_group_scan_bytes_complete,
            decoded_bytes=int(decoded_bytes),
            decoded_bytes_complete=decoded_bytes_complete,
            selected_decoded_bytes=int(selected_decoded_bytes),
            selected_decoded_bytes_complete=selected_decoded_bytes_complete,
            proof_decoded_bytes=int(proof_decoded_bytes),
            proof_decoded_bytes_complete=proof_decoded_bytes_complete,
        )
