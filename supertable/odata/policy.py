# route: supertable.odata.policy
"""Policy fingerprints — proof that what a page returns is still allowed.

An OData page is not served in one shot. The server resolves policy, starts a
query, streams rows, and only then writes the response. A role can be revoked,
a share withdrawn, or a column mask tightened in the middle of that, and the
rows already in flight would be served under a policy that no longer exists.

The server guards against it by fingerprinting the policy before the read and
re-checking afterwards. This module produces the fingerprints.

WHAT IS COVERED

All of it, because a fingerprint that omits one input is worse than none — it
reports "unchanged" for a change it cannot see:

    role policy     which columns the role may read, and its row filter
    share filters   row predicates injected by a share grant
    column masks    the allowed-column list, which is the mask

Two fingerprints, because they answer different questions. ``role`` changes
when the role's own grant changes. ``effective`` changes when anything the
reader will actually apply changes — including a share filter the role knows
nothing about. The server compares the one that matches what it is protecting.

WHY CANONICAL, NOT REPR

The inputs are dicts and lists whose order is not meaningful. Hashing their
repr would make a fingerprint change when nothing did — a false conflict that
fails a page for no reason — so everything is sorted before hashing.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional

#: Bumped if the canonical form changes, so an old fingerprint never compares
#: equal to a new one computed over different material.
_FINGERPRINT_VERSION = 1


def _canonical_view(view: Any) -> Dict[str, Any]:
    """One table's effective policy, in a form that hashes stably."""
    allowed = list(getattr(view, "allowed_columns", None) or [])
    where = str(getattr(view, "where_clause", "") or "")
    return {
        # Sorted: the reader does not care about column order, so neither may
        # the fingerprint — otherwise a reordered grant reads as a change.
        "columns": sorted(str(c) for c in allowed),
        "where": where.strip(),
    }


def fingerprint_views(rbac_views: Optional[Dict[str, Any]]) -> str:
    """Fingerprint a mapping of ``alias -> RbacViewDef``.

    An empty mapping is NOT the same as no restriction, and both are
    representable: ``{}`` means "no view applies", which still hashes to a
    stable value distinct from a mapping that grants everything explicitly.
    """
    material = {
        "v": _FINGERPRINT_VERSION,
        "tables": {
            str(alias): _canonical_view(view)
            for alias, view in sorted((rbac_views or {}).items())
        },
    }
    blob = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def query_sql_policy_fingerprint(
    organization: str,
    super_name: str,
    sql: str,
    role_name: str,
    *,
    engine: Any = None,
) -> Dict[str, str]:
    """Resolve policy for this query and fingerprint it, without reading data.

    Returns ``{"role_policy_fingerprint": ..., "effective_policy_fingerprint":
    ...}``. Cheap by design — it resolves policy and stops, so the server can
    re-check after streaming without paying for a second query.

    The two differ whenever a share contributes a row filter: ``role`` is the
    grant as RBAC resolved it, ``effective`` is what the reader would actually
    apply.
    """
    from supertable.rbac.access_control import restrict_read_access
    from supertable.utils.sql_parser import SQLParser

    parser = SQLParser(super_name=super_name, query=sql, dialect="duckdb")
    tables = parser.get_table_tuples()
    physical = parser.get_physical_tables()

    role_views = restrict_read_access(
        super_name=super_name,
        organization=organization,
        role_name=role_name,
        tables=tables,
        physical_tables=physical,
    ) or {}

    role_fp = fingerprint_views(role_views)
    effective = apply_share_filters(organization, super_name, role_name,
                                    physical, dict(role_views))
    return {
        "role_policy_fingerprint": role_fp,
        "effective_policy_fingerprint": fingerprint_views(effective),
    }


def apply_share_filters(
    organization: str,
    super_name: str,
    role_name: str,
    physical_tables: Any,
    views: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge share row filters into a copy of the role's views.

    Mirrors what ``DataReader.execute`` does inline, so the fingerprint covers
    the same predicate the reader will apply. A share that contributes no
    filter leaves the views untouched, which is why an unshared table produces
    identical role and effective fingerprints.

    Deliberately tolerant: a share lookup that fails must not make the
    fingerprint fall back to the unshared one, because that would report
    "unchanged" while the reader applies a filter. It raises instead.
    """
    from supertable.data_classes import RbacViewDef

    filters = _share_row_filters(organization, super_name, role_name,
                                 physical_tables)
    if not filters:
        return views

    merged = dict(views)
    for alias, predicate in filters.items():
        if not predicate:
            continue
        existing = merged.get(alias)
        if existing is None:
            merged[alias] = RbacViewDef(allowed_columns=["*"],
                                        where_clause=predicate)
        else:
            base = str(getattr(existing, "where_clause", "") or "").strip()
            combined = f"({base}) AND ({predicate})" if base else predicate
            merged[alias] = RbacViewDef(
                allowed_columns=list(getattr(existing, "allowed_columns", ["*"])),
                where_clause=combined,
            )
    return merged


def _share_row_filters(organization: str, super_name: str, role_name: str,
                       physical_tables: Any) -> Dict[str, str]:
    """Row predicates a share grant contributes, keyed by table alias.

    A linked share carries its filter on the catalog leaf payload as
    ``_row_filter``; ``DataReader.execute`` reads it from exactly there and
    injects it as a synthetic RBAC filter. This reads the same field from the
    same place, because a fingerprint computed from a different source would
    drift from the predicate actually applied.

    A leaf that cannot be read PROPAGATES rather than being skipped. Treating
    an unreadable leaf as "no filter" would fingerprint a wider policy than the
    reader enforces — the one direction this must never fail in.
    """
    from supertable.redis_catalog import RedisCatalog

    catalog = RedisCatalog()
    out: Dict[str, str] = {}
    for td in physical_tables or []:
        leaf = catalog.get_leaf(organization, td.super_name, td.simple_name)
        payload = (leaf or {}).get("payload") if isinstance(leaf, dict) else None
        if isinstance(payload, dict):
            predicate = payload.get("_row_filter")
            if predicate and isinstance(predicate, str):
                out[td.alias] = predicate
    return out
