# route: supertable.reflection.routes
"""
Unified route registry for the Reflection UI.

Every attach_*_routes call lives here.  common.py imports this module at
the bottom so all shared symbols (router, templates, settings, …) are
defined before any route registration runs.

Module order matters — modules that define endpoints with overlapping
path prefixes must be registered in the correct precedence order.
"""
from __future__ import annotations

import logging

from supertable.reflection.common import (
    router,
    templates,
    settings,
    redis_client,
    catalog,
    _is_authorized,
    _no_store,
    _get_provided_token,
    _fmt_ts,
    discover_pairs,
    resolve_pair,
    inject_session_into_ctx,
    get_session,
    logged_in_guard_api,
    admin_guard_api,
    is_superuser,
    list_users,
    list_roles,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tables UI + API routes
# ---------------------------------------------------------------------------

from supertable.reflection.tables import attach_tables_routes  # noqa: E402

attach_tables_routes(
    router,
    templates=templates,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    list_users=list_users,
    fmt_ts=_fmt_ts,
    catalog=catalog,
    admin_guard_api=admin_guard_api,
    get_session=get_session,
)


# ---------------------------------------------------------------------------
# Execute (SQL query) UI + API routes
# ---------------------------------------------------------------------------

from supertable.reflection.execute import attach_execute_routes  # noqa: E402

attach_execute_routes(
    router,
    templates=templates,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    get_session=get_session,
    logged_in_guard_api=logged_in_guard_api,
    admin_guard_api=admin_guard_api,
)


# ---------------------------------------------------------------------------
# Ingestion UI + API routes (staging, pipes, load, ingestion page)
# ---------------------------------------------------------------------------

from supertable.reflection.ingestion import attach_ingestion_routes  # noqa: E402

attach_ingestion_routes(
    router,
    templates=templates,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    get_session=get_session,
    is_superuser=is_superuser,
    logged_in_guard_api=logged_in_guard_api,
    admin_guard_api=admin_guard_api,
    redis_client=redis_client,
)



# ---------------------------------------------------------------------------
# SQL Engine UI (spark-thrift focused view, reuses compute CRUD)
# ---------------------------------------------------------------------------

from supertable.reflection.engine import attach_engine_routes  # noqa: E402

attach_engine_routes(
    router,
    templates=templates,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    logged_in_guard_api=logged_in_guard_api,
    admin_guard_api=admin_guard_api,
)


# ---------------------------------------------------------------------------
# RBAC Views tab (no-op stub — CRUD moved to security.py)
# ---------------------------------------------------------------------------

from supertable.reflection.rbac import attach_rbac_routes  # noqa: E402

attach_rbac_routes(
    router,
    templates=templates,
    settings=settings,
    redis_client=redis_client,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    get_session=get_session,
    admin_guard_api=admin_guard_api,
)


# ---------------------------------------------------------------------------
# Security (unified RBAC + Users page + OData endpoints)
# ---------------------------------------------------------------------------

from supertable.reflection.security import attach_security_routes  # noqa: E402

attach_security_routes(
    router,
    templates=templates,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    list_users=list_users,
    list_roles=list_roles,
    redis_client=redis_client,
    settings=settings,
    get_session=get_session,
    admin_guard_api=admin_guard_api,
)


# ---------------------------------------------------------------------------
# Monitoring (unified reads + writes monitoring page)
# ---------------------------------------------------------------------------

from supertable.reflection.monitoring import attach_monitoring_routes  # noqa: E402

attach_monitoring_routes(
    router,
    templates=templates,
    redis_client=redis_client,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    logged_in_guard_api=logged_in_guard_api,
)


# ---------------------------------------------------------------------------
# Data Quality UI + API routes
# ---------------------------------------------------------------------------

from supertable.reflection.quality.routes import attach_quality_routes  # noqa: E402

attach_quality_routes(
    router,
    templates=templates,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    logged_in_guard_api=logged_in_guard_api,
    admin_guard_api=admin_guard_api,
    catalog=catalog,
    redis_client=redis_client,
    get_session=get_session,
)
