# path: supertable/reflection/not_common.py
"""
Route registration for extracted modules (studio, jobs, vault, compute, ingestion, rbac).

All attach_*_routes calls live here so that common.py stays focused on
session/auth/home. This file is imported by common.py at the bottom.
"""
from __future__ import annotations

import logging

from supertable.reflection.common import (
    router,
    templates,
    settings,
    redis_client,
    _is_authorized,
    _no_store,
    _get_provided_token,
    discover_pairs,
    resolve_pair,
    inject_session_into_ctx,
    get_session,
    logged_in_guard_api,
    admin_guard_api,
    is_superuser,
)

logger = logging.getLogger(__name__)


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
# Studio (formerly Lab/Notebooks) UI + API routes
# ---------------------------------------------------------------------------

from supertable.reflection.studio import attach_studio_routes  # noqa: E402

attach_studio_routes(
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
# Jobs UI routes
# ---------------------------------------------------------------------------

from supertable.reflection.jobs import attach_jobs_routes  # noqa: E402

attach_jobs_routes(
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
# Connectors UI + API routes
# ---------------------------------------------------------------------------

from supertable.reflection.vault import attach_vault_routes  # noqa: E402

attach_vault_routes(
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
# Compute UI + API routes
# ---------------------------------------------------------------------------

from supertable.reflection.compute import attach_compute_routes  # noqa: E402

attach_compute_routes(
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
# RBAC Views tab
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