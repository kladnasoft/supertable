"""Shared constants for the supertable examples.

Every example imports identifiers from this module so the whole suite operates
on a single Organization / SuperTable / SimpleTable.

The user_hash for a given user can be resolved at runtime via:

    from supertable.rbac.user_manager import UserManager
    um = UserManager(super_name=super_name, organization=organization)
    user_hash = um.get_or_create_default_user()
"""
import logging
from enum import Enum

from supertable.config import defaults

logging.getLogger("supertable").setLevel(logging.INFO)

defaults.default.IS_SHOW_TIMING = True

# --- Identity ---------------------------------------------------------------
organization = "kladna-soft"
super_name = "example"
simple_name = "facts"

# --- Roles (must match those created in 1.2. create_roles.py) ---------------
role_name = "superadmin"

# --- Write configuration ----------------------------------------------------
overwrite_columns = ["day", "client"]

# --- Staging / pipes --------------------------------------------------------
staging_name = "stage_demo"

# --- Misc -------------------------------------------------------------------
generated_data_dir = "generated_data"

# Placeholder. Examples that need a real value resolve it at runtime via
# UserManager.get_or_create_default_user(); see 2.3.2 create_pipe.py.
user_hash = ""


class MonitorType(Enum):
    """Monitoring categories used by `MonitoringReader` / `get_monitoring_logger`."""
    PLANS = "plans"
    STATS = "stats"
    METRICS = "metrics"
