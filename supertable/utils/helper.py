# route: supertable.utils.helper
import hashlib
import secrets
import pandas as pd

from datetime import datetime, timezone
from typing import Any, Dict, Optional


def dict_keys_to_lowercase(dict_to_change: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a new dictionary with all top-level keys converted to lowercase.
    Only affects the immediate keys, not nested dictionaries.
    """
    return {key.lower(): value for key, value in dict_to_change.items()}


def collect_schema(model_df: pd.DataFrame) -> Dict[str, str]:
    """
    Collects the schema (column names and dtypes) of a Pandas DataFrame.
    Returns a dictionary mapping column name -> dtype as string.
    """
    return {col: str(model_df[col].dtype) for col in model_df.columns}


def generate_filename(alias: str, extension: str = "json") -> str:
    """
    Generates a unique filename using current UTC timestamp, a random token, and an alias.
    """
    utc_timestamp = int(datetime.now().timestamp() * 1000)
    random_token = secrets.token_hex(8)
    return f"{utc_timestamp}_{random_token}_{alias}.{extension}"


def hourly_partition_subpath(ts: Optional[datetime] = None) -> str:
    """Hive-style UTC hour partition: ``year=YYYY/month=MM/day=DD/hour=HH``.

    Used to spread immutable metadata artifacts — the tombstone deletion-vector
    and the column-stats parquet — across per-hour subdirectories instead of a
    single flat folder.  Each write emits a NEW versioned artifact (the previous
    one is retained for snapshot/version history), so under heavy write volume a
    flat ``tombstone/`` or ``stats/`` folder would accumulate hundreds of
    thousands of objects — making directory listing and per-file creation slow
    on object stores and real filesystems alike.

    These artifacts are always addressed by the full path stored in the snapshot
    metadata, so the partition is purely organisational: it needs no read-path
    change and no migration (pre-existing flat-layout files keep resolving by
    their stored paths).  The layout mirrors the audit writer's
    ``year=/month=/day=`` convention, extended to the hour.

    *ts* defaults to the current UTC time — the same UTC basis
    :func:`generate_filename` uses for its millisecond token.
    """
    dt = ts or datetime.now(timezone.utc)
    return f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}"


def generate_hash_uid(name: str) -> str:
    """
    Returns an MD5 hash of the given name.
    """
    return hashlib.md5(name.encode()).hexdigest()
