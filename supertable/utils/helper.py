# route: supertable.utils.helper
import hashlib
import secrets
import pandas as pd

from datetime import datetime
from typing import Any, Dict


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


def generate_hash_uid(name: str) -> str:
    """
    Returns an MD5 hash of the given name.
    """
    return hashlib.md5(name.encode()).hexdigest()
