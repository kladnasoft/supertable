import hashlib
import secrets
import pandas as pd
import pyarrow as pa

from io import StringIO
from datetime import datetime, timezone
from dateutil import parser
from typing import Any, Dict, List, Optional


def dict_keys_to_lowercase(dict_to_change: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a new dictionary with all top-level keys converted to lowercase.
    Only affects the immediate keys, not nested dictionaries.
    """
    return {key.lower(): value for key, value in dict_to_change.items()}


def format_size(size: float) -> str:
    """
    Determines a human-readable size from a size in bytes.
    """
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"  # Fallback in case it exceeds TB


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


def date_string_to_timestamp(date_string: str) -> int:
    """
    Parses a date string and returns the timestamp in seconds as an integer.
    """
    date_obj = parser.parse(date_string)
    return int(date_obj.timestamp())


def timestamp_to_iso(timestamp_ms: int) -> str:
    """
    Converts a timestamp in milliseconds to an ISO 8601 string in UTC.
    """
    timestamp_sec = timestamp_ms / 1000.0
    dt = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
    return dt.isoformat()


def generate_hash_uid(name: str) -> str:
    """
    Returns an MD5 hash of the given name.
    """
    return hashlib.md5(name.encode()).hexdigest()


def split_dataframe(df: pa.Table, chunk_size: int) -> List[pa.Table]:
    """
    Splits a PyArrow Table into a list of smaller Tables, each up to `chunk_size` rows.
    """
    num_rows = df.num_rows
    num_chunks = (num_rows + chunk_size - 1) // chunk_size
    return [
        df.slice(chunk_size * i, min(chunk_size * (i + 1), num_rows))
        for i in range(num_chunks)
    ]


def extract_schema_info(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Given a list of records (each with a 'schema' dict),
    compiles a dictionary of distinct items and their types.
    """
    schema_info: Dict[str, Any] = {}
    for entry in data:
        schema = entry.get("schema", {})
        for key, value in schema.items():
            if key not in schema_info:
                schema_info[key] = value.get("type")
    return schema_info


def convert_csv_to_parquet(
    file_path: str,
    chunksize: Optional[int] = None
) -> pa.Table:
    """
    Reads a CSV file from a local path and converts it to a PyArrow Table.
    Optionally uses a chunksize to limit memory usage for very large CSVs.
    Note that the final return is still a single in-memory Table.
    """
    if chunksize is None:
        # Simple read
        with open(file_path, "r", encoding="utf-8") as file:
            file_content = file.read()
        df = pd.read_csv(StringIO(file_content))
        return pa.Table.from_pandas(df)
    else:
        # Chunked read
        tables = []
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            tables.append(pa.Table.from_pandas(chunk))
        return pa.concat_tables(tables)
