"""Small shared helpers for the medcenter demo: query + write round-trips."""

import polars as pl

from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.demo.medcenter.defaults import (
    organization,
    overwrite_columns_by_table,
    role_name,
    super_name,
)


def run_query(query: str) -> pl.DataFrame:
    """Execute a read against the medcenter SuperTable, raising on error."""
    reader = DataReader(
        super_name=super_name, organization=organization, query=query
    )
    df, status, message = reader.execute(
        role_name=role_name, with_scan=False, engine=engine.AUTO
    )
    if status.value != "ok":
        raise RuntimeError(f"Query failed: {message}\n{query}")
    return df


def write_df(simple_name: str, df: pl.DataFrame) -> tuple[int, int, int, int]:
    """Upsert a DataFrame into a simple table using its configured key."""
    data_writer = DataWriter(super_name, organization)
    return data_writer.write(
        role_name=role_name,
        simple_name=simple_name,
        # The writer takes Arrow, so the frame goes straight over without a
        # pandas hop — which is also what keeps a nullable integer column an
        # integer instead of silently becoming float.
        data=df.to_arrow(),
        overwrite_columns=overwrite_columns_by_table[simple_name],
    )
