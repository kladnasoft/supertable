"""Tiny helpers shared across a couple of quickstart scripts.

Kept separate so the numbered scripts stay focused on a single concept.
"""
from supertable.data_reader import DataReader, engine


def count_rows(super_name: str, organization: str, simple_name: str, role_name: str) -> int:
    """Return COUNT(*) for the given SimpleTable, executed via DataReader."""
    dr = DataReader(
        super_name=super_name,
        organization=organization,
        query=f"SELECT count(*) AS cnt FROM {simple_name}",
    )
    df, _, _ = dr.execute(role_name=role_name, engine=engine.AUTO)
    return int(df.iloc[0, 0]) if df.shape[0] else 0
