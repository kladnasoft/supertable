# route: supertable.engine.arrow_result
"""Turn streamed Arrow batches into the DataFrame callers expect.

Every read now goes through the streaming path, so this is the single place
where Arrow becomes pandas. Two conversions need help; pyarrow's defaults are
wrong for both in this context.

DECIMALS
    DuckDB types ``SUM`` of an integer as ``decimal128(38, 0)`` — exact, scale
    zero, i.e. an integer. pyarrow maps decimals to ``object`` (Python
    ``Decimal``), which is exact but useless for arithmetic, and the old
    ``fetchdf()`` path mapped them to ``float64``, which is ergonomic but wrong:

        9007199254740995   exact
        9007199254740996   via float64

    A scale-zero decimal casts to ``int64`` losslessly, which is both. Only
    above 2**63 does that fail, and then float64 is the honest fallback — the
    value is beyond exact representation either way.

DATES
    ``date32`` becomes ``object`` (``datetime.date``) by default, where the old
    path produced ``datetime64``. ``date_as_object=False`` keeps the column
    usable without changing any value.
"""

from __future__ import annotations

from typing import List, Optional

import pandas as pd
import pyarrow as pa

from supertable.config.defaults import logger


def normalize_arrow_types(table: pa.Table) -> pa.Table:
    """Cast columns whose Arrow type converts badly to pandas."""
    if table.num_columns == 0:
        return table

    fields, changed = [], False
    for field in table.schema:
        target = field.type
        if pa.types.is_decimal(field.type) and field.type.scale == 0:
            target = pa.int64()
            changed = True
        if target is not field.type:
            fields.append(pa.field(field.name, target, field.nullable))
        else:
            fields.append(field)
    if not changed:
        return table

    try:
        return table.cast(pa.schema(fields))
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
        # A sum past 2**63. Exactness is already gone at that magnitude, so
        # widen to float rather than fail the query.
        logger.debug(f"[arrow] exact int cast failed, widening to float: {e}")
        wide = [pa.field(f.name, pa.float64(), f.nullable)
                if pa.types.is_decimal(f.type) and f.type.scale == 0 else f
                for f in table.schema]
        try:
            return table.cast(pa.schema(wide))
        except Exception:
            return table          # leave it as Decimal rather than lose data


def table_to_pandas(table: pa.Table) -> pd.DataFrame:
    return normalize_arrow_types(table).to_pandas(date_as_object=False)


def batches_to_pandas(batches: List[pa.RecordBatch],
                      schema: Optional[pa.Schema] = None) -> pd.DataFrame:
    """Assemble streamed batches into one DataFrame.

    An empty result still needs its columns, which is why the schema is passed
    separately — a caller that selects nothing should get an empty frame with
    the right shape, not a shapeless one.
    """
    if not batches:
        if schema is None:
            return pd.DataFrame()
        return table_to_pandas(schema.empty_table())
    return table_to_pandas(pa.Table.from_batches(batches, schema=schema))


def materialize(handle) -> pd.DataFrame:
    """Drain a stream handle into a DataFrame, always closing it.

    This is what makes ``execute`` a thin wrapper over ``stream`` rather than a
    second implementation: there is one way to read, and buffering is just a
    consumer that keeps everything.
    """
    if handle is None:
        return pd.DataFrame()
    try:
        batches = list(handle.batches())
        return batches_to_pandas(batches, getattr(handle, "schema", None))
    finally:
        handle.close()
