# route: supertable.engine.arrow_result
"""Turn streamed Arrow batches into the DataFrame callers expect.

Every read goes through the streaming path, so this is the single place where
Arrow becomes a DataFrame — a polars one. pyarrow's defaults need help in one
place; see below.

DECIMALS WITH SCALE ZERO
    DuckDB types ``SUM`` of an integer as ``decimal128(38, 0)`` — exact, scale
    zero, i.e. an integer. pyarrow maps decimals to ``object`` (Python
    ``Decimal``), which is exact but useless for arithmetic, and the old
    ``fetchdf()`` path mapped them to ``float64``, which is ergonomic but wrong:

        9007199254740995   exact
        9007199254740996   via float64

    A scale-zero decimal casts to ``int64`` losslessly, which is both. Only
    above 2**63 does that fail, and then float64 is the honest fallback — the
    value is beyond exact representation either way.

DECIMALS WITH A SCALE
    Left as float64, matching the old path. Keeping them exact would be
    defensible, but it turns an ordinary numeric column into Python ``Decimal``
    objects for every consumer, and the characterization suite seals float here.
    Only the scale-zero case is changed, because there the exact answer is also
    the ergonomic one — there is no trade to make.

Both of those problems were pandas problems. polars holds a null inside an
Int64 and has a real date type, so the coercions this module was originally
written to work around simply do not arise.
"""

from __future__ import annotations

from typing import List, Optional

import pyarrow as pa

from supertable.config.defaults import logger


def normalize_arrow_schema(schema: pa.Schema, *, for_pandas: bool = False):
    """Apply the type rule to a schema alone. Returns ``(schema, changed)``.

    Split out so the buffered and streamed paths cannot drift: a stream must
    declare its schema before it has seen any data, so it needs the rule
    without a table to apply it to, and every batch is then cast to what this
    returned. See :func:`normalize_arrow_types` for the rule itself.
    """
    fields, changed = [], False
    for field in schema:
        target = field.type
        if pa.types.is_decimal(field.type):
            if field.type.scale == 0:
                target = pa.int64()
                changed = True
            elif for_pandas:
                target = pa.float64()
                changed = True
        if target is not field.type:
            fields.append(pa.field(field.name, target, field.nullable))
        else:
            fields.append(field)
    return (pa.schema(fields) if changed else schema), changed


def normalize_arrow_types(table: pa.Table, *, for_pandas: bool = False) -> pa.Table:
    """Cast columns whose Arrow type converts badly to the target frame.

    The two targets need different things, so this is not one rule:

    scale-zero decimals become int64 for BOTH. DuckDB types an integer SUM as
    decimal128(38, 0), and an integer is what it is — int64 is exact and usable.

    scaled decimals are left alone. They were widened to float64 when the
    target could be pandas, which has no Decimal type; polars does, and for a
    money column that exactness is the entire point — float64 silently loses
    cents at scale. The ``for_pandas`` switch is kept because the rule it
    encodes is real, but nothing in the library asks for pandas any more.
    """
    if table.num_columns == 0:
        return table

    target_schema, changed = normalize_arrow_schema(table.schema, for_pandas=for_pandas)
    fields = list(target_schema)
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


def deduplicate_column_names(table: pa.Table) -> pa.Table:
    """Give repeated column names a suffix, as SQL engines do.

    Arrow permits duplicate field names and DuckDB produces them routinely —
    ``SELECT * FROM a JOIN b ON a.id = b.id`` yields two columns called ``id``.
    polars refuses to build a frame from that, so an ordinary join raised
    DuplicateError through ``execute()`` while ``stream()`` returned the rows
    unbothered: the same query, two different outcomes, depending only on how
    the caller asked for it.

    Renaming rather than dropping, because both columns carry real and
    different data. The first occurrence keeps the bare name so an unambiguous
    reference still resolves; later ones take ``_1``, ``_2``, which is what the
    pandas path did before the polars migration. A suffix that would itself
    collide is skipped rather than silently overwriting.
    """
    names = table.schema.names
    if len(set(names)) == len(names):
        return table                       # the overwhelmingly common case

    taken = set()
    renamed: List[str] = []
    for name in names:
        if name not in taken:
            renamed.append(name)
            taken.add(name)
            continue
        n = 1
        while f"{name}_{n}" in taken or f"{name}_{n}" in names:
            n += 1
        renamed.append(f"{name}_{n}")
        taken.add(f"{name}_{n}")
    logger.debug(f"[arrow] duplicate column names resolved: {names} -> {renamed}")
    return table.rename_columns(renamed)


def batches_to_polars(batches: List[pa.RecordBatch],
                      schema: Optional[pa.Schema] = None):
    """Assemble streamed batches into one polars frame.

    An empty result still needs its columns, which is why the schema is passed
    separately — a caller that selects nothing should get an empty frame with
    the right shape, not a shapeless one.
    """
    import polars as pl

    if not batches:
        if schema is None:
            return pl.DataFrame()
        return pl.from_arrow(deduplicate_column_names(
            normalize_arrow_types(schema.empty_table(), for_pandas=False)))
    table = normalize_arrow_types(pa.Table.from_batches(batches, schema=schema),
                                  for_pandas=False)
    return pl.from_arrow(deduplicate_column_names(table))


def materialize(handle):
    """Drain a stream handle into a DataFrame, always closing it.

    This is what makes ``execute`` a thin wrapper over ``stream`` rather than a
    second implementation: there is one way to read, and buffering is just a
    consumer that keeps everything.
    """
    import polars as pl

    if handle is None:
        return pl.DataFrame()
    try:
        batches = list(handle.batches())
        return batches_to_polars(batches, getattr(handle, "schema", None))
    finally:
        handle.close()
