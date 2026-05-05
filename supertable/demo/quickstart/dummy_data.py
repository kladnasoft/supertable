"""Dummy datasets used by the example suite.

`get_dummy_data(ds)` returns an Arrow table for one of seven canonical fixtures.
All fixtures share a base schema (`datastream_name`, `client`, `day`,
`datetime`, `value`); fixtures 6 and 7 deliberately add/remove columns to
exercise schema evolution in the writer.

All examples write these fixtures into the SimpleTable named in
`examples.defaults.simple_name` (currently "facts").
"""
import polars as pl


_DATASETS = {
    1: {
        "datastream_name": ["First partition"] * 5,
        "client":          ["client1", "client1", "client1", "client1", "client2"],
        "day":             ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
        "datetime":        ["2023-01-01 08:00:00", "2023-01-02 08:00:00", "2023-01-03 08:00:00", "2023-01-04 08:00:00", "2023-01-05 08:00:00"],
        "value":           [11, 20, 30, 40, 50],
    },
    2: {
        "datastream_name": ["First partition"] * 5,
        "client":          ["client1"] * 5,
        "day":             ["2023-01-06", "2023-01-07", "2023-01-07", "2023-01-08", "2023-01-08"],
        "datetime":        ["2023-01-06 08:00:00", "2023-01-07 08:00:00", "2023-01-07 08:05:00", "2023-01-08 08:00:00", "2023-01-08 08:05:00"],
        "value":           [10, 20, 30, 40, 50],
    },
    3: {
        "datastream_name": ["First partition"] * 5,
        "client":          ["client1"] * 5,
        "day":             ["2023-01-05", "2023-01-05", "2023-01-06", "2023-01-06", "2023-01-06"],
        "datetime":        ["2023-01-05 08:00:00", "2023-01-05 12:00:00", "2023-01-06 08:00:00", "2023-01-06 12:00:00", "2023-01-06 16:00:00"],
        "value":           [10, 20, 30, 40, 50],
    },
    4: {
        "datastream_name": ["First partition"] * 5,
        "client":          ["client1"] * 5,
        "day":             ["2023-01-01", "2023-01-02", "2023-01-08", "2023-01-10", "2023-01-11"],
        "datetime":        ["2023-01-01 08:00:00", "2023-01-02 08:00:00", "2023-01-08 08:00:00", "2023-01-10 08:00:00", "2023-01-11 08:00:00"],
        "value":           [10, 20, 30, 40, 50],
    },
    5: {
        "datastream_name": ["Second partition"] * 5,
        "client":          ["client1"] * 5,
        "day":             ["2023-01-01", "2023-01-02", "2023-01-05", "2023-01-05", "2023-01-05"],
        "datetime":        ["2023-01-01 08:00:00", "2023-01-02 08:00:00", "2023-01-05 08:00:00", "2023-01-05 12:00:00", "2023-01-05 16:00:00"],
        "value":           [10, 20, 30, 40, 50],
    },
    # Schema evolution: adds an extra numeric column
    6: {
        "datastream_name": ["First partition"] * 5,
        "client":          ["client1"] * 5,
        "day":             ["2023-01-12", "2023-01-13", "2023-01-14", "2023-01-15", "2023-01-16"],
        "datetime":        ["2023-01-12 08:00:00", "2023-01-13 08:00:00", "2023-01-14 08:00:00", "2023-01-15 08:00:00", "2023-01-16 08:00:00"],
        "value":           [60, 70, 80, 90, 100],
        "extra":           [1.1, 2.2, 3.3, 4.4, 5.5],
    },
    # Schema evolution: removes 'value' entirely
    7: {
        "datastream_name": ["First partition"] * 5,
        "client":          ["client1"] * 5,
        "day":             ["2023-01-17", "2023-01-18", "2023-01-19", "2023-01-20", "2023-01-21"],
        "datetime":        ["2023-01-17 08:00:00", "2023-01-18 08:00:00", "2023-01-19 08:00:00", "2023-01-20 08:00:00", "2023-01-21 08:00:00"],
    },
}


def get_dummy_data(ds: int):
    """Return an Arrow table for fixture `ds` (1..7).

    All fixtures are intended to be written into the same SimpleTable
    (``examples.defaults.simple_name``); the writer handles schema evolution
    for fixtures 6 and 7.
    """
    if ds not in _DATASETS:
        raise ValueError(f"Unknown ds code: {ds}")

    df = pl.DataFrame(_DATASETS[ds]).with_columns(
        pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
    )
    print(f"Loaded dummy ds={ds}, schema={df.schema}")
    return df.to_arrow()
