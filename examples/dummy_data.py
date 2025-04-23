import polars as pl

def get_dummy_data(ds):
    data = None
    if ds == 1:
        data = {
            "datastream_name": ["First partition"] * 5,
            "client":         ["client1", "client1", "client1", "client1", "client2"],
            "day":            ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
            "datetime":       ["2023-01-01 08:00:00", "2023-01-02 08:00:00", "2023-01-03 08:00:00", "2023-01-04 08:00:00", "2023-01-05 08:00:00"],
            "value":          [11, 20, 30, 40, 50],
        }
    elif ds == 2:
        data = {
            "datastream_name": ["First partition"] * 5,
            "client":         ["client1"] * 5,
            "day":            ["2023-01-06", "2023-01-07", "2023-01-07", "2023-01-08", "2023-01-08"],
            "datetime":       ["2023-01-06 08:00:00", "2023-01-07 08:00:00", "2023-01-07 08:05:00", "2023-01-08 08:00:00", "2023-01-08 08:05:00"],
            "value":          [10, 20, 30, 40, 50],
        }
    elif ds == 3:
        data = {
            "datastream_name": ["First partition"] * 5,
            "client":         ["client1"] * 5,
            "day":            ["2023-01-05", "2023-01-05", "2023-01-06", "2023-01-06", "2023-01-06"],
            "datetime":       ["2023-01-05 08:00:00", "2023-01-05 12:00:00", "2023-01-06 08:00:00", "2023-01-06 12:00:00", "2023-01-06 16:00:00"],
            "value":          [10, 20, 30, 40, 50],
        }
    elif ds == 4:
        data = {
            "datastream_name": ["First partition"] * 5,
            "client":         ["client1"] * 5,
            "day":            ["2023-01-01", "2023-01-02", "2023-01-08", "2023-01-10", "2023-01-11"],
            "datetime":       ["2023-01-01 08:00:00", "2023-01-02 08:00:00", "2023-01-08 08:00:00", "2023-01-10 08:00:00", "2023-01-11 08:00:00"],
            "value":          [10, 20, 30, 40, 50],
        }
    elif ds == 5:
        data = {
            "datastream_name": ["Second partition"] * 5,
            "client":         ["client1"] * 5,
            "day":            ["2023-01-01", "2023-01-02", "2023-01-05", "2023-01-05", "2023-01-05"],
            "datetime":       ["2023-01-01 08:00:00", "2023-01-02 08:00:00", "2023-01-05 08:00:00", "2023-01-05 12:00:00", "2023-01-05 16:00:00"],
            "value":          [10, 20, 30, 40, 50],
        }
    # New schema: adds an extra numeric column
    elif ds == 6:
        data = {
            "datastream_name": ["First partition"] * 5,
            "client":         ["client1"] * 5,
            "day":            ["2023-01-12", "2023-01-13", "2023-01-14", "2023-01-15", "2023-01-16"],
            "datetime":       ["2023-01-12 08:00:00", "2023-01-13 08:00:00", "2023-01-14 08:00:00", "2023-01-15 08:00:00", "2023-01-16 08:00:00"],
            "value":          [60, 70, 80, 90, 100],
            "extra":          [1.1, 2.2, 3.3, 4.4, 5.5],  # new column
        }
    # Schema drop: removes 'value' entirely
    elif ds == 7:
        data = {
            "datastream_name": ["First partition"] * 5,
            "client":         ["client1"] * 5,
            "day":            ["2023-01-17", "2023-01-18", "2023-01-19", "2023-01-20", "2023-01-21"],
            "datetime":       ["2023-01-17 08:00:00", "2023-01-18 08:00:00", "2023-01-19 08:00:00", "2023-01-20 08:00:00", "2023-01-21 08:00:00"],
            # 'value' column intentionally omitted
        }
    else:
        raise ValueError(f"Unknown ds code: {ds}")

    # Build DataFrame and parse datetime
    df = pl.DataFrame(data).with_columns(
        # parse 'day' if needed:
        # pl.col("day").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
    )

    # Determine which table this belongs to
    table_name = "table1" if ds in range(1, 7) else "table2"
    arrow_table = df.to_arrow()
    print(f"Loaded dummy ds={ds}, schema={df.schema}")
    return table_name, arrow_table
