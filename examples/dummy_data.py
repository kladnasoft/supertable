import polars as pl

def get_dummy_data(ds):
    data = None
    if ds == 1:
        data = {
            "datastream_name": [
                "First partition",
                "First partition",
                "First partition",
                "First partition",
                "First partition",
            ],
            "client": [
                "client1",
                "client1",
                "client1",
                "client1",
                "client2",
            ],
            "day": [
                "2023-01-01",
                "2023-01-02",
                "2023-01-03",
                "2023-01-04",
                "2023-01-05",
            ],
            # New datetime column
            "datetime": [
                "2023-01-01 08:00:00",
                "2023-01-02 08:00:00",
                "2023-01-03 08:00:00",
                "2023-01-04 08:00:00",
                "2023-01-05 08:00:00",
            ],
            "value": [11, 20, 30, 40, 50],
        }
    elif ds == 2:
        data = {
            "datastream_name": [
                "First partition",
                "First partition",
                "First partition",
                "First partition",
                "First partition",
            ],
            "client": [
                "client1",
                "client1",
                "client1",
                "client1",
                "client1",
            ],
            "day": [
                "2023-01-06",
                "2023-01-07",
                "2023-01-07",
                "2023-01-08",
                "2023-01-08",
            ],
            "datetime": [
                "2023-01-06 08:00:00",
                "2023-01-07 08:00:00",
                "2023-01-07 08:05:00",
                "2023-01-08 08:00:00",
                "2023-01-08 08:05:00",
            ],
            "value": [10, 20, 30, 40, 50],
        }
    elif ds == 3:
        data = {
            "datastream_name": [
                "First partition",
                "First partition",
                "First partition",
                "First partition",
                "First partition",
            ],
            "client": [
                "client1",
                "client1",
                "client1",
                "client1",
                "client1",
            ],
            "day": [
                "2023-01-05",
                "2023-01-05",
                "2023-01-06",
                "2023-01-06",
                "2023-01-06",
            ],
            "datetime": [
                "2023-01-05 08:00:00",
                "2023-01-05 12:00:00",
                "2023-01-06 08:00:00",
                "2023-01-06 12:00:00",
                "2023-01-06 16:00:00",
            ],
            "value": [10, 20, 30, 40, 50],
        }
    elif ds == 4:
        data = {
            "datastream_name": [
                "First partition",
                "First partition",
                "First partition",
                "First partition",
                "First partition",
            ],
            "client": [
                "client1",
                "client1",
                "client1",
                "client1",
                "client1",
            ],
            "day": [
                "2023-01-01",
                "2023-01-02",
                "2023-01-08",
                "2023-01-10",
                "2023-01-11",
            ],
            "datetime": [
                "2023-01-01 08:00:00",
                "2023-01-02 08:00:00",
                "2023-01-08 08:00:00",
                "2023-01-10 08:00:00",
                "2023-01-11 08:00:00",
            ],
            "value": [10, 20, 30, 40, 50],
        }
    elif ds == 5:
        data = {
            "datastream_name": [
                "Second partition",
                "Second partition",
                "Second partition",
                "Second partition",
                "Second partition",
            ],
            "client": [
                "client1",
                "client1",
                "client1",
                "client1",
                "client1",
            ],
            "day": [
                "2023-01-01",
                "2023-01-02",
                "2023-01-05",
                "2023-01-05",
                "2023-01-05",
            ],
            "datetime": [
                "2023-01-01 08:00:00",
                "2023-01-02 08:00:00",
                "2023-01-05 08:00:00",
                "2023-01-05 12:00:00",
                "2023-01-05 16:00:00",
            ],
            "value": [10, 20, 30, 40, 50],
        }

    df = pl.DataFrame(data)
    # Optionally, parse `day` and `timestamp` into proper date/time types:
    df = df.with_columns(
    #     pl.col("day").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
    )

    table_data = df.to_arrow()
    table_name = "table1" if ds in range(1, 4) else "table2"
    print("-" * 32)
    return table_name, table_data
