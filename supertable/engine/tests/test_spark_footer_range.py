import io

import pyarrow as pa
import pyarrow.parquet as pq

from supertable.engine.spark_thrift import _read_parquet_schema


class _RangeOnlyStorage:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.ranges = []

    def size(self, _path: str) -> int:
        return len(self.payload)

    def read_range(self, _path: str, offset: int, length: int, **_kwargs) -> bytes:
        self.ranges.append((offset, length))
        return self.payload[offset:offset + length]

    def read_bytes(self, _path: str) -> bytes:
        raise AssertionError("footer reader must not download the full object")


def test_spark_schema_reader_uses_bounded_ranges() -> None:
    output = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2]}), output)
    storage = _RangeOnlyStorage(output.getvalue())
    schema = _read_parquet_schema(storage, "logical", raw_key="logical")
    assert schema is not None
    assert schema.names == ["id"]
    assert storage.ranges
