# SuperTable

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

**SuperTable — The simplest data warehouse & cataloging system.**

SuperTable is a data warehouse and cataloging platform that stores structured data on object storage (S3, MinIO, Azure Blob, GCP Cloud Storage, or local filesystem), manages metadata in Redis, and queries everything through DuckDB or Spark SQL. It is the engine behind the Data Island product line, published as the `supertable` Python package.

The platform serves three roles simultaneously. It is a storage layer that organizes data into versioned, append-only tables with snapshot isolation. It is a query engine that routes SQL to the fastest available backend (DuckDB for small-to-medium, Spark for large). And it is an operational control plane with built-in RBAC, audit logging, data quality checks, and monitoring — everything a team needs to run a production data platform without assembling a dozen tools.

---

## Install

```bash
pip install supertable

# With storage backends
pip install supertable[s3]        # Amazon S3
pip install supertable[minio]     # MinIO
pip install supertable[azure]     # Azure Blob Storage
pip install supertable[gcp]       # Google Cloud Storage
pip install supertable[all]       # All backends
```

Requires Python 3.11+, Redis 6+, and an object storage backend (MinIO, S3, Azure, GCP, or local filesystem).

---

## Write data

```python
from examples.dummy_data import get_dummy_data
from examples.defaults import super_name, role_name, simple_name, organization
from supertable.data_writer import DataWriter

overwrite_columns = ["day"]
data = get_dummy_data(1)[1]

data_writer = DataWriter(super_name=super_name, organization=organization)

columns, rows, inserted, deleted = data_writer.write(
    role_name=role_name,
    simple_name=simple_name,
    data=data,
    overwrite_columns=overwrite_columns,
    lineage="example.single_data"
)
```

The `overwrite_columns` parameter defines the upsert key — incoming rows with matching keys replace existing rows. Omit it to append without dedup. Set `delete_only=True` to remove matching rows via tombstones. The `newer_than` parameter names a timestamp column for staleness filtering — prevents replayed data from overwriting newer updates.

---

## Read data

```python
from supertable.data_reader import DataReader, engine
from examples.defaults import super_name, user_hash, simple_name, organization, role_name

query = f"select * as cnt from {super_name} where 1=1 limit 10"

dr = DataReader(super_name=super_name, organization=organization, query=query)
result = dr.execute(role_name=role_name, with_scan=False, engine=engine.SPARK_SQL)

print("-" * 52)
print("Rows: ", result[0].shape[0], ", Columns: ", result[0].shape[1], ", " , result[1], ", Message: ", result[2])
print("-" * 52)
print(dr.timer.timings)
print("-" * len(str(dr.timer.timings)))
print(dr.plan_stats.stats)
print("-" * len(str(dr.plan_stats.stats)))


dr2 = DataReader(super_name=super_name, organization=organization, query=query)
result2 = dr2.execute(role_name=role_name, with_scan=False, engine=engine.AUTO)
print("-" * 52)
print("Rows: ", result2[0].shape[0], ", Columns: ", result2[0].shape[1], ", " , result2[1], ", Message: ", result2[2])
print("-" * 52)
print(dr2.timer.timings)
print("-" * len(str(dr2.timer.timings)))
print(dr2.plan_stats.stats)
print("-" * len(str(dr2.plan_stats.stats)))
```

`DataReader.execute()` returns a tuple of `(DataFrame, Status, message)`. The `engine` parameter controls backend selection — `engine.AUTO` picks the best backend based on data size, `engine.SPARK_SQL` forces Spark, and DuckDB is used by default for small-to-medium queries. Every query is filtered through RBAC — row-level and column-level security filters are applied automatically based on the role.

---

## Configuration

SuperTable reads all configuration from environment variables. Minimum required:

```bash
export SUPERTABLE_ORGANIZATION=my-org
export SUPERTABLE_REDIS_HOST=localhost
export SUPERTABLE_REDIS_PORT=6379
export STORAGE_TYPE=MINIO
export STORAGE_ENDPOINT_URL=http://localhost:9000
export STORAGE_ACCESS_KEY=minioadmin
export STORAGE_SECRET_KEY=minioadmin123!
export STORAGE_BUCKET=supertable
```

See [Configuration & Settings](https://github.com/kladnasoft/supertable/blob/master/docs/02_configuration.md) for all 100+ variables.

---

## Storage backends

| Backend | `STORAGE_TYPE` | Key variables |
|---|---|---|
| MinIO | `MINIO` | `STORAGE_ENDPOINT_URL`, `STORAGE_ACCESS_KEY`, `STORAGE_SECRET_KEY` |
| Amazon S3 | `S3` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` |
| Azure Blob | `AZURE` | `AZURE_STORAGE_CONNECTION_STRING` or managed identity |
| GCP Storage | `GCP` | `GOOGLE_APPLICATION_CREDENTIALS` or `GCP_SA_JSON` |
| Local disk | `LOCAL` | `SUPERTABLE_HOME` (data directory) |

---

## Server components

The `supertable` package also powers the Data Island Core server components (not included in the PyPI package — use the [Docker image](https://hub.docker.com/r/kladnasoft/supertable) or clone the [repository](https://github.com/kladnasoft/supertable)):

- **WebUI** — browser admin interface for operating a SuperTable instance
- **REST API** — 82+ JSON endpoints for all data operations
- **MCP server** — AI tool integration for Claude Desktop, Cursor, and other LLM clients
- **OData server** — OData 4.0 feeds for Excel, Power BI, and other BI tools

---

## Links

- **GitHub**: [github.com/kladnasoft/supertable](https://github.com/kladnasoft/supertable)
- **Docker Hub**: [hub.docker.com/r/kladnasoft/supertable](https://hub.docker.com/r/kladnasoft/supertable)
- **Documentation**: [github.com/kladnasoft/supertable/tree/master/docs](https://github.com/kladnasoft/supertable/tree/master/docs)

---

## License

Super Table Public Use License (STPUL) v1.0

Copyright © Kladna Soft Kft. All rights reserved.
