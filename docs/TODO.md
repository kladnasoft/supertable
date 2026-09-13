# Implementation gaps

This page records follow-up work supported by executable code in the reviewed tree. It is not a reconstructed historical roadmap. The items below describe current behavior and the concrete missing behavior that would need implementation or validation.

## Query result delivery

- [ ] Correct Spark stream handoff. `SparkThriftExecutor.execute` sets `handed_to_stream=True` and returns a handle, but a bare `return` in `finally` overrides it with `None`. The shared materializer returns an empty Polars DataFrame for `None`; the reader stream API raises because it received no handle. Verify materialized and streamed results after changing the handoff. Sources: [Spark adapter](../supertable/engine/spark_thrift.py), [executor](../supertable/engine/executor.py), [materializer](../supertable/engine/arrow_result.py).
- [ ] Align OData engine selection and diagnostics with execution. `query_odata_sql_stream` accepts an engine argument but does not pass it to `DataReader.stream`; its selected-engine diagnostic can fall back to `duckdb` without actual engine telemetry. Source: [OData stream helper](../supertable/odata/stream.py).
- [ ] Make the default-limit helper compose with SQL terminators. `_ensure_sql_limit` strips a semicolon for its check but appends the new limit to the original SQL; SQL ending in `;` can become an invalid second statement. Source: [reader helper](../supertable/data_reader.py).
- [ ] Define restricted-role access to `SHOW STATS`. The command checks READ authority but returns the raw statistics artifact without applying row/column views, potentially revealing statistics outside the role's normal query scope. Source: [statistics command](../supertable/data_reader.py).

See [query engine](09_query_engine.md) and [data reader](10_data_reader.md).

## Publication and artifact lifecycle

- [ ] Define an atomic publication contract across leaf metadata, root version, and lease ownership. The leaf scripts increment/replace a single document without an expected-version comparison, ownership verification happens separately, and the root increments afterward. These are separate operations from object writes. Sources: [catalog scripts](../supertable/redis_catalog.py), [writer publication](../supertable/data_writer.py).
- [ ] Cover path-only leaf fallback for deletion and share controls. The writer can fall back to publishing only a snapshot path, while `DataReader.execute` attaches tombstones and `_row_filter` from an embedded leaf payload. That loop does not recover those controls from the snapshot file when the payload is absent. Sources: [writer fallback](../supertable/data_writer.py), [reader control lookup](../supertable/data_reader.py).
- [ ] Add an explicit artifact retention/reclamation policy if historical storage cleanup is required. Normal write/compaction paths remove resources from new snapshots but do not delete their old objects or provide a general vacuum. Whole-table deletion removes storage before catalog metadata and can partially complete on failure. Sources: [processing](../supertable/processing.py), [SimpleTable](../supertable/simple_table.py), [SuperTable](../supertable/super_table.py).
- [ ] Make artifact-read failure guarantees consistent. Some paths request required tombstone reads; generic compaction/export helpers can skip unreadable files, and `FileNotFoundError` is treated as an absent resource even by `_read_parquet_safe(required=True)`. Source: [processing reads](../supertable/processing.py).

See [data writer](06_data_writer.md), [catalog](05_redis_catalog.md), and [locking](08_locking.md).

## Mirroring and audit

- [ ] Apply native deletions when producing mirrors that must match query-visible rows. Mirror writers copy physical snapshot resources without applying native tombstone parts. Their output can therefore contain retired rows. Sources: [Parquet mirror](../supertable/mirroring/mirror_parquet.py), [Delta mirror](../supertable/mirroring/mirror_delta.py), [Iceberg mirror](../supertable/mirroring/mirror_iceberg.py).
- [ ] Distinguish interoperable Iceberg output from fallback output operationally. The Iceberg writer can fall back to custom `iceberg-lite` JSON metadata if its standard writer fails. Treating every enabled Iceberg mirror as a standard readable Iceberg table would be incorrect. Source: [Iceberg mirror](../supertable/mirroring/mirror_iceberg.py).
- [ ] Reconcile audit chain construction and verification. The logger includes event content in its batch hash, while `verify_chain_integrity` recomputes using event IDs. Verification accepts a first batch without recomputing its hash and accepts empty results. It does not establish reliable end-to-end event integrity in its current form. Sources: [audit logger](../supertable/audit/logger.py), [audit reader](../supertable/audit/reader.py), [chain helpers](../supertable/audit/chain.py).
- [ ] Define durable delivery and scheduling where required. Audit Redis and Parquet writes are independent best-effort sinks. Retention/trim and proof-related helpers do not establish a running scheduler or proof producer in this tree. Source: [audit package](../supertable/audit/__init__.py), [retention](../supertable/audit/retention.py), [consumers](../supertable/audit/consumers.py).

See [mirroring](13_mirroring.md) and [audit](12_audit.md).

## API and configuration consistency

- [ ] Decide the behavior of `force_tombstones`. Explicit compaction processes any nonempty deletion vector regardless of the argument, although the value appears in the result and lineage. Source: [DataWriter.compact](../supertable/data_writer.py).
- [ ] Reconcile snapshot schema representations. The normal schema collector returns a dictionary, but `schemaString` serializes that value as `fields` in a struct; ordinary writes also record the incoming frame rather than a union over every retained resource. Sources: [schema collector](../supertable/utils/helper.py), [snapshot update](../supertable/simple_table.py).
- [ ] Align declared Redis settings with connector behavior. The main connector does not consume the URL/username settings, forces decoded responses and strict Sentinel selection, and does not pass the direct-client SSL option to its Sentinel branch. Sources: [settings](../supertable/config/settings.py), [connector](../supertable/redis_connector.py).
- [ ] Align engine configuration with routing. AUTO uses active clusters and their byte thresholds; declared lite byte/freshness controls do not participate in that decision. The executor supplies the `lite` configuration for DuckDB execution. Sources: [runtime configuration](../supertable/engine/engine_config.py), [executor](../supertable/engine/executor.py).
- [ ] Reconcile version literals. `pyproject.toml` and `supertable.__version__` say `3.3.0`; `setup.py` declares `3.0.7`. Sources: [project metadata](../pyproject.toml), [package version](../supertable/__init__.py), [setup invocation](../setup.py).

See [configuration](02_configuration.md) and [Python SDK](15_python_sdk.md).

## Service boundaries

- [ ] Supply a stage/pipe consumer if automatic ingestion is required. `Staging` writes files/indexes and `SuperPipe` stores definitions; no stage-consuming worker exists in the active package. The `streaming` package runs SQL result jobs instead. Sources: [staging](../supertable/staging_area.py), [pipes](../supertable/super_pipe.py), [result runner](../supertable/streaming/runner.py).
- [ ] Integrate explicit scheduler startup and maintenance calls in an application that needs them. Quality exposes `start_scheduler`; monitoring partition draining and audit retention are helpers, not evidence of a process automatically invoking them. Sources: [quality scheduler](../supertable/quality/scheduler.py), [monitoring partitions](../supertable/monitoring/partitions.py), [audit retention](../supertable/audit/retention.py).

These documentation updates do not implement the changes listed here.
