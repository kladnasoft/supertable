from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def run_probes(output: str | Path) -> list[dict[str, Any]]:
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq

    from .dataset import arrow_schemas, build_dataset

    output_dir = Path(output)
    probe_dir = output_dir / "ingest_probes"
    probe_dir.mkdir(parents=True, exist_ok=True)
    source = pa.Table.from_pylist(
        build_dataset()["events"][:8], schema=arrow_schemas()["events"]
    )
    frame = pl.from_arrow(source)
    writers = [
        (
            "polars_native_decimal",
            "Polars native Parquet writer, statistics enabled",
            lambda path: frame.write_parquet(path, compression="zstd", statistics=True),
        ),
        (
            "polars_pyarrow_decimal",
            "Polars Parquet writer with use_pyarrow=True",
            lambda path: frame.write_parquet(
                path, compression="zstd", statistics=True, use_pyarrow=True
            ),
        ),
        (
            "pyarrow_default_decimal",
            "PyArrow default decimal physical encoding",
            lambda path: pq.write_table(source, path, compression="zstd"),
        ),
        (
            "pyarrow_integer_decimal",
            "PyArrow store_decimal_as_integer=True",
            lambda path: pq.write_table(
                source, path, compression="zstd", store_decimal_as_integer=True
            ),
        ),
        (
            "polars_decimal_without_statistics",
            "Polars native Parquet writer, statistics disabled",
            lambda path: frame.write_parquet(path, compression="zstd", statistics=False),
        ),
    ]
    results: list[dict[str, Any]] = []
    for probe_id, description, write in writers:
        path = probe_dir / f"{probe_id}.parquet"
        result: dict[str, Any] = {
            "probe_id": probe_id,
            "description": description,
            "path": str(path),
            "versions": {"polars": pl.__version__, "pyarrow": pa.__version__},
            "rows": source.num_rows,
            "source_money_type": str(source.schema.field("money").type),
            "status": "error",
        }
        try:
            write(path)
            parquet = pq.ParquetFile(path)
            result["all_values_roundtrip"] = parquet.read().to_pylist() == source.to_pylist()
            result["money_type"] = str(parquet.schema_arrow.field("money").type)
            result["money_type_unchanged"] = (
                parquet.schema_arrow.field("money").type == source.schema.field("money").type
            )
            field_results: list[dict[str, Any]] = []
            failures: list[dict[str, str]] = []
            for index in range(parquet.metadata.num_columns):
                column = parquet.metadata.row_group(0).column(index)
                stat = column.statistics
                field_result: dict[str, Any] = {
                    "field": column.path_in_schema,
                    "physical_type": column.physical_type,
                    "logical_type": str(parquet.schema.column(index).logical_type),
                    "has_statistics": stat is not None,
                }
                if stat is not None:
                    field_result["has_min_max"] = stat.has_min_max
                    field_result["null_count"] = stat.null_count
                    for attribute in ("min", "max", "min_raw", "max_raw"):
                        try:
                            value = getattr(stat, attribute)
                            field_result[attribute] = (
                                {"hex": value.hex()}
                                if isinstance(value, bytes)
                                else str(value) if value is not None else None
                            )
                        except Exception as exc:
                            failure = {
                                "field": column.path_in_schema,
                                "attribute": attribute,
                                "error_type": type(exc).__name__,
                                "message": str(exc),
                            }
                            field_result[f"{attribute}_error"] = failure
                            failures.append(failure)
                field_results.append(field_result)
            result["fields"] = field_results
            result["statistics_failures"] = failures
            result["statistics_accessible"] = not failures
            result["money_physical_type"] = next(
                field["physical_type"] for field in field_results if field["field"] == "money"
            )
            result["status"] = (
                "pass"
                if result["all_values_roundtrip"]
                and result["money_type_unchanged"]
                and not failures
                else "fail"
            )
        except Exception as exc:
            result["error_type"] = type(exc).__name__
            result["message"] = str(exc)
        results.append(result)
    (output_dir / "ingest_probes.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    results = run_probes(args.output)
    print(
        json.dumps(
            [
                {
                    key: result.get(key)
                    for key in (
                        "probe_id",
                        "status",
                        "money_physical_type",
                        "all_values_roundtrip",
                        "statistics_failures",
                    )
                }
                for result in results
            ],
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
