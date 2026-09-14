from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import traceback
from typing import Any

from .models import QueryCase
from .runtime import ORG, SUPER


class _SetupFailure(RuntimeError):
    pass


def run_checks(writer, catalog, output, execute_case) -> list[dict[str, Any]]:
    import pyarrow as pa
    from supertable import SimpleTable

    table_name = "cachecheck"
    output_dir = Path(output) / "lifecycle"
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    mutations: list[dict[str, Any]] = []
    storage = writer.super_table.storage
    schema = pa.schema([("kid", pa.int64()), ("value", pa.int64()), ("revision", pa.int64())])
    columns = ("kid", "value", "revision")
    query = "SELECT kid, value, revision FROM cachecheck ORDER BY kid"
    initial = [
        (1, 10, 1),
        (2, 20, 1),
        (3, 30, 1),
        (4, 40, 1),
        (5, 50, 1),
        (6, 60, 1),
        (7, 70, 1),
        (8, 80, 1),
    ]
    after_upsert = [
        (1, 10, 1),
        (2, 220, 2),
        (3, 30, 1),
        (4, 40, 1),
        (5, 50, 1),
        (6, 660, 2),
        (7, 70, 1),
        (8, 80, 1),
    ]
    after_delete = [
        (1, 10, 1),
        (2, 220, 2),
        (4, 40, 1),
        (5, 50, 1),
        (6, 660, 2),
        (7, 70, 1),
        (8, 80, 1),
    ]
    final_rows = [
        (1, 10, 1),
        (2, 220, 2),
        (4, 40, 1),
        (5, 50, 1),
        (6, 660, 2),
        (7, 70, 1),
        (8, 80, 1),
        (9, 90, 1),
        (10, 100, 1),
    ]
    expectations = {
        "initial": initial,
        "after_upsert": after_upsert,
        "after_delete": after_delete,
        "after_append_stale_and_compaction": final_rows,
    }
    fault_number = 0

    def persist() -> None:
        (output_dir / "results.json").write_text(
            json.dumps(results, indent=2, default=str) + "\n", encoding="utf-8"
        )
        (output_dir / "mutations.json").write_text(
            json.dumps(mutations, indent=2, default=str) + "\n", encoding="utf-8"
        )

    def failure(name: str, exc: Exception, **details) -> None:
        results.append(
            {
                "case_id": "lifecycle_" + name,
                "status": "fail",
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(limit=10),
                **details,
            }
        )
        persist()

    def mutate(name: str, action) -> Any:
        try:
            result = action()
        except Exception as exc:
            mutations.append({"operation": name, "status": "fail", "error": str(exc)})
            failure("setup_" + name, exc, table=table_name)
            raise _SetupFailure(name) from exc
        mutations.append({"operation": name, "status": "pass", "result": result})
        persist()
        return result

    def write_rows(rows, overwrite=None, newer_than=None):
        arrow = pa.Table.from_pylist([dict(zip(columns, row)) for row in rows], schema=schema)
        return writer.write(
            "superadmin",
            table_name,
            arrow,
            overwrite or [],
            newer_than=newer_than,
        )

    def check(
        name: str,
        expected,
        *,
        sql=query,
        result_columns=columns,
        mode="pruned",
        error_contains=(),
        note="",
    ) -> None:
        case = QueryCase(
            case_id="lifecycle_" + name,
            category="lifecycle",
            sql=sql,
            columns=tuple(result_columns),
            expected=list(expected),
            ordered=True,
            error_contains=tuple(error_contains),
            note=note,
        )
        item: dict[str, Any] = {
            "case_id": case.case_id,
            "sql": case.sql,
            "columns": case.columns,
            "expected_rows": case.expected,
            "expected_error_contains": case.error_contains,
            "note": note,
        }
        try:
            run = execute_case(case, mode)
            item.update(status=run.get("status", "fail"), run=run)
            for key in ("error", "mismatch"):
                if key in run:
                    item[key] = run[key]
        except Exception as exc:
            item.update(
                status="fail",
                error=f"{type(exc).__name__}: {exc}",
                traceback=traceback.format_exc(limit=10),
            )
        results.append(item)
        persist()

    def publish(payload, path) -> None:
        catalog.set_leaf_payload_cas(ORG, SUPER, table_name, payload, path)
        catalog.bump_root(ORG, SUPER)

    def fault(name: str, transform, queries, *, path_only=False) -> None:
        nonlocal fault_number
        saved = deepcopy(catalog.get_leaf(ORG, SUPER, table_name))
        if not isinstance(saved, dict) or not isinstance(saved.get("payload"), dict):
            raise _SetupFailure("Fault setup requires an intact inline snapshot payload")
        saved_payload = saved["payload"]
        saved_path = saved["path"]
        try:
            fault_number += 1
            if path_only and transform is None:
                catalog.set_leaf_path_cas(ORG, SUPER, table_name, saved_path)
                catalog.bump_root(ORG, SUPER)
            else:
                payload = deepcopy(saved_payload)
                transform(payload)
                payload["snapshot_version"] = int(saved_payload.get("snapshot_version", 0)) + 10000 + fault_number
                path = str(Path(table.snapshot_dir) / f"lifecycle_fault_{name}.json")
                storage.write_json(path, payload)
                if path_only:
                    catalog.set_leaf_path_cas(ORG, SUPER, table_name, path)
                    catalog.bump_root(ORG, SUPER)
                else:
                    publish(payload, path)
            mutations.append({"operation": "inject_" + name, "status": "pass"})
            for kwargs in queries:
                check(**kwargs)
        except Exception as exc:
            failure("fault_setup_" + name, exc, table=table_name, expected_rows=final_rows)
        finally:
            try:
                publish(saved_payload, saved_path)
                mutations.append({"operation": "restore_" + name, "status": "pass"})
            except Exception as exc:
                failure("fault_restore_" + name, exc, table=table_name)
                raise _SetupFailure("Restoration failed; later table operations were skipped") from exc
            persist()

    (output_dir / "expectations.json").write_text(
        json.dumps(expectations, indent=2) + "\n", encoding="utf-8"
    )
    try:
        if catalog.leaf_exists(ORG, SUPER, table_name):
            raise _SetupFailure("Disposable cachecheck table already exists; refusing to reuse unknown rows")
        mutate(
            "configure",
            lambda: writer.configure_table(
                "superadmin",
                table_name,
                max_memory_chunk_size=64 * 1024 * 1024,
                max_overlapping_files=10000,
                max_tombstone_rows=1000000,
            ),
        )
        mutate("initial_first_chunk", lambda: write_rows(initial[:4]))
        mutate("initial_second_chunk", lambda: write_rows(initial[4:]))
        table = SimpleTable(writer.super_table, table_name, create_if_missing=False)
        check("initial_cold", initial)
        check("initial_warmed_repeat", initial)
        mutate("upsert", lambda: write_rows([(2, 220, 2), (6, 660, 2)], ["kid"], "revision"))
        check("after_upsert", after_upsert)
        mutate(
            "delete",
            lambda: writer.write(
                "superadmin", table_name, pa.table({"kid": [3]}), ["kid"], delete_only=True
            ),
        )
        check("after_delete", after_delete)
        mutate("append", lambda: write_rows([(9, 90, 1), (10, 100, 1)]))
        check("after_append", final_rows)
        stale_result = mutate(
            "stale_newer_than",
            lambda: write_rows([(2, 2222, 1), (6, 6666, 1)], ["kid"], "revision"),
        )
        if tuple(stale_result[-2:]) != (0, 0):
            results.append(
                {
                    "case_id": "lifecycle_stale_write_counts",
                    "status": "fail",
                    "mismatch": {
                        "kind": "stale_mutation_counts",
                        "expected_inserted_deleted": [0, 0],
                        "actual_write_result": stale_result,
                    },
                }
            )
        check("after_stale_rejection", final_rows)
        check(
            "aggregate_after_mutations",
            [(9, 1320, 9)],
            sql="SELECT COUNT(*) AS n, SUM(value) AS total, COUNT(DISTINCT kid) AS unique_ids FROM cachecheck",
            result_columns=("n", "total", "unique_ids"),
        )
        leaf = catalog.get_leaf(ORG, SUPER, table_name)
        if not leaf or not leaf.get("payload", {}).get("tombstone"):
            raise _SetupFailure("Mutation setup produced no tombstone pointer; fault tests would be vacuous")
        (output_dir / "before_faults_leaf.json").write_text(
            json.dumps(leaf, indent=2, default=str) + "\n", encoding="utf-8"
        )
        fault(
            "path_only_leaf",
            None,
            [
                {"name": "path_only_leaf_pruned", "expected": final_rows},
                {"name": "path_only_leaf_stream", "expected": final_rows, "mode": "stream"},
            ],
            path_only=True,
        )
        check("restored_after_path_only", final_rows)
        share_rows = [(2, 220, 2), (6, 660, 2), (10, 100, 1)]
        fault(
            "inline_share_filter",
            lambda payload: payload.update(_row_filter="value >= 100"),
            [
                {"name": "inline_share_filter_pruned", "expected": share_rows},
                {"name": "inline_share_filter_stream", "expected": share_rows, "mode": "stream"},
            ],
        )
        fault(
            "path_only_share_filter",
            lambda payload: payload.update(_row_filter="value >= 100"),
            [
                {
                    "name": "path_only_share_filter_pruned",
                    "expected": share_rows,
                    "note": "Path-only snapshot loading must retain the share row filter and deletion vector.",
                },
                {
                    "name": "path_only_share_filter_stream",
                    "expected": share_rows,
                    "mode": "stream",
                },
            ],
            path_only=True,
        )
        missing_stats = str(Path(table.simple_dir) / "stats" / "lifecycle_missing_stats.parquet")
        corrupt_stats = str(Path(table.simple_dir) / "stats" / "lifecycle_corrupt_stats.parquet")
        missing_tombstone = str(Path(table.simple_dir) / "tombstone" / "lifecycle_missing_tombstone.parquet")
        missing_data = str(Path(table.data_dir) / "lifecycle_missing_data.parquet")
        for path in (missing_stats, missing_tombstone, missing_data):
            if storage.exists(path):
                raise _SetupFailure(f"Expected a nonexistent disposable fault path: {path}")
        filtered = final_rows[1:]
        stats_sql = "SELECT kid, value, revision FROM cachecheck WHERE kid >= 2 ORDER BY kid"
        fault(
            "missing_statistics",
            lambda payload: payload.update(stats_file=missing_stats),
            [
                {"name": "missing_stats_fallback", "sql": stats_sql, "expected": filtered},
                {"name": "missing_stats_stream", "sql": stats_sql, "expected": filtered, "mode": "stream"},
            ],
        )
        mutate("create_corrupt_statistics", lambda: storage.write_bytes(corrupt_stats, b"invalid parquet footer for lifecycle probe"))
        fault(
            "unreadable_statistics",
            lambda payload: payload.update(stats_file=corrupt_stats),
            [{"name": "unreadable_stats_fallback", "sql": stats_sql, "expected": filtered}],
        )
        fault(
            "missing_tombstone",
            lambda payload: payload.update(tombstone=[missing_tombstone]),
            [
                {
                    "name": "missing_tombstone_rejected",
                    "expected": [],
                    "result_columns": (),
                    "error_contains": (f'No files found that match the pattern "{missing_tombstone}"',),
                    "note": "A missing deletion vector must fail rather than expose superseded or deleted rows.",
                },
                {
                    "name": "missing_tombstone_stream_rejected",
                    "expected": [],
                    "result_columns": (),
                    "mode": "stream",
                    "error_contains": (f'No files found that match the pattern "{missing_tombstone}"',),
                },
            ],
        )

        def replace_resource(payload):
            resources = deepcopy(payload["resources"])
            if not resources:
                raise _SetupFailure("No resource exists to replace with a nonexistent path")
            resources[0]["file"] = missing_data
            payload["resources"] = resources

        fault(
            "missing_datafile",
            replace_resource,
            [
                {
                    "name": "missing_datafile_rejected",
                    "expected": [],
                    "result_columns": (),
                    "error_contains": (f'No files found that match the pattern "{missing_data}"',),
                    "note": "A missing selected resource must fail rather than return an incomplete relation.",
                },
                {
                    "name": "missing_datafile_stream_rejected",
                    "expected": [],
                    "result_columns": (),
                    "mode": "stream",
                    "error_contains": (f'No files found that match the pattern "{missing_data}"',),
                },
            ],
        )
        mutate("compact", lambda: writer.compact("superadmin", table_name, small_only=False))
        check("after_compaction", final_rows)
        check("after_compaction_stream", final_rows, mode="stream")
        check("after_compaction_warmed_repeat", final_rows)
    except _SetupFailure as exc:
        if not results or results[-1].get("status") != "fail" or not results[-1].get("case_id", "").startswith("lifecycle_setup_"):
            failure("setup_aborted", exc, table=table_name, expected_rows=final_rows)
    except Exception as exc:
        failure("setup_unexpected_error", exc, table=table_name, expected_rows=final_rows)
    finally:
        persist()
    return results
