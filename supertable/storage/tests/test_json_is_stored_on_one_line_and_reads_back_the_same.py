"""
``write_json`` stores a document on one line, and it reads back the same.

Every backend used to indent what it stored. Indentation takes ``json.dumps``
off its C encoder and onto the Python one, and a table's snapshot is written
on every write: for a table of 3,000 files that was 30 ms of encoding against
8 ms, and a file a fifth larger.

One function, ``json_text``, now serialises for all five backends. What is
checked here is that only the layout changed: the text parses to the document
that was given, to the same document the indented text parsed to, and each
backend stores exactly that text where and how it stored the indented one.
"""
from __future__ import annotations

import json
import math
import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.storage.local_storage import LocalStorage  # noqa: E402
from supertable.storage.storage_interface import json_text  # noqa: E402


def _snapshot(files: int) -> dict:
    """A snapshot as ``SimpleTable.update`` hands it to ``write_json``."""
    return {
        "simple_name": "orders",
        "location": "acme/lake/tables/orders",
        "snapshot_version": 4321,
        "last_updated_ms": 1795000000000,
        "previous_snapshot": "acme/lake/tables/orders/snapshots/1794999999999_ab_orders.json",
        "schema": {"id": "Int64", "name": "String", "amount": "Float64",
                   "__rowid__": "Int64", "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')"},
        "schemaString": json.dumps({"type": "struct", "fields": {"id": "Int64"}}, separators=(",", ":")),
        "resources": [
            {"file": f"acme/lake/tables/orders/data/year=2026/month=11/day=25/data_{i:06d}.parquet",
             "file_size": 12_582_912 + i, "rows": 1_100_000 + i, "columns": 5}
            for i in range(files)
        ],
        "tombstone": ["acme/lake/tables/orders/tombstone/t1.parquet"],
        "tombstone_rows": 17,
        "stats_file": "acme/lake/tables/orders/stats/s.parquet",
        "stats_rows": files * 5,
        "lineage": {"source_type": "write", "role_name": "writer", "overwrite_columns": [],
                    "delete_only": False, "incoming_rows": 100, "write_id": "0b7c"},
        "rowid_watermark": 123456789012345678,
    }


DOCUMENTS = {
    "a new table": {"simple_name": "t", "schema": [], "resources": [], "tombstone": None,
                    "tombstone_rows": 0, "stats_file": None, "stats_rows": 0},
    "a snapshot of three files": _snapshot(3),
    "a snapshot of 500 files": _snapshot(500),
    "text outside ASCII": {"emoji": "🎉", "jp": "日本語", "hu": "árvíztűrő tükörfúrógép",
                           "key ű": ["é", {"ß": "ø"}]},
    "text that needs escaping": {"quote": 'say "hi"', "backslash": "a\\b", "newline": "a\nb",
                                 "tab": "a\tb", "control": "\u0001", "slash": "a/b",
                                 "separators": "a, b: c", "braces": "{[]}"},
    "numbers": {"zero": 0, "negative": -17, "big": 2 ** 63 - 1, "bigger": 10 ** 30,
                "float": 1.5, "whole float": 2.0, "tiny": 1e-300, "huge": 1.7976931348623157e308,
                "pi": math.pi, "true": True, "false": False, "none": None},
    "empty and nested": {"a": {}, "b": [], "c": [[], {}, [{}]], "d": {"e": {"f": {"g": []}}},
                         "": "empty key"},
    "a list at the top": [1, "two", {"three": 3}],
    "the staging index of an empty area": [],
}


@pytest.mark.parametrize("name", sorted(DOCUMENTS))
class TestTheText:

    def test_it_parses_to_the_document_it_was_given(self, name):
        document = DOCUMENTS[name]
        assert json.loads(json_text(document)) == document

    def test_it_parses_to_what_the_indented_text_parsed_to(self, name):
        document = DOCUMENTS[name]
        indented = json.dumps(document, ensure_ascii=False, indent=2)
        assert json.loads(json_text(document)) == json.loads(indented)

    def test_keys_stay_in_the_order_they_were_given(self, name):
        document = DOCUMENTS[name]
        indented = json.loads(json.dumps(document, ensure_ascii=False, indent=2),
                              object_pairs_hook=list)
        assert json.loads(json_text(document), object_pairs_hook=list) == indented

    def test_it_is_one_line_and_no_longer_than_the_indented_text(self, name):
        document = DOCUMENTS[name]
        text = json_text(document)
        assert "\n" not in text
        assert len(text) <= len(json.dumps(document, ensure_ascii=False, indent=2))


class TestWhatIsKeptAsItWas:

    def test_text_outside_ascii_is_written_as_itself(self):
        assert json_text({"jp": "日本語"}) == '{"jp":"日本語"}'

    def test_a_number_that_is_not_finite_is_written_as_it_was(self):
        """Not valid JSON, and what the indented writer wrote too: the reader
        accepts it, and a snapshot that held one before must still be written."""
        for value, token in ((float("nan"), "NaN"), (float("inf"), "Infinity"),
                             (float("-inf"), "-Infinity")):
            assert json_text({"x": value}) == '{"x":%s}' % token
            assert token in json.dumps({"x": value}, ensure_ascii=False, indent=2)

    def test_what_cannot_be_serialised_is_refused_as_it_was(self):
        from datetime import datetime
        for value in (datetime(2026, 1, 1), {1, 2}, object()):
            with pytest.raises(TypeError):
                json_text({"x": value})
            with pytest.raises(TypeError):
                json.dumps({"x": value}, ensure_ascii=False, indent=2)

    def test_a_snapshot_of_many_files_is_smaller(self):
        document = _snapshot(1000)
        compact = len(json_text(document).encode("utf-8"))
        indented = len(json.dumps(document, ensure_ascii=False, indent=2).encode("utf-8"))
        assert compact < 0.9 * indented


# ---------------------------------------------------------------------------
# LocalStorage: the atomic write is the same write, with other text
# ---------------------------------------------------------------------------

@pytest.fixture
def local(tmp_path):
    return LocalStorage(), tmp_path


@pytest.mark.parametrize("name", sorted(DOCUMENTS))
def test_local_storage_stores_that_text_and_reads_the_document_back(local, name):
    storage, folder = local
    path = str(folder / "deep" / "er" / "doc.json")
    document = DOCUMENTS[name]

    storage.write_json(path, document)

    with open(path, "rb") as handle:
        assert handle.read() == json_text(document).encode("utf-8")
    assert storage.read_json(path) == document
    assert os.listdir(os.path.dirname(path)) == ["doc.json"], "no temporary file is left"


def test_local_storage_still_syncs_and_swaps_the_file_into_place(local):
    storage, folder = local
    path = str(folder / "doc.json")
    storage.write_json(path, {"v": 1})
    synced, replaced = [], []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd):
        synced.append(fd)
        return real_fsync(fd)

    def replace(source, target):
        replaced.append((os.path.basename(source).startswith(".tmp-json-"), target))
        with open(source, "rb") as handle:
            assert handle.read() == b'{"v":2}', "complete before it is swapped in"
        with open(target, "rb") as handle:
            assert handle.read() == b'{"v":1}', "the old document until the swap"
        return real_replace(source, target)

    with patch("os.fsync", fsync), patch("os.replace", replace):
        storage.write_json(path, {"v": 2})

    assert replaced == [(True, path)]
    assert len(synced) == 2, "the file, then its directory"
    assert storage.read_json(path) == {"v": 2}


def test_local_storage_keeps_the_old_document_when_the_new_one_cannot_be_serialised(local):
    storage, folder = local
    path = str(folder / "doc.json")
    storage.write_json(path, {"v": 1})

    with pytest.raises(TypeError):
        storage.write_json(path, {"v": object()})

    assert storage.read_json(path) == {"v": 1}
    assert os.listdir(folder) == ["doc.json"], "the temporary file is removed"


def test_local_storage_keeps_the_old_document_when_the_text_cannot_be_encoded(local):
    """A lone surrogate serialises and cannot be written as UTF-8."""
    storage, folder = local
    path = str(folder / "doc.json")
    storage.write_json(path, {"v": 1})

    with pytest.raises(UnicodeEncodeError):
        storage.write_json(path, {"v": "\ud800"})

    assert storage.read_json(path) == {"v": 1}
    assert os.listdir(folder) == ["doc.json"]


# ---------------------------------------------------------------------------
# The object stores: the same bytes go where the indented bytes went
# ---------------------------------------------------------------------------

DOCUMENT = DOCUMENTS["a snapshot of three files"]
EXPECTED = json_text(DOCUMENT).encode("utf-8")


def test_s3_puts_that_text_as_application_json():
    from supertable.storage.s3_storage import S3Storage
    storage = object.__new__(S3Storage)
    storage.base_prefix = "lake"
    storage.bucket_name = "bucket"
    storage._ensure_bucket_region = lambda: None
    storage._call = MagicMock()

    storage.write_json("orders/snapshots/s.json", DOCUMENT)

    storage._call.assert_called_once_with(
        "put_object", Bucket="bucket", Key="lake/orders/snapshots/s.json",
        Body=EXPECTED, ContentType="application/json")


def test_minio_puts_that_text_as_application_json():
    from supertable.storage.minio_storage import MinioStorage
    storage = object.__new__(MinioStorage)
    storage.base_prefix = "lake"
    storage.bucket_name = "bucket"
    storage.client = MagicMock()

    storage.write_json("orders/snapshots/s.json", DOCUMENT)

    (bucket, key), options = storage.client.put_object.call_args
    assert (bucket, key) == ("bucket", "lake/orders/snapshots/s.json")
    assert options["data"].getvalue() == EXPECTED
    assert options["length"] == len(EXPECTED)
    assert options["content_type"] == "application/json"


def test_azure_uploads_that_text_as_application_json():
    from supertable.storage.azure_storage import AzureBlobStorage
    storage = object.__new__(AzureBlobStorage)
    storage.base_prefix = "lake"
    storage.container = MagicMock()

    storage.write_json("orders/snapshots/s.json", DOCUMENT)

    storage.container.get_blob_client.assert_called_once_with("lake/orders/snapshots/s.json")
    (body,), options = storage.container.get_blob_client.return_value.upload_blob.call_args
    assert body == EXPECTED
    assert options["overwrite"] is True
    assert options["content_settings"].content_type == "application/json"


def test_gcs_uploads_that_text_as_application_json():
    from supertable.storage.gcp_storage import GCSStorage
    storage = object.__new__(GCSStorage)
    storage.base_prefix = "lake"
    storage.bucket = MagicMock()

    storage.write_json("orders/snapshots/s.json", DOCUMENT)

    storage.bucket.blob.assert_called_once_with("lake/orders/snapshots/s.json")
    storage.bucket.blob.return_value.upload_from_string.assert_called_once_with(
        EXPECTED, content_type="application/json")


def test_what_each_store_was_sent_reads_back_as_the_document():
    assert json.loads(EXPECTED.decode("utf-8")) == DOCUMENT
