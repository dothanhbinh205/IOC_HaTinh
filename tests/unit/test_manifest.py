from unittest.mock import patch

import pytest

from jobs.common.manifest import resolve_input_batch
from jobs.common.minio_client import manifest_file_keys, to_s3a_paths


def test_manifest_accepts_string_and_object_entries():
    manifest = {"files": ["loading/a.json", {"key": "loading/b.JSON"}]}
    assert manifest_file_keys(manifest) == ["loading/a.json", "loading/b.JSON"]


def test_manifest_rejects_duplicate_keys():
    with pytest.raises(ValueError, match="trùng"):
        manifest_file_keys({"files": ["a.json", "a.json"]})


def test_s3a_paths():
    assert to_s3a_paths("lakehouse", ["/loading/a.json"]) == [
        "s3a://lakehouse/loading/a.json"
    ]


@patch("jobs.common.manifest.list_json_keys", return_value=["loading/a.json", "loading/nested/b.json"])
def test_prefix_resolves_all_nested_json(list_keys):
    batch = resolve_input_batch("lakehouse", prefix="loading/")
    assert batch.keys == ["loading/a.json", "loading/nested/b.json"]
    assert batch.batch_id.startswith("prefix:loading/:")
    list_keys.assert_called_once_with("lakehouse", "loading/")


def test_exactly_one_source_is_required():
    with pytest.raises(ValueError, match="một trong hai"):
        resolve_input_batch("lakehouse")
