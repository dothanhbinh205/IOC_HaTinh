from hashlib import sha256
from io import BytesIO
from unittest.mock import patch

import pytest

from jobs.common.manifest import (
    resolve_input_batch,
    validate_manifest_objects,
    verify_object,
)
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


@patch("jobs.common.manifest.create_s3_client")
def test_validate_manifest_object_size_and_sha256(create_client):
    payload = b'{"id": 1}\n'
    client = create_client.return_value
    client.head_object.return_value = {"ContentLength": len(payload)}
    client.get_object.return_value = {"Body": BytesIO(payload)}
    manifest = {
        "files": [
            {
                "key": "landing/a.json",
                "size_bytes": len(payload),
                "sha256": sha256(payload).hexdigest(),
            }
        ]
    }

    validate_manifest_objects("lakehouse", manifest)

    client.head_object.assert_called_once_with(
        Bucket="lakehouse", Key="landing/a.json"
    )
    client.get_object.assert_called_once_with(
        Bucket="lakehouse", Key="landing/a.json"
    )


@patch("jobs.common.manifest.create_s3_client")
def test_validate_manifest_rejects_wrong_size_without_downloading(create_client):
    client = create_client.return_value
    client.head_object.return_value = {"ContentLength": 10}
    manifest = {
        "files": [
            {"key": "landing/a.json", "size_bytes": 9, "sha256": "0" * 64}
        ]
    }

    with pytest.raises(ValueError, match="SIZE_MISMATCH"):
        validate_manifest_objects("lakehouse", manifest)

    client.get_object.assert_not_called()


@patch("jobs.common.manifest.create_s3_client")
def test_validate_manifest_rejects_wrong_sha256(create_client):
    payload = b'{"id": 1}\n'
    client = create_client.return_value
    client.head_object.return_value = {"ContentLength": len(payload)}
    client.get_object.return_value = {"Body": BytesIO(payload)}
    manifest = {
        "files": [
            {
                "key": "landing/a.json",
                "size_bytes": len(payload),
                "sha256": "0" * 64,
            }
        ]
    }

    with pytest.raises(ValueError, match="SHA256_MISMATCH"):
        validate_manifest_objects("lakehouse", manifest)


def test_validate_manifest_requires_integrity_fields():
    with patch("jobs.common.manifest.create_s3_client"):
        with pytest.raises(ValueError, match="key, size_bytes và sha256"):
            validate_manifest_objects("lakehouse", {"files": ["landing/a.json"]})


def test_verify_object_accepts_matching_content():
    raw = b'{"data": []}\n'
    info = {"size_bytes": len(raw), "sha256": sha256(raw).hexdigest()}
    assert verify_object(raw, info) is None


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"size_bytes": 1}, "SIZE_MISMATCH"),
        ({"sha256": "0" * 64}, "SHA256_MISMATCH"),
        ({"size_bytes": True}, "INVALID_SIZE_METADATA"),
        ({"size_bytes": 0}, "INVALID_SIZE_METADATA"),
        ({"sha256": "A" * 64}, "INVALID_SHA256_METADATA"),
        ({"sha256": "not-a-hash"}, "INVALID_SHA256_METADATA"),
    ],
)
def test_verify_object_rejects_invalid_content(change, expected):
    raw = b'{"data": []}\n'
    info = {"size_bytes": len(raw), "sha256": sha256(raw).hexdigest()}
    info.update(change)
    with pytest.raises(ValueError, match=expected):
        verify_object(raw, info)
