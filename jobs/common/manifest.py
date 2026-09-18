from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from jobs.common.minio_client import (
    list_json_keys,
    manifest_file_keys,
    read_manifest,
    validate_objects_exist,
)


@dataclass(frozen=True)
class InputBatch:
    batch_id: str
    keys: list[str]
    manifest_key: str | None = None


def resolve_input_batch(
    bucket: str,
    *,
    manifest_key: str | None = None,
    prefix: str | None = None,
) -> InputBatch:
    """Lấy đúng danh sách JSON từ manifest hoặc toàn bộ JSON trong prefix."""
    if bool(manifest_key) == bool(prefix):
        raise ValueError("Chỉ truyền một trong hai: manifest_key hoặc prefix")

    if manifest_key:
        manifest: dict[str, Any] = read_manifest(bucket, manifest_key)
        keys = manifest_file_keys(manifest)
        validate_objects_exist(bucket, keys)
        batch_id = str(manifest.get("batch_id") or manifest_key)
        return InputBatch(batch_id=batch_id, keys=keys, manifest_key=manifest_key)

    keys = list_json_keys(bucket, prefix or "")
    if not keys:
        raise FileNotFoundError(f"Không có file JSON trong s3://{bucket}/{prefix}")
    fingerprint = sha256("\n".join(keys).encode("utf-8")).hexdigest()[:16]
    return InputBatch(batch_id=f"prefix:{prefix}:{fingerprint}", keys=keys)
