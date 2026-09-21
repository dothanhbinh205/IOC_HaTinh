import hmac
import re
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from botocore.exceptions import ClientError

from jobs.common.minio_client import (
    create_s3_client,
    list_json_keys,
    manifest_file_keys,
    read_manifest,
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
HASH_CHUNK_SIZE = 8 * 1024 * 1024


@dataclass(frozen=True)
class InputBatch:
    batch_id: str
    keys: list[str]
    manifest_key: str | None = None


def _integrity_metadata(file_info: Mapping[str, object]) -> tuple[int, str]:
    size = file_info.get("size_bytes")
    digest = file_info.get("sha256")
    if type(size) is not int or size <= 0:
        raise ValueError("INVALID_SIZE_METADATA")
    if not isinstance(digest, str) or SHA256_PATTERN.fullmatch(digest) is None:
        raise ValueError("INVALID_SHA256_METADATA")
    return size, digest


def verify_object(raw: bytes, file_info: Mapping[str, object]) -> None:
    """Kiểm kích thước và SHA-256 của một object theo manifest."""
    expected_size, expected_sha256 = _integrity_metadata(file_info)
    if len(raw) != expected_size:
        raise ValueError("SIZE_MISMATCH")
    actual_sha256 = sha256(raw).hexdigest()
    if not hmac.compare_digest(actual_sha256, expected_sha256):
        raise ValueError("SHA256_MISMATCH")


def validate_manifest_objects(bucket: str, manifest: dict[str, Any]) -> None:
    """Đối chiếu kích thước và SHA-256 của từng object với manifest.

    Mỗi phần tử ``files`` phải có dạng::

        {"key": "path/file.json", "size_bytes": 123, "sha256": "..."}

    Object được đọc theo từng chunk để không đưa toàn bộ file vào bộ nhớ.
    """
    client = create_s3_client()

    for item in manifest.get("files", []):
        if not isinstance(item, dict):
            raise ValueError(
                "Mỗi phần tử files phải là object có key, size_bytes và sha256"
            )

        key = item.get("key")
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"Manifest có key không hợp lệ: {key!r}")
        key = key.lstrip("/")
        try:
            expected_size, expected_sha256 = _integrity_metadata(item)
        except ValueError as exc:
            raise ValueError(f"{exc}: {key}") from exc

        try:
            actual_size = int(
                client.head_object(Bucket=bucket, Key=key)["ContentLength"]
            )
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(
                    f"Không tìm thấy object trong MinIO: {key}"
                ) from exc
            raise

        if actual_size != expected_size:
            raise ValueError(
                f"SIZE_MISMATCH: {key}: "
                f"manifest={expected_size}, MinIO={actual_size}"
            )

        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        digest = sha256()
        try:
            while chunk := body.read(HASH_CHUNK_SIZE):
                digest.update(chunk)
        finally:
            body.close()

        actual_sha256 = digest.hexdigest()
        if not hmac.compare_digest(actual_sha256, expected_sha256):
            raise ValueError(
                f"SHA256_MISMATCH: {key}: "
                f"manifest={expected_sha256}, MinIO={actual_sha256}"
            )


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
        validate_manifest_objects(bucket, manifest)
        batch_id = str(manifest.get("batch_id") or manifest_key)
        return InputBatch(batch_id=batch_id, keys=keys, manifest_key=manifest_key)

    keys = list_json_keys(bucket, prefix or "")
    if not keys:
        raise FileNotFoundError(f"Không có file JSON trong s3://{bucket}/{prefix}")
    fingerprint = sha256("\n".join(keys).encode("utf-8")).hexdigest()[:16]
    return InputBatch(batch_id=f"prefix:{prefix}:{fingerprint}", keys=keys)
