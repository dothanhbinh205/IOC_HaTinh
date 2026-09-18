import json
import os
from collections.abc import Iterable
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường bắt buộc: {name}")
    return value


def create_s3_client() -> BaseClient:
    """Tạo S3 client dùng được với MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=_required_env("S3_ENDPOINT"),
        aws_access_key_id=_required_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_required_env("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        use_ssl=os.getenv("S3_USE_SSL", "false").lower() == "true",
    )


def read_manifest(bucket: str, manifest_key: str) -> dict[str, Any]:
    """Đọc và kiểm tra manifest đánh dấu một batch đã upload hoàn tất."""
    response = create_s3_client().get_object(Bucket=bucket, Key=manifest_key)
    manifest = json.loads(response["Body"].read().decode("utf-8"))

    if manifest.get("status") != "COMPLETE":
        raise ValueError("Manifest chưa ở trạng thái COMPLETE")
    if not manifest.get("files"):
        raise ValueError("Manifest không có danh sách files")
    return manifest


def manifest_file_keys(manifest: dict[str, Any]) -> list[str]:
    """Hỗ trợ cả files: [\"a.json\"] và files: [{\"key\": \"a.json\"}]."""
    keys: list[str] = []
    for item in manifest.get("files", []):
        key = item if isinstance(item, str) else item.get("key")
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"Phần tử files không hợp lệ: {item!r}")
        if not key.lower().endswith(".json"):
            raise ValueError(f"Manifest chứa file không phải JSON: {key}")
        keys.append(key.lstrip("/"))

    if len(keys) != len(set(keys)):
        raise ValueError("Manifest chứa object key bị trùng")
    return keys


def list_json_keys(bucket: str, prefix: str = "") -> list[str]:
    """Liệt kê toàn bộ JSON bên dưới prefix, có phân trang và thư mục con."""
    paginator = create_s3_client().get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.lstrip("/")):
        keys.extend(
            item["Key"]
            for item in page.get("Contents", [])
            if item["Key"].lower().endswith(".json")
        )
    return sorted(keys)


def validate_objects_exist(bucket: str, keys: Iterable[str]) -> None:
    """Fail sớm nếu manifest trỏ đến object chưa tồn tại."""
    client = create_s3_client()
    missing: list[str] = []
    for key in keys:
        try:
            client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                missing.append(key)
            else:
                raise
    if missing:
        raise FileNotFoundError(f"Không tìm thấy object trong MinIO: {missing}")


def to_s3a_paths(bucket: str, keys: Iterable[str]) -> list[str]:
    return [f"s3a://{bucket}/{key.lstrip('/')}" for key in keys]
