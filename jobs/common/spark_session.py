import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường bắt buộc: {name}")
    return value


def create_spark_session(app_name: str = "IOC-HaTinh-Bronze") -> SparkSession:
    """Tạo Spark session cho MinIO và Iceberg REST/Polaris catalog."""
    endpoint = _required_env("S3_ENDPOINT")
    access_key = _required_env("AWS_ACCESS_KEY_ID")
    secret_key = _required_env("AWS_SECRET_ACCESS_KEY")
    ssl_enabled = os.getenv("S3_USE_SSL", str(endpoint.startswith("https://"))).lower()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.redaction.regex", "(?i)secret|password|token|access[.]?key|credential")
    )

    if master := os.getenv("SPARK_MASTER"):
        builder = builder.master(master)
    if packages := os.getenv("SPARK_PACKAGES"):
        builder = builder.config("spark.jars.packages", packages)

    catalog = os.getenv("ICEBERG_CATALOG", "ioc")
    polaris_uri = os.getenv("POLARIS_URI")
    warehouse = os.getenv("ICEBERG_WAREHOUSE")
    if polaris_uri and warehouse:
        catalog_prefix = f"spark.sql.catalog.{catalog}"
        builder = (
            builder.config(catalog_prefix, "org.apache.iceberg.spark.SparkCatalog")
            .config(f"{catalog_prefix}.type", "rest")
            .config(f"{catalog_prefix}.uri", polaris_uri)
            .config(f"{catalog_prefix}.warehouse", warehouse)
            .config(f"{catalog_prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"{catalog_prefix}.s3.endpoint", endpoint)
            .config(f"{catalog_prefix}.s3.path-style-access", "true")
            .config(f"{catalog_prefix}.s3.access-key-id", access_key)
            .config(f"{catalog_prefix}.s3.secret-access-key", secret_key)
            .config(f"{catalog_prefix}.client.region", os.getenv("AWS_REGION", "us-east-1"))
        )

        client_id = os.getenv("POLARIS_CLIENT_ID")
        client_secret = os.getenv("POLARIS_CLIENT_SECRET")
        if bool(client_id) != bool(client_secret):
            raise RuntimeError("Phải cấu hình đồng thời POLARIS_CLIENT_ID và POLARIS_CLIENT_SECRET")
        if client_id and client_secret:
            builder = (
                builder.config(f"{catalog_prefix}.credential", f"{client_id}:{client_secret}")
                .config(f"{catalog_prefix}.scope", os.getenv("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL"))
                .config(f"{catalog_prefix}.token-refresh-enabled", "true")
            )

    return builder.getOrCreate()
