import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường bắt buộc: {name}")
    return value


def create_spark_session(app_name: str = "IOC-HaTinh-Bronze") -> SparkSession:
    """Tạo Spark session dùng Hadoop Iceberg Catalog lưu trực tiếp trên MinIO."""
    endpoint = _required_env("S3_ENDPOINT")
    access_key = _required_env("AWS_ACCESS_KEY_ID")
    secret_key = _required_env("AWS_SECRET_ACCESS_KEY")
    ssl_enabled = os.getenv("S3_USE_SSL", str(endpoint.startswith("https://"))).lower()
    ivy_dir = Path(os.getenv("SPARK_IVY_DIR", Path.cwd() / ".ivy2")).resolve()
    ivy_dir.mkdir(parents=True, exist_ok=True)
    spark_temp_dir = Path(
        os.getenv("SPARK_TEMP_DIR", Path.cwd() / ".spark-tmp")
    ).resolve()
    s3a_buffer_dir = spark_temp_dir / "s3a"
    hadoop_temp_dir = spark_temp_dir / "hadoop"
    s3a_buffer_dir.mkdir(parents=True, exist_ok=True)
    hadoop_temp_dir.mkdir(parents=True, exist_ok=True)
    s3a_buffer = s3a_buffer_dir.as_posix()
    hadoop_temp = hadoop_temp_dir.as_posix()
    local_jars_dir = Path(
        os.getenv("SPARK_LOCAL_JARS_DIR", ivy_dir / "jars")
    ).resolve()

    # Giúp launcher pip-installed PySpark tìm đúng Python trên Windows.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.buffer.dir", s3a_buffer)
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.hadoop.tmp.dir", hadoop_temp)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        # A regex containing | can be interpreted as a pipe by spark-submit.cmd.
        .config("spark.jars.ivy", str(ivy_dir))
    )

    if master := os.getenv("SPARK_MASTER"):
        builder = builder.master(master)
    local_iceberg_jars = list(local_jars_dir.glob("*iceberg-spark-runtime*.jar"))
    if local_iceberg_jars:
        # Native Windows không có winutils.exe. Đặt JAR đã cache thẳng vào
        # classpath sẽ tránh bước spark.jars.packages gọi chmod qua winutils.
        classpath = str(local_jars_dir / "*")
        builder = (
            builder.config("spark.driver.extraClassPath", classpath)
            .config("spark.executor.extraClassPath", classpath)
        )
    elif packages := os.getenv("SPARK_PACKAGES"):
        builder = builder.config("spark.jars.packages", packages)

    catalog = os.getenv("ICEBERG_CATALOG", "ioc")
    catalog_type = os.getenv("ICEBERG_CATALOG_TYPE", "hadoop").lower()
    warehouse = _required_env("ICEBERG_WAREHOUSE")
    if catalog_type != "hadoop":
        raise RuntimeError("ICEBERG_CATALOG_TYPE phải là 'hadoop'")
    if not warehouse.startswith(("s3://", "s3a://")):
        raise RuntimeError("ICEBERG_WAREHOUSE phải là đường dẫn s3:// hoặc s3a://")

    catalog_prefix = f"spark.sql.catalog.{catalog}"
    builder = (
        builder.config(catalog_prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog_prefix}.type", "hadoop")
        .config(f"{catalog_prefix}.warehouse", warehouse.rstrip("/"))
        .config(f"{catalog_prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{catalog_prefix}.s3.endpoint", endpoint)
        .config(f"{catalog_prefix}.s3.path-style-access", "true")
        .config(f"{catalog_prefix}.s3.access-key-id", access_key)
        .config(f"{catalog_prefix}.s3.secret-access-key", secret_key)
        .config(f"{catalog_prefix}.client.region", os.getenv("AWS_REGION", "us-east-1"))
    )

    return builder.getOrCreate()
