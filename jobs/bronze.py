import argparse
import logging
import re
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F

from jobs.common.audit import BatchResult, log_batch_result
from jobs.common.manifest import InputBatch, resolve_input_batch
from jobs.common.minio_client import to_s3a_paths
from jobs.common.spark_session import create_spark_session

TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}$")


def read_json_files(spark: SparkSession, paths: list[str], multiline: bool = False) -> DataFrame:
    """Đọc nhiều JSON trong một lần để Spark hợp nhất schema trên toàn batch."""
    return (
        spark.read.option("mode", "PERMISSIVE")
        .option("multiLine", str(multiline).lower())
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(paths)
    )


def add_ingestion_metadata(df: DataFrame, batch: InputBatch) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.lit(datetime.now(timezone.utc)))
        .withColumn("_batch_id", F.lit(batch.batch_id))
        .withColumn("_manifest_key", F.lit(batch.manifest_key).cast("string"))
        .withColumn("_source_file", F.input_file_name())
    )


def ensure_target(spark: SparkSession, table: str, df: DataFrame) -> None:
    catalog, namespace, _ = table.split(".")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    if not spark.catalog.tableExists(table):
        (
            df.limit(0)
            .writeTo(table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )


def batch_already_loaded(spark: SparkSession, table: str, batch_id: str) -> bool:
    if not spark.catalog.tableExists(table):
        return False
    return spark.table(table).where(F.col("_batch_id") == batch_id).limit(1).count() > 0


def remove_loaded_files(
    spark: SparkSession,
    table: str,
    bucket: str,
    batch: InputBatch,
) -> InputBatch:
    """Trong prefix mode, chỉ giữ các object chưa từng được nạp vào bảng."""
    if batch.manifest_key or not spark.catalog.tableExists(table):
        return batch

    paths = to_s3a_paths(bucket, batch.keys)
    loaded = {
        row["_source_file"]
        for row in (
            spark.table(table)
            .where(F.col("_source_file").isin(paths))
            .select("_source_file")
            .distinct()
            .collect()
        )
    }
    remaining_keys = [key for key, path in zip(batch.keys, paths) if path not in loaded]
    return InputBatch(batch_id=batch.batch_id, keys=remaining_keys)


def run(
    *,
    bucket: str,
    table: str,
    manifest_key: str | None = None,
    prefix: str | None = None,
    multiline: bool = False,
) -> BatchResult:
    if not TABLE_NAME_PATTERN.fullmatch(table):
        raise ValueError("Tên bảng phải có dạng catalog.namespace.table và chỉ gồm chữ, số, dấu gạch dưới")

    batch = resolve_input_batch(bucket, manifest_key=manifest_key, prefix=prefix)
    spark = create_spark_session()
    try:
        batch = remove_loaded_files(spark, table, bucket, batch)
        if not batch.keys:
            logging.info("Không có file JSON mới bên dưới prefix: %s", prefix)
            return BatchResult(batch.batch_id, 0, 0, 0, table)

        if batch_already_loaded(spark, table, batch.batch_id):
            logging.info("Bỏ qua batch đã nạp thành công: %s", batch.batch_id)
            return BatchResult(batch.batch_id, len(batch.keys), 0, 0, table)

        raw_df = read_json_files(spark, to_s3a_paths(bucket, batch.keys), multiline)
        source_rows = raw_df.count()
        if source_rows == 0:
            raise ValueError("Batch không có bản ghi JSON nào")

        output_df = add_ingestion_metadata(raw_df, batch)
        ensure_target(spark, table, output_df)
        output_df.writeTo(table).option("mergeSchema", "true").append()

        result = BatchResult(batch.batch_id, len(batch.keys), source_rows, source_rows, table)
        log_batch_result(result)
        return result
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nạp nhiều file JSON từ MinIO vào Iceberg Bronze")
    parser.add_argument("--bucket", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--manifest-key")
    source.add_argument("--prefix")
    parser.add_argument("--table", default="ioc.bronze.raw_json")
    parser.add_argument("--multiline", action="store_true", help="Dùng khi mỗi file là JSON array/nhiều dòng")
    return parser.parse_args()


if __name__ == "__main__":
    # Windows có thể mặc định cp1252 khiến argparse/log tiếng Việt bị lỗi.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(**vars(parse_args()))
