import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchResult:
    batch_id: str
    source_files: int
    source_rows: int
    written_rows: int
    target_table: str


def log_batch_result(result: BatchResult) -> None:
    logger.info(
        "batch_complete batch_id=%s source_files=%d source_rows=%d written_rows=%d target=%s",
        result.batch_id,
        result.source_files,
        result.source_rows,
        result.written_rows,
        result.target_table,
    )
