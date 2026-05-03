from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from etl.paths import is_local_mode, resolve_output_uri

logger = logging.getLogger(__name__)

PROCESSED_PREFIX = "processed/nba/team_game_stats"
FEATURES_PREFIX = "features/nba/rolling_team_stats"


def write_processed(df: DataFrame, s3_bucket: str) -> str:
    if not s3_bucket and not is_local_mode():
        raise ValueError("s3_bucket is required when LOCAL_OUTPUT_DIR is not set")

    output_path = resolve_output_uri(s3_bucket, PROCESSED_PREFIX)
    (df.write.mode("overwrite").partitionBy("season", "game_date").parquet(output_path))
    logger.info("Wrote processed data to %s", output_path)
    return output_path


def write_processed_to_path(df: DataFrame, output_path: str) -> str:
    normalized = output_path.replace("\\", "/").rstrip("/")
    parts = normalized.split("/")
    if "raw" in parts:
        raise ValueError("write_processed must not write to a raw/ prefix")
    (df.write.mode("overwrite").partitionBy("season", "game_date").parquet(output_path))
    logger.info("Wrote processed data to %s", output_path)
    return output_path


def write_features(df: DataFrame, s3_bucket: str) -> str:
    if not s3_bucket and not is_local_mode():
        raise ValueError("s3_bucket is required when LOCAL_OUTPUT_DIR is not set")

    output_path = resolve_output_uri(s3_bucket, FEATURES_PREFIX)
    df.write.mode("overwrite").partitionBy("season").parquet(output_path)
    logger.info("Wrote feature data to %s", output_path)
    return output_path


def write_features_to_path(df: DataFrame, output_path: str) -> str:
    normalized = output_path.replace("\\", "/").rstrip("/")
    parts = normalized.split("/")
    if "raw" in parts or "processed" in parts:
        raise ValueError("write_features must not write to raw/ or processed/")
    df.write.mode("overwrite").partitionBy("season").parquet(output_path)
    logger.info("Wrote feature data to %s", output_path)
    return output_path
