"""Airflow DAG: nba_etl_pipeline.

Pulls the previous calendar day's NBA box scores via ``nba_api``,
aggregates them with PySpark, and writes partitioned Parquet to either
the local ``LOCAL_OUTPUT_DIR`` (dev) or an S3 bucket (prod).

Conventions enforced:
- No heavy imports (pyspark, etl.*, nba_api) at module level — Airflow
  parses this file constantly and would pay the import cost every time.
  All real work happens inside task callables.
- Each task creates and stops its own ``SparkSession`` so retries are
  isolated.
- Inter-task communication is by XCom path strings only — never
  DataFrames.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_ID = "nba_etl_pipeline"
DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def _ingest_raw(ds: str, **_: object) -> str:
    """Pull box scores for ``ds`` and write the raw Parquet snapshot."""
    from etl.ingest import ingest_box_scores
    from etl.transform import get_spark

    season = os.environ.get("NBA_SEASON", "2025-26")
    season_type = os.environ.get("NBA_SEASON_TYPE", "Playoffs")
    s3_bucket = os.environ.get("S3_BUCKET", "")

    spark = get_spark(f"nba-etl-ingest-{ds}")
    try:
        return ingest_box_scores(
            season=season,
            game_date=ds,
            season_type=season_type,
            s3_bucket=s3_bucket,
            spark=spark,
        )
    finally:
        spark.stop()


def _transform_and_aggregate(ti, ds: str, **_: object) -> str:
    """Read raw, compute team-game aggregations, write to a staging path."""
    from etl.paths import resolve_output_uri
    from etl.schema import RAW_BOX_SCORE_SCHEMA
    from etl.transform import aggregate_team_game, get_spark, join_top_players

    raw_path = ti.xcom_pull(task_ids="ingest_raw")
    if not raw_path:
        raise RuntimeError("ingest_raw did not produce a raw_path")

    s3_bucket = os.environ.get("S3_BUCKET", "")
    staging_uri = resolve_output_uri(
        s3_bucket,
        f"staging/nba/team_game_stats/run_date={ds}",
    )

    spark = get_spark(f"nba-etl-transform-{ds}")
    try:
        raw_df = spark.read.schema(RAW_BOX_SCORE_SCHEMA).parquet(raw_path)
        team_game = aggregate_team_game(raw_df)
        processed = join_top_players(team_game, raw_df)
        (
            processed.write.mode("overwrite")
            .partitionBy("season", "game_date")
            .parquet(staging_uri)
        )
        return staging_uri
    finally:
        spark.stop()


def _write_processed(ti, **_: object) -> str:
    """Promote staging output to the canonical processed/ prefix."""
    from etl.transform import get_spark
    from etl.write import write_processed

    staging_uri = ti.xcom_pull(task_ids="transform_and_aggregate")
    if not staging_uri:
        raise RuntimeError("transform_and_aggregate did not produce a staging_uri")

    s3_bucket = os.environ.get("S3_BUCKET", "")

    spark = get_spark("nba-etl-write-processed")
    try:
        df = spark.read.parquet(staging_uri)
        return write_processed(df, s3_bucket)
    finally:
        spark.stop()


def _write_features(ti, **_: object) -> str:
    """Read processed history, build 10-game rolling features, write features/."""
    from etl.features import build_rolling_features
    from etl.transform import get_spark
    from etl.write import write_features

    processed_uri = ti.xcom_pull(task_ids="write_processed")
    if not processed_uri:
        raise RuntimeError("write_processed did not produce a processed_uri")

    s3_bucket = os.environ.get("S3_BUCKET", "")

    spark = get_spark("nba-etl-features")
    try:
        # Read the *whole* processed history so the rolling window has data
        # from prior runs to look back over, not just today's slice.
        processed_df = spark.read.parquet(processed_uri)
        features_df = build_rolling_features(processed_df)
        return write_features(features_df, s3_bucket)
    finally:
        spark.stop()


def _notify_done(ti, ds: str, **_: object) -> None:
    """Log a one-line summary so the run is human-readable from the UI."""
    import logging

    logger = logging.getLogger(__name__)
    raw_path = ti.xcom_pull(task_ids="ingest_raw")
    staging_uri = ti.xcom_pull(task_ids="transform_and_aggregate")
    processed_uri = ti.xcom_pull(task_ids="write_processed")
    features_uri = ti.xcom_pull(task_ids="write_features")
    logger.info(
        "nba_etl_pipeline ds=%s raw=%s staging=%s processed=%s features=%s",
        ds,
        raw_path,
        staging_uri,
        processed_uri,
        features_uri,
    )


with DAG(
    dag_id=DAG_ID,
    description="Daily NBA box-score ETL: nba_api -> Spark -> partitioned Parquet",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nba", "etl", "spark"],
    max_active_runs=1,
) as dag:
    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=_ingest_raw,
    )

    transform_and_aggregate = PythonOperator(
        task_id="transform_and_aggregate",
        python_callable=_transform_and_aggregate,
    )

    write_processed = PythonOperator(
        task_id="write_processed",
        python_callable=_write_processed,
    )

    write_features = PythonOperator(
        task_id="write_features",
        python_callable=_write_features,
    )

    notify_done = PythonOperator(
        task_id="notify_done",
        python_callable=_notify_done,
        trigger_rule="all_done",
    )

    (
        ingest_raw
        >> transform_and_aggregate
        >> write_processed
        >> write_features
        >> notify_done
    )
