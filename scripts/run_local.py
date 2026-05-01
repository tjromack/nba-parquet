from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_HADOOP_DIR = REPO_ROOT / ".hadoop"
if _HADOOP_DIR.is_dir() and not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = str(_HADOOP_DIR)
    os.environ["PATH"] = (
        str(_HADOOP_DIR / "bin") + os.pathsep + os.environ.get("PATH", "")
    )

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from etl.ingest import ingest_box_scores  # noqa: E402
from etl.paths import is_local_mode  # noqa: E402
from etl.schema import RAW_BOX_SCORE_SCHEMA  # noqa: E402
from etl.transform import aggregate_team_game, get_spark, join_top_players  # noqa: E402
from etl.write import write_processed  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run_local")


def _resolve_ingest_date(raw: str | None) -> str:
    if raw:
        return raw
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")

    s3_bucket = os.environ.get("S3_BUCKET", "")
    season = os.environ.get("NBA_SEASON", "2025-26")
    season_type = os.environ.get("NBA_SEASON_TYPE", "Playoffs")
    ingest_date = _resolve_ingest_date(os.environ.get("NBA_INGEST_DATE"))
    local_dir = os.environ.get("LOCAL_OUTPUT_DIR", "")

    if not s3_bucket and not is_local_mode():
        logger.error(
            "Set S3_BUCKET (for S3/LocalStack) or LOCAL_OUTPUT_DIR (for local disk)"
        )
        return 2

    destination = f"local:{local_dir}" if is_local_mode() else f"s3a://{s3_bucket}"
    logger.info(
        "Starting NBA ETL: season=%s date=%s type=%s destination=%s",
        season,
        ingest_date,
        season_type,
        destination,
    )

    started = time.time()
    spark = get_spark("nba-etl-local")

    try:
        raw_path = ingest_box_scores(
            season=season,
            game_date=ingest_date,
            season_type=season_type,
            s3_bucket=s3_bucket,
            spark=spark,
        )
        logger.info("Raw written to %s", raw_path)

        raw_df = spark.read.schema(RAW_BOX_SCORE_SCHEMA).parquet(raw_path)
        raw_count = raw_df.count()

        team_game = aggregate_team_game(raw_df)
        processed = join_top_players(team_game, raw_df)
        processed_count = processed.count()

        processed_path = write_processed(processed, s3_bucket)

        elapsed = time.time() - started
        logger.info(
            "Done. raw_rows=%d processed_rows=%d output=%s elapsed=%.1fs",
            raw_count,
            processed_count,
            processed_path,
            elapsed,
        )
        return 0
    except Exception:
        logger.exception("Pipeline failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
