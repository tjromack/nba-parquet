"""End-to-end bulk-load of a full NBA season into the local Parquet layout.

Companion to ``run_local.py`` but built for the regular-season scale-up:
fetches every game for ``NBA_SEASON`` + ``NBA_SEASON_TYPE`` in one shot
(one ``LeagueGameLog`` call + one ``BoxScoreTraditionalV2`` per game,
respecting the 0.6s nba_api rate limit) and then re-runs the same
aggregate / features write path the daily DAG uses. Output lives under
``LOCAL_OUTPUT_DIR`` (or ``S3_BUCKET`` if set), partition layout
identical to the daily path so the two interleave cleanly under
``raw/nba/box_scores/``.

Wall-clock expectation: ~12-20 minutes for a full regular season
(~1230 games) at the 0.6s sleep cadence. Resumable in the sense that
re-running overwrites only the partitions it touches (dynamic partition
overwrite for raw + processed; features is a season-level overwrite).

Run:
    $env:LOCAL_OUTPUT_DIR = "C:\\dev\\nba-parquet\\out"
    $env:NBA_SEASON = "2025-26"
    $env:NBA_SEASON_TYPE = "Regular Season"
    python scripts/bulk_load_season.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
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

from etl.features import build_rolling_features  # noqa: E402
from etl.ingest import ingest_box_scores_bulk  # noqa: E402
from etl.paths import is_local_mode  # noqa: E402
from etl.schema import RAW_BOX_SCORE_SCHEMA  # noqa: E402
from etl.transform import aggregate_team_game, get_spark, join_top_players  # noqa: E402
from etl.write import write_features, write_processed  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bulk_load_season")


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")

    s3_bucket = os.environ.get("S3_BUCKET", "")
    season = os.environ.get("NBA_SEASON", "2025-26")
    season_type = os.environ.get("NBA_SEASON_TYPE", "Regular Season")

    if not s3_bucket and not is_local_mode():
        logger.error(
            "Set S3_BUCKET (for S3/LocalStack) or LOCAL_OUTPUT_DIR (for local disk)"
        )
        return 2

    destination = (
        f"local:{os.environ.get('LOCAL_OUTPUT_DIR', '')}"
        if is_local_mode()
        else f"s3a://{s3_bucket}"
    )
    logger.info(
        "Bulk season load: season=%s type=%s destination=%s",
        season,
        season_type,
        destination,
    )

    started = time.time()
    spark = get_spark("nba-bulk-load")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        raw_path = ingest_box_scores_bulk(
            season=season,
            season_type=season_type,
            s3_bucket=s3_bucket,
            spark=spark,
        )
        ingest_elapsed = time.time() - started
        logger.info("Raw bulk-ingest done in %.1fs at %s", ingest_elapsed, raw_path)

        raw_df = spark.read.schema(RAW_BOX_SCORE_SCHEMA).parquet(raw_path)
        raw_count = raw_df.count()
        if raw_count == 0:
            logger.warning("Raw layer empty — nothing downstream to do")
            return 0

        team_game = aggregate_team_game(raw_df)
        processed = join_top_players(team_game, raw_df)
        processed_path = write_processed(processed, s3_bucket)

        full_processed = spark.read.parquet(processed_path)
        processed_count = full_processed.count()

        features = build_rolling_features(full_processed)
        features_path = write_features(features, s3_bucket)
        features_count = spark.read.parquet(features_path).count()

        elapsed = time.time() - started
        logger.info(
            "Bulk load complete. raw_rows=%d processed_rows=%d features_rows=%d "
            "raw=%s processed=%s features=%s elapsed=%.1fs",
            raw_count,
            processed_count,
            features_count,
            raw_path,
            processed_path,
            features_path,
            elapsed,
        )
        return 0
    except Exception:
        logger.exception("Bulk load failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
