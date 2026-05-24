"""Rebuild processed/ + features/ from the existing raw/ layer.

Use when the raw zones are already on disk (from prior bulk-loads or
daily catch-ups) and you want to re-derive downstream layers without
spending another ~25-40 min on API calls. Typical case: a code change
in transform / features and you want the new columns materialized over
the full history.

Reads both ``raw/nba/box_scores/`` and ``raw/nba/box_scores_advanced/``
when present; if the advanced zone is empty or missing, the join falls
through to NULL advanced columns (which the rolling-features builder
handles by leaving rolling_ortg / rolling_drtg / etc. NULL too).

Run:
    $env:LOCAL_OUTPUT_DIR = "$PWD\\out"
    python scripts/rebuild_from_raw.py
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
from etl.paths import is_local_mode, resolve_output_uri  # noqa: E402
from etl.schema import (  # noqa: E402
    RAW_BOX_SCORE_ADVANCED_SCHEMA,
    RAW_BOX_SCORE_SCHEMA,
)
from etl.transform import (  # noqa: E402
    aggregate_team_advanced,
    aggregate_team_game,
    get_spark,
    join_team_advanced,
    join_top_players,
)
from etl.write import write_features, write_processed  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("rebuild_from_raw")


def _advanced_zone_has_data(spark, advanced_path: str) -> bool:
    """Best-effort probe: does the advanced raw zone hold any rows?

    Returns False if the path doesn't exist, holds zero partitions, or
    only schema-only empty files. The transform path silently degrades
    to NULL advanced columns in that case.
    """
    try:
        adv = spark.read.schema(RAW_BOX_SCORE_ADVANCED_SCHEMA).parquet(advanced_path)
        return adv.limit(1).count() > 0
    except Exception:
        return False


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")

    s3_bucket = os.environ.get("S3_BUCKET", "")
    if not s3_bucket and not is_local_mode():
        logger.error(
            "Set S3_BUCKET (for S3/LocalStack) or LOCAL_OUTPUT_DIR (for local disk)"
        )
        return 2

    started = time.time()
    spark = get_spark("nba-rebuild-from-raw")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        raw_path = resolve_output_uri(s3_bucket, "raw/nba/box_scores")
        advanced_path = resolve_output_uri(s3_bucket, "raw/nba/box_scores_advanced")

        raw_df = spark.read.schema(RAW_BOX_SCORE_SCHEMA).parquet(raw_path)
        raw_count = raw_df.count()
        logger.info("Loaded %d raw traditional rows from %s", raw_count, raw_path)

        team_game = aggregate_team_game(raw_df)
        processed = join_top_players(team_game, raw_df)

        if _advanced_zone_has_data(spark, advanced_path):
            adv_raw = spark.read.schema(RAW_BOX_SCORE_ADVANCED_SCHEMA).parquet(
                advanced_path
            )
            adv_count = adv_raw.count()
            logger.info("Loaded %d advanced rows from %s", adv_count, advanced_path)
            team_advanced = aggregate_team_advanced(adv_raw)
            processed = join_team_advanced(processed, team_advanced)
        else:
            logger.info(
                "Advanced zone is empty/missing; processed will have NULL "
                "advanced columns. Run scripts/bulk_load_advanced_only.py "
                "to populate it."
            )
            for col in ("off_rating", "def_rating", "net_rating", "pace"):
                from pyspark.sql import functions as F

                processed = processed.withColumn(col, F.lit(None).cast("double"))

        processed_path = write_processed(processed, s3_bucket)
        full_processed = spark.read.parquet(processed_path)
        processed_count = full_processed.count()

        features = build_rolling_features(full_processed)
        features_path = write_features(features, s3_bucket)
        features_count = spark.read.parquet(features_path).count()

        elapsed = time.time() - started
        logger.info(
            "Rebuild complete. raw=%d processed=%d features=%d "
            "processed_path=%s features_path=%s elapsed=%.1fs",
            raw_count,
            processed_count,
            features_count,
            processed_path,
            features_path,
            elapsed,
        )
        return 0
    except Exception:
        logger.exception("Rebuild failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
