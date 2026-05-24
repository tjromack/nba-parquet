"""Advanced-only bulk-load — companion to ``bulk_load_season.py``.

Phase A landed the side-by-side advanced layer (raw/nba/box_scores_advanced/).
The full ``bulk_load_season.py`` runs both traditional and advanced
fetches back-to-back; when the traditional zone is already on disk
(from a prior bulk-load) you don't want to redo it. This script just
runs the advanced bulk-ingest, so re-runs / endpoint-swap recoveries
cost ~12-14 minutes instead of ~25-40.

Run:
    $env:LOCAL_OUTPUT_DIR = "$PWD\\out"
    $env:NBA_SEASON = "2025-26"
    $env:NBA_SEASON_TYPE = "Regular Season"
    python scripts/bulk_load_advanced_only.py

Idempotent — uses dynamic partition overwrite, so re-running only
replaces the partitions touched this run.
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

# Bootstrap PySpark env BEFORE importing anything that triggers a
# SparkSession. get_spark() also does this defensively now, but the
# wrapper-scripts pattern in this repo sets it at module top so the
# log output starts clean.
_HADOOP_DIR = REPO_ROOT / ".hadoop"
if _HADOOP_DIR.is_dir() and not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = str(_HADOOP_DIR)
    os.environ["PATH"] = (
        str(_HADOOP_DIR / "bin") + os.pathsep + os.environ.get("PATH", "")
    )

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from etl.ingest import ingest_advanced_box_scores_bulk  # noqa: E402
from etl.paths import is_local_mode  # noqa: E402
from etl.transform import get_spark  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bulk_load_advanced_only")


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
        "Advanced-only bulk load: season=%s type=%s destination=%s",
        season,
        season_type,
        destination,
    )

    started = time.time()
    spark = get_spark("nba-bulk-advanced-only")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        adv_path = ingest_advanced_box_scores_bulk(
            season=season,
            season_type=season_type,
            s3_bucket=s3_bucket,
            spark=spark,
        )
        elapsed = time.time() - started
        logger.info(
            "Advanced bulk-ingest complete in %.1fs at %s",
            elapsed,
            adv_path,
        )
        return 0
    except Exception:
        logger.exception("Advanced bulk load failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
