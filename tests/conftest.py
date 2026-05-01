from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_HADOOP_DIR = REPO_ROOT / ".hadoop"
if _HADOOP_DIR.is_dir() and not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = str(_HADOOP_DIR)
    os.environ["PATH"] = (
        str(_HADOOP_DIR / "bin") + os.pathsep + os.environ.get("PATH", "")
    )

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

FIXTURES = REPO_ROOT / "tests" / "fixtures"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("nba-etl-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture(scope="session")
def sample_box_scores_path(fixtures_dir: Path) -> Path:
    return fixtures_dir / "sample_box_scores.csv"


@pytest.fixture()
def raw_df(spark: SparkSession, sample_box_scores_path: Path):
    from etl.schema import RAW_BOX_SCORE_SCHEMA

    return (
        spark.read.option("header", "true")
        .schema(RAW_BOX_SCORE_SCHEMA)
        .csv(str(sample_box_scores_path))
    )
