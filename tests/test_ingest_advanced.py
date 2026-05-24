"""Tests for the advanced box-score ingest path.

The advanced layer is a side-by-side companion to the traditional layer
(``raw/nba/box_scores_advanced/`` vs. ``raw/nba/box_scores/``). It shares
the same partition layout — ``season=YYYY/game_date=YYYY-MM-DD/`` for
the daily path, dynamic-overwrite ``season=YYYY/game_date=YYYY-MM-DD/``
for the bulk path — so the existing transform/join logic doesn't have
to special-case either zone.

Tests mirror ``test_ingest.py``'s structure: a fake ``LeagueGameLog``
plus per-game ``BoxScoreAdvancedV2`` payloads, monkey-patched into the
ingest module so no nba_api / network calls happen.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pyspark.sql import DataFrameWriter

from etl import ingest


def _fake_daily_game_log_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "GAME_ID": "0042500201",
                "TEAM_ID": 1610612738,
                "TEAM_ABBREVIATION": "BOS",
                "MATCHUP": "BOS vs. NYK",
                "WL": "W",
            },
            {
                "GAME_ID": "0042500201",
                "TEAM_ID": 1610612752,
                "TEAM_ABBREVIATION": "NYK",
                "MATCHUP": "NYK @ BOS",
                "WL": "L",
            },
        ]
    )


def _fake_advanced_player_stats(game_id: str) -> pd.DataFrame:
    """Two players (one per team) — shape matches BoxScoreAdvancedV3."""
    return pd.DataFrame(
        [
            {
                "gameId": game_id,
                "teamId": 1610612738,
                "teamTricode": "BOS",
                "teamCity": "Boston",
                "personId": 201939,
                "firstName": "Jayson",
                "familyName": "Tatum",
                "minutes": "38:00",
                "estimatedOffensiveRating": 118.5,
                "offensiveRating": 117.2,
                "estimatedDefensiveRating": 108.0,
                "defensiveRating": 109.4,
                "estimatedNetRating": 10.5,
                "netRating": 7.8,
                "assistPercentage": 0.21,
                "assistToTurnover": 1.67,
                "assistRatio": 12.4,
                "offensiveReboundPercentage": 0.04,
                "defensiveReboundPercentage": 0.18,
                "reboundPercentage": 0.11,
                "effectiveFieldGoalPercentage": 0.582,
                "trueShootingPercentage": 0.612,
                "usagePercentage": 0.288,
                "estimatedUsagePercentage": 0.291,
                "pace": 99.4,
                "estimatedPace": 99.1,
                "PIE": 0.182,
            },
            {
                "gameId": game_id,
                "teamId": 1610612752,
                "teamTricode": "NYK",
                "teamCity": "New York",
                "personId": 202703,
                "firstName": "Jalen",
                "familyName": "Brunson",
                "minutes": "40:00",
                "estimatedOffensiveRating": 112.8,
                "offensiveRating": 113.1,
                "estimatedDefensiveRating": 114.5,
                "defensiveRating": 115.7,
                "estimatedNetRating": -1.7,
                "netRating": -2.6,
                "assistPercentage": 0.34,
                "assistToTurnover": 2.0,
                "assistRatio": 19.6,
                "offensiveReboundPercentage": 0.01,
                "defensiveReboundPercentage": 0.08,
                "reboundPercentage": 0.045,
                "effectiveFieldGoalPercentage": 0.524,
                "trueShootingPercentage": 0.561,
                "usagePercentage": 0.305,
                "estimatedUsagePercentage": 0.300,
                "pace": 99.4,
                "estimatedPace": 99.1,
                "PIE": 0.151,
            },
        ]
    )


@pytest.fixture()
def patched_advanced_daily_api(monkeypatch):
    fake_log_df = _fake_daily_game_log_df()

    def fake_list_game_ids(season, season_type, game_date):
        return sorted(fake_log_df["GAME_ID"].astype(str).unique().tolist())

    def fake_lookup(season, season_type, game_date):
        matchups: dict[str, dict[int, str]] = {}
        results: dict[str, dict[int, str]] = {}
        for game_id, group in fake_log_df.groupby("GAME_ID"):
            gid = str(game_id)
            matchups[gid] = dict(zip(group["TEAM_ID"].astype(int), group["MATCHUP"]))
            results[gid] = dict(zip(group["TEAM_ID"].astype(int), group["WL"]))
        return matchups, results

    monkeypatch.setattr(ingest, "_list_game_ids", fake_list_game_ids)
    monkeypatch.setattr(ingest, "_team_matchup_lookup", fake_lookup)
    monkeypatch.setattr(
        ingest, "_fetch_advanced_box_score", _fake_advanced_player_stats
    )
    monkeypatch.setattr(ingest, "_apply_s3a_config", lambda _s: None)


def _redirect_parquet_writes(monkeypatch, target_dir: Path) -> None:
    original_parquet = DataFrameWriter.parquet

    def patched_parquet(self, path, *args, **kwargs):
        if isinstance(path, str) and path.startswith("s3a://"):
            return original_parquet(self, str(target_dir), *args, **kwargs)
        return original_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "parquet", patched_parquet)


def test_ingest_advanced_writes_to_expected_path_and_columns(
    spark, patched_advanced_daily_api, monkeypatch, tmp_path: Path
):
    sleep_mock = MagicMock()
    monkeypatch.setattr(ingest, "_rate_limit_sleep", sleep_mock)

    target_dir = tmp_path / "advanced_out"
    _redirect_parquet_writes(monkeypatch, target_dir)

    s3_path = ingest.ingest_advanced_box_scores(
        season="2025-26",
        game_date="2026-04-28",
        season_type="Playoffs",
        s3_bucket="test-bucket",
        spark=spark,
    )

    assert s3_path == (
        "s3a://test-bucket/raw/nba/box_scores_advanced/"
        "season=2025/game_date=2026-04-28/"
    )

    df = spark.read.parquet(str(target_dir))
    assert df.count() == 2
    cols = set(df.columns)
    # Critical model-feature columns must round-trip through Parquet.
    assert {"off_rating", "def_rating", "net_rating", "pace", "usg_pct", "pie"} <= cols
    # Identity columns also present so the join with traditional works.
    assert {"game_id", "team_id", "player_id", "season", "season_type"} <= cols

    # Spot-check one numeric value to catch silent type-coercion bugs:
    # PACE for the BOS row should round-trip as 99.4 (a Double).
    bos = df.filter(df["team_abbreviation"] == "BOS").collect()[0]
    assert abs(bos["pace"] - 99.4) < 1e-6
    assert abs(bos["off_rating"] - 117.2) < 1e-6
    # V3 splits firstName + familyName; the normalizer reconstructs
    # the single player_name field so the schema stays consistent
    # with the traditional zone.
    assert bos["player_name"] == "Jayson Tatum"


def test_ingest_advanced_no_games_writes_empty_dataset(
    spark, monkeypatch, tmp_path: Path
):
    monkeypatch.setattr(ingest, "_list_game_ids", lambda *a, **k: [])
    monkeypatch.setattr(ingest, "_team_matchup_lookup", lambda *a, **k: ({}, {}))
    monkeypatch.setattr(ingest, "_apply_s3a_config", lambda _s: None)
    fetch_mock = MagicMock()
    monkeypatch.setattr(ingest, "_fetch_advanced_box_score", fetch_mock)

    target_dir = tmp_path / "advanced_empty"
    _redirect_parquet_writes(monkeypatch, target_dir)

    ingest.ingest_advanced_box_scores(
        season="2025-26",
        game_date="2026-04-28",
        season_type="Playoffs",
        s3_bucket="test-bucket",
        spark=spark,
    )
    assert fetch_mock.call_count == 0
    df = spark.read.parquet(str(target_dir))
    assert df.count() == 0


def test_ingest_advanced_requires_bucket(spark):
    with pytest.raises(ValueError):
        ingest.ingest_advanced_box_scores(
            season="2025-26",
            game_date="2026-04-28",
            season_type="Playoffs",
            s3_bucket="",
            spark=spark,
        )


# --------------------------------------------------------------------------
# Bulk season ingest for the advanced layer
# --------------------------------------------------------------------------


def _bulk_advanced_season_games_df() -> pd.DataFrame:
    """4 games across 2 dates — same shape as the traditional bulk test."""
    return pd.DataFrame(
        [
            {
                "GAME_ID": "0022500001",
                "GAME_DATE": "2025-10-22",
                "TEAM_ID": 1610612738,
                "MATCHUP": "BOS vs. NYK",
                "WL": "W",
            },
            {
                "GAME_ID": "0022500001",
                "GAME_DATE": "2025-10-22",
                "TEAM_ID": 1610612752,
                "MATCHUP": "NYK @ BOS",
                "WL": "L",
            },
            {
                "GAME_ID": "0022500002",
                "GAME_DATE": "2025-10-23",
                "TEAM_ID": 1610612738,
                "MATCHUP": "BOS vs. LAL",
                "WL": "W",
            },
            {
                "GAME_ID": "0022500002",
                "GAME_DATE": "2025-10-23",
                "TEAM_ID": 1610612747,
                "MATCHUP": "LAL @ BOS",
                "WL": "L",
            },
        ]
    )


_BULK_ADV_TEAM_ABBR = {
    1610612738: "BOS",
    1610612752: "NYK",
    1610612747: "LAL",
}


_BULK_ADV_GAME_TEAMS = {
    "0022500001": (1610612738, 1610612752),
    "0022500002": (1610612738, 1610612747),
}


def _bulk_advanced_player_stats(game_id: str) -> pd.DataFrame:
    home_id, away_id = _BULK_ADV_GAME_TEAMS[game_id]
    rows = []
    for tid, marker in ((home_id, "Home"), (away_id, "Away")):
        rows.append(
            {
                "gameId": game_id,
                "teamId": tid,
                "teamTricode": _BULK_ADV_TEAM_ABBR[tid],
                "teamCity": f"{marker} City",
                "personId": 100 if marker == "Home" else 200,
                "firstName": marker,
                "familyName": "Star",
                "minutes": "30:00",
                "estimatedOffensiveRating": 115.0,
                "offensiveRating": 113.0,
                "estimatedDefensiveRating": 110.0,
                "defensiveRating": 109.0,
                "estimatedNetRating": 5.0,
                "netRating": 4.0,
                "assistPercentage": 0.2,
                "assistToTurnover": 1.5,
                "assistRatio": 14.0,
                "offensiveReboundPercentage": 0.03,
                "defensiveReboundPercentage": 0.15,
                "reboundPercentage": 0.09,
                "effectiveFieldGoalPercentage": 0.55,
                "trueShootingPercentage": 0.58,
                "usagePercentage": 0.28,
                "estimatedUsagePercentage": 0.27,
                "pace": 100.0,
                "estimatedPace": 99.8,
                "PIE": 0.15,
            }
        )
    return pd.DataFrame(rows)


@pytest.fixture()
def patched_bulk_advanced_api(monkeypatch):
    monkeypatch.setattr(
        ingest, "_list_season_games", lambda *a, **k: _bulk_advanced_season_games_df()
    )
    monkeypatch.setattr(
        ingest, "_fetch_advanced_box_score", _bulk_advanced_player_stats
    )
    monkeypatch.setattr(ingest, "_apply_s3a_config", lambda _s: None)


def test_ingest_advanced_bulk_writes_partitioned_with_sleeps(
    spark, patched_bulk_advanced_api, monkeypatch, tmp_path: Path
):
    sleep_mock = MagicMock()
    monkeypatch.setattr(ingest, "_rate_limit_sleep", sleep_mock)

    target_dir = tmp_path / "bulk_advanced_out"
    _redirect_parquet_writes(monkeypatch, target_dir)

    s3_path = ingest.ingest_advanced_box_scores_bulk(
        season="2025-26",
        season_type="Regular Season",
        s3_bucket="test-bucket",
        spark=spark,
    )

    assert s3_path == "s3a://test-bucket/raw/nba/box_scores_advanced/"
    # 2 games -> exactly 1 inter-call sleep
    assert sleep_mock.call_count == 1

    df = spark.read.parquet(str(target_dir))
    assert df.count() == 4  # 2 games * 2 players
    date_dirs = list(target_dir.glob("season=*/game_date=*"))
    assert len(date_dirs) == 2


def test_ingest_advanced_bulk_requires_bucket(spark):
    with pytest.raises(ValueError):
        ingest.ingest_advanced_box_scores_bulk(
            season="2025-26",
            season_type="Regular Season",
            s3_bucket="",
            spark=spark,
        )
