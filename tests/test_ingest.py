from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pyspark.sql import DataFrameWriter

from etl import ingest


def _fake_game_log_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "GAME_ID": "0042500101",
                "TEAM_ID": 1610612738,
                "TEAM_ABBREVIATION": "BOS",
                "MATCHUP": "BOS vs. NYK",
                "WL": "W",
            },
            {
                "GAME_ID": "0042500101",
                "TEAM_ID": 1610612752,
                "TEAM_ABBREVIATION": "NYK",
                "MATCHUP": "NYK @ BOS",
                "WL": "L",
            },
            {
                "GAME_ID": "0042500102",
                "TEAM_ID": 1610612747,
                "TEAM_ABBREVIATION": "LAL",
                "MATCHUP": "LAL @ DEN",
                "WL": "L",
            },
            {
                "GAME_ID": "0042500102",
                "TEAM_ID": 1610612743,
                "TEAM_ABBREVIATION": "DEN",
                "MATCHUP": "DEN vs. LAL",
                "WL": "W",
            },
        ]
    )


def _fake_player_stats(game_id: str) -> pd.DataFrame:
    if game_id == "0042500101":
        rows = [
            {
                "GAME_ID": game_id,
                "TEAM_ID": 1610612738,
                "TEAM_ABBREVIATION": "BOS",
                "TEAM_CITY": "Boston",
                "PLAYER_ID": 201939,
                "PLAYER_NAME": "Jayson Tatum",
                "START_POSITION": "F",
                "MIN": "38:00",
                "PTS": 32,
                "REB": 8,
                "AST": 5,
                "STL": 2,
                "BLK": 1,
                "TO": 3,
                "PF": 2,
                "FGM": 11,
                "FGA": 22,
                "FG3M": 4,
                "FG3A": 9,
                "FTM": 6,
                "FTA": 7,
                "PLUS_MINUS": 12,
            },
            {
                "GAME_ID": game_id,
                "TEAM_ID": 1610612752,
                "TEAM_ABBREVIATION": "NYK",
                "TEAM_CITY": "New York",
                "PLAYER_ID": 202703,
                "PLAYER_NAME": "Jalen Brunson",
                "START_POSITION": "G",
                "MIN": "40:00",
                "PTS": 30,
                "REB": 4,
                "AST": 7,
                "STL": 1,
                "BLK": 0,
                "TO": 4,
                "PF": 3,
                "FGM": 10,
                "FGA": 21,
                "FG3M": 4,
                "FG3A": 9,
                "FTM": 6,
                "FTA": 8,
                "PLUS_MINUS": -9,
            },
        ]
    else:
        rows = [
            {
                "GAME_ID": game_id,
                "TEAM_ID": 1610612747,
                "TEAM_ABBREVIATION": "LAL",
                "TEAM_CITY": "Los Angeles",
                "PLAYER_ID": 1629029,
                "PLAYER_NAME": "Luka Doncic",
                "START_POSITION": "G",
                "MIN": "40:00",
                "PTS": 33,
                "REB": 7,
                "AST": 9,
                "STL": 2,
                "BLK": 0,
                "TO": 5,
                "PF": 3,
                "FGM": 11,
                "FGA": 24,
                "FG3M": 5,
                "FG3A": 12,
                "FTM": 6,
                "FTA": 8,
                "PLUS_MINUS": -4,
            },
            {
                "GAME_ID": game_id,
                "TEAM_ID": 1610612743,
                "TEAM_ABBREVIATION": "DEN",
                "TEAM_CITY": "Denver",
                "PLAYER_ID": 203999,
                "PLAYER_NAME": "Nikola Jokic",
                "START_POSITION": "C",
                "MIN": "40:00",
                "PTS": 35,
                "REB": 14,
                "AST": 11,
                "STL": 2,
                "BLK": 1,
                "TO": 3,
                "PF": 2,
                "FGM": 12,
                "FGA": 21,
                "FG3M": 2,
                "FG3A": 5,
                "FTM": 9,
                "FTA": 11,
                "PLUS_MINUS": 8,
            },
        ]
    return pd.DataFrame(rows)


@pytest.fixture()
def patched_nba_api(monkeypatch):
    fake_log_df = _fake_game_log_df()

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
    monkeypatch.setattr(ingest, "_fetch_box_score", _fake_player_stats)
    monkeypatch.setattr(ingest, "_apply_s3a_config", lambda _s: None)
    return fake_log_df


def _redirect_parquet_writes(monkeypatch, target_dir: Path) -> None:
    original_parquet = DataFrameWriter.parquet

    def patched_parquet(self, path, *args, **kwargs):
        if isinstance(path, str) and path.startswith("s3a://"):
            return original_parquet(self, str(target_dir), *args, **kwargs)
        return original_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "parquet", patched_parquet)


def test_ingest_writes_parquet_and_respects_rate_limit(
    spark, patched_nba_api, monkeypatch, tmp_path: Path
):
    sleep_mock = MagicMock()
    monkeypatch.setattr(ingest, "_rate_limit_sleep", sleep_mock)

    target_dir = tmp_path / "raw_out"
    _redirect_parquet_writes(monkeypatch, target_dir)

    s3_path = ingest.ingest_box_scores(
        season="2025-26",
        game_date="2026-04-28",
        season_type="Playoffs",
        s3_bucket="test-bucket",
        spark=spark,
    )

    assert s3_path == (
        "s3a://test-bucket/raw/nba/box_scores/season=2025/game_date=2026-04-28/"
    )
    # Two games → one sleep between them
    assert sleep_mock.call_count == 1

    df = spark.read.parquet(str(target_dir))
    assert df.count() == 4
    assert set(df.columns) >= {
        "game_id",
        "team_id",
        "player_name",
        "pts",
        "matchup",
        "wl",
        "season",
        "season_type",
    }
    matchups = {row["matchup"] for row in df.select("matchup").collect()}
    assert "BOS vs. NYK" in matchups
    assert "DEN vs. LAL" in matchups
    # Regression: nba_api returns turnovers as "TO"; schema calls it "tov".
    # Every fake player has a non-null TO, so tov must be populated for all rows.
    tovs = [row["tov"] for row in df.select("tov").collect()]
    assert all(t is not None for t in tovs), tovs
    assert sum(tovs) > 0


def test_ingest_no_games_writes_empty_dataset(spark, monkeypatch, tmp_path: Path):
    monkeypatch.setattr(ingest, "_list_game_ids", lambda *a, **k: [])
    monkeypatch.setattr(ingest, "_team_matchup_lookup", lambda *a, **k: ({}, {}))
    monkeypatch.setattr(ingest, "_apply_s3a_config", lambda _s: None)

    target_dir = tmp_path / "empty_out"
    _redirect_parquet_writes(monkeypatch, target_dir)

    s3_path = ingest.ingest_box_scores(
        season="2025-26",
        game_date="2026-04-28",
        season_type="Playoffs",
        s3_bucket="test-bucket",
        spark=spark,
    )

    assert s3_path.endswith("game_date=2026-04-28/")
    df = spark.read.parquet(str(target_dir))
    assert df.count() == 0


def test_ingest_requires_bucket(spark):
    with pytest.raises(ValueError):
        ingest.ingest_box_scores(
            season="2025-26",
            game_date="2026-04-28",
            season_type="Playoffs",
            s3_bucket="",
            spark=spark,
        )


# --------------------------------------------------------------------------
# Bulk season ingest (Phase 4b post-script: regular-season scale-up)
# --------------------------------------------------------------------------


def _bulk_season_games_df() -> pd.DataFrame:
    """4 games across 2 dates, 4 distinct teams — minimal but realistic."""
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
                "GAME_DATE": "2025-10-22",
                "TEAM_ID": 1610612747,
                "MATCHUP": "LAL @ DEN",
                "WL": "L",
            },
            {
                "GAME_ID": "0022500002",
                "GAME_DATE": "2025-10-22",
                "TEAM_ID": 1610612743,
                "MATCHUP": "DEN vs. LAL",
                "WL": "W",
            },
            {
                "GAME_ID": "0022500003",
                "GAME_DATE": "2025-10-23",
                "TEAM_ID": 1610612738,
                "MATCHUP": "BOS vs. LAL",
                "WL": "W",
            },
            {
                "GAME_ID": "0022500003",
                "GAME_DATE": "2025-10-23",
                "TEAM_ID": 1610612747,
                "MATCHUP": "LAL @ BOS",
                "WL": "L",
            },
            {
                "GAME_ID": "0022500004",
                "GAME_DATE": "2025-10-23",
                "TEAM_ID": 1610612752,
                "MATCHUP": "NYK @ DEN",
                "WL": "L",
            },
            {
                "GAME_ID": "0022500004",
                "GAME_DATE": "2025-10-23",
                "TEAM_ID": 1610612743,
                "MATCHUP": "DEN vs. NYK",
                "WL": "W",
            },
        ]
    )


_BULK_TEAM_ABBR = {
    1610612738: "BOS",
    1610612752: "NYK",
    1610612747: "LAL",
    1610612743: "DEN",
}

_BULK_GAME_TEAMS = {
    "0022500001": (1610612738, 1610612752),  # BOS, NYK
    "0022500002": (1610612747, 1610612743),  # LAL, DEN
    "0022500003": (1610612738, 1610612747),  # BOS, LAL
    "0022500004": (1610612752, 1610612743),  # NYK, DEN
}


def _bulk_player_stats(game_id: str) -> pd.DataFrame:
    home_id, away_id = _BULK_GAME_TEAMS[game_id]
    return pd.DataFrame(
        [
            {
                "GAME_ID": game_id,
                "TEAM_ID": home_id,
                "TEAM_ABBREVIATION": _BULK_TEAM_ABBR[home_id],
                "TEAM_CITY": "Home City",
                "PLAYER_ID": 100,
                "PLAYER_NAME": "Home Star",
                "MIN": "30:00",
                "PTS": 25,
                "REB": 5,
                "AST": 5,
                "STL": 1,
                "BLK": 0,
                "TO": 2,
                "PF": 2,
                "FGM": 9,
                "FGA": 18,
                "FG3M": 3,
                "FG3A": 6,
                "FTM": 4,
                "FTA": 5,
                "PLUS_MINUS": 10,
            },
            {
                "GAME_ID": game_id,
                "TEAM_ID": away_id,
                "TEAM_ABBREVIATION": _BULK_TEAM_ABBR[away_id],
                "TEAM_CITY": "Away City",
                "PLAYER_ID": 200,
                "PLAYER_NAME": "Away Star",
                "MIN": "30:00",
                "PTS": 22,
                "REB": 4,
                "AST": 6,
                "STL": 1,
                "BLK": 1,
                "TO": 3,
                "PF": 2,
                "FGM": 8,
                "FGA": 17,
                "FG3M": 2,
                "FG3A": 5,
                "FTM": 4,
                "FTA": 4,
                "PLUS_MINUS": -10,
            },
        ]
    )


@pytest.fixture()
def patched_bulk_api(monkeypatch):
    monkeypatch.setattr(
        ingest, "_list_season_games", lambda *a, **k: _bulk_season_games_df()
    )
    monkeypatch.setattr(ingest, "_fetch_box_score", _bulk_player_stats)
    monkeypatch.setattr(ingest, "_apply_s3a_config", lambda _s: None)


def test_ingest_bulk_writes_partitioned_by_date_and_respects_rate_limit(
    spark, patched_bulk_api, monkeypatch, tmp_path: Path
):
    """4 games across 2 dates -> 8 player rows in 2 partitions, with 3
    rate-limit sleeps (one between each consecutive game)."""
    sleep_mock = MagicMock()
    monkeypatch.setattr(ingest, "_rate_limit_sleep", sleep_mock)

    target_dir = tmp_path / "bulk_out"
    _redirect_parquet_writes(monkeypatch, target_dir)

    s3_path = ingest.ingest_box_scores_bulk(
        season="2025-26",
        season_type="Regular Season",
        s3_bucket="test-bucket",
        spark=spark,
    )

    assert s3_path == "s3a://test-bucket/raw/nba/box_scores/"
    # 4 games -> exactly 3 inter-call sleeps (NOT 4 — first call doesn't sleep).
    assert sleep_mock.call_count == 3

    df = spark.read.parquet(str(target_dir))
    assert df.count() == 8  # 4 games * 2 players each
    distinct_dates = {row["game_date"] for row in df.select("game_date").collect()}
    assert len(distinct_dates) == 2

    # Partition layout: season=2025/game_date=D/ subdirs on disk.
    season_dirs = list(target_dir.glob("season=*"))
    assert season_dirs
    date_dirs = list(target_dir.glob("season=*/game_date=*"))
    assert len(date_dirs) == 2


def test_ingest_bulk_empty_season_writes_nothing_calls_no_box_scores(
    spark, monkeypatch, tmp_path: Path
):
    monkeypatch.setattr(ingest, "_list_season_games", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(ingest, "_apply_s3a_config", lambda _s: None)
    fetch_mock = MagicMock()
    monkeypatch.setattr(ingest, "_fetch_box_score", fetch_mock)
    sleep_mock = MagicMock()
    monkeypatch.setattr(ingest, "_rate_limit_sleep", sleep_mock)

    target_dir = tmp_path / "bulk_empty"
    _redirect_parquet_writes(monkeypatch, target_dir)

    ingest.ingest_box_scores_bulk(
        season="2025-26",
        season_type="Regular Season",
        s3_bucket="test-bucket",
        spark=spark,
    )

    # No games -> zero API box-score fetches, zero sleeps, empty Parquet.
    assert fetch_mock.call_count == 0
    assert sleep_mock.call_count == 0
    df = spark.read.parquet(str(target_dir))
    assert df.count() == 0


def test_ingest_bulk_requires_bucket(spark):
    with pytest.raises(ValueError):
        ingest.ingest_box_scores_bulk(
            season="2025-26",
            season_type="Regular Season",
            s3_bucket="",
            spark=spark,
        )
