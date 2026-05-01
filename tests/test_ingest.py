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
