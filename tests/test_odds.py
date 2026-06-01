"""Tests for the sportsbook odds ingestion path.

The Odds API response shape is the contract this module is gluing
to: a list of game objects, each with nested bookmakers -> markets ->
outcomes. We normalize that into a long-format table where every
(game, sportsbook, market, outcome) is one row, partitioned by
``game_date`` (NBA-canonical US/Eastern date).

All tests mock the API entirely — zero network calls. The fixture
mirrors a real Odds API response shape from their public docs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark.sql import DataFrameWriter

from etl import odds


def _fake_odds_response() -> list[dict]:
    """Two-game payload that mirrors The Odds API v4 response shape.

    Game A: Finals Game 1 (OKC -6.5 home vs IND), three bookmakers
    Game B: hypothetical second game, two bookmakers, h2h only

    Designed so the normalizer produces a predictable row count:
      A: 3 books * (h2h=2 outcomes + spreads=2 + totals=2) = 18 rows
      B: 2 books * (h2h=2 outcomes) = 4 rows
    Total: 22 rows.
    """
    return [
        {
            "id": "game_a_hash",
            "sport_key": "basketball_nba",
            "sport_title": "NBA",
            "commence_time": "2026-06-04T00:30:00Z",  # 8:30pm ET June 3
            "home_team": "Oklahoma City Thunder",
            "away_team": "Indiana Pacers",
            "bookmakers": [
                {
                    "key": "pinnacle",
                    "title": "Pinnacle",
                    "last_update": "2026-06-03T20:00:00Z",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "Oklahoma City Thunder", "price": -260},
                                {"name": "Indiana Pacers", "price": 215},
                            ],
                        },
                        {
                            "key": "spreads",
                            "outcomes": [
                                {
                                    "name": "Oklahoma City Thunder",
                                    "price": -108,
                                    "point": -6.5,
                                },
                                {
                                    "name": "Indiana Pacers",
                                    "price": -102,
                                    "point": 6.5,
                                },
                            ],
                        },
                        {
                            "key": "totals",
                            "outcomes": [
                                {"name": "Over", "price": -110, "point": 222.5},
                                {"name": "Under", "price": -110, "point": 222.5},
                            ],
                        },
                    ],
                },
                {
                    "key": "draftkings",
                    "title": "DraftKings",
                    "last_update": "2026-06-03T20:00:00Z",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "Oklahoma City Thunder", "price": -275},
                                {"name": "Indiana Pacers", "price": 220},
                            ],
                        },
                        {
                            "key": "spreads",
                            "outcomes": [
                                {
                                    "name": "Oklahoma City Thunder",
                                    "price": -110,
                                    "point": -6.5,
                                },
                                {
                                    "name": "Indiana Pacers",
                                    "price": -110,
                                    "point": 6.5,
                                },
                            ],
                        },
                        {
                            "key": "totals",
                            "outcomes": [
                                {"name": "Over", "price": -112, "point": 222.5},
                                {"name": "Under", "price": -108, "point": 222.5},
                            ],
                        },
                    ],
                },
                {
                    "key": "fanduel",
                    "title": "FanDuel",
                    "last_update": "2026-06-03T20:00:00Z",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "Oklahoma City Thunder", "price": -270},
                                {"name": "Indiana Pacers", "price": 222},
                            ],
                        },
                        {
                            "key": "spreads",
                            "outcomes": [
                                {
                                    "name": "Oklahoma City Thunder",
                                    "price": -110,
                                    "point": -6.5,
                                },
                                {
                                    "name": "Indiana Pacers",
                                    "price": -110,
                                    "point": 6.5,
                                },
                            ],
                        },
                        {
                            "key": "totals",
                            "outcomes": [
                                {"name": "Over", "price": -110, "point": 223.0},
                                {"name": "Under", "price": -110, "point": 223.0},
                            ],
                        },
                    ],
                },
            ],
        },
        {
            "id": "game_b_hash",
            "sport_key": "basketball_nba",
            "sport_title": "NBA",
            "commence_time": "2026-06-06T23:00:00Z",  # 7pm ET June 6
            "home_team": "Indiana Pacers",
            "away_team": "Oklahoma City Thunder",
            "bookmakers": [
                {
                    "key": "pinnacle",
                    "title": "Pinnacle",
                    "last_update": "2026-06-06T18:00:00Z",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "Indiana Pacers", "price": 145},
                                {"name": "Oklahoma City Thunder", "price": -160},
                            ],
                        }
                    ],
                },
                {
                    "key": "draftkings",
                    "title": "DraftKings",
                    "last_update": "2026-06-06T18:00:00Z",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "Indiana Pacers", "price": 150},
                                {"name": "Oklahoma City Thunder", "price": -165},
                            ],
                        }
                    ],
                },
            ],
        },
    ]


def test_normalize_odds_produces_long_format_rows():
    """Every (game, sportsbook, market, outcome) becomes one row."""
    fetched_at = datetime(2026, 6, 3, 20, 30, tzinfo=timezone.utc)
    df = odds._normalize_odds(_fake_odds_response(), fetched_at)
    # 18 from Game A + 4 from Game B = 22
    assert len(df) == 22
    # Schema columns all present
    expected_cols = {
        "game_id",
        "game_date",
        "commence_time",
        "home_team",
        "away_team",
        "sportsbook",
        "market_type",
        "outcome_name",
        "price",
        "point",
        "fetched_at",
    }
    assert expected_cols <= set(df.columns)


def test_normalize_odds_assigns_us_eastern_game_date():
    """An 8:30pm ET tipoff (00:30 UTC the next day) must map to the
    ET calendar date, not the UTC date. This is the NBA-canonical
    partition key that matches what nba_api uses for game_date."""
    fetched_at = datetime(2026, 6, 3, 20, 30, tzinfo=timezone.utc)
    df = odds._normalize_odds(_fake_odds_response(), fetched_at)
    game_a = df[df["game_id"] == "game_a_hash"].iloc[0]
    # commence_time is 2026-06-04T00:30:00Z = 2026-06-03 8:30pm ET
    assert str(game_a["game_date"]) == "2026-06-03"


def test_normalize_odds_h2h_has_no_point():
    """Moneyline outcomes don't have a spread/total point. spreads and
    totals must."""
    fetched_at = datetime(2026, 6, 3, 20, 30, tzinfo=timezone.utc)
    df = odds._normalize_odds(_fake_odds_response(), fetched_at)
    h2h_rows = df[df["market_type"] == "h2h"]
    spread_rows = df[df["market_type"] == "spreads"]
    total_rows = df[df["market_type"] == "totals"]
    assert h2h_rows["point"].isna().all(), "h2h rows must have NULL point"
    assert spread_rows["point"].notna().all(), "spreads rows must have a point"
    assert total_rows["point"].notna().all(), "totals rows must have a point"


def test_normalize_odds_preserves_american_odds_signs():
    """Negative prices are favorites, positive are dogs. Both sides
    of a market round-trip with sign intact."""
    fetched_at = datetime(2026, 6, 3, 20, 30, tzinfo=timezone.utc)
    df = odds._normalize_odds(_fake_odds_response(), fetched_at)
    pinnacle_h2h = df[(df["sportsbook"] == "pinnacle") & (df["market_type"] == "h2h")]
    okc = pinnacle_h2h[pinnacle_h2h["outcome_name"] == "Oklahoma City Thunder"]
    ind = pinnacle_h2h[pinnacle_h2h["outcome_name"] == "Indiana Pacers"]
    assert okc["price"].iloc[0] == -260
    assert ind["price"].iloc[0] == 215


def test_normalize_odds_empty_response_yields_empty_frame():
    fetched_at = datetime(2026, 6, 3, 20, 30, tzinfo=timezone.utc)
    df = odds._normalize_odds([], fetched_at)
    assert df.empty


def _redirect_parquet_writes(monkeypatch, target_dir: Path) -> None:
    original_parquet = DataFrameWriter.parquet

    def patched_parquet(self, path, *args, **kwargs):
        if isinstance(path, str) and path.startswith("s3a://"):
            return original_parquet(self, str(target_dir), *args, **kwargs)
        return original_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "parquet", patched_parquet)


def test_ingest_odds_writes_parquet_partitioned_by_game_date(
    spark, monkeypatch, tmp_path: Path
):
    """End-to-end: mocked API → normalized → Parquet with partitions."""
    monkeypatch.setenv("ODDS_API_KEY", "test-key-not-real")

    with patch.object(odds, "_fetch_odds") as mock_fetch:
        mock_fetch.return_value = _fake_odds_response()
        target_dir = tmp_path / "odds_out"
        _redirect_parquet_writes(monkeypatch, target_dir)

        out_path = odds.ingest_odds(spark, s3_bucket="test-bucket")

    assert out_path == "s3a://test-bucket/raw/nba/odds/"
    df = spark.read.parquet(str(target_dir))
    assert df.count() == 22
    # Game A is 2026-06-03 ET; Game B is 2026-06-06 ET — two partitions.
    date_dirs = list(target_dir.glob("game_date=*"))
    assert len(date_dirs) == 2, [d.name for d in date_dirs]


def test_ingest_odds_requires_api_key(spark, monkeypatch):
    monkeypatch.delenv("ODDS_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="ODDS_API_KEY"):
        odds.ingest_odds(spark, s3_bucket="test-bucket")


def test_ingest_odds_requires_bucket_or_local_mode(spark, monkeypatch):
    monkeypatch.setenv("ODDS_API_KEY", "test-key-not-real")
    monkeypatch.delenv("LOCAL_OUTPUT_DIR", raising=False)
    with pytest.raises(ValueError, match="s3_bucket"):
        odds.ingest_odds(spark, s3_bucket="")


def test_ingest_odds_empty_response_still_writes_schema(
    spark, monkeypatch, tmp_path: Path
):
    """Off-season day with no NBA games scheduled — empty response
    should still produce a schema-valid Parquet write, not crash."""
    monkeypatch.setenv("ODDS_API_KEY", "test-key-not-real")
    with patch.object(odds, "_fetch_odds") as mock_fetch:
        mock_fetch.return_value = []
        target_dir = tmp_path / "odds_empty"
        _redirect_parquet_writes(monkeypatch, target_dir)
        odds.ingest_odds(spark, s3_bucket="test-bucket")
    df = spark.read.parquet(str(target_dir))
    assert df.count() == 0
