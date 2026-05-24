from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path

import pytest

from etl.features import build_rolling_features
from etl.schema import FEATURE_SCHEMA
from etl.write import write_features_to_path


def _team_game_row(
    *,
    team_id: int,
    team_abbreviation: str,
    game_index: int,
    pts: int,
    is_home: bool,
    win: bool = True,
    season: int = 2025,
) -> dict:
    """Construct one processed-layer row with hand-picked stats."""
    return {
        "season": season,
        "game_date": date(2025, 11, 1) + timedelta(days=game_index),
        "game_id": f"00425{team_id}{game_index:03d}",
        "season_type": "Regular Season",
        "team_id": team_id,
        "team_abbreviation": team_abbreviation,
        "opponent_abbreviation": "OPP",
        "is_home": is_home,
        "win": win,
        "pts": pts,
        "reb": 40,
        "ast": 25,
        "tov": 12,
        "fg_pct": 0.475,
        "fg3_pct": 0.36,
        "ft_pct": 0.78,
        "effective_fg_pct": 0.5 + 0.005 * game_index,
        "true_shooting_pct": 0.55 + 0.005 * game_index,
        "assist_to_turnover": 2.0 + 0.05 * game_index,
        "top_scorer": "A. Player",
        "top_rebounder": "A. Player",
        "top_playmaker": "A. Player",
    }


@pytest.fixture()
def twelve_game_df(spark):
    """12 BOS games (alternating home/away, ramp pts 101..112) + 4 LAL games."""
    rows = []
    for i in range(1, 13):
        rows.append(
            _team_game_row(
                team_id=1610612738,
                team_abbreviation="BOS",
                game_index=i,
                pts=100 + i,
                is_home=(i % 2 == 1),  # odd index = home
                win=True,
            )
        )
    for i in range(1, 5):
        rows.append(
            _team_game_row(
                team_id=1610612747,
                team_abbreviation="LAL",
                game_index=i,
                pts=90 + i,
                is_home=(i % 2 == 0),
                win=(i % 2 == 0),  # 50% win rate
            )
        )
    return spark.createDataFrame(rows)


def test_build_rolling_features_schema_matches(twelve_game_df):
    out = build_rolling_features(twelve_game_df, window=10)
    expected = {f.name for f in FEATURE_SCHEMA.fields}
    actual = set(out.columns)
    assert expected == actual, f"missing={expected - actual} extra={actual - expected}"


def test_rolling_features_full_window_for_12th_bos_game(twelve_game_df):
    """At game 12 the full 10-game lookback covers games 3..12."""
    out = build_rolling_features(twelve_game_df, window=10).cache()
    rows = out.filter("team_abbreviation = 'BOS'").orderBy("game_date").collect()
    assert len(rows) == 12

    final = rows[-1]
    assert final["games_in_window"] == 10
    # pts in games 3..12 = 103..112  →  avg = 107.5
    assert math.isclose(final["rolling_pts"], 107.5, rel_tol=1e-9)
    # home games in window = indexes 3,5,7,9,11  → pts 103,105,107,109,111
    assert math.isclose(final["rolling_pts_home"], 107.0, rel_tol=1e-9)
    # away games in window = indexes 4,6,8,10,12  → pts 104,106,108,110,112
    assert math.isclose(final["rolling_pts_away"], 108.0, rel_tol=1e-9)
    # All BOS games are wins
    assert math.isclose(final["rolling_win_pct"], 1.0, rel_tol=1e-9)


def test_rolling_features_partial_window_at_start(twelve_game_df):
    """Game 1 has only itself in the lookback (games_in_window=1)."""
    out = build_rolling_features(twelve_game_df, window=10).cache()
    first = out.filter("team_abbreviation = 'BOS'").orderBy("game_date").first()
    assert first["games_in_window"] == 1
    assert math.isclose(first["rolling_pts"], 101.0, rel_tol=1e-9)
    # Game 1 is home, so home avg = 101 and away avg is null (no away games yet)
    assert math.isclose(first["rolling_pts_home"], 101.0, rel_tol=1e-9)
    assert first["rolling_pts_away"] is None


def test_rolling_features_partition_by_team(twelve_game_df):
    """LAL features must be computed against LAL's 4 games only."""
    out = build_rolling_features(twelve_game_df, window=10).cache()
    lal_rows = out.filter("team_abbreviation = 'LAL'").orderBy("game_date").collect()
    assert len(lal_rows) == 4

    final = lal_rows[-1]
    assert final["games_in_window"] == 4
    # LAL pts = 91, 92, 93, 94  →  avg = 92.5
    assert math.isclose(final["rolling_pts"], 92.5, rel_tol=1e-9)
    # 2 wins out of 4 (indexes 2 and 4)
    assert math.isclose(final["rolling_win_pct"], 0.5, rel_tol=1e-9)


def test_rolling_features_window_size_validation(twelve_game_df):
    with pytest.raises(ValueError):
        build_rolling_features(twelve_game_df, window=0)


def test_write_features_creates_partition_layout(twelve_game_df, tmp_path: Path):
    out = build_rolling_features(twelve_game_df, window=10)
    output = tmp_path / "features"
    write_features_to_path(out, str(output))

    season_dirs = list(output.glob("season=*"))
    assert season_dirs, "expected at least one season=* partition dir"
    for d in season_dirs:
        assert list(d.glob("*.parquet")), f"no parquet files in {d}"


def test_write_features_rejects_processed_prefix(twelve_game_df, tmp_path: Path):
    out = build_rolling_features(twelve_game_df, window=10)
    bad_path = str(tmp_path / "processed" / "leaked")
    with pytest.raises(ValueError):
        write_features_to_path(out, bad_path)


# --------------------------------------------------------------------------
# Phase B: rolling advanced features
# --------------------------------------------------------------------------


def test_rolling_advanced_features_use_window_average(spark):
    """When the input frame carries advanced columns, the rolling
    builder produces minutes-window averages alongside the traditional
    rolling stats. Hand-computable on a 3-game ramp."""
    rows = []
    for i in range(1, 4):
        row = _team_game_row(
            team_id=1610612738,
            team_abbreviation="BOS",
            game_index=i,
            pts=100 + i,
            is_home=(i % 2 == 1),
            win=True,
        )
        # ORtg ramps 110, 115, 120; DRtg flat 105; pace flat 99.
        row["off_rating"] = 105.0 + 5.0 * i
        row["def_rating"] = 105.0
        row["net_rating"] = (105.0 + 5.0 * i) - 105.0
        row["pace"] = 99.0
        rows.append(row)
    df = spark.createDataFrame(rows)
    out = build_rolling_features(df, window=10).orderBy("game_date").collect()
    assert len(out) == 3
    final = out[-1]
    # Avg of 110, 115, 120 = 115
    assert math.isclose(final["rolling_ortg"], 115.0, rel_tol=1e-9)
    # DRtg flat -> avg = 105
    assert math.isclose(final["rolling_drtg"], 105.0, rel_tol=1e-9)
    # Net: 5, 10, 15 -> avg = 10
    assert math.isclose(final["rolling_net_rtg"], 10.0, rel_tol=1e-9)
    assert math.isclose(final["rolling_pace"], 99.0, rel_tol=1e-9)


def test_rolling_advanced_features_null_when_source_missing(spark):
    """If the processed frame has no advanced columns (pre-Phase-B
    history, daily ingest that never ingested advanced), the rolling
    builder emits NULL rolling advanced cols rather than raising."""
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    pre_phase_b_schema = StructType(
        [
            StructField("season", IntegerType()),
            StructField("game_date", DateType()),
            StructField("game_id", StringType()),
            StructField("season_type", StringType()),
            StructField("team_id", IntegerType()),
            StructField("team_abbreviation", StringType()),
            StructField("opponent_abbreviation", StringType()),
            StructField("is_home", BooleanType()),
            StructField("win", BooleanType()),
            StructField("pts", LongType()),
            StructField("reb", LongType()),
            StructField("ast", LongType()),
            StructField("tov", LongType()),
            StructField("fg_pct", DoubleType()),
            StructField("fg3_pct", DoubleType()),
            StructField("ft_pct", DoubleType()),
            StructField("effective_fg_pct", DoubleType()),
            StructField("true_shooting_pct", DoubleType()),
            StructField("assist_to_turnover", DoubleType()),
            StructField("top_scorer", StringType()),
            StructField("top_rebounder", StringType()),
            StructField("top_playmaker", StringType()),
        ]
    )
    legacy_rows = [
        {
            "season": 2025,
            "game_date": date(2025, 11, 1),
            "game_id": "g_legacy",
            "season_type": "Regular Season",
            "team_id": 1,
            "team_abbreviation": "BOS",
            "opponent_abbreviation": "NYK",
            "is_home": True,
            "win": True,
            "pts": 110,
            "reb": 40,
            "ast": 25,
            "tov": 12,
            "fg_pct": 0.48,
            "fg3_pct": 0.36,
            "ft_pct": 0.78,
            "effective_fg_pct": 0.52,
            "true_shooting_pct": 0.58,
            "assist_to_turnover": 2.1,
            "top_scorer": "P",
            "top_rebounder": "P",
            "top_playmaker": "P",
        }
    ]
    legacy = spark.createDataFrame(legacy_rows, schema=pre_phase_b_schema)
    out = build_rolling_features(legacy, window=10).collect()
    assert len(out) == 1
    for col in ("rolling_ortg", "rolling_drtg", "rolling_net_rtg", "rolling_pace"):
        assert out[0][col] is None, f"{col} should be NULL on pre-Phase-B input"
