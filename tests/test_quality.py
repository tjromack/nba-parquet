"""Tests for the features-layer data-quality gate.

Each test corrupts exactly one property of an otherwise-valid features
frame and asserts the gate catches it. The point is not that the checks
run -- it is that each one fails for its own reason, so a green gate
means something specific rather than "no exception was raised".
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from pyspark.sql import functions as F

from etl.features import build_rolling_features
from etl.quality import (
    NULLABLE_COLUMNS,
    FeatureQualityError,
    check_feature_quality,
    partition_row_counts,
)


def _processed_row(
    *,
    team_id: int,
    team_abbreviation: str,
    game_index: int,
    season: int = 2025,
) -> dict:
    """One well-formed processed-layer row."""
    return {
        "season": season,
        "game_date": date(2025, 11, 1) + timedelta(days=game_index),
        "game_id": f"00425{team_id}{game_index:03d}",
        "season_type": "Regular Season",
        "team_id": team_id,
        "team_abbreviation": team_abbreviation,
        "opponent_abbreviation": "OPP",
        "is_home": game_index % 2 == 1,
        "win": game_index % 3 != 0,
        "pts": 100 + game_index,
        "reb": 40,
        "ast": 25,
        "tov": 12,
        "fg_pct": 0.475,
        "fg3_pct": 0.36,
        "ft_pct": 0.78,
        "effective_fg_pct": 0.52,
        "true_shooting_pct": 0.57,
        "assist_to_turnover": 2.1,
        "top_scorer": "A. Player",
        "top_rebounder": "A. Player",
        "top_playmaker": "A. Player",
    }


@pytest.fixture()
def processed_df(spark):
    """Two teams, six games each -- enough for partial and full windows."""
    rows = [
        _processed_row(team_id=1610612738, team_abbreviation="BOS", game_index=i)
        for i in range(1, 7)
    ] + [
        _processed_row(team_id=1610612747, team_abbreviation="LAL", game_index=i)
        for i in range(1, 7)
    ]
    return spark.createDataFrame(rows)


@pytest.fixture()
def features_df(processed_df):
    return build_rolling_features(processed_df)


def test_valid_features_pass_the_gate(features_df, processed_df):
    report = check_feature_quality(features_df, processed_df=processed_df)
    assert report.ok, report.violations
    assert report.rows == 12
    assert report.metrics["teams"] == 2
    assert report.metrics["processed_rows"] == 12
    # raise_for_status is chainable and must not raise on a clean report.
    assert report.raise_for_status() is report


def test_summary_reports_pass_state_and_numbers(features_df):
    summary = check_feature_quality(features_df).summary()
    assert "PASS" in summary
    assert "rows=12" in summary


def test_duplicate_team_game_is_caught(features_df):
    """The grain check is the one that matters most: a duplicated row
    silently double-counts a team in every downstream rolling average."""
    doubled = features_df.union(features_df.limit(1))
    report = check_feature_quality(doubled)
    assert not report.ok
    assert any("grain is not one row per" in v for v in report.violations)
    with pytest.raises(FeatureQualityError, match="grain"):
        report.raise_for_status()


def test_null_in_a_required_column_is_caught(features_df):
    broken = features_df.withColumn(
        "rolling_ts_pct",
        F.when(F.col("team_abbreviation") == "LAL", None).otherwise(
            F.col("rolling_ts_pct")
        ),
    )
    report = check_feature_quality(broken)
    assert not report.ok
    assert any("rolling_ts_pct must never be null" in v for v in report.violations)


def test_nulls_in_a_nullable_column_are_reported_not_failed(features_df):
    """rolling_pts_home is legitimately null before a team plays at home.
    That must show up as a metric, never as a violation -- otherwise the
    gate fails on correct data every opening night."""
    nulled = features_df.withColumn("rolling_pts_home", F.lit(None).cast("double"))
    report = check_feature_quality(nulled)
    assert report.ok, report.violations
    assert report.metrics["null_rate__rolling_pts_home"] == "100.0%"


def test_every_nullable_column_has_a_stated_reason():
    """The nullable allowlist is what gives the non-null set meaning, so
    an entry without a reason is a bug in the contract itself."""
    for col, reason in NULLABLE_COLUMNS.items():
        assert reason and len(reason) > 15, f"{col} has no real reason: {reason!r}"


def test_window_completeness_is_bounded(features_df):
    over = features_df.withColumn("games_in_window", F.lit(99))
    report = check_feature_quality(over, window=10)
    assert not report.ok
    assert any("games_in_window outside [1, 10]" in v for v in report.violations)

    zero = features_df.withColumn("games_in_window", F.lit(0))
    assert not check_feature_quality(zero, window=10).ok


def test_out_of_range_rate_is_caught(features_df):
    """A win percentage above 1.0 means the averaging is wrong, not that
    a team won more often than always."""
    broken = features_df.withColumn("rolling_win_pct", F.lit(1.4))
    report = check_feature_quality(broken)
    assert not report.ok
    assert any("rolling_win_pct outside" in v for v in report.violations)


def test_row_count_reconciliation_against_processed(features_df, processed_df):
    """Features and processed share a grain, so a count delta means the
    rolling build dropped or invented rows."""
    short = features_df.limit(10)
    report = check_feature_quality(short, processed_df=processed_df)
    assert not report.ok
    assert any("row-count reconciliation failed" in v for v in report.violations)
    assert any("delta -2" in v for v in report.violations)


def test_empty_features_layer_is_a_violation(features_df):
    empty = features_df.limit(0)
    report = check_feature_quality(empty)
    assert not report.ok
    assert report.violations == ["features layer is empty"]


def test_missing_required_column_is_caught(features_df):
    report = check_feature_quality(features_df.drop("rolling_win_pct"))
    assert not report.ok
    assert any("missing required column" in v for v in report.violations)


def test_partition_row_counts_reports_per_season(spark):
    rows = [
        _processed_row(
            team_id=1610612738, team_abbreviation="BOS", game_index=i, season=2025
        )
        for i in range(1, 4)
    ] + [
        _processed_row(
            team_id=1610612738, team_abbreviation="BOS", game_index=i, season=2026
        )
        for i in range(4, 6)
    ]
    features = build_rolling_features(spark.createDataFrame(rows))
    assert partition_row_counts(features) == {"2025": 3, "2026": 2}


def test_write_features_refuses_to_publish_a_broken_layer(features_df, tmp_path):
    """The gate has to be wired into the write, not merely available.
    A corrupt frame must fail the task rather than land in features/."""
    from etl.write import write_features_to_path

    broken = features_df.union(features_df.limit(1))
    dest = tmp_path / "features/nba/rolling_team_stats"

    with pytest.raises(FeatureQualityError):
        write_features_to_path(broken, str(dest))
    assert not dest.exists(), "a failed quality gate must not leave output behind"


def test_write_features_publishes_a_clean_layer(features_df, tmp_path):
    from etl.write import write_features_to_path

    dest = tmp_path / "features/nba/rolling_team_stats"
    write_features_to_path(features_df, str(dest))
    assert dest.exists()
