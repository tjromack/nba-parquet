"""Idempotency and backfill behaviour of the processed-layer write.

The claim these tests defend: **re-running any date is safe**. A failed
2am run can be re-run at 8am, and a backfill can sweep a range of dates,
without duplicating rows and without disturbing days it did not touch.

That property does not come from the write code. It comes from one
Spark setting -- ``spark.sql.sources.partitionOverwriteMode=dynamic`` --
combined with ``mode("overwrite").partitionBy("season", "game_date")``.
Under the default *static* mode the same call wipes the entire
``processed/`` prefix and replaces it with whatever the current run
holds, so a single-day re-run silently destroys the rest of the season.
That is not a hypothetical: it is the bug this setting was added to fix.

So there are two tests here, and both are needed:

- the behavioural ones set the conf and exercise write -> re-write,
  proving the semantics are what the docs claim;
- ``test_get_spark_configures_dynamic_partition_overwrite`` pins the
  production session to that same setting, so the behavioural tests
  cannot drift into testing a configuration the pipeline doesn't use.

The session fixture is shared, so each test restores the previous value
rather than leaving the conf set for whatever runs next.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import pytest

from etl.write import write_processed_to_path

PARTITION_MODE = "spark.sql.sources.partitionOverwriteMode"


@contextmanager
def dynamic_partition_overwrite(spark):
    """Apply the production partition-overwrite mode, then restore it."""
    previous = spark.conf.get(PARTITION_MODE, "static")
    spark.conf.set(PARTITION_MODE, "dynamic")
    try:
        yield
    finally:
        spark.conf.set(PARTITION_MODE, previous)


def _row(*, game_date: date, team_abbreviation: str, pts: int, season: int = 2025):
    return {
        "season": season,
        "game_date": game_date,
        "game_id": f"002{game_date.strftime('%m%d')}{team_abbreviation}",
        "season_type": "Regular Season",
        "team_id": 1610612738 if team_abbreviation == "BOS" else 1610612747,
        "team_abbreviation": team_abbreviation,
        "opponent_abbreviation": "OPP",
        "is_home": True,
        "win": True,
        "pts": pts,
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


def _day(spark, day: date, pts: int = 110):
    """One game day: two team rows."""
    return spark.createDataFrame(
        [
            _row(game_date=day, team_abbreviation="BOS", pts=pts),
            _row(game_date=day, team_abbreviation="LAL", pts=pts - 5),
        ]
    )


DAY_1 = date(2026, 4, 18)
DAY_2 = date(2026, 4, 19)


def test_get_spark_configures_dynamic_partition_overwrite():
    """Pin the production setting.

    Everything else in this file tests behaviour *given* dynamic mode.
    If get_spark ever stops setting it, those tests would keep passing
    while the pipeline silently reverted to clobbering whole prefixes --
    so the setting itself gets an assertion.
    """
    import inspect

    from etl import transform

    source = inspect.getsource(transform.get_spark)
    assert PARTITION_MODE in source, "get_spark no longer sets the overwrite mode"
    assert '"dynamic"' in source, "partition overwrite mode is not dynamic"


def test_rerunning_the_same_day_does_not_duplicate_rows(spark, tmp_path):
    """The 2am-failed, 8am-rerun case."""
    dest = str(tmp_path / "processed/nba/team_game_stats")
    with dynamic_partition_overwrite(spark):
        write_processed_to_path(_day(spark, DAY_1), dest)
        first = spark.read.parquet(dest).count()

        # Exactly the same input, written again.
        write_processed_to_path(_day(spark, DAY_1), dest)
        second = spark.read.parquet(dest).count()

    assert first == 2
    assert second == 2, f"re-running one day duplicated rows: {first} -> {second}"


def test_rerunning_one_day_leaves_other_partitions_intact(spark, tmp_path):
    """The regression that motivated dynamic mode.

    Under static overwrite this assertion fails: writing day 2 wipes
    day 1 along with the rest of the prefix.
    """
    dest = str(tmp_path / "processed/nba/team_game_stats")
    with dynamic_partition_overwrite(spark):
        write_processed_to_path(_day(spark, DAY_1), dest)
        write_processed_to_path(_day(spark, DAY_2), dest)

        after = spark.read.parquet(dest)
        dates = {r["game_date"] for r in after.select("game_date").distinct().collect()}

    assert dates == {DAY_1, DAY_2}, f"a sibling partition was clobbered: {dates}"
    assert after.count() == 4


def test_rerunning_a_day_replaces_that_days_data(spark, tmp_path):
    """Idempotent does not mean append-only.

    A re-run exists to *correct* a day -- late-arriving or revised box
    scores are normal. The partition must end up holding the new values,
    not both versions.
    """
    dest = str(tmp_path / "processed/nba/team_game_stats")
    with dynamic_partition_overwrite(spark):
        write_processed_to_path(_day(spark, DAY_1, pts=110), dest)
        write_processed_to_path(_day(spark, DAY_2), dest)

        # Day 1 is re-ingested with corrected scoring.
        write_processed_to_path(_day(spark, DAY_1, pts=125), dest)

        after = spark.read.parquet(dest)
        day_one = after.filter(after.game_date == DAY_1)
        day_two = after.filter(after.game_date == DAY_2)
        corrected = {r["pts"] for r in day_one.select("pts").collect()}
        untouched = {r["pts"] for r in day_two.select("pts").collect()}

    assert day_one.count() == 2, "the corrected day should not have grown"
    assert corrected == {125, 120}, f"day 1 kept stale values: {corrected}"
    assert untouched == {110, 105}, "correcting day 1 changed day 2"


def test_a_backfill_range_converges_to_one_row_per_team_game(spark, tmp_path):
    """A backfill is just N single-day writes, so re-running an
    overlapping range has to be safe. This mirrors
    `airflow dags backfill` sweeping a window that includes days already
    loaded."""
    dest = str(tmp_path / "processed/nba/team_game_stats")
    days = [date(2026, 4, d) for d in range(18, 23)]

    with dynamic_partition_overwrite(spark):
        for day in days:
            write_processed_to_path(_day(spark, day), dest)
        first_pass = spark.read.parquet(dest).count()

        # Overlapping re-run of the last three days.
        for day in days[2:]:
            write_processed_to_path(_day(spark, day), dest)
        after = spark.read.parquet(dest)

    assert first_pass == len(days) * 2
    assert after.count() == len(days) * 2, "the overlapping backfill duplicated rows"
    grain = after.select("game_date", "team_abbreviation").distinct().count()
    assert grain == after.count(), "one row per (game_date, team) no longer holds"


@pytest.mark.parametrize("bad_prefix", ["raw", "processed"])
def test_features_write_still_refuses_the_wrong_prefix(spark, tmp_path, bad_prefix):
    """Idempotent writes only help if they land in the right zone."""
    from etl.write import write_features_to_path

    with pytest.raises(ValueError, match="must not write to"):
        write_features_to_path(
            _day(spark, DAY_1), str(tmp_path / bad_prefix / "nba" / "x")
        )
