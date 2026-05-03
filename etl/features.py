"""Rolling-window feature engineering for the team-game DataFrame.

Inputs the processed layer (one row per team per game) and produces
trailing-window aggregations suitable for downstream prediction models.

Window definition: for each team, ordered by ``game_date``, take the
current row plus the prior ``window - 1`` rows. Defaults to 10 games.

Note: ``Window.rowsBetween(-N, 0)`` works on row count, not calendar
days, so a team that played 4 games last week and 6 this week gets a
true 10-game lookback regardless of pace.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DEFAULT_WINDOW = 10


def build_rolling_features(df: DataFrame, window: int = DEFAULT_WINDOW) -> DataFrame:
    """Attach trailing-window rolling features to each (team, game) row.

    The output keeps the (season, game_date, game_id, team_id,
    team_abbreviation) keys plus rolling columns; it intentionally does
    NOT carry the raw per-game stats so downstream consumers join back
    to the processed layer when they want both.
    """
    if window < 1:
        raise ValueError(f"window must be >= 1, got {window}")

    team_window = (
        Window.partitionBy("team_id")
        .orderBy("game_date", "game_id")
        .rowsBetween(-(window - 1), 0)
    )

    win_int = F.when(F.col("win").isNotNull(), F.col("win").cast("int"))
    pts_d = F.col("pts").cast("double")

    # Conditional averages for the home/away split. Spark's avg() ignores
    # NULLs, so wrapping in F.when(...) lets us average pts only across
    # the rows where is_home matches the side we want.
    pts_home_only = F.when(F.col("is_home") == F.lit(True), pts_d)
    pts_away_only = F.when(F.col("is_home") == F.lit(False), pts_d)

    enriched = (
        df.withColumn("games_in_window", F.count("game_id").over(team_window))
        .withColumn("rolling_pts", F.avg(pts_d).over(team_window))
        .withColumn(
            "rolling_efg_pct", F.avg(F.col("effective_fg_pct")).over(team_window)
        )
        .withColumn(
            "rolling_ts_pct", F.avg(F.col("true_shooting_pct")).over(team_window)
        )
        .withColumn(
            "rolling_ast_to_tov",
            F.avg(F.col("assist_to_turnover")).over(team_window),
        )
        .withColumn("rolling_win_pct", F.avg(win_int).over(team_window))
        .withColumn("rolling_pts_home", F.avg(pts_home_only).over(team_window))
        .withColumn("rolling_pts_away", F.avg(pts_away_only).over(team_window))
    )

    return enriched.select(
        "season",
        "game_date",
        "game_id",
        "team_id",
        "team_abbreviation",
        "games_in_window",
        "rolling_pts",
        "rolling_efg_pct",
        "rolling_ts_pct",
        "rolling_ast_to_tov",
        "rolling_win_pct",
        "rolling_pts_home",
        "rolling_pts_away",
    )
