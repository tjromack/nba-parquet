from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from etl.paths import is_local_mode

S3A_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4," "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def _bootstrap_pyspark_env() -> None:
    """Set ``PYSPARK_PYTHON`` / ``PYSPARK_DRIVER_PYTHON`` and
    ``HADOOP_HOME`` defensively so ``get_spark()`` works regardless of
    how it's invoked.

    Without this, calling ``get_spark()`` directly (e.g. from a Python
    one-liner or notebook) hits two Windows-specific failures that the
    ``scripts/`` wrappers were silently papering over:

    1. Spark workers try to spawn ``python3``, which doesn't exist on
       Windows venvs — every task fails with
       ``CreateProcess error=2``.
    2. Spark falls back to the system Hadoop install (which isn't
       present), emitting ``Did not find winutils.exe`` and refusing
       to write parquet.

    Both are fixed by exporting ``sys.executable`` as the worker Python
    and pointing ``HADOOP_HOME`` at the vendored ``.hadoop/`` directory.
    The wrapper scripts still do the same thing — this is a belt-and-
    suspenders fix so direct ``get_spark()`` callers don't have to know.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    repo_root = Path(__file__).resolve().parents[1]
    hadoop_dir = repo_root / ".hadoop"
    if hadoop_dir.is_dir() and not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = str(hadoop_dir)
        os.environ["PATH"] = (
            str(hadoop_dir / "bin") + os.pathsep + os.environ.get("PATH", "")
        )


def get_spark(app_name: str = "nba-etl") -> SparkSession:
    _bootstrap_pyspark_env()
    builder = (
        SparkSession.builder.appName(app_name).master(
            os.environ.get("SPARK_MASTER", "local[*]")
        )
        # Dynamic partition overwrite is essential for incremental writes:
        # a daily run touching one (season, game_date) partition must not
        # wipe sibling partitions for other days in the same prefix.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if not is_local_mode():
        builder = (
            builder.config("spark.jars.packages", S3A_PACKAGES)
            .config(
                "spark.sql.sources.commitProtocolClass",
                "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
            )
            .config(
                "spark.sql.parquet.output.committer.class",
                "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
            )
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
        )

        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        region = os.environ.get("AWS_DEFAULT_REGION")

        if access_key and secret_key:
            builder = builder.config(
                "spark.hadoop.fs.s3a.access.key", access_key
            ).config("spark.hadoop.fs.s3a.secret.key", secret_key)
        if endpoint:
            builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint).config(
                "spark.hadoop.fs.s3a.path.style.access", "true"
            )
        if region:
            builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", region)

    return builder.getOrCreate()


def _safe_div(numerator, denominator):
    return F.when(
        (denominator.isNotNull()) & (denominator != 0), numerator / denominator
    ).otherwise(F.lit(None).cast("double"))


def aggregate_team_game(df: DataFrame) -> DataFrame:
    group_cols = [
        "season",
        "game_date",
        "game_id",
        "season_type",
        "team_id",
        "team_abbreviation",
    ]

    aggregated = df.groupBy(*group_cols).agg(
        F.first("matchup", ignorenulls=True).alias("matchup"),
        F.first("wl", ignorenulls=True).alias("wl"),
        F.sum("pts").alias("pts"),
        F.sum("reb").alias("reb"),
        F.sum("ast").alias("ast"),
        F.sum("tov").alias("tov"),
        F.sum("fgm").alias("fgm"),
        F.sum("fga").alias("fga"),
        F.sum("fg3m").alias("fg3m"),
        F.sum("fg3a").alias("fg3a"),
        F.sum("ftm").alias("ftm"),
        F.sum("fta").alias("fta"),
    )

    fgm = F.col("fgm").cast("double")
    fga = F.col("fga").cast("double")
    fg3m = F.col("fg3m").cast("double")
    fg3a = F.col("fg3a").cast("double")
    ftm = F.col("ftm").cast("double")
    fta = F.col("fta").cast("double")
    pts = F.col("pts").cast("double")
    ast = F.col("ast").cast("double")
    tov = F.col("tov").cast("double")

    is_home = F.when(F.col("matchup").contains("vs."), F.lit(True)).when(
        F.col("matchup").contains("@"), F.lit(False)
    )
    opponent = F.trim(
        F.regexp_replace(
            F.col("matchup"),
            r"^[A-Z]{2,4}\s+(vs\.|@)\s+",
            "",
        )
    )
    win = F.when(F.col("wl") == "W", F.lit(True)).when(F.col("wl") == "L", F.lit(False))

    enriched = aggregated.select(
        F.col("season"),
        F.col("game_date"),
        F.col("game_id"),
        F.col("season_type"),
        F.col("team_id"),
        F.col("team_abbreviation"),
        opponent.alias("opponent_abbreviation"),
        is_home.alias("is_home"),
        win.alias("win"),
        F.col("pts"),
        F.col("reb"),
        F.col("ast"),
        F.col("tov"),
        _safe_div(fgm, fga).alias("fg_pct"),
        _safe_div(fg3m, fg3a).alias("fg3_pct"),
        _safe_div(ftm, fta).alias("ft_pct"),
        _safe_div(fgm + F.lit(0.5) * fg3m, fga).alias("effective_fg_pct"),
        _safe_div(pts, F.lit(2.0) * (fga + F.lit(0.44) * fta)).alias(
            "true_shooting_pct"
        ),
        _safe_div(ast, tov).alias("assist_to_turnover"),
    )

    return enriched


def get_top_player(
    df: DataFrame,
    stat_col: str,
    group_cols: list[str],
    out_col: str,
) -> DataFrame:
    window = Window.partitionBy(*group_cols).orderBy(
        F.col(stat_col).desc_nulls_last(), F.col("player_name").asc_nulls_last()
    )
    ranked = (
        df.filter(F.col("player_name").isNotNull())
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .select(*group_cols, F.col("player_name").alias(out_col))
    )
    return ranked


def join_top_players(team_game: DataFrame, raw: DataFrame) -> DataFrame:
    join_keys = ["game_id", "team_id"]
    top_scorer = get_top_player(raw, "pts", join_keys, "top_scorer")
    top_rebounder = get_top_player(raw, "reb", join_keys, "top_rebounder")
    top_playmaker = get_top_player(raw, "ast", join_keys, "top_playmaker")

    return (
        team_game.join(top_scorer, on=join_keys, how="left")
        .join(top_rebounder, on=join_keys, how="left")
        .join(top_playmaker, on=join_keys, how="left")
    )


def aggregate_team_advanced(adv_raw: DataFrame) -> DataFrame:
    """Roll per-player advanced rows up to one row per (game_id, team_id).

    nba_api's BoxScoreAdvancedV3 returns per-player advanced metrics —
    a player's individual on-court rating, not the team's. The team's
    overall rating in a game is approximated here as the **minutes-
    weighted average** of the players who actually saw the floor.
    Pace is a team-level metric repeated per player, so a simple max
    picks the canonical value while ignoring any null rows from
    inactive players. Players with zero/null minutes (DNPs) are
    dropped before weighting so they don't drag the average.

    Returns columns: ``game_id``, ``team_id``, ``off_rating``,
    ``def_rating``, ``net_rating``, ``pace``. All nullable doubles —
    if a (game, team) has no usable rows the join downstream produces
    NULLs (honest representation of "no advanced data here").
    """
    # "MM:SS" -> double minutes. Players with "" / null / "0:00" get 0
    # and are filtered out before the weighting math.
    minutes_d = F.regexp_extract(F.col("min"), r"^(\d+)", 1).cast("double")
    played = adv_raw.withColumn("_min_num", minutes_d).filter(
        F.col("_min_num").isNotNull() & (F.col("_min_num") > 0)
    )

    weighted = played.withColumn(
        "_ortg_w", F.col("off_rating") * F.col("_min_num")
    ).withColumn("_drtg_w", F.col("def_rating") * F.col("_min_num"))

    grouped = weighted.groupBy("game_id", "team_id").agg(
        F.sum("_min_num").alias("_total_min"),
        F.sum("_ortg_w").alias("_ortg_w_sum"),
        F.sum("_drtg_w").alias("_drtg_w_sum"),
        F.max("pace").alias("pace"),
    )

    return grouped.select(
        F.col("game_id"),
        F.col("team_id"),
        _safe_div(F.col("_ortg_w_sum"), F.col("_total_min")).alias("off_rating"),
        _safe_div(F.col("_drtg_w_sum"), F.col("_total_min")).alias("def_rating"),
        (
            _safe_div(F.col("_ortg_w_sum"), F.col("_total_min"))
            - _safe_div(F.col("_drtg_w_sum"), F.col("_total_min"))
        ).alias("net_rating"),
        F.col("pace"),
    )


def join_team_advanced(team_game: DataFrame, team_advanced: DataFrame) -> DataFrame:
    """Left-join the advanced team aggregates onto the traditional one.

    Left join is deliberate: games whose advanced raw partition hasn't
    been ingested yet (daily catch-ups that only run the traditional
    path, or pre-Phase-A history) come through with NULL advanced
    columns rather than disappearing. ``rolling_*`` features on the
    downstream side use ``avg(... ignoring nulls)`` so a partial-data
    rolling window degrades gracefully instead of erroring.
    """
    return team_game.join(team_advanced, on=["game_id", "team_id"], how="left")


def with_null_advanced_columns(df: DataFrame) -> DataFrame:
    """Add the four Phase B advanced columns as NULL doubles.

    Used by callers that don't have an advanced raw zone to join (the
    daily catch-up path, pre-Phase-A backfills, tests that don't need
    advanced data). Keeps the processed-frame schema stable so the
    rest of the pipeline reads/writes a single column shape regardless
    of upstream provenance.
    """
    for col in ("off_rating", "def_rating", "net_rating", "pace"):
        df = df.withColumn(col, F.lit(None).cast("double"))
    return df
