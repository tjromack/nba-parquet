from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from etl.paths import is_local_mode

S3A_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4," "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def get_spark(app_name: str = "nba-etl") -> SparkSession:
    builder = SparkSession.builder.appName(app_name).master(
        os.environ.get("SPARK_MASTER", "local[*]")
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
