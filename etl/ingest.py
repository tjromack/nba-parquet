from __future__ import annotations

import logging
import os
import time
from datetime import datetime  # noqa: I001

import pandas as pd
from pyspark.sql import SparkSession

from etl.paths import is_local_mode, resolve_output_uri
from etl.schema import RAW_BOX_SCORE_ADVANCED_SCHEMA, RAW_BOX_SCORE_SCHEMA

logger = logging.getLogger(__name__)

NBA_API_SLEEP_SECONDS = 0.6
NBA_API_TIMEOUT_SECONDS = 30

_RAW_COLUMNS = [f.name for f in RAW_BOX_SCORE_SCHEMA.fields]
_RAW_ADVANCED_COLUMNS = [f.name for f in RAW_BOX_SCORE_ADVANCED_SCHEMA.fields]
_ADVANCED_INT_COLUMNS = {"team_id", "player_id", "season"}
_ADVANCED_DOUBLE_COLUMNS = {
    f.name
    for f in RAW_BOX_SCORE_ADVANCED_SCHEMA.fields
    if f.dataType.simpleString() == "double"
}
_API_COLUMN_ALIASES = {
    # nba_api's BoxScoreTraditionalV2 names the turnover column "TO";
    # our schema (and the rest of the codebase) calls it "tov".
    "to": "tov",
}
_INT_COLUMNS = {
    "team_id",
    "player_id",
    "pts",
    "reb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "fgm",
    "fga",
    "fg3m",
    "fg3a",
    "ftm",
    "fta",
    "plus_minus",
    "season",
}


def _rate_limit_sleep(seconds: float = NBA_API_SLEEP_SECONDS) -> None:
    """Indirection so tests can patch sleep without affecting the rest of Spark."""
    time.sleep(seconds)


def _to_spark_rows(df: pd.DataFrame) -> list[dict]:
    """Convert a normalized pandas frame into Python-native rows.

    Spark's pandas → DataFrame path leaks ``5.0``-style floats into
    ``IntegerType`` fields (because pandas widens columns containing NaN
    to float64). Building rows of dicts with real ``int`` / ``None``
    values sidesteps that entirely.
    """
    if df.empty:
        return []
    rows: list[dict] = []
    for record in df.to_dict(orient="records"):
        clean: dict = {}
        for col in _RAW_COLUMNS:
            value = record.get(col)
            if value is None or (isinstance(value, float) and pd.isna(value)):
                clean[col] = None
                continue
            if pd.isna(value):
                clean[col] = None
                continue
            if col in _INT_COLUMNS:
                clean[col] = int(value)
            else:
                clean[col] = value
        rows.append(clean)
    return rows


def _season_start_year(season: str) -> int:
    return int(season.split("-")[0])


def _list_game_ids(season: str, season_type: str, game_date: str) -> list[str]:
    from nba_api.stats.endpoints import LeagueGameLog

    log = LeagueGameLog(
        season=season,
        season_type_all_star=season_type,
        date_from_nullable=game_date,
        date_to_nullable=game_date,
        timeout=NBA_API_TIMEOUT_SECONDS,
    )
    games_df = log.get_data_frames()[0]
    if games_df.empty:
        return []
    return sorted(games_df["GAME_ID"].astype(str).unique().tolist())


def _fetch_box_score(game_id: str) -> pd.DataFrame:
    from nba_api.stats.endpoints import BoxScoreTraditionalV2

    box = BoxScoreTraditionalV2(game_id=game_id, timeout=NBA_API_TIMEOUT_SECONDS)
    return box.player_stats.get_data_frame()


def _normalize_player_rows(
    raw: pd.DataFrame,
    *,
    game_id: str,
    game_date: str,
    season: str,
    season_type: str,
    matchups: dict[int, str],
    results: dict[int, str],
) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=_RAW_COLUMNS)

    df = raw.rename(columns={c: c.lower() for c in raw.columns})
    df = df.rename(columns=_API_COLUMN_ALIASES)

    season_year = _season_start_year(season)
    df["game_id"] = game_id
    df["game_date"] = pd.to_datetime(game_date).date()
    df["season"] = season_year
    df["season_type"] = season_type
    df["matchup"] = df["team_id"].map(matchups)
    df["wl"] = df["team_id"].map(results)

    for col in _INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in _RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None

    return df[_RAW_COLUMNS]


def _list_season_games(season: str, season_type: str) -> pd.DataFrame:
    """One ``LeagueGameLog`` call for an entire season+type.

    Returns the raw uppercase-column nba_api frame (GAME_ID, GAME_DATE,
    TEAM_ID, MATCHUP, WL, ...). The bulk-ingest path uses this single
    call to enumerate all games for the season instead of one per date,
    saving ~N-1 API calls vs the day-by-day path on a full season.
    """
    from nba_api.stats.endpoints import LeagueGameLog

    log = LeagueGameLog(
        season=season,
        season_type_all_star=season_type,
        timeout=NBA_API_TIMEOUT_SECONDS,
    )
    return log.get_data_frames()[0]


def _team_matchup_lookup(
    season: str, season_type: str, game_date: str
) -> tuple[dict[str, dict[int, str]], dict[str, dict[int, str]]]:
    from nba_api.stats.endpoints import LeagueGameLog

    log = LeagueGameLog(
        season=season,
        season_type_all_star=season_type,
        date_from_nullable=game_date,
        date_to_nullable=game_date,
        timeout=NBA_API_TIMEOUT_SECONDS,
    )
    df = log.get_data_frames()[0]
    matchups: dict[str, dict[int, str]] = {}
    results: dict[str, dict[int, str]] = {}
    if df.empty:
        return matchups, results
    df = df.rename(columns={c: c.upper() for c in df.columns})
    for game_id, group in df.groupby("GAME_ID"):
        gid = str(game_id)
        matchups[gid] = dict(zip(group["TEAM_ID"].astype(int), group["MATCHUP"]))
        results[gid] = dict(zip(group["TEAM_ID"].astype(int), group["WL"]))
    return matchups, results


def _apply_s3a_config(spark: SparkSession) -> None:
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", endpoint)
        hadoop_conf.set("fs.s3a.path.style.access", "true")


def ingest_box_scores(
    season: str,
    game_date: str,
    season_type: str,
    s3_bucket: str,
    spark: SparkSession,
) -> str:
    if not s3_bucket and not is_local_mode():
        raise ValueError("s3_bucket is required when LOCAL_OUTPUT_DIR is not set")
    datetime.strptime(game_date, "%Y-%m-%d")

    if not is_local_mode():
        _apply_s3a_config(spark)

    game_ids = _list_game_ids(season, season_type, game_date)
    matchups, results = _team_matchup_lookup(season, season_type, game_date)

    season_year = _season_start_year(season)
    output_path = resolve_output_uri(
        s3_bucket,
        f"raw/nba/box_scores/season={season_year}/game_date={game_date}",
    )

    if not game_ids:
        logger.warning(
            "No games found for season=%s date=%s type=%s",
            season,
            game_date,
            season_type,
        )
        empty = spark.createDataFrame([], RAW_BOX_SCORE_SCHEMA)
        empty.write.mode("overwrite").parquet(output_path)
        return output_path

    frames: list[pd.DataFrame] = []
    for idx, gid in enumerate(game_ids):
        if idx > 0:
            _rate_limit_sleep()
        raw_players = _fetch_box_score(gid)
        normalized = _normalize_player_rows(
            raw_players,
            game_id=gid,
            game_date=game_date,
            season=season,
            season_type=season_type,
            matchups=matchups.get(gid, {}),
            results=results.get(gid, {}),
        )
        frames.append(normalized)

    combined = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=_RAW_COLUMNS)
    )

    rows = _to_spark_rows(combined)
    sdf = spark.createDataFrame(rows, schema=RAW_BOX_SCORE_SCHEMA)
    sdf.write.mode("overwrite").parquet(output_path)
    logger.info("Wrote %d raw rows to %s", len(rows), output_path)
    return output_path


def ingest_box_scores_bulk(
    season: str,
    season_type: str,
    s3_bucket: str,
    spark: SparkSession,
) -> str:
    """Bulk-ingest every game for ``season`` + ``season_type`` in one shot.

    Lays down the same partition layout as the per-date ``ingest_box_scores``
    (``season=YYYY/game_date=YYYY-MM-DD/``) so downstream ``transform`` /
    ``write_features`` read identically — bulk and daily output interleave
    cleanly under ``raw/nba/box_scores/``. Uses a single ``LeagueGameLog``
    call to enumerate all games, then one ``BoxScoreTraditionalV2`` call per
    game with ``NBA_API_SLEEP_SECONDS`` between consecutive calls. Writes at
    the season root with ``partitionBy("season", "game_date")`` and dynamic
    partition overwrite so only the touched partitions are replaced.
    """
    if not s3_bucket and not is_local_mode():
        raise ValueError("s3_bucket is required when LOCAL_OUTPUT_DIR is not set")

    if not is_local_mode():
        _apply_s3a_config(spark)

    output_path = resolve_output_uri(s3_bucket, "raw/nba/box_scores")

    games_df = _list_season_games(season, season_type)

    if games_df.empty:
        logger.warning("No games found for season=%s type=%s", season, season_type)
        empty = spark.createDataFrame([], RAW_BOX_SCORE_SCHEMA)
        # No partitionBy: an empty partitioned write leaves zero files and
        # Spark cannot infer the schema on read. A flat empty Parquet at the
        # root drops a schema-bearing file so spark.read.parquet works.
        empty.write.mode("overwrite").parquet(output_path)
        return output_path

    games_df = games_df.rename(columns={c: c.upper() for c in games_df.columns})
    matchups: dict[str, dict[int, str]] = {}
    results: dict[str, dict[int, str]] = {}
    game_dates: dict[str, str] = {}
    for game_id, group in games_df.groupby("GAME_ID"):
        gid = str(game_id)
        matchups[gid] = dict(zip(group["TEAM_ID"].astype(int), group["MATCHUP"]))
        results[gid] = dict(zip(group["TEAM_ID"].astype(int), group["WL"]))
        game_dates[gid] = str(pd.to_datetime(group["GAME_DATE"].iloc[0]).date())

    sorted_gids = sorted(matchups.keys())
    frames: list[pd.DataFrame] = []
    for idx, gid in enumerate(sorted_gids):
        if idx > 0:
            _rate_limit_sleep()
        raw_players = _fetch_box_score(gid)
        normalized = _normalize_player_rows(
            raw_players,
            game_id=gid,
            game_date=game_dates[gid],
            season=season,
            season_type=season_type,
            matchups=matchups.get(gid, {}),
            results=results.get(gid, {}),
        )
        frames.append(normalized)

    combined = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=_RAW_COLUMNS)
    )

    rows = _to_spark_rows(combined)
    sdf = spark.createDataFrame(rows, schema=RAW_BOX_SCORE_SCHEMA)
    (
        sdf.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("season", "game_date")
        .parquet(output_path)
    )
    logger.info(
        "Bulk-wrote %d raw rows across %d games to %s",
        len(rows),
        len(sorted_gids),
        output_path,
    )
    return output_path


# ---------------------------------------------------------------------------
# Advanced box score (BoxScoreAdvancedV2) — companion zone for the model.
# Same partition layout as the traditional layer, written to
# ``raw/nba/box_scores_advanced/`` so the two zones live side-by-side and
# the transform layer joins them on (game_id, team_id, player_id).
# ---------------------------------------------------------------------------


def _fetch_advanced_box_score(game_id: str) -> pd.DataFrame:
    from nba_api.stats.endpoints import BoxScoreAdvancedV2

    box = BoxScoreAdvancedV2(game_id=game_id, timeout=NBA_API_TIMEOUT_SECONDS)
    return box.player_stats.get_data_frame()


def _advanced_rows_to_spark(df: pd.DataFrame) -> list[dict]:
    """Pandas → native-typed rows for the advanced schema.

    Mirrors ``_to_spark_rows`` but with the advanced column types: most
    fields are ``DoubleType`` (ratings, percentages, pace, PIE) so we
    only int-coerce the few identifier columns. Keeping the conversion
    explicit avoids pandas widening doubles to ``object`` on NaN rows
    and tripping Spark's nullability checks.
    """
    if df.empty:
        return []
    rows: list[dict] = []
    for record in df.to_dict(orient="records"):
        clean: dict = {}
        for col in _RAW_ADVANCED_COLUMNS:
            value = record.get(col)
            if value is None or (isinstance(value, float) and pd.isna(value)):
                clean[col] = None
                continue
            if pd.isna(value):
                clean[col] = None
                continue
            if col in _ADVANCED_INT_COLUMNS:
                clean[col] = int(value)
            elif col in _ADVANCED_DOUBLE_COLUMNS:
                clean[col] = float(value)
            else:
                clean[col] = value
        rows.append(clean)
    return rows


def _normalize_advanced_player_rows(
    raw: pd.DataFrame,
    *,
    game_id: str,
    game_date: str,
    season: str,
    season_type: str,
) -> pd.DataFrame:
    """Lowercase + tag the advanced payload to match the schema."""
    if raw.empty:
        return pd.DataFrame(columns=_RAW_ADVANCED_COLUMNS)

    df = raw.rename(columns={c: c.lower() for c in raw.columns})

    season_year = _season_start_year(season)
    df["game_id"] = game_id
    df["game_date"] = pd.to_datetime(game_date).date()
    df["season"] = season_year
    df["season_type"] = season_type

    for col in _RAW_ADVANCED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    return df[_RAW_ADVANCED_COLUMNS]


def ingest_advanced_box_scores(
    season: str,
    game_date: str,
    season_type: str,
    s3_bucket: str,
    spark: SparkSession,
) -> str:
    """Daily advanced-box-score ingest.

    Reuses ``_list_game_ids`` and ``_team_matchup_lookup`` from the
    traditional path so the two layers stay in lock-step on which games
    they enumerate. Writes to ``raw/nba/box_scores_advanced/season=YYYY/
    game_date=YYYY-MM-DD/`` — same shape as the traditional zone so the
    daily catch-up DAG can call both.
    """
    if not s3_bucket and not is_local_mode():
        raise ValueError("s3_bucket is required when LOCAL_OUTPUT_DIR is not set")
    datetime.strptime(game_date, "%Y-%m-%d")

    if not is_local_mode():
        _apply_s3a_config(spark)

    game_ids = _list_game_ids(season, season_type, game_date)

    season_year = _season_start_year(season)
    output_path = resolve_output_uri(
        s3_bucket,
        f"raw/nba/box_scores_advanced/season={season_year}/game_date={game_date}",
    )

    if not game_ids:
        logger.warning(
            "No games found (advanced) for season=%s date=%s type=%s",
            season,
            game_date,
            season_type,
        )
        empty = spark.createDataFrame([], RAW_BOX_SCORE_ADVANCED_SCHEMA)
        empty.write.mode("overwrite").parquet(output_path)
        return output_path

    frames: list[pd.DataFrame] = []
    for idx, gid in enumerate(game_ids):
        if idx > 0:
            _rate_limit_sleep()
        raw_players = _fetch_advanced_box_score(gid)
        normalized = _normalize_advanced_player_rows(
            raw_players,
            game_id=gid,
            game_date=game_date,
            season=season,
            season_type=season_type,
        )
        frames.append(normalized)

    combined = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=_RAW_ADVANCED_COLUMNS)
    )

    rows = _advanced_rows_to_spark(combined)
    sdf = spark.createDataFrame(rows, schema=RAW_BOX_SCORE_ADVANCED_SCHEMA)
    sdf.write.mode("overwrite").parquet(output_path)
    logger.info("Wrote %d advanced rows to %s", len(rows), output_path)
    return output_path


def ingest_advanced_box_scores_bulk(
    season: str,
    season_type: str,
    s3_bucket: str,
    spark: SparkSession,
) -> str:
    """Bulk advanced-box-score ingest for a full season+type.

    Same orchestration as ``ingest_box_scores_bulk`` — one
    ``LeagueGameLog`` call + one ``BoxScoreAdvancedV2`` per game with
    the standard 0.6s sleep between consecutive calls — but writes the
    advanced payload to ``raw/nba/box_scores_advanced/`` with the same
    ``partitionBy("season", "game_date")`` dynamic-overwrite layout.
    """
    if not s3_bucket and not is_local_mode():
        raise ValueError("s3_bucket is required when LOCAL_OUTPUT_DIR is not set")

    if not is_local_mode():
        _apply_s3a_config(spark)

    output_path = resolve_output_uri(s3_bucket, "raw/nba/box_scores_advanced")

    games_df = _list_season_games(season, season_type)

    if games_df.empty:
        logger.warning(
            "No games found (advanced bulk) for season=%s type=%s",
            season,
            season_type,
        )
        empty = spark.createDataFrame([], RAW_BOX_SCORE_ADVANCED_SCHEMA)
        empty.write.mode("overwrite").parquet(output_path)
        return output_path

    games_df = games_df.rename(columns={c: c.upper() for c in games_df.columns})
    game_dates: dict[str, str] = {}
    for game_id, group in games_df.groupby("GAME_ID"):
        gid = str(game_id)
        game_dates[gid] = str(pd.to_datetime(group["GAME_DATE"].iloc[0]).date())

    sorted_gids = sorted(game_dates.keys())
    frames: list[pd.DataFrame] = []
    for idx, gid in enumerate(sorted_gids):
        if idx > 0:
            _rate_limit_sleep()
        raw_players = _fetch_advanced_box_score(gid)
        normalized = _normalize_advanced_player_rows(
            raw_players,
            game_id=gid,
            game_date=game_dates[gid],
            season=season,
            season_type=season_type,
        )
        frames.append(normalized)

    combined = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=_RAW_ADVANCED_COLUMNS)
    )

    rows = _advanced_rows_to_spark(combined)
    sdf = spark.createDataFrame(rows, schema=RAW_BOX_SCORE_ADVANCED_SCHEMA)
    (
        sdf.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("season", "game_date")
        .parquet(output_path)
    )
    logger.info(
        "Bulk-wrote %d advanced rows across %d games to %s",
        len(rows),
        len(sorted_gids),
        output_path,
    )
    return output_path
