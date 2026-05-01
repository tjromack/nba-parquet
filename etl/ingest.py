from __future__ import annotations

import logging
import os
import time
from datetime import datetime  # noqa: I001

import pandas as pd
from pyspark.sql import SparkSession

from etl.paths import is_local_mode, resolve_output_uri
from etl.schema import RAW_BOX_SCORE_SCHEMA

logger = logging.getLogger(__name__)

NBA_API_SLEEP_SECONDS = 0.6
NBA_API_TIMEOUT_SECONDS = 30

_RAW_COLUMNS = [f.name for f in RAW_BOX_SCORE_SCHEMA.fields]
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
