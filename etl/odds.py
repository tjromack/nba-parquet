"""Sportsbook odds ingestion via The Odds API (v4).

Pulls NBA moneyline, spread, and total prices from multiple US
sportsbooks and writes a long-format Parquet zone at
``raw/nba/odds/game_date=YYYY-MM-DD/``. Each (game, sportsbook,
market, outcome) is one row, which makes downstream joins to the
processed/features layers and per-book CLV analytics straightforward.

The Odds API uses its own opaque game IDs (hash strings); joining
back to nba_api's ``GAME_ID`` happens in ``models/market.py`` via
``(game_date, home_team, away_team)`` — the two systems share neither
the ID space nor the team-name format perfectly, so the join is name-
normalized rather than ID-keyed.

Timezone note: ``commence_time`` from the API is UTC. ``game_date`` is
the NBA-canonical US/Eastern date (matches what nba_api reports for
``GAME_DATE``), so partitions interleave with the existing raw/box
zones cleanly.

Environment:
    ODDS_API_KEY     required, free tier at the-odds-api.com gives
                     500 requests/month
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from pyspark.sql import SparkSession

from etl.paths import is_local_mode, resolve_output_uri
from etl.schema import RAW_ODDS_SCHEMA

logger = logging.getLogger(__name__)

ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds/"
# Regions: ``us`` for the books a US bettor could actually use
# (DraftKings, FanDuel, BetMGM, Caesars, ...), ``eu`` to pull Pinnacle
# as the sharp-anchor reference for de-vigging. Pinnacle doesn't
# legally operate in most US states, so it's absent from a pure
# ``us`` response — but its lines are the lowest-hold market in the
# world and the canonical sharp anchor. Quota cost is 6 units per
# call (3 markets x 2 regions); fine for the Finals demo (~120 units
# total across the series, well inside the free-tier 500/month).
ODDS_API_REGIONS = "us,eu"
ODDS_API_MARKETS = "h2h,spreads,totals"
ODDS_API_TIMEOUT_SECONDS = 15

_ODDS_COLUMNS = [f.name for f in RAW_ODDS_SCHEMA.fields]


def _fetch_odds(api_key: str) -> list[dict]:
    """Single HTTP GET against The Odds API. Returns raw JSON payload.

    Caller is responsible for catching network errors and deciding
    whether to retry — this function does the minimum (raises on
    non-2xx responses, returns parsed JSON otherwise).
    """
    response = requests.get(
        ODDS_API_URL,
        params={
            "apiKey": api_key,
            "regions": ODDS_API_REGIONS,
            "markets": ODDS_API_MARKETS,
            "oddsFormat": "american",
        },
        timeout=ODDS_API_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def _normalize_odds(raw_games: list[dict], fetched_at: datetime) -> pd.DataFrame:
    """Flatten nested API payload into long-format rows.

    The API returns games × bookmakers × markets × outcomes (4-level
    nesting); the long-format output is one row per leaf, which is
    the right shape for SQL joins / aggregations downstream.

    Empty response (off-day, no games) returns an empty DataFrame
    with the schema columns intact so the caller can still write a
    schema-valid Parquet partition.
    """
    if not raw_games:
        return pd.DataFrame(columns=_ODDS_COLUMNS)

    rows = []
    for game in raw_games:
        # commence_time is ISO UTC ("2026-06-04T00:30:00Z"). Pandas
        # parses the Z suffix as UTC; tz_convert to US/Eastern gives
        # the calendar date NBA scheduling uses.
        commence_utc = pd.Timestamp(game["commence_time"])
        game_date = commence_utc.tz_convert("US/Eastern").date()
        commence_naive_utc = commence_utc.tz_convert("UTC").tz_localize(None)
        commence_py = commence_naive_utc.to_pydatetime()

        for book in game.get("bookmakers", []):
            sportsbook = book["key"]
            for market in book.get("markets", []):
                market_type = market["key"]
                for outcome in market.get("outcomes", []):
                    rows.append(
                        {
                            "game_id": game["id"],
                            "game_date": game_date,
                            "commence_time": commence_py,
                            "home_team": game["home_team"],
                            "away_team": game["away_team"],
                            "sportsbook": sportsbook,
                            "market_type": market_type,
                            "outcome_name": outcome["name"],
                            "price": outcome.get("price"),
                            "point": outcome.get("point"),
                            "fetched_at": fetched_at.replace(tzinfo=None),
                        }
                    )

    if not rows:
        return pd.DataFrame(columns=_ODDS_COLUMNS)
    return pd.DataFrame(rows)[_ODDS_COLUMNS]


def _to_spark_rows(df: pd.DataFrame) -> list[dict]:
    """Pandas → Python-native rows for ``spark.createDataFrame``.

    Same pattern as ``etl.ingest._to_spark_rows``: convert NaN to
    None, coerce ints/doubles explicitly so pandas type widening
    doesn't trip Spark's nullability checks. Timestamps need
    ``.to_pydatetime()`` because pandas re-coerces stored datetimes
    to ``pd.Timestamp`` in the DataFrame, and Spark's TimestampType
    rejects Timestamp objects directly.
    """
    if df.empty:
        return []
    rows: list[dict] = []
    for record in df.to_dict(orient="records"):
        clean: dict = {}
        for col in _ODDS_COLUMNS:
            value = record.get(col)
            if value is None:
                clean[col] = None
                continue
            if isinstance(value, pd.Timestamp):
                if pd.isna(value):
                    clean[col] = None
                else:
                    clean[col] = value.to_pydatetime()
                continue
            if isinstance(value, float) and pd.isna(value):
                clean[col] = None
                continue
            if col == "price":
                clean[col] = int(value)
            elif col == "point":
                clean[col] = float(value)
            else:
                clean[col] = value
        rows.append(clean)
    return rows


def ingest_odds(spark: SparkSession, s3_bucket: str) -> str:
    """Fetch current NBA odds, write to ``raw/nba/odds/``.

    Returns the output URI. ``ODDS_API_KEY`` env var is required;
    raises ``RuntimeError`` if unset rather than silently failing
    halfway through. The write uses dynamic partition overwrite
    keyed on ``game_date``, so re-running the same day only
    replaces that day's snapshot — earlier days are preserved as
    CLV history.
    """
    if not s3_bucket and not is_local_mode():
        raise ValueError("s3_bucket is required when LOCAL_OUTPUT_DIR is not set")

    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ODDS_API_KEY environment variable is not set. Get a free "
            "key at https://the-odds-api.com/ and add it to .env."
        )

    output_path = resolve_output_uri(s3_bucket, "raw/nba/odds")
    fetched_at = datetime.now(timezone.utc)
    raw_games = _fetch_odds(api_key)
    logger.info("Fetched odds for %d NBA games", len(raw_games))

    df = _normalize_odds(raw_games, fetched_at)
    if df.empty:
        logger.warning("No NBA odds returned (off-day?); writing empty zone")
        empty = spark.createDataFrame([], RAW_ODDS_SCHEMA)
        empty.write.mode("overwrite").parquet(output_path)
        return output_path

    rows = _to_spark_rows(df)
    sdf = spark.createDataFrame(rows, schema=RAW_ODDS_SCHEMA)
    (
        sdf.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("game_date")
        .parquet(output_path)
    )
    logger.info(
        "Wrote %d odds rows across %d games to %s",
        len(rows),
        df["game_id"].nunique(),
        output_path,
    )
    return output_path
