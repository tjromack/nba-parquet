"""Publish a model-driven pick for a given NBA game date.

Reads:
  - latest features from out/features/ (for the model's input vector)
  - latest persisted model from models/artifacts/winner_hgb.joblib
  - latest odds snapshot from out/raw/odds/ for the target date

For each game that has both odds and feature data, generates a Pick
via models.picks.generate_pick and writes:
  - picks/<game_id>.json   (git-trackable; commit timestamp = pre-tipoff proof)
  - out/picks/season=YYYY/game_date=YYYY-MM-DD/<game_id>.parquet

Usage:
    $env:LOCAL_OUTPUT_DIR = "$PWD\\out"
    $env:ODDS_API_KEY = "..."   # if you also want to refresh odds first
    python scripts/publish_pick.py --game-date 2026-06-04 [--refresh-odds]

After running, commit the picks/ JSON files to git and push — the
GitHub commit timestamp is the verifiable claim that the pick existed
before tipoff.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_HADOOP_DIR = REPO_ROOT / ".hadoop"
if _HADOOP_DIR.is_dir() and not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = str(_HADOOP_DIR)
    os.environ["PATH"] = (
        str(_HADOOP_DIR / "bin") + os.pathsep + os.environ.get("PATH", "")
    )

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pandas as pd  # noqa: E402

from models.dataset import ROLLING_FEATURE_COLS  # noqa: E402
from models.picks import (  # noqa: E402
    abbr_for_team_name,
    generate_pick,
    publish_pick,
)
from models.predict import (  # noqa: E402
    latest_team_features,
    load_model,
    predict_matchup,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("publish_pick")


def _current_git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=REPO_ROOT,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _load_odds_for_date(local_root: Path, game_date_iso: str) -> pd.DataFrame:
    """Read the odds zone, filter to one game_date."""
    odds_path = local_root / "raw" / "nba" / "odds"
    if not odds_path.exists():
        raise FileNotFoundError(
            f"No odds zone at {odds_path}. Run odds ingestion first: "
            "scripts/ingest_odds.py (or pass --refresh-odds)."
        )
    df = pd.read_parquet(odds_path)
    if "game_date" not in df.columns:
        raise RuntimeError(f"Odds zone at {odds_path} missing game_date column")
    df["game_date"] = pd.to_datetime(df["game_date"].astype(str)).dt.date
    target = pd.to_datetime(game_date_iso).date()
    return df[df["game_date"] == target].copy()


def _refresh_odds(local_root: Path) -> None:
    """Pull a fresh odds snapshot via etl.odds.ingest_odds."""
    from etl.odds import ingest_odds
    from etl.transform import get_spark

    spark = get_spark("nba-publish-pick-odds-refresh")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        ingest_odds(spark, s3_bucket="")
    finally:
        spark.stop()


def _features_row_for_team(features: pd.DataFrame, team_abbr: str) -> pd.Series:
    """Latest rolling-features row for a team. None if no history."""
    rows = features[features["team_abbreviation"] == team_abbr]
    if rows.empty:
        return None
    return rows.sort_values(["game_date", "game_id"]).iloc[-1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--game-date",
        required=True,
        help="YYYY-MM-DD (ET) — only games on this date are picked",
    )
    parser.add_argument(
        "--refresh-odds",
        action="store_true",
        help="Pull a fresh odds snapshot before generating picks",
    )
    args = parser.parse_args(argv)

    load_dotenv(REPO_ROOT / ".env")

    local_root_str = os.environ.get("LOCAL_OUTPUT_DIR")
    if not local_root_str:
        logger.error("LOCAL_OUTPUT_DIR is not set")
        return 2
    local_root = Path(local_root_str).resolve()

    if args.refresh_odds:
        if not os.environ.get("ODDS_API_KEY"):
            logger.error("ODDS_API_KEY not set; cannot --refresh-odds")
            return 2
        logger.info("Refreshing odds snapshot via The Odds API")
        _refresh_odds(local_root)

    odds_df = _load_odds_for_date(local_root, args.game_date)
    if odds_df.empty:
        logger.error(
            "No odds found for game_date=%s in %s. Did you ingest odds?",
            args.game_date,
            local_root,
        )
        return 2

    features = pd.read_parquet(local_root / "features" / "nba" / "rolling_team_stats")
    features["game_date"] = pd.to_datetime(features["game_date"].astype(str))

    model = load_model()
    if model is None:
        logger.error(
            "No trained model artifact found. Run `python -m models.train` first."
        )
        return 2

    model_version = _current_git_sha()
    latest = latest_team_features(features)

    # One pick per (game_id) on the target date
    games = odds_df[["game_id", "home_team", "away_team"]].drop_duplicates()
    n_published = 0
    for _, game in games.iterrows():
        try:
            home_abbr = abbr_for_team_name(game["home_team"])
            away_abbr = abbr_for_team_name(game["away_team"])
        except KeyError as e:
            logger.warning("Skipping game %s: %s", game["game_id"], e)
            continue

        home_row = _features_row_for_team(latest, home_abbr)
        away_row = _features_row_for_team(latest, away_abbr)
        if home_row is None or away_row is None:
            logger.warning(
                "Skipping %s vs %s: missing features for one or both teams",
                home_abbr,
                away_abbr,
            )
            continue

        pred = predict_matchup(model, home_row, away_row)
        model_prob = pred["home_win_prob"]

        # The features that drove the prediction, flattened for the
        # forensic record. We only carry the ROLLING_FEATURE_COLS here
        # to avoid leaking unrelated metadata into the JSON.
        model_features = {}
        for col in ROLLING_FEATURE_COLS:
            if col in home_row.index:
                model_features[f"home_{col}"] = (
                    None if pd.isna(home_row[col]) else float(home_row[col])
                )
            if col in away_row.index:
                model_features[f"away_{col}"] = (
                    None if pd.isna(away_row[col]) else float(away_row[col])
                )
        # Drop None entries (NaN features) so the JSON stays clean
        model_features = {k: v for k, v in model_features.items() if v is not None}

        try:
            pick = generate_pick(
                home_team_abbr=home_abbr,
                away_team_abbr=away_abbr,
                model_prob_home_win=model_prob,
                odds_df=odds_df[odds_df["game_id"] == game["game_id"]],
                model_version=model_version,
                model_features=model_features,
                published_at=datetime.now(timezone.utc),
            )
        except ValueError as e:
            logger.warning("Skipping %s vs %s: %s", home_abbr, away_abbr, e)
            continue

        json_path = publish_pick(
            pick,
            repo_root=REPO_ROOT,
            parquet_root=local_root / "picks",
        )
        n_published += 1

        # Concise human-readable summary on the console
        ev_str = f"{pick.expected_value:+.4f}" if pick.expected_value else "  (no_bet)"
        kelly_str = (
            f"{pick.kelly_fraction_half:.4f}" if pick.kelly_fraction_half else "—"
        )
        logger.info(
            "Pick %s | %s @ %s | model %s=%.4f | side=%s odds=%s EV=%s halfKelly=%s",
            pick.pick_id[:8],
            f"{away_abbr}@{home_abbr}",
            pick.market.sportsbook,
            home_abbr,
            model_prob,
            pick.pick_side,
            pick.pick_american_odds if pick.pick_american_odds is not None else "—",
            ev_str,
            kelly_str,
        )
        logger.info("  -> wrote %s", json_path.relative_to(REPO_ROOT))

    if n_published == 0:
        logger.warning("No picks published")
        return 1

    logger.info(
        "Published %d pick(s). Commit picks/ to git for verifiability:\n"
        "  git add picks/ && git commit -m 'Picks: %s' && git push origin main",
        n_published,
        args.game_date,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
