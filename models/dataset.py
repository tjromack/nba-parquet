"""Leak-free training-frame construction for the Phase 4b prediction model.

This module turns the per-(team, game) rolling **features** layer into a
per-**game** training matrix suitable for a binary winner classifier.

The single most important property here is **no target leakage**. The
``features/`` rolling columns for game N are computed over a window that
*includes game N itself*. Using them to predict game N would leak the
outcome into the inputs and make every downstream metric a lie. So for
each team we use their rolling features **as they stood entering the
game** — i.e. lagged by one game within that team's chronological
sequence. A team's very first game has no prior window and is therefore
not a usable training row (dropped).

Pure pandas by design: the model layer (sklearn / mlflow, later) is
pandas-native, the game-level frame is tiny (≈70 playoff games, ≈1,200
for a full regular season — both trivially in-memory), and keeping this
decoupled from Spark makes the leakage test fast and JVM-free.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Rolling feature columns produced by etl.features.build_rolling_features.
# These are the only columns lagged and fed to the model.
ROLLING_FEATURE_COLS: list[str] = [
    "games_in_window",
    "rolling_pts",
    "rolling_efg_pct",
    "rolling_ts_pct",
    "rolling_ast_to_tov",
    "rolling_win_pct",
    "rolling_pts_home",
    "rolling_pts_away",
]

# "Has at least one prior game" sentinel. games_in_window is always
# populated in the features layer (never null), so after the per-team
# lag-1 shift, a NaN here uniquely identifies a team's first game —
# the one row that can't be a training example. Other rolling_* cols
# can be legitimately NaN (e.g. no home games in window yet), so they
# must NOT be used as the drop sentinel.
_HISTORY_SENTINEL = "games_in_window"

OUTPUT_COLUMNS: list[str] = (
    ["game_id", "game_date", "season", "home_team", "away_team"]
    + [f"home_{c}" for c in ROLLING_FEATURE_COLS]
    + [f"away_{c}" for c in ROLLING_FEATURE_COLS]
    + ["label"]
)

# Identifiers / target — never fed to the model. Everything else in
# OUTPUT_COLUMNS (the home_* / away_* lagged rolling features) is.
_NON_FEATURE_COLUMNS = frozenset(
    ["game_id", "game_date", "season", "home_team", "away_team", "label"]
)


def feature_columns() -> list[str]:
    """The model input columns — single source of truth.

    Defined by *exclusion* (everything in OUTPUT_COLUMNS that isn't an
    identifier or the label) so a new rolling feature added upstream is
    automatically picked up, and an identifier can never accidentally
    leak in as a predictor.
    """
    return [c for c in OUTPUT_COLUMNS if c not in _NON_FEATURE_COLUMNS]


def _empty_training_frame() -> pd.DataFrame:
    """Empty frame with the full output schema (off-day / cold-start safe)."""
    return pd.DataFrame({c: pd.Series(dtype="object") for c in OUTPUT_COLUMNS})


def build_training_frame(
    features: pd.DataFrame, processed: pd.DataFrame
) -> pd.DataFrame:
    """Assemble a leak-free, one-row-per-game training matrix.

    Parameters
    ----------
    features:
        The rolling-features layer — one row per (team, game) with the
        ``ROLLING_FEATURE_COLS`` plus ``team_abbreviation``, ``game_id``,
        ``game_date``, ``season``.
    processed:
        The team-game layer — one row per (team, game) with ``game_id``,
        ``game_date``, ``team_abbreviation``, ``opponent_abbreviation``,
        ``is_home``, ``win``.

    Returns
    -------
    One row per game: the home team's *lagged* rolling features
    (``home_*``), the away team's *lagged* rolling features (``away_*``),
    and ``label`` = 1 if the home team won. Games where either team has
    no prior history (their first game in the dataset) are dropped.
    """
    if features.empty or processed.empty:
        return _empty_training_frame()

    # --- 1. Lag each team's rolling features by one game ---
    # Sorted within team by (game_date, game_id); shift(1) makes each
    # row carry the team's PREVIOUS game's rolling values = "as they
    # stood entering this game". This is the leakage firewall.
    feat = features.sort_values(
        ["team_abbreviation", "game_date", "game_id"]
    ).reset_index(drop=True)
    lagged = feat.groupby("team_abbreviation", sort=False)[ROLLING_FEATURE_COLS].shift(
        1
    )
    lagged.columns = [f"lag_{c}" for c in ROLLING_FEATURE_COLS]
    feat = pd.concat(
        [
            feat[["game_id", "game_date", "season", "team_abbreviation"]],
            lagged,
        ],
        axis=1,
    )
    # A team's first game has no prior window -> not a usable training row.
    feat = feat.dropna(subset=[f"lag_{_HISTORY_SENTINEL}"]).reset_index(drop=True)

    # --- 2. Per-game orientation + label from the processed layer ---
    # Drop any game_id that doesn't resolve to exactly one home + one away
    # row. Two real causes in practice:
    #   * Neutral-site games (NBA Cup knockout in Vegas, NBA Global Games)
    #     where nba_api encodes both teams' MATCHUP with "@", so both rows
    #     parse to is_home=False. These genuinely have no home team and
    #     cannot be a row in a home-team-win frame.
    #   * Genuinely corrupt rows (duplicate team-game from a re-ingest, etc).
    # Both get silently dropped with a warning — the model layer should be
    # defensive, not brittle, so a few unusable games don't sink an
    # otherwise-clean 1,200-game training frame.
    home = processed.loc[
        processed["is_home"] == True,  # noqa: E712  (pandas mask, not `is`)
        ["game_id", "team_abbreviation", "win"],
    ]
    away = processed.loc[
        processed["is_home"] == False,  # noqa: E712
        ["game_id", "team_abbreviation"],
    ]
    valid_home = set(home["game_id"][~home["game_id"].duplicated(keep=False)])
    valid_away = set(away["game_id"][~away["game_id"].duplicated(keep=False)])
    usable = valid_home & valid_away
    all_games = set(processed["game_id"].unique())
    dropped = all_games - usable
    if dropped:
        logger.warning(
            "build_training_frame: dropped %d/%d games with non-standard "
            "home/away orientation (neutral-site or corrupt). Sample: %s",
            len(dropped),
            len(all_games),
            sorted(dropped)[:5],
        )
    home = home[home["game_id"].isin(usable)]
    away = away[away["game_id"].isin(usable)]
    home = home.rename(columns={"team_abbreviation": "home_team", "win": "label"})
    away = away.rename(columns={"team_abbreviation": "away_team"})

    games = home.merge(away, on="game_id", how="inner")

    # --- 3. Attach each side's lagged features ---
    home_feat = feat.rename(
        columns={
            "team_abbreviation": "home_team",
            **{f"lag_{c}": f"home_{c}" for c in ROLLING_FEATURE_COLS},
        }
    )[
        ["game_id", "home_team", "game_date", "season"]
        + [f"home_{c}" for c in ROLLING_FEATURE_COLS]
    ]
    away_feat = feat.rename(
        columns={
            "team_abbreviation": "away_team",
            **{f"lag_{c}": f"away_{c}" for c in ROLLING_FEATURE_COLS},
        }
    )[["game_id", "away_team"] + [f"away_{c}" for c in ROLLING_FEATURE_COLS]]

    # Inner joins drop any game where either side lacks prior history.
    out = games.merge(home_feat, on=["game_id", "home_team"], how="inner")
    out = out.merge(away_feat, on=["game_id", "away_team"], how="inner")

    out["label"] = out["label"].astype(int)
    return (
        out[OUTPUT_COLUMNS].sort_values(["game_date", "game_id"]).reset_index(drop=True)
    )
