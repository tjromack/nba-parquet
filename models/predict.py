"""Scoring layer for the winner-prediction model (Phase 4b, session 2c).

Two honest uses, no future-schedule feed required (the pipeline only
ingests *completed* games):

- ``predict_matchup`` — given the latest rolling-feature snapshot of a
  home and an away team, the model's win probability for a hypothetical
  next game. Forward-looking, no leakage (there's no actual outcome to
  compare against), so using each team's most-recent features is correct.
- ``score_recent_games`` — attach the model's pick + probability to the
  *leak-free* per-game training frame next to the actual label, for the
  "how did it actually do" scorecard. Leakage matters here (these are
  real past games) so this consumes ``build_training_frame`` output,
  which is already lagged pre-game.

``load_model`` returns ``None`` when no artifact exists so the Streamlit
view degrades gracefully on a fresh clone (the .joblib is a gitignored
build output — `python -m models.train` produces it).
"""

from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd

from models.dataset import ROLLING_FEATURE_COLS, feature_columns
from models.train import DEFAULT_ARTIFACT_DIR, PRIMARY_MODEL


def load_model(artifact_dir: Path | str = DEFAULT_ARTIFACT_DIR):
    """Load the persisted primary model, or None if it hasn't been trained."""
    path = Path(artifact_dir) / f"winner_{PRIMARY_MODEL}.joblib"
    if not path.exists():
        return None
    return joblib.load(path)


def latest_team_features(features: pd.DataFrame) -> pd.DataFrame:
    """Each team's most recent rolling-feature row (its 'as of now' state).

    One row per team — the chronologically last game's rolling features,
    the correct pre-game state to feed a hypothetical next matchup.
    """
    if features.empty:
        return features
    ordered = features.sort_values(["team_abbreviation", "game_date", "game_id"])
    return (
        ordered.groupby("team_abbreviation", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )


def predict_matchup(model, home_row: pd.Series, away_row: pd.Series) -> dict:
    """Model prediction for a hypothetical home-vs-away game.

    ``home_row`` / ``away_row`` are single rows from
    ``latest_team_features`` (carry the ROLLING_FEATURE_COLS). Returns
    the predicted winner, the home win probability, and the feature
    vector that drove it (for display).
    """
    # Tolerate rolling cols that the team's feature row doesn't carry
    # (e.g. pre-Phase-B features data has no rolling_ortg). The pipeline's
    # imputer fills NaN at predict time, identical to how it handles
    # legit missing-history NaNs at fit time.
    x = {
        f"home_{c}": home_row[c] if c in home_row.index else float("nan")
        for c in ROLLING_FEATURE_COLS
    }
    x.update(
        {
            f"away_{c}": away_row[c] if c in away_row.index else float("nan")
            for c in ROLLING_FEATURE_COLS
        }
    )
    frame = pd.DataFrame([x])[feature_columns()]
    home_win_prob = float(model.predict_proba(frame)[:, 1][0])
    return {
        "predicted_home_win": bool(home_win_prob >= 0.5),
        "home_win_prob": round(home_win_prob, 4),
        "features": x,
    }


def score_recent_games(model, training_frame: pd.DataFrame) -> pd.DataFrame:
    """Attach model pick + probability to the leak-free training frame.

    The frame must come from ``build_training_frame`` (already pre-game
    lagged). Adds ``model_home_win_prob``, ``model_pick`` (1=home),
    ``correct``. Preserves row count and order.
    """
    if training_frame.empty:
        out = training_frame.copy()
        for col in ("model_home_win_prob", "model_pick", "correct"):
            out[col] = pd.Series(dtype="float64")
        return out
    feats = feature_columns()
    proba = model.predict_proba(training_frame[feats])[:, 1]
    out = training_frame.copy()
    out["model_home_win_prob"] = proba.round(4)
    out["model_pick"] = (proba >= 0.5).astype(int)
    out["correct"] = (out["model_pick"] == out["label"]).astype(int)
    return out
