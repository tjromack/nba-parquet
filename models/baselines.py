"""Naive baselines for the winner-prediction model.

A model is only worth anything if it beats the obvious heuristics. These
three are the bar:

1. ``always_home``      — predict the home team wins, every game. NBA
   home-court advantage is real; on the current playoff slice this
   alone is ~0.54 accuracy (measured 2026-05-18). Any model that can't
   clear this is useless.
2. ``better_win_pct``   — predict whichever team had the better trailing
   ``rolling_win_pct`` entering the game.
3. ``better_ts_pct``    — predict whichever team had the better trailing
   ``rolling_ts_pct`` entering the game.

Predictions are over the per-game training frame from
``models.dataset.build_training_frame`` (``label`` = 1 if home won, and
the ``home_*`` / ``away_*`` columns are already leak-free / pre-game).

Pure pandas, no model libraries — these exist to make the model's
reported lift honest, so they must be trivially correct.
"""

from __future__ import annotations

import pandas as pd

BASELINE_NAMES = ("always_home", "better_win_pct", "better_ts_pct")


def predict_always_home(frame: pd.DataFrame) -> pd.Series:
    """Predict the home team wins every game (constant 1)."""
    return pd.Series(1, index=frame.index, name="always_home")


def predict_better_win_pct(frame: pd.DataFrame) -> pd.Series:
    """Predict home iff home's trailing win pct >= away's.

    Ties go to the home team (consistent with always_home's prior — a
    coin-flip on a tie would just add noise to the comparison).
    """
    pick_home = frame["home_rolling_win_pct"] >= frame["away_rolling_win_pct"]
    return pick_home.astype(int).rename("better_win_pct")


def predict_better_ts_pct(frame: pd.DataFrame) -> pd.Series:
    """Predict home iff home's trailing true-shooting pct >= away's."""
    pick_home = frame["home_rolling_ts_pct"] >= frame["away_rolling_ts_pct"]
    return pick_home.astype(int).rename("better_ts_pct")


_PREDICTORS = {
    "always_home": predict_always_home,
    "better_win_pct": predict_better_win_pct,
    "better_ts_pct": predict_better_ts_pct,
}


def accuracy(predictions: pd.Series, labels: pd.Series) -> float:
    """Fraction of predictions that match the label. 0.0 on an empty set."""
    if len(labels) == 0:
        return 0.0
    return float((predictions.values == labels.values).mean())


def baseline_accuracies(frame: pd.DataFrame) -> dict[str, float]:
    """Accuracy of all three baselines on ``frame`` (keyed by name).

    NaN guard: ``better_*`` baselines compare two columns; a NaN on
    either side makes the comparison False (picks away). That is a
    deterministic, documented behavior — not silently dropped — so the
    accuracy stays comparable across the model run.
    """
    if frame.empty:
        return {name: 0.0 for name in BASELINE_NAMES}
    labels = frame["label"]
    return {
        name: accuracy(predictor(frame), labels)
        for name, predictor in _PREDICTORS.items()
    }
