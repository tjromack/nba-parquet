"""Train + evaluate the NBA winner-prediction model (Phase 4b).

Design contract (the parts that keep this honest):

- **Preprocessing-leakage firewall.** Every model is an sklearn
  ``Pipeline`` whose first step is a median ``SimpleImputer``. The
  pipeline is re-fit on *each walk-forward fold's training rows only* —
  never on the test rows or the full dataset. Fitting an imputer/scaler
  on all data before splitting is a classic silent leak; the
  fit-inside-the-fold-loop pattern makes that impossible by construction.
- **Same imputation for every model** so the baseline-vs-model
  comparison is apples-to-apples (logreg can't take NaN; HistGBM can —
  forcing both through the identical imputer keeps the delta meaningful).
- **Time-series evaluation only** via
  ``models.evaluation.walk_forward_splits`` (no random k-fold).
- **Deterministic**: fixed seed everywhere → identical metrics across
  runs, so MLflow numbers are reproducible from a clean clone.

``python -m models.train`` reads the live ``features/`` + ``processed/``
Parquet (same ``LOCAL_OUTPUT_DIR`` contract as the rest of the repo),
builds the leak-free training frame, evaluates baselines + both models
with walk-forward CV, logs everything to a local MLflow file store
(``./mlruns``), persists the chosen model to ``models/artifacts/``, and
prints an honest comparison table.
"""

from __future__ import annotations

import os
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from models.baselines import baseline_accuracies
from models.dataset import feature_columns
from models.evaluation import walk_forward_splits

SEED = 42
DEFAULT_N_SPLITS = 4
PRIMARY_MODEL = "hgb"  # the artifact that gets persisted (spec: HistGBM)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TRACKING_URI = (REPO_ROOT / "mlruns").as_uri()
DEFAULT_ARTIFACT_DIR = REPO_ROOT / "models" / "artifacts"


def make_model(model_name: str, seed: int = SEED) -> Pipeline:
    """Fresh sklearn Pipeline for ``model_name`` ('logreg' | 'hgb').

    Both pipelines start with the SAME median imputer so the
    baseline-vs-model comparison is fair (logreg cannot ingest NaN;
    HistGBM can — forcing both through identical imputation keeps the
    delta meaningful). A new instance every call: the walk-forward loop
    refits from scratch per fold so no fitted state survives across
    folds (the preprocessing-leak firewall).
    """
    if model_name == "logreg":
        return Pipeline(
            [
                ("impute", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("clf", LogisticRegression(max_iter=1000, random_state=seed)),
            ]
        )
    if model_name == "hgb":
        return Pipeline(
            [
                ("impute", SimpleImputer(strategy="median")),
                (
                    "clf",
                    HistGradientBoostingClassifier(
                        # Default min_samples_leaf=20 is far too large for
                        # this regime: walk-forward early folds train on
                        # ~15-30 games, so with the default the booster
                        # cannot make a single split and just predicts the
                        # majority class. 5 lets it actually learn on small
                        # folds. This is the documented small-data
                        # adaptation the spec anticipated — defensible
                        # because the honest evaluation is the point, not
                        # squeezing a thin playoff sample.
                        min_samples_leaf=5,
                        random_state=seed,
                    ),
                ),
            ]
        )
    raise ValueError(f"unknown model_name {model_name!r} (use 'logreg' or 'hgb')")


def _round(value: float, places: int = 6) -> float:
    """Round so the metrics dict is bit-stable run-to-run (determinism)."""
    return float(round(float(value), places))


def evaluate_walk_forward(
    frame: pd.DataFrame,
    model_name: str,
    n_splits: int = DEFAULT_N_SPLITS,
    seed: int = SEED,
) -> dict:
    """Walk-forward CV for one model. Returns out-of-fold metrics + the
    baseline accuracies computed on the *same* OOF test rows."""
    feats = feature_columns()
    ordered = frame.sort_values(["game_date", "game_id"]).reset_index(drop=True)
    splits = walk_forward_splits(frame, n_splits=n_splits)

    y_true: list[int] = []
    y_pred: list[int] = []
    y_proba: list[float] = []
    test_index: list[int] = []

    for train_idx, test_idx in splits:
        train = ordered.loc[train_idx]
        test = ordered.loc[test_idx]
        model = make_model(model_name, seed)  # fresh per fold
        model.fit(train[feats], train["label"])
        proba = model.predict_proba(test[feats])[:, 1]
        y_true.extend(test["label"].tolist())
        y_pred.extend((proba >= 0.5).astype(int).tolist())
        y_proba.extend(proba.tolist())
        test_index.extend(test_idx.tolist())

    oof = ordered.loc[test_index]
    baselines = baseline_accuracies(oof)

    return {
        "model": model_name,
        "n_splits_used": len(splits),
        "n_test": len(y_true),
        "accuracy": _round(accuracy_score(y_true, y_pred)),
        "log_loss": _round(log_loss(y_true, y_proba, labels=[0, 1])),
        "brier": _round(brier_score_loss(y_true, y_proba)),
        "baseline_always_home": _round(baselines["always_home"]),
        "baseline_better_win_pct": _round(baselines["better_win_pct"]),
        "baseline_better_ts_pct": _round(baselines["better_ts_pct"]),
    }


def oof_scored_frame(
    frame: pd.DataFrame,
    model_name: str = PRIMARY_MODEL,
    n_splits: int = DEFAULT_N_SPLITS,
    seed: int = SEED,
) -> pd.DataFrame:
    """Out-of-fold predictions joined back to game metadata.

    The honest "model vs reality" view: each row is scored by a model
    that did NOT train on it (the walk-forward test folds). Use this for
    any scorecard — NEVER score the persisted all-data model against the
    training games, which is in-sample and trivially ~100%, contradicting
    the real walk-forward accuracy. Games in the first block never appear
    in a test fold, so they correctly have no OOF prediction and are
    absent here. Returned chronologically.
    """
    feats = feature_columns()
    ordered = frame.sort_values(["game_date", "game_id"]).reset_index(drop=True)
    if ordered.empty:
        cols = [
            "game_id",
            "game_date",
            "home_team",
            "away_team",
            "label",
            "model_home_win_prob",
            "model_pick",
            "correct",
        ]
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    # Carry whatever identifier columns exist — real build_training_frame
    # output has home_team/away_team; minimal synthetic test frames may
    # only have game_id/game_date/label. The accuracy consistency with
    # walk-forward is independent of which metadata tags along.
    meta_cols = [
        c
        for c in ("game_id", "game_date", "home_team", "away_team", "label")
        if c in ordered.columns
    ]

    splits = walk_forward_splits(frame, n_splits=n_splits)
    pieces = []
    for train_idx, test_idx in splits:
        train = ordered.loc[train_idx]
        test = ordered.loc[test_idx]
        model = make_model(model_name, seed)  # fresh per fold, train-only fit
        model.fit(train[feats], train["label"])
        proba = model.predict_proba(test[feats])[:, 1]
        piece = test[meta_cols].copy()
        piece["model_home_win_prob"] = proba.round(4)
        piece["model_pick"] = (proba >= 0.5).astype(int)
        pieces.append(piece)

    out = pd.concat(pieces, ignore_index=True)
    out["correct"] = (out["model_pick"] == out["label"]).astype(int)
    return out.sort_values(["game_date", "game_id"]).reset_index(drop=True)


def evaluate_all(
    frame: pd.DataFrame, n_splits: int = DEFAULT_N_SPLITS, seed: int = SEED
) -> dict:
    """Baselines + both models on the same walk-forward test rows.

    Both models share identical deterministic splits, so the baseline
    numbers are identical between them and reported once.
    """
    logreg = evaluate_walk_forward(frame, "logreg", n_splits, seed)
    hgb = evaluate_walk_forward(frame, "hgb", n_splits, seed)
    best_baseline = max(
        logreg["baseline_always_home"],
        logreg["baseline_better_win_pct"],
        logreg["baseline_better_ts_pct"],
    )
    return {
        "n_games": len(frame),
        "n_test": hgb["n_test"],
        "n_splits_used": hgb["n_splits_used"],
        "logreg_accuracy": logreg["accuracy"],
        "logreg_log_loss": logreg["log_loss"],
        "logreg_brier": logreg["brier"],
        "hgb_accuracy": hgb["accuracy"],
        "hgb_log_loss": hgb["log_loss"],
        "hgb_brier": hgb["brier"],
        "baseline_always_home": hgb["baseline_always_home"],
        "baseline_better_win_pct": hgb["baseline_better_win_pct"],
        "baseline_better_ts_pct": hgb["baseline_better_ts_pct"],
        "best_baseline": best_baseline,
        "hgb_minus_best_baseline": _round(hgb["accuracy"] - best_baseline),
        "logreg_minus_best_baseline": _round(logreg["accuracy"] - best_baseline),
    }


def persist_model(
    frame: pd.DataFrame,
    model_name: str,
    artifact_dir: Path,
    seed: int = SEED,
) -> Path:
    """Refit ``model_name`` on ALL rows and joblib-dump it. Returns path.

    The persisted artifact is the deployable model; refitting on the
    full history (not a CV fold) is correct here — CV was for *honest
    evaluation*, the shipped model should use every game available.
    """
    artifact_dir = Path(artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    feats = feature_columns()
    model = make_model(model_name, seed)
    model.fit(frame[feats], frame["label"])
    path = artifact_dir / f"winner_{model_name}.joblib"
    joblib.dump(model, path)
    return path


def train_and_log(
    frame: pd.DataFrame,
    *,
    tracking_uri: str,
    artifact_dir: Path,
    n_splits: int = DEFAULT_N_SPLITS,
    seed: int = SEED,
) -> dict:
    """Evaluate, log the run to MLflow at ``tracking_uri``, persist the
    primary model to ``artifact_dir``. Returns the summary dict."""
    import mlflow

    summary = evaluate_all(frame, n_splits=n_splits, seed=seed)

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("nba-parquet-winner")
    with mlflow.start_run():
        mlflow.log_params(
            {
                "seed": seed,
                "n_splits": n_splits,
                "primary_model": PRIMARY_MODEL,
                "n_games": summary["n_games"],
            }
        )
        mlflow.log_metrics(
            {k: float(v) for k, v in summary.items() if isinstance(v, (int, float))}
        )
        model_path = persist_model(frame, PRIMARY_MODEL, artifact_dir, seed)
        mlflow.log_artifact(str(model_path))

    return summary


def main() -> None:  # pragma: no cover - thin CLI wrapper
    from etl.paths import LOCAL_OUTPUT_ENV
    from models.dataset import build_training_frame

    root = Path(os.environ.get(LOCAL_OUTPUT_ENV, "./out"))
    features = pd.read_parquet(root / "features/nba/rolling_team_stats")
    processed = pd.read_parquet(root / "processed/nba/team_game_stats")
    features["game_date"] = pd.to_datetime(features["game_date"].astype(str))
    processed["game_date"] = pd.to_datetime(processed["game_date"].astype(str))

    frame = build_training_frame(features, processed)
    summary = train_and_log(
        frame,
        tracking_uri=DEFAULT_TRACKING_URI,
        artifact_dir=DEFAULT_ARTIFACT_DIR,
    )
    print("\n=== Walk-forward winner-prediction summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":  # pragma: no cover
    main()
