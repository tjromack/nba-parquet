"""Time-series evaluation for the winner-prediction model.

The spec flags the train/test splitter as the second place target
leakage sneaks back in (the first being the feature lag in
``models.dataset``). Random k-fold on time-ordered games is leakage:
it lets the model "train on the future." So evaluation is **strict
walk-forward** — every test game is chronologically after every
training game.

``walk_forward_splits`` partitions on the *date* axis, never row count,
so games played on the same day are never split across the train/test
boundary. Expanding window: each successive fold trains on everything
up to a cutoff date and tests on the next block of dates.
"""

from __future__ import annotations

import pandas as pd

DATE_COL = "game_date"


def walk_forward_splits(
    frame: pd.DataFrame, n_splits: int = 4
) -> list[tuple[pd.Index, pd.Index]]:
    """Expanding-window walk-forward folds over a per-game frame.

    Returns a list of ``(train_index, test_index)`` pairs (positional
    indices into a date-sorted copy of ``frame``). For every fold,
    ``max(train game_date) < min(test game_date)`` — strictly, because
    the split falls on a date boundary, so no calendar day is ever
    split between train and test.

    Raises ``ValueError`` if there are fewer than ``n_splits + 1``
    distinct game dates (can't form that many non-empty ordered folds).
    """
    if n_splits < 1:
        raise ValueError(f"n_splits must be >= 1, got {n_splits}")
    if frame.empty:
        raise ValueError("cannot split an empty frame")

    ordered = frame.sort_values([DATE_COL, "game_id"]).reset_index(drop=True)
    distinct_dates = sorted(ordered[DATE_COL].unique())
    n_blocks = n_splits + 1
    if len(distinct_dates) < n_blocks:
        raise ValueError(
            f"need >= {n_blocks} distinct game dates for {n_splits} "
            f"walk-forward folds, got {len(distinct_dates)}"
        )

    # Partition the distinct-date axis into n_blocks contiguous groups;
    # boundary_dates[j] is the first date of block j (j = 1..n_splits).
    per = len(distinct_dates) // n_blocks
    boundary_dates = [distinct_dates[per * j] for j in range(1, n_blocks)]

    splits: list[tuple[pd.Index, pd.Index]] = []
    for j, cutoff in enumerate(boundary_dates):
        next_cutoff = boundary_dates[j + 1] if j + 1 < len(boundary_dates) else None
        train_mask = ordered[DATE_COL] < cutoff
        if next_cutoff is None:
            test_mask = ordered[DATE_COL] >= cutoff
        else:
            test_mask = (ordered[DATE_COL] >= cutoff) & (
                ordered[DATE_COL] < next_cutoff
            )
        train_idx = ordered.index[train_mask]
        test_idx = ordered.index[test_mask]
        if len(train_idx) and len(test_idx):
            splits.append((train_idx, test_idx))
    return splits
