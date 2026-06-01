"""Calibration diagnostics for binary-classifier probabilities.

The v1.3.x story made it explicit that accuracy alone doesn't tell us
whether the model's *confidence* is honest. A model that predicts
0.79 home win in a matchup the market prices at 0.62 is making a
specific quantitative claim: "I'm right at this confidence level
some fraction of the time." Calibration measures whether the claim
holds.

Reliability diagram: bucket predictions by probability (e.g., 10
equal-width bins on [0,1]), compute the mean predicted probability
and the actual win rate within each bucket. A perfectly calibrated
model produces a diagonal line. An overconfident model produces a
curve that's flatter than the diagonal at the extremes — when it
says 80%, it's only right 65% of the time.

Expected Calibration Error (ECE): weighted average distance between
predicted and actual across bins; the headline scalar.
Maximum Calibration Error (MCE): worst single-bin gap; useful for
spotting one pathological bucket among otherwise-OK calibration.

Pure-math module — no sklearn, no I/O. Test-first against hand-
computable synthetic distributions in tests/test_calibration.py.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np


def _validate(
    y_true: Sequence[int], y_prob: Sequence[float]
) -> tuple[np.ndarray, np.ndarray]:
    y = np.asarray(y_true, dtype=float)
    p = np.asarray(y_prob, dtype=float)
    if y.size == 0 or p.size == 0:
        raise ValueError("y_true and y_prob must be non-empty")
    if y.shape != p.shape:
        raise ValueError(f"y_true and y_prob length mismatch: {y.shape} vs {p.shape}")
    if np.any((p < 0.0) | (p > 1.0)):
        raise ValueError(
            "y_prob values must be in [0, 1] — got " f"min={p.min()}, max={p.max()}"
        )
    return y, p


def calibration_report(
    y_true: Sequence[int], y_prob: Sequence[float], n_bins: int = 10
) -> dict:
    """Reliability-diagram data for the given predictions + outcomes.

    Returns a dict with:
      - ``n_bins`` (int): bucket count
      - ``ece`` (float): Expected Calibration Error
      - ``mce`` (float): Maximum Calibration Error
      - ``bins`` (list[dict]): one entry per bucket with
        ``lower``, ``upper``, ``count``, ``mean_pred``, ``actual_rate``.
        Empty bins return ``mean_pred=None`` and ``actual_rate=None``
        (not zero — distinguishes "no data" from "predictions were
        all-zero").

    Bins are equal-width on [0,1], inclusive on the lower edge and
    exclusive on the upper (the last bin includes 1.0 to avoid losing
    boundary samples).
    """
    y, p = _validate(y_true, y_prob)
    if n_bins < 1:
        raise ValueError(f"n_bins must be >= 1, got {n_bins}")

    edges = np.linspace(0.0, 1.0, n_bins + 1)
    bins = []
    weighted_gap_sum = 0.0
    max_gap = 0.0
    total = len(y)

    for i in range(n_bins):
        lo, hi = float(edges[i]), float(edges[i + 1])
        if i == n_bins - 1:
            mask = (p >= lo) & (p <= hi)
        else:
            mask = (p >= lo) & (p < hi)
        count = int(mask.sum())
        if count == 0:
            bins.append(
                {
                    "lower": lo,
                    "upper": hi,
                    "count": 0,
                    "mean_pred": None,
                    "actual_rate": None,
                }
            )
            continue
        bin_p = p[mask]
        bin_y = y[mask]
        mean_pred = float(bin_p.mean())
        actual_rate = float(bin_y.mean())
        gap = abs(mean_pred - actual_rate)
        weighted_gap_sum += gap * count
        if gap > max_gap:
            max_gap = gap
        bins.append(
            {
                "lower": lo,
                "upper": hi,
                "count": count,
                "mean_pred": mean_pred,
                "actual_rate": actual_rate,
            }
        )

    ece = weighted_gap_sum / total
    return {
        "n_bins": n_bins,
        "ece": float(ece),
        "mce": float(max_gap),
        "bins": bins,
    }


def expected_calibration_error(
    y_true: Sequence[int], y_prob: Sequence[float], n_bins: int = 10
) -> float:
    """Just the ECE scalar — convenience wrapper around
    ``calibration_report``."""
    return calibration_report(y_true, y_prob, n_bins=n_bins)["ece"]


def maximum_calibration_error(
    y_true: Sequence[int], y_prob: Sequence[float], n_bins: int = 10
) -> float:
    """Just the MCE scalar — the worst-bucket calibration gap."""
    return calibration_report(y_true, y_prob, n_bins=n_bins)["mce"]


def format_reliability_table(report: dict) -> str:
    """ASCII reliability table for the train.py CLI summary.

    One row per bucket: lower-upper range, count, mean predicted,
    actual rate, gap. Empty buckets show ``-``.
    """
    lines = []
    lines.append(
        f"Reliability diagram (n_bins={report['n_bins']}, "
        f"ECE={report['ece']:.4f}, MCE={report['mce']:.4f})"
    )
    lines.append("  bucket          n    mean_pred   actual_rate   gap")
    lines.append("  " + "-" * 56)
    for b in report["bins"]:
        rng = f"[{b['lower']:.2f},{b['upper']:.2f}]"
        if b["count"] == 0:
            lines.append(f"  {rng:14s}  {b['count']:4d}      -            -        -")
        else:
            gap = abs(b["mean_pred"] - b["actual_rate"])
            lines.append(
                f"  {rng:14s}  {b['count']:4d}    {b['mean_pred']:.4f}      "
                f"{b['actual_rate']:.4f}     {gap:.4f}"
            )
    return "\n".join(lines)
