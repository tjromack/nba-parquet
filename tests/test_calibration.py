"""Tests for the calibration diagnostic layer.

Reliability-diagram math: bucket predictions by probability, compare
predicted-average to actual-frequency in each bucket. Expected
Calibration Error (ECE) is the weighted average distance, Maximum
Calibration Error (MCE) is the worst-bucket distance.

Hand-computed values against canonical synthetic distributions.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from models.calibration import (
    calibration_report,
    expected_calibration_error,
    maximum_calibration_error,
)


def _perfectly_calibrated_synthetic(n: int = 1000, seed: int = 0):
    """Probabilities uniform on [0,1]; outcome ~ Bernoulli(prob).
    A perfectly calibrated model — ECE should approach 0 as n grows.
    """
    rng = np.random.default_rng(seed)
    probs = rng.uniform(0.05, 0.95, size=n)
    outcomes = rng.binomial(1, probs)
    return outcomes, probs


def _overconfident_synthetic(n: int = 1000, seed: int = 0):
    """Predictions cluster at extremes (0.1 or 0.9) but actual win
    rate is 50% regardless. ECE should be large (~0.4)."""
    rng = np.random.default_rng(seed)
    probs = rng.choice([0.1, 0.9], size=n)
    outcomes = rng.binomial(1, 0.5, size=n)
    return outcomes, probs


# ---------------------------------------------------------------------------
# expected_calibration_error
# ---------------------------------------------------------------------------


def test_ece_perfectly_calibrated_is_near_zero():
    y, p = _perfectly_calibrated_synthetic(n=5000)
    ece = expected_calibration_error(y, p, n_bins=10)
    # Sample-size-limited; 5000 samples / 10 bins -> 500 per bin
    # Should be very small but not exactly 0
    assert ece < 0.03, f"perfectly calibrated should give ECE near 0, got {ece}"


def test_ece_overconfident_is_large():
    y, p = _overconfident_synthetic(n=2000)
    ece = expected_calibration_error(y, p, n_bins=10)
    # The model says 0.1 or 0.9 with 50/50 actual outcomes
    # bucket [0.0,0.1] sees actual 0.5: gap = 0.4-ish
    # bucket [0.9,1.0] sees actual 0.5: gap = 0.4-ish
    # ECE weighted average = ~0.4
    assert ece > 0.35, f"badly miscalibrated should give large ECE, got {ece}"


def test_ece_rejects_invalid_inputs():
    with pytest.raises(ValueError):
        expected_calibration_error([], [], n_bins=10)
    with pytest.raises(ValueError):
        expected_calibration_error([0, 1], [0.5], n_bins=10)  # length mismatch
    with pytest.raises(ValueError):
        # Probabilities out of [0,1]
        expected_calibration_error([0, 1], [-0.1, 1.5], n_bins=10)


def test_ece_handles_constant_predictions():
    """If every prediction is 0.5 and base rate is 0.5, ECE is 0."""
    y = [0, 1, 0, 1, 0, 1, 0, 1]
    p = [0.5] * 8
    ece = expected_calibration_error(y, p, n_bins=10)
    assert math.isclose(ece, 0.0, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# maximum_calibration_error
# ---------------------------------------------------------------------------


def test_mce_finds_worst_bucket():
    """One pathological bucket where prob=0.9 but actual=0.0; MCE = 0.9."""
    # 10 predictions, all at 0.9, all wrong (label = 0)
    y = [0] * 10
    p = [0.9] * 10
    mce = maximum_calibration_error(y, p, n_bins=10)
    assert math.isclose(mce, 0.9, abs_tol=0.01)


def test_mce_is_always_ge_ece():
    """MCE is the worst bucket; ECE is the weighted average. MCE >= ECE always."""
    y, p = _overconfident_synthetic(n=2000)
    ece = expected_calibration_error(y, p, n_bins=10)
    mce = maximum_calibration_error(y, p, n_bins=10)
    assert mce >= ece - 1e-9


# ---------------------------------------------------------------------------
# calibration_report
# ---------------------------------------------------------------------------


def test_report_returns_structured_dict_with_bin_details():
    y, p = _perfectly_calibrated_synthetic(n=2000)
    report = calibration_report(y, p, n_bins=10)
    assert "ece" in report
    assert "mce" in report
    assert "n_bins" in report
    assert report["n_bins"] == 10
    assert "bins" in report
    assert len(report["bins"]) == 10
    # Each bin entry has the expected fields
    for b in report["bins"]:
        assert {"lower", "upper", "count", "mean_pred", "actual_rate"} <= set(b)


def test_report_bins_are_ordered_and_non_overlapping():
    y, p = _perfectly_calibrated_synthetic(n=1000)
    report = calibration_report(y, p, n_bins=10)
    for i, b in enumerate(report["bins"]):
        assert b["lower"] >= 0.0
        assert b["upper"] <= 1.0
        assert b["lower"] <= b["upper"]
        if i > 0:
            assert b["lower"] >= report["bins"][i - 1]["upper"] - 1e-9


def test_report_total_count_matches_input_size():
    y, p = _perfectly_calibrated_synthetic(n=1234)
    report = calibration_report(y, p, n_bins=10)
    total = sum(b["count"] for b in report["bins"])
    assert total == 1234


def test_report_empty_bins_have_none_actual_rate():
    """A bucket with no predictions in it should report actual_rate=None,
    not divide-by-zero."""
    # All predictions in [0.4, 0.6] — bins outside that range are empty
    y = [0, 1, 0, 1, 0, 1]
    p = [0.45, 0.45, 0.5, 0.5, 0.55, 0.55]
    report = calibration_report(y, p, n_bins=10)
    empty_bins = [b for b in report["bins"] if b["count"] == 0]
    assert empty_bins, "expected some empty bins given the input range"
    for b in empty_bins:
        assert b["actual_rate"] is None
        assert b["mean_pred"] is None
