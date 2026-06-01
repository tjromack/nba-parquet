"""Tests for the market-math layer.

Pure-math module — no I/O, no Spark, no network. Every test uses
hand-computable values against the canonical sports-betting formulas
so any drift in the math fails loudly.

Canonical references baked into the test cases:
  - American odds -> implied prob:
      negative odds (favorite, e.g. -150): p = |odds| / (|odds| + 100)
      positive odds (dog,      e.g. +150): p = 100 / (odds + 100)
  - -110 (standard vig line) → 0.5238 implied prob → 0.0238 vig per side
  - Two-way vig removal (de-vigging): normalize p_home + p_away to sum to 1
  - EV per $1 stake: model_prob * (decimal_odds - 1) - (1 - model_prob)
  - Kelly fraction: (model_prob * (b + 1) - 1) / b   where b = decimal_odds - 1
"""

from __future__ import annotations

import math

import pytest

from models.market import (
    american_to_decimal,
    american_to_implied_prob,
    devig_two_way,
    expected_value,
    kelly_fraction,
)

# ---------------------------------------------------------------------------
# american_to_implied_prob
# ---------------------------------------------------------------------------


def test_implied_prob_minus_110_is_canonical_vig_line():
    """-110 is the standard US sportsbook line: 0.5238 implied prob,
    which gives 0.0238 of vig per side (4.76% total hold on -110/-110).
    """
    assert math.isclose(american_to_implied_prob(-110), 110 / 210, rel_tol=1e-9)
    assert math.isclose(american_to_implied_prob(-110), 0.52381, abs_tol=1e-4)


def test_implied_prob_heavy_favorite():
    """-250: |odds| / (|odds| + 100) = 250 / 350 = 0.7143"""
    assert math.isclose(american_to_implied_prob(-250), 250 / 350, rel_tol=1e-9)


def test_implied_prob_underdog():
    """+220: 100 / (220 + 100) = 100 / 320 = 0.3125"""
    assert math.isclose(american_to_implied_prob(220), 100 / 320, rel_tol=1e-9)


def test_implied_prob_even_money():
    """+100 (or -100) is even money: implied prob exactly 0.5."""
    assert math.isclose(american_to_implied_prob(100), 0.5, rel_tol=1e-9)
    assert math.isclose(american_to_implied_prob(-100), 0.5, rel_tol=1e-9)


def test_implied_prob_rejects_zero():
    with pytest.raises(ValueError):
        american_to_implied_prob(0)


# ---------------------------------------------------------------------------
# american_to_decimal
# ---------------------------------------------------------------------------


def test_decimal_minus_110_is_1_9091():
    """-110 American = 1.9091 decimal (round to 4dp). decimal = 100/110 + 1."""
    assert math.isclose(american_to_decimal(-110), 1 + 100 / 110, rel_tol=1e-9)


def test_decimal_plus_220_is_3_20():
    """+220 American = 3.20 decimal. decimal = 220/100 + 1."""
    assert math.isclose(american_to_decimal(220), 3.2, rel_tol=1e-9)


def test_decimal_even_money_is_2():
    assert math.isclose(american_to_decimal(100), 2.0, rel_tol=1e-9)
    assert math.isclose(american_to_decimal(-100), 2.0, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# devig_two_way
# ---------------------------------------------------------------------------


def test_devig_minus_110_each_side_is_50_50():
    """-110 / -110 is the canonical balanced market. Raw implied probs
    are 0.5238 each = 1.0476 sum. De-vigging by proportional scaling
    yields fair 0.50 / 0.50."""
    fair_home, fair_away = devig_two_way(-110, -110)
    assert math.isclose(fair_home, 0.5, rel_tol=1e-9)
    assert math.isclose(fair_away, 0.5, rel_tol=1e-9)


def test_devig_asymmetric_market_sums_to_one_exactly():
    """An asymmetric market (-260 home / +215 away) still de-vigs to
    probs summing exactly to 1. The relative magnitude is preserved."""
    fair_home, fair_away = devig_two_way(-260, 215)
    assert math.isclose(fair_home + fair_away, 1.0, rel_tol=1e-9)
    # Home is the favorite, so fair_home > 0.5
    assert fair_home > 0.5
    # Specific hand-computed value:
    # raw p_home = 260/360 = 0.72222
    # raw p_away = 100/315 = 0.31746
    # sum = 1.03968 (3.97% vig)
    # fair_home = 0.72222 / 1.03968 = 0.69466
    assert math.isclose(fair_home, (260 / 360) / (260 / 360 + 100 / 315), rel_tol=1e-9)


def test_devig_rejects_invalid_market():
    """A market that doesn't have positive vig (probs sum to <= 1) is
    suspicious — either bad data or an arb. Raise so the caller
    notices rather than silently propagating bogus 'fair' probs."""
    # Construct prices whose implied probs sum to < 1 (theoretical arb)
    # +200 / +200: 1/3 + 1/3 = 0.667, no vig. Raise.
    with pytest.raises(ValueError, match="vig"):
        devig_two_way(200, 200)


# ---------------------------------------------------------------------------
# expected_value
# ---------------------------------------------------------------------------


def test_ev_break_even_at_implied_prob():
    """If your model's probability exactly matches the market's
    implied probability, EV per $1 stake is 0 (you'd break even,
    minus the vig)."""
    # -110 has implied prob ~0.5238. Model says 0.5238 too.
    ev = expected_value(model_prob=110 / 210, american_odds=-110)
    assert math.isclose(ev, 0.0, abs_tol=1e-9)


def test_ev_positive_when_model_disagrees_with_market():
    """Model says home wins 60% of the time, market is -110 (implied
    52.38%). EV per $1 stake = 0.60 * 0.9091 - 0.40 * 1 = 0.1455."""
    # decimal = 100/110 + 1 = 1.9091; b = 0.9091
    ev = expected_value(model_prob=0.6, american_odds=-110)
    expected = 0.6 * (100 / 110) - 0.4
    assert math.isclose(ev, expected, rel_tol=1e-9)
    assert ev > 0  # positive EV bet


def test_ev_negative_when_market_disagrees_with_model_other_way():
    """Model says home wins only 40% of the time, market is -110.
    EV is negative — we'd be betting into the vig wall."""
    ev = expected_value(model_prob=0.4, american_odds=-110)
    assert ev < 0


def test_ev_positive_on_dog_when_model_thinks_dog_undervalued():
    """+220 dog. Implied prob = 31.25%. Model says 40%. Positive EV:
    EV = 0.4 * 2.2 - 0.6 = 0.28"""
    ev = expected_value(model_prob=0.4, american_odds=220)
    assert math.isclose(ev, 0.4 * 2.2 - 0.6, rel_tol=1e-9)
    assert ev > 0


def test_ev_rejects_invalid_probability():
    with pytest.raises(ValueError):
        expected_value(model_prob=1.5, american_odds=-110)
    with pytest.raises(ValueError):
        expected_value(model_prob=-0.1, american_odds=-110)


# ---------------------------------------------------------------------------
# kelly_fraction
# ---------------------------------------------------------------------------


def test_kelly_zero_when_no_edge():
    """Kelly fraction is 0 when model_prob equals market-implied prob —
    no edge means don't bet."""
    f = kelly_fraction(model_prob=110 / 210, american_odds=-110)
    assert math.isclose(f, 0.0, abs_tol=1e-9)


def test_kelly_positive_when_edge_exists():
    """Standard Kelly with 0.60 model_prob @ -110 odds.
    b = 0.9091, p = 0.6, q = 0.4
    f = (p * (b + 1) - 1) / b = (0.6 * 1.9091 - 1) / 0.9091 = 0.16
    """
    f = kelly_fraction(model_prob=0.6, american_odds=-110)
    b = 100 / 110
    expected = (0.6 * (b + 1) - 1) / b
    assert math.isclose(f, expected, rel_tol=1e-9)
    assert 0 < f < 1


def test_kelly_clamps_negative_edges_to_zero():
    """A negative-edge bet shouldn't return a negative Kelly fraction
    (which would mean 'bet the other side at our offered odds', not
    meaningful for a single-side bet decision). Clamp to 0."""
    f = kelly_fraction(model_prob=0.3, american_odds=-110)
    assert f == 0.0


def test_kelly_fractional_scaling():
    """Half-Kelly (fraction=0.5) is the common practical bet sizing
    — variance much lower, geometric growth still strong. Verify the
    scale knob works."""
    full = kelly_fraction(model_prob=0.6, american_odds=-110, scale=1.0)
    half = kelly_fraction(model_prob=0.6, american_odds=-110, scale=0.5)
    quarter = kelly_fraction(model_prob=0.6, american_odds=-110, scale=0.25)
    assert math.isclose(half, full * 0.5, rel_tol=1e-9)
    assert math.isclose(quarter, full * 0.25, rel_tol=1e-9)
