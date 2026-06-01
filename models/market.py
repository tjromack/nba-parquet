"""Market math: American odds <-> implied probability, vig removal,
expected value, Kelly bet sizing.

Pure-math module — no I/O, no Spark, no network. Every function takes
in scalars and returns scalars (or a small tuple). Test-first against
canonical sports-betting formulas in tests/test_market.py.

Why this lives in models/ rather than etl/: it's part of the
inference / decision layer (consumes model probabilities + market
prices, produces betting recommendations), not the data layer.

Disclaimer: this module computes expected values from a probabilistic
model against a sportsbook market. It does not constitute financial
or betting advice. The model has documented edge limits (currently
at baseline parity on the OOF test set — see README Phase 4b).
Anyone using these computations to size real bets accepts the
responsibility for verifying the model has positive CLV against the
markets they intend to bet, on a sample large enough to be
statistically meaningful (typically 200+ bets).
"""

from __future__ import annotations


def american_to_implied_prob(american_odds: int) -> float:
    """American odds -> implied probability (the market's "fair" prob
    INCLUDING the bookmaker's vig).

    Formula:
      negative odds (favorite):  p = |odds| / (|odds| + 100)
      positive odds (underdog):  p = 100 / (odds + 100)

    Note: this is the *vig-inclusive* probability, not the
    bookmaker's true belief. Use ``devig_two_way`` to recover the
    de-vigged "fair" probability for EV math against a sharp book.
    """
    if american_odds == 0:
        raise ValueError("American odds cannot be 0")
    if american_odds < 0:
        return abs(american_odds) / (abs(american_odds) + 100)
    return 100 / (american_odds + 100)


def american_to_decimal(american_odds: int) -> float:
    """American odds -> decimal odds (used as the multiplier on
    winning bets; payout = stake * decimal_odds, profit = stake * (b)
    where b = decimal_odds - 1).

    Formula:
      negative odds: decimal = 1 + 100 / |odds|
      positive odds: decimal = 1 + odds / 100
    """
    if american_odds == 0:
        raise ValueError("American odds cannot be 0")
    if american_odds < 0:
        return 1.0 + 100.0 / abs(american_odds)
    return 1.0 + american_odds / 100.0


def devig_two_way(home_odds: int, away_odds: int) -> tuple[float, float]:
    """Strip the bookmaker's vig from a two-way market.

    Standard two-way de-vig is proportional scaling: take the raw
    implied probabilities, divide each by their sum so they total
    exactly 1. This is the simplest of several de-vig methods; it
    assumes the book applies vig symmetrically (proportionally to
    each side's true prob), which is close enough for moneyline at
    sharp books like Pinnacle. Power and logit methods exist for
    edge cases — defer until needed.

    Returns ``(fair_home_prob, fair_away_prob)`` summing to 1.0.

    Raises ``ValueError`` if the raw implied probs sum to <= 1
    (theoretical arb or bad data — caller should investigate rather
    than have a silently-bad "fair" prob propagate).
    """
    raw_home = american_to_implied_prob(home_odds)
    raw_away = american_to_implied_prob(away_odds)
    total = raw_home + raw_away
    if total <= 1.0:
        raise ValueError(
            f"Market has zero or negative vig (raw probs sum to {total:.4f}). "
            "This is either a true arb or bad odds data; refusing to "
            "de-vig until the caller verifies."
        )
    return raw_home / total, raw_away / total


def expected_value(model_prob: float, american_odds: int) -> float:
    """Expected value per $1 stake.

    EV = model_prob * (decimal_odds - 1) - (1 - model_prob)
       = model_prob * b - (1 - model_prob)            where b = decimal - 1

    Positive EV means the model thinks this bet is +EV at the offered
    price; negative EV means betting it loses money on average over
    many repetitions. Zero EV is the break-even point — where the
    model's probability matches the market's implied probability.

    A 5% EV bet on a $100 stake has expected profit $5 *per bet*;
    realized profit varies wildly trial-to-trial.
    """
    if not 0.0 <= model_prob <= 1.0:
        raise ValueError(f"model_prob must be in [0, 1], got {model_prob}")
    b = american_to_decimal(american_odds) - 1.0
    return model_prob * b - (1.0 - model_prob)


def kelly_fraction(
    model_prob: float,
    american_odds: int,
    scale: float = 1.0,
) -> float:
    """Fraction of bankroll to bet under the Kelly criterion.

    Full Kelly:   f* = (p * (b + 1) - 1) / b   where b = decimal_odds - 1

    Where p is the model's probability and b is the net decimal odds.
    Full Kelly maximizes expected geometric growth but has high
    variance — practical bankrolls use fractional Kelly (often
    half- or quarter-Kelly) for lower drawdown risk.

    ``scale`` multiplies the fraction (0.5 = half-Kelly, 0.25 =
    quarter-Kelly, etc). Negative-edge bets clamp to 0 (don't bet)
    rather than returning a negative fraction — the math allows
    negative f* but it would mean "bet the other side at our offered
    odds", which isn't actionable for a single-side bet decision.
    """
    if not 0.0 <= model_prob <= 1.0:
        raise ValueError(f"model_prob must be in [0, 1], got {model_prob}")
    if scale < 0:
        raise ValueError(f"scale must be >= 0, got {scale}")
    b = american_to_decimal(american_odds) - 1.0
    full_kelly = (model_prob * (b + 1.0) - 1.0) / b
    if full_kelly <= 0:
        return 0.0
    return full_kelly * scale
