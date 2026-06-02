"""Tests for the pick-generation layer.

generate_pick joins a model probability to a market snapshot and
produces a structured Pick record with EV, Kelly, and an audit trail.
publish_pick writes the Pick to both a parquet zone (queryable) and
a git-trackable JSON snapshot (verifiable timestamp).

Hand-computed values from the formulas in tests/test_market.py.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from models.picks import (
    TEAM_NAME_TO_ABBR,
    MarketSnapshot,
    Pick,
    abbr_for_team_name,
    generate_pick,
    publish_pick,
)


def _odds_row(
    *,
    sportsbook="pinnacle",
    market_type="h2h",
    outcome_name,
    price,
    point=None,
    game_id="g1",
    home_team="Oklahoma City Thunder",
    away_team="Indiana Pacers",
    game_date=date(2026, 6, 4),
    commence_time=datetime(2026, 6, 5, 0, 30, tzinfo=timezone.utc),
    fetched_at=datetime(2026, 6, 4, 18, 0, tzinfo=timezone.utc),
):
    return {
        "game_id": game_id,
        "game_date": game_date,
        "commence_time": commence_time,
        "home_team": home_team,
        "away_team": away_team,
        "sportsbook": sportsbook,
        "market_type": market_type,
        "outcome_name": outcome_name,
        "price": price,
        "point": point,
        "fetched_at": fetched_at,
    }


def _h2h_odds_df(*, home_odds: int, away_odds: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            _odds_row(outcome_name="Oklahoma City Thunder", price=home_odds),
            _odds_row(outcome_name="Indiana Pacers", price=away_odds),
        ]
    )


# ---------------------------------------------------------------------------
# TEAM_NAME_TO_ABBR
# ---------------------------------------------------------------------------


def test_team_name_map_has_30_teams():
    """All 30 NBA franchises mapped."""
    assert len(TEAM_NAME_TO_ABBR) >= 30
    # Sanity check a few
    assert TEAM_NAME_TO_ABBR["Oklahoma City Thunder"] == "OKC"
    assert TEAM_NAME_TO_ABBR["Indiana Pacers"] == "IND"
    assert TEAM_NAME_TO_ABBR["Los Angeles Lakers"] == "LAL"
    assert TEAM_NAME_TO_ABBR["Philadelphia 76ers"] == "PHI"


def test_abbr_lookup_handles_alternate_clippers_spellings():
    """The Odds API has historically returned both 'LA Clippers' and
    'Los Angeles Clippers'. Either should resolve to LAC."""
    assert abbr_for_team_name("LA Clippers") == "LAC"
    assert abbr_for_team_name("Los Angeles Clippers") == "LAC"


def test_abbr_lookup_raises_on_unknown_team():
    with pytest.raises(KeyError, match="Unknown team name"):
        abbr_for_team_name("Springfield Isotopes")


# ---------------------------------------------------------------------------
# generate_pick — happy path
# ---------------------------------------------------------------------------


def test_generate_pick_picks_home_when_model_says_home_is_undervalued():
    """Model says OKC wins 75% of the time. Pinnacle prices OKC at
    -260 (raw implied 0.7222), IND at +215 (raw implied 0.3175).
    De-vig: 1.0397 sum -> fair OKC = 0.6947, fair IND = 0.3053.
    Model 0.75 > fair 0.6947 -> +EV bet on OKC at -260 offered.
    """
    odds = _h2h_odds_df(home_odds=-260, away_odds=215)
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.75,
        odds_df=odds,
        model_version="test-sha",
        model_features={"home_rolling_ortg": 117.0, "away_rolling_drtg": 110.0},
    )
    assert pick.pick_side == "home"
    assert pick.pick_american_odds == -260
    # EV at p=0.75, b=(100/260)=0.3846
    # EV = 0.75 * 0.3846 - 0.25 = 0.0385
    assert math.isclose(pick.expected_value, 0.75 * (100 / 260) - 0.25, rel_tol=1e-9)
    assert pick.expected_value > 0
    # Kelly at half-scale must be > 0
    assert pick.kelly_fraction_half is not None
    assert pick.kelly_fraction_half > 0


def test_generate_pick_picks_away_when_dog_is_undervalued():
    """Model says IND wins 35% of the time (more than fair 0.3053).
    Pinnacle offers IND at +215 -> +EV bet on IND.
    """
    odds = _h2h_odds_df(home_odds=-260, away_odds=215)
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.65,  # 0.35 implied for IND
        odds_df=odds,
        model_version="test-sha",
        model_features={},
    )
    # fair IND prob = 0.3053; model gives 0.35 to IND -> +EV bet
    assert pick.pick_side == "away"
    assert pick.pick_american_odds == 215
    # EV = 0.35 * 2.15 - 0.65 = 0.1025
    assert math.isclose(pick.expected_value, 0.35 * 2.15 - 0.65, rel_tol=1e-9)


def test_generate_pick_no_bet_when_neither_side_has_edge():
    """Model agrees with the de-vigged market within a few bps —
    both sides are effectively zero EV (or slightly negative once
    you account for the vig at offered prices). No bet."""
    odds = _h2h_odds_df(home_odds=-260, away_odds=215)
    # fair_home_prob ~0.6947 from the -260/+215 market.
    # Model says exactly 0.6947 -> both sides go from -EV to ~0.
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.6947,
        odds_df=odds,
        model_version="test-sha",
        model_features={},
    )
    assert pick.pick_side == "no_bet"
    assert pick.pick_american_odds is None
    # EV may be slightly negative due to vig on either side
    # (which is exactly why no_bet is correct)
    assert pick.kelly_fraction_half is None


def test_generate_pick_captures_market_snapshot_for_audit():
    """The Pick must carry the exact market state at publish time —
    sportsbook used, raw + de-vigged probs, vig percent."""
    odds = _h2h_odds_df(home_odds=-110, away_odds=-110)
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.55,
        odds_df=odds,
        model_version="test-sha",
        model_features={},
    )
    snap = pick.market
    assert snap.sportsbook == "pinnacle"
    assert snap.home_odds_american == -110
    assert snap.away_odds_american == -110
    assert math.isclose(snap.home_implied_prob, 110 / 210, rel_tol=1e-9)
    assert math.isclose(snap.fair_home_prob, 0.5, rel_tol=1e-9)
    # Vig at -110/-110 is 2 * 0.5238 - 1 = 0.0476 (4.76%)
    assert math.isclose(snap.vig_percent, 4.7619, abs_tol=1e-3)


def test_generate_pick_carries_model_features_for_forensic_review():
    """The features that drove the prediction get embedded in the
    pick. Someone reviewing the pick later can see exactly what
    rolling stats the model saw."""
    odds = _h2h_odds_df(home_odds=-150, away_odds=130)
    features = {
        "home_rolling_ortg": 117.5,
        "home_rolling_drtg": 109.0,
        "away_rolling_ortg": 114.2,
        "away_rolling_drtg": 113.1,
    }
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.62,
        odds_df=odds,
        model_version="test-sha",
        model_features=features,
    )
    assert pick.model_features == features


# ---------------------------------------------------------------------------
# generate_pick — failure modes
# ---------------------------------------------------------------------------


def test_generate_pick_raises_when_no_anchor_book_in_odds():
    """If Pinnacle isn't in the odds_df, generate_pick must raise
    rather than silently fall back to another book — the de-vig
    assumes sharp pricing, and DraftKings/FanDuel hold is higher."""
    odds = pd.DataFrame(
        [
            _odds_row(
                sportsbook="draftkings",
                outcome_name="Oklahoma City Thunder",
                price=-275,
            ),
            _odds_row(
                sportsbook="draftkings",
                outcome_name="Indiana Pacers",
                price=220,
            ),
        ]
    )
    with pytest.raises(ValueError, match="anchor book"):
        generate_pick(
            home_team_abbr="OKC",
            away_team_abbr="IND",
            model_prob_home_win=0.6,
            odds_df=odds,
            model_version="test-sha",
            model_features={},
        )


def test_generate_pick_raises_when_h2h_market_missing():
    """A game without a h2h market (e.g., only spreads/totals) can't
    be used for moneyline EV — refuse rather than guess."""
    odds = pd.DataFrame(
        [
            _odds_row(
                market_type="spreads",
                outcome_name="Oklahoma City Thunder",
                price=-110,
                point=-6.5,
            ),
            _odds_row(
                market_type="spreads",
                outcome_name="Indiana Pacers",
                price=-110,
                point=6.5,
            ),
        ]
    )
    with pytest.raises(ValueError, match="h2h"):
        generate_pick(
            home_team_abbr="OKC",
            away_team_abbr="IND",
            model_prob_home_win=0.6,
            odds_df=odds,
            model_version="test-sha",
            model_features={},
        )


def test_generate_pick_rejects_invalid_probability():
    odds = _h2h_odds_df(home_odds=-110, away_odds=-110)
    with pytest.raises(ValueError):
        generate_pick(
            home_team_abbr="OKC",
            away_team_abbr="IND",
            model_prob_home_win=1.5,
            odds_df=odds,
            model_version="test-sha",
            model_features={},
        )


# ---------------------------------------------------------------------------
# publish_pick
# ---------------------------------------------------------------------------


def _sample_pick() -> Pick:
    return Pick(
        pick_id="g1",
        published_at=datetime(2026, 6, 4, 18, 30, tzinfo=timezone.utc),
        game_date=date(2026, 6, 4),
        commence_time=datetime(2026, 6, 5, 0, 30, tzinfo=timezone.utc),
        home_team_abbr="OKC",
        away_team_abbr="IND",
        home_team_full="Oklahoma City Thunder",
        away_team_full="Indiana Pacers",
        model_version="abc1234",
        model_prob_home_win=0.62,
        model_features={"home_rolling_ortg": 117.5, "away_rolling_drtg": 113.1},
        market=MarketSnapshot(
            sportsbook="pinnacle",
            home_odds_american=-180,
            away_odds_american=160,
            home_implied_prob=180 / 280,
            away_implied_prob=100 / 260,
            fair_home_prob=0.6235,
            fair_away_prob=0.3765,
            vig_percent=3.0,
        ),
        pick_side="home",
        pick_american_odds=-180,
        expected_value=0.04,
        kelly_fraction_half=0.025,
        notes="Educational/demo pick. Model has documented edge limits.",
        disagreement_pp=0.35,
        no_bet_reason=None,
        kelly_was_capped=False,
        max_disagreement_pp_used=10.0,
        max_kelly_fraction_used=0.05,
    )


def test_publish_pick_writes_json_snapshot_with_all_fields(tmp_path: Path):
    pick = _sample_pick()
    repo_root = tmp_path
    json_path = publish_pick(pick, repo_root=repo_root, parquet_root=None)
    assert json_path == repo_root / "picks" / "g1.json"
    assert json_path.is_file()

    data = json.loads(json_path.read_text(encoding="utf-8"))
    # Core verifiability fields all present
    for field in (
        "pick_id",
        "published_at",
        "game_date",
        "commence_time",
        "home_team_abbr",
        "away_team_abbr",
        "model_version",
        "model_prob_home_win",
        "market",
        "pick_side",
        "expected_value",
        "notes",
    ):
        assert field in data, f"missing {field}"
    assert data["pick_side"] == "home"
    assert data["market"]["sportsbook"] == "pinnacle"


def test_publish_pick_writes_parquet_row_when_root_given(tmp_path: Path):
    pick = _sample_pick()
    parquet_root = tmp_path / "out" / "picks"
    publish_pick(pick, repo_root=tmp_path, parquet_root=parquet_root)
    files = list(parquet_root.rglob("*.parquet"))
    assert files, f"no parquet files under {parquet_root}"
    df = pd.read_parquet(parquet_root)
    assert len(df) == 1
    assert df.iloc[0]["pick_id"] == "g1"
    assert df.iloc[0]["pick_side"] == "home"


def test_publish_pick_json_is_deterministic_per_pick(tmp_path: Path):
    """Same Pick -> same JSON bytes. Important for reproducibility
    and for git-diff-friendly commits."""
    pick = _sample_pick()
    p1 = publish_pick(pick, repo_root=tmp_path / "a", parquet_root=None)
    p2 = publish_pick(pick, repo_root=tmp_path / "b", parquet_root=None)
    assert p1.read_bytes() == p2.read_bytes()


# ---------------------------------------------------------------------------
# v1.4.0 commit 2: disagreement guardrail + sizing cap
# ---------------------------------------------------------------------------


def test_disagreement_guardrail_blocks_picks_outside_threshold():
    """The Finals Game 1 case: model says 0.79 home, market fair says
    0.62. 17pp gap. With the default 10pp guardrail, this becomes
    no_bet with reason='disagreement_too_large', no matter how
    positive the raw EV math looks."""
    odds = _h2h_odds_df(home_odds=-260, away_odds=215)
    pick = generate_pick(
        home_team_abbr="OKC",  # fair_home ~0.694
        away_team_abbr="IND",
        model_prob_home_win=0.85,  # 15.6pp above fair — over threshold
        odds_df=odds,
        model_version="test-sha",
        model_features={},
    )
    assert pick.pick_side == "no_bet"
    assert pick.no_bet_reason == "disagreement_too_large"
    assert pick.expected_value is None
    assert pick.kelly_fraction_half is None
    # Disagreement is captured for the audit trail even when no_bet
    assert pick.disagreement_pp is not None
    assert pick.disagreement_pp > 10  # documented threshold


def test_disagreement_guardrail_threshold_is_configurable():
    """A more permissive caller (e.g. for diagnostic publishing of
    extreme picks) can raise the threshold; a more conservative
    caller can lower it. Default lives in the picks module so the
    "what's reasonable" decision is auditable in one place."""
    odds = _h2h_odds_df(home_odds=-260, away_odds=215)
    # Same 15.6pp disagreement; with threshold=20 it should pass through.
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.85,
        odds_df=odds,
        model_version="test-sha",
        model_features={},
        max_disagreement_pp=20.0,
    )
    assert pick.pick_side == "home"  # 0.85 vs offered -260: positive EV
    assert pick.no_bet_reason is None


def test_disagreement_guardrail_triggers_in_either_direction():
    """A large disagreement in the OPPOSITE direction (model thinks
    home wins way LESS than market) should also flag no_bet — the
    model is likely wrong, not finding a contrarian edge."""
    odds = _h2h_odds_df(home_odds=-260, away_odds=215)  # fair_home ~0.694
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.45,  # 24pp below fair — large gap toward dog
        odds_df=odds,
        model_version="test-sha",
        model_features={},
    )
    assert pick.pick_side == "no_bet"
    assert pick.no_bet_reason == "disagreement_too_large"


def test_no_bet_reason_distinguishes_no_edge_from_disagreement():
    """If both sides are slightly -EV (market agrees with model within
    the vig), no_bet_reason is 'no_edge'. If disagreement is huge
    but model picks the favorite side at positive raw EV, no_bet_reason
    is 'disagreement_too_large'. These are different signals and the
    JSON should make the distinction visible."""
    odds = _h2h_odds_df(home_odds=-260, away_odds=215)
    # Model agrees with fair (0.694) — no_bet because no edge
    pick_no_edge = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.694,
        odds_df=odds,
        model_version="test-sha",
        model_features={},
    )
    assert pick_no_edge.pick_side == "no_bet"
    assert pick_no_edge.no_bet_reason == "no_edge"


def test_kelly_cap_clamps_to_max_fraction():
    """A high-edge bet (e.g. model 0.85 at -150 odds) computes a Kelly
    fraction that would otherwise recommend a reckless bankroll
    fraction. Default cap is 5% — half-Kelly clamped at that
    ceiling protects against an unverified-edge model recommending
    20%+ bankroll bets."""
    odds = _h2h_odds_df(home_odds=-150, away_odds=130)
    # Permissive disagreement threshold so we actually get a "home"
    # pick rather than no_bet — we want to test the Kelly clamp.
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.85,
        odds_df=odds,
        model_version="test-sha",
        model_features={},
        max_disagreement_pp=50.0,  # let it through
        max_kelly_fraction=0.05,
    )
    assert pick.pick_side == "home"
    assert pick.kelly_fraction_half is not None
    assert pick.kelly_fraction_half <= 0.05 + 1e-9
    assert pick.kelly_was_capped is True


def test_kelly_cap_not_triggered_on_modest_edges():
    """Edge ~3% with -110 odds → uncapped Kelly fraction (something
    like 0.05-0.06 half-Kelly) — should NOT be clamped if it lands
    below the cap."""
    odds = _h2h_odds_df(home_odds=-110, away_odds=-110)
    # Model says 0.55 (fair = 0.5), small positive EV
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.55,
        odds_df=odds,
        model_version="test-sha",
        model_features={},
        max_disagreement_pp=50.0,
        max_kelly_fraction=0.1,  # generous cap
    )
    assert pick.pick_side == "home"
    assert pick.kelly_was_capped is False


def test_pick_json_includes_guardrail_audit_fields():
    """JSON snapshot must carry the guardrail decision audit trail:
    disagreement_pp, no_bet_reason, kelly_was_capped, the thresholds
    that were used. So a reviewer can reconstruct WHY a pick was
    no_bet or capped."""
    odds = _h2h_odds_df(home_odds=-260, away_odds=215)
    pick = generate_pick(
        home_team_abbr="OKC",
        away_team_abbr="IND",
        model_prob_home_win=0.85,
        odds_df=odds,
        model_version="test-sha",
        model_features={},
    )
    from models.picks import _pick_to_dict

    d = _pick_to_dict(pick)
    assert "disagreement_pp" in d
    assert "no_bet_reason" in d
    assert "kelly_was_capped" in d
    assert "max_disagreement_pp_used" in d
    assert "max_kelly_fraction_used" in d
