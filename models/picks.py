"""Pick generation + publishing.

A ``Pick`` joins a model probability to a market snapshot and applies
the EV / Kelly math from ``models.market``. ``publish_pick`` writes
the Pick to a git-trackable JSON file under ``picks/`` AND to a
parquet zone under ``out/picks/`` — the git timestamp on the JSON
is the verifiability anchor (commit before tipoff = cryptographic
proof the pick existed in advance), the parquet is for analytics.

The 30-team ``TEAM_NAME_TO_ABBR`` dict bridges The Odds API's full
team names with nba_api's 3-letter abbreviations. Audit-friendly
(easy to inspect, no fuzzy logic), one-line update if the league
ever rebrands a team.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from models.market import (
    american_to_implied_prob,
    devig_two_way,
    expected_value,
    kelly_fraction,
)

# Full official team name (as The Odds API returns it) -> nba_api
# 3-letter abbreviation. The Clippers have appeared as both
# "LA Clippers" and "Los Angeles Clippers" in API responses over
# time; both map to LAC for safety.
TEAM_NAME_TO_ABBR: dict[str, str] = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "LA Clippers": "LAC",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}


_PICK_DISCLAIMER = (
    "Educational/demo pick. Model has documented edge limits "
    "(see README Phase 4b). Not financial or betting advice. "
    "Past performance does not predict future results. If gambling "
    "stops being fun, call 1-800-GAMBLER."
)


# Default guardrails — chosen for an unverified-edge model. Tighten
# (or relax) deliberately, not by accident; the JSON record captures
# which thresholds were applied so a reviewer can see the policy.
#
# DEFAULT_MAX_DISAGREEMENT_PP: if |model_prob - de_vigged_fair_prob|
#   exceeds this percentage-point threshold in either direction, the
#   pick is flagged no_bet rather than betting on what's most likely
#   a model failure outside its calibrated range. The Game 1 Finals
#   dry run had 17.2pp disagreement; with the 10pp default that pick
#   correctly does not fire.
# DEFAULT_MAX_KELLY_FRACTION: half-Kelly is clamped to this maximum
#   bankroll fraction. 5% is the practical-bankroll ceiling for any
#   unverified-edge model — full half-Kelly off a model with no CLV
#   record is reckless even when the math says +EV.
DEFAULT_MAX_DISAGREEMENT_PP = 10.0
DEFAULT_MAX_KELLY_FRACTION = 0.05


def abbr_for_team_name(full_name: str) -> str:
    """Look up the 3-letter abbreviation for a full team name.

    Raises ``KeyError`` with an actionable message if the name isn't
    recognized — usually means The Odds API has introduced a new
    spelling that needs adding to ``TEAM_NAME_TO_ABBR``.
    """
    try:
        return TEAM_NAME_TO_ABBR[full_name]
    except KeyError as e:
        raise KeyError(
            f"Unknown team name {full_name!r}. The Odds API may have "
            "introduced a new spelling; add it to TEAM_NAME_TO_ABBR."
        ) from e


@dataclass
class MarketSnapshot:
    """Exact market state used to generate a pick.

    Captured at pick-publish time so the EV math is reproducible
    from the saved record alone (no need to re-query the API later).
    """

    sportsbook: str
    home_odds_american: int
    away_odds_american: int
    home_implied_prob: float  # vig-inclusive
    away_implied_prob: float
    fair_home_prob: float  # de-vigged
    fair_away_prob: float
    vig_percent: float  # bookmaker hold (0.0476 = 4.76%)


@dataclass
class Pick:
    """A model-driven pick with full audit trail.

    The JSON serialization of this object, committed to git at
    ``picks/<pick_id>.json``, is the verifiable artifact: the commit
    timestamp on GitHub proves the pick existed before tipoff. The
    parquet row in ``out/picks/`` is the analytics shape.
    """

    pick_id: str
    published_at: datetime  # UTC
    game_date: date  # ET
    commence_time: datetime  # UTC tipoff
    home_team_abbr: str
    away_team_abbr: str
    home_team_full: str
    away_team_full: str
    model_version: str  # short git SHA, identifies exact code state
    model_prob_home_win: float
    model_features: dict[str, float]  # input vector for forensic review
    market: MarketSnapshot
    pick_side: str  # "home" | "away" | "no_bet"
    pick_american_odds: int | None  # None for no_bet
    expected_value: float | None  # per $1 stake; None for no_bet
    kelly_fraction_half: float | None  # half-Kelly bankroll fraction
    notes: str  # disclaimer + edge caveats
    # v1.4.0 guardrail audit fields
    disagreement_pp: float  # |model_prob - fair_home_prob| * 100
    no_bet_reason: str | None  # "no_edge" | "disagreement_too_large" | None
    kelly_was_capped: bool  # True if max_kelly_fraction clamp triggered
    max_disagreement_pp_used: float  # threshold applied at decision time
    max_kelly_fraction_used: float  # ceiling applied at decision time


def _select_outcome_price(h2h_df: pd.DataFrame, team_abbr: str) -> int:
    """Find the price for the outcome matching ``team_abbr`` in an
    anchor-book h2h slice. Raises if the team isn't represented."""
    rows = h2h_df[
        h2h_df["outcome_name"].apply(
            lambda n: TEAM_NAME_TO_ABBR.get(str(n)) == team_abbr
        )
    ]
    if rows.empty:
        raise ValueError(
            f"No h2h outcome for {team_abbr!r} in the anchor book's "
            "market — check that the team name in the odds payload "
            "maps to this abbreviation in TEAM_NAME_TO_ABBR."
        )
    return int(rows.iloc[0]["price"])


def generate_pick(
    *,
    home_team_abbr: str,
    away_team_abbr: str,
    model_prob_home_win: float,
    odds_df: pd.DataFrame,
    model_version: str,
    model_features: dict[str, float],
    anchor_sportsbook: str = "pinnacle",
    kelly_scale: float = 0.5,
    published_at: datetime | None = None,
    max_disagreement_pp: float = DEFAULT_MAX_DISAGREEMENT_PP,
    max_kelly_fraction: float = DEFAULT_MAX_KELLY_FRACTION,
) -> Pick:
    """Combine model probability + market odds into a ``Pick``.

    EV is computed at the anchor book's offered prices. De-vig also
    uses the anchor book — defaults to Pinnacle as the canonical
    sharp anchor. If Pinnacle's not in the odds_df or has no h2h
    market for this game, raises ``ValueError`` rather than silently
    falling back to a softer book (different hold, different
    de-vigged probability).

    Half-Kelly is the default ``kelly_scale`` — variance reduction
    that practical bankrolls use. Full Kelly is reckless against an
    unverified model.

    Guardrails (v1.4.0):
    - ``max_disagreement_pp`` (default 10): if the model's probability
      diverges from the de-vigged sharp-anchor probability by more
      than this percentage-point threshold in either direction, the
      pick is auto-flagged ``no_bet`` with reason
      ``"disagreement_too_large"``. Designed to catch model
      overconfidence — large disagreement with a sharp market is more
      likely model failure than genuine edge.
    - ``max_kelly_fraction`` (default 0.05): half-Kelly is clamped at
      this maximum bankroll fraction. Even with positive EV at every
      threshold, recommending >5% of bankroll on an unverified-edge
      model is irresponsible.
    """
    if not 0.0 <= model_prob_home_win <= 1.0:
        raise ValueError(
            f"model_prob_home_win must be in [0,1], got {model_prob_home_win}"
        )

    anchor_h2h = odds_df[
        (odds_df["sportsbook"] == anchor_sportsbook) & (odds_df["market_type"] == "h2h")
    ]
    if anchor_h2h.empty:
        anchor_any = odds_df[odds_df["sportsbook"] == anchor_sportsbook]
        if anchor_any.empty:
            available = sorted(odds_df["sportsbook"].unique().tolist())
            raise ValueError(
                f"No anchor book {anchor_sportsbook!r} in odds_df. "
                "De-vigging requires sharp pricing (Pinnacle is the "
                "default); refusing to fall back to a softer book. "
                f"Books actually present in odds_df: {available}. "
                "If Pinnacle is genuinely unavailable for this market, "
                "verify ODDS_API_REGIONS includes 'eu' in etl/odds.py."
            )
        raise ValueError(
            f"No h2h market for {anchor_sportsbook!r}; cannot price moneyline."
        )

    home_odds = _select_outcome_price(anchor_h2h, home_team_abbr)
    away_odds = _select_outcome_price(anchor_h2h, away_team_abbr)

    home_implied = american_to_implied_prob(home_odds)
    away_implied = american_to_implied_prob(away_odds)
    fair_home, fair_away = devig_two_way(home_odds, away_odds)
    vig_percent = ((home_implied + away_implied) - 1.0) * 100.0

    snapshot = MarketSnapshot(
        sportsbook=anchor_sportsbook,
        home_odds_american=home_odds,
        away_odds_american=away_odds,
        home_implied_prob=home_implied,
        away_implied_prob=away_implied,
        fair_home_prob=fair_home,
        fair_away_prob=fair_away,
        vig_percent=vig_percent,
    )

    # Disagreement guardrail (v1.4.0): how far is the model from the
    # sharp market's de-vigged probability? Large in either direction
    # = likely model failure outside its calibrated range, not edge.
    disagreement_pp = abs(model_prob_home_win - fair_home) * 100.0
    disagreement_blocks_bet = disagreement_pp > max_disagreement_pp

    # EV against the offered prices (after vig). If both negative
    # the pick is no_bet — model agrees with market or model is on
    # the wrong side, either way no edge.
    ev_home = expected_value(model_prob_home_win, home_odds)
    ev_away = expected_value(1.0 - model_prob_home_win, away_odds)

    no_bet_reason: str | None = None
    kelly_was_capped = False

    if disagreement_blocks_bet:
        # Auto-flag regardless of EV math; the math is built on an
        # unreliable probability when the model disagrees this hard.
        pick_side = "no_bet"
        pick_odds: int | None = None
        ev: float | None = None
        kelly: float | None = None
        no_bet_reason = "disagreement_too_large"
    elif ev_home > 0 and ev_home >= ev_away:
        pick_side = "home"
        pick_odds = home_odds
        ev = ev_home
        raw_kelly = kelly_fraction(model_prob_home_win, home_odds, scale=kelly_scale)
        kelly_was_capped = raw_kelly > max_kelly_fraction
        kelly = min(raw_kelly, max_kelly_fraction)
    elif ev_away > 0:
        pick_side = "away"
        pick_odds = away_odds
        ev = ev_away
        raw_kelly = kelly_fraction(
            1.0 - model_prob_home_win, away_odds, scale=kelly_scale
        )
        kelly_was_capped = raw_kelly > max_kelly_fraction
        kelly = min(raw_kelly, max_kelly_fraction)
    else:
        pick_side = "no_bet"
        pick_odds = None
        ev = None
        kelly = None
        no_bet_reason = "no_edge"

    # Game metadata from the anchor h2h row
    game_row = anchor_h2h.iloc[0]
    game_id = str(game_row["game_id"])
    commence_raw = pd.Timestamp(game_row["commence_time"])
    if commence_raw.tzinfo is None:
        commence_time = commence_raw.tz_localize("UTC").to_pydatetime()
    else:
        commence_time = commence_raw.tz_convert("UTC").to_pydatetime()
    game_date_val = game_row["game_date"]
    if isinstance(game_date_val, pd.Timestamp):
        game_date_val = game_date_val.date()

    return Pick(
        pick_id=game_id,
        published_at=published_at or datetime.now(timezone.utc),
        game_date=game_date_val,
        commence_time=commence_time,
        home_team_abbr=home_team_abbr,
        away_team_abbr=away_team_abbr,
        home_team_full=str(game_row["home_team"]),
        away_team_full=str(game_row["away_team"]),
        model_version=model_version,
        model_prob_home_win=model_prob_home_win,
        model_features=dict(model_features),
        market=snapshot,
        pick_side=pick_side,
        pick_american_odds=pick_odds,
        expected_value=ev,
        kelly_fraction_half=kelly,
        notes=_PICK_DISCLAIMER,
        disagreement_pp=disagreement_pp,
        no_bet_reason=no_bet_reason,
        kelly_was_capped=kelly_was_capped,
        max_disagreement_pp_used=max_disagreement_pp,
        max_kelly_fraction_used=max_kelly_fraction,
    )


def _pick_to_dict(pick: Pick) -> dict:
    """Serialize a Pick to a JSON-stable dict.

    All datetimes are normalized to UTC + ISO 8601. Floats are
    rounded so the same Pick produces byte-identical JSON across
    runs (important for git-diff cleanliness).
    """
    return {
        "pick_id": pick.pick_id,
        "published_at": pick.published_at.astimezone(timezone.utc).isoformat(),
        "game_date": pick.game_date.isoformat(),
        "commence_time": pick.commence_time.astimezone(timezone.utc).isoformat(),
        "home_team_abbr": pick.home_team_abbr,
        "away_team_abbr": pick.away_team_abbr,
        "home_team_full": pick.home_team_full,
        "away_team_full": pick.away_team_full,
        "model_version": pick.model_version,
        "model_prob_home_win": round(pick.model_prob_home_win, 6),
        "model_features": {
            k: round(float(v), 6) for k, v in pick.model_features.items()
        },
        "market": {
            "sportsbook": pick.market.sportsbook,
            "home_odds_american": pick.market.home_odds_american,
            "away_odds_american": pick.market.away_odds_american,
            "home_implied_prob": round(pick.market.home_implied_prob, 6),
            "away_implied_prob": round(pick.market.away_implied_prob, 6),
            "fair_home_prob": round(pick.market.fair_home_prob, 6),
            "fair_away_prob": round(pick.market.fair_away_prob, 6),
            "vig_percent": round(pick.market.vig_percent, 4),
        },
        "pick_side": pick.pick_side,
        "pick_american_odds": pick.pick_american_odds,
        "expected_value": (
            round(pick.expected_value, 6) if pick.expected_value is not None else None
        ),
        "kelly_fraction_half": (
            round(pick.kelly_fraction_half, 6)
            if pick.kelly_fraction_half is not None
            else None
        ),
        # v1.4.0 guardrail audit trail
        "disagreement_pp": round(pick.disagreement_pp, 4),
        "no_bet_reason": pick.no_bet_reason,
        "kelly_was_capped": pick.kelly_was_capped,
        "max_disagreement_pp_used": pick.max_disagreement_pp_used,
        "max_kelly_fraction_used": pick.max_kelly_fraction_used,
        "notes": pick.notes,
    }


def publish_pick(
    pick: Pick,
    repo_root: Path,
    parquet_root: Path | None,
) -> Path:
    """Write Pick to ``picks/<pick_id>.json`` (git-tracked) and
    optionally to a partitioned parquet zone.

    Returns the path to the JSON file. The git commit timestamp on
    that file is the verifiability anchor: anyone can
    ``git log picks/<pick_id>.json`` to prove the pick existed
    before tipoff.
    """
    repo_root = Path(repo_root)
    picks_dir = repo_root / "picks"
    picks_dir.mkdir(parents=True, exist_ok=True)
    json_path = picks_dir / f"{pick.pick_id}.json"
    data = _pick_to_dict(pick)
    json_path.write_text(
        json.dumps(data, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if parquet_root is not None:
        parquet_root = Path(parquet_root)
        # Hive-style partitioning: game_date in the directory name only
        # (not in the file body) so pyarrow recovers it from the path
        # on read without a schema collision.
        partition_dir = parquet_root / f"game_date={pick.game_date.isoformat()}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        flat = {
            "pick_id": pick.pick_id,
            "published_at": pick.published_at.astimezone(timezone.utc).replace(
                tzinfo=None
            ),
            "commence_time": pick.commence_time.astimezone(timezone.utc).replace(
                tzinfo=None
            ),
            "home_team_abbr": pick.home_team_abbr,
            "away_team_abbr": pick.away_team_abbr,
            "home_team_full": pick.home_team_full,
            "away_team_full": pick.away_team_full,
            "model_version": pick.model_version,
            "model_prob_home_win": pick.model_prob_home_win,
            "anchor_sportsbook": pick.market.sportsbook,
            "home_odds_american": pick.market.home_odds_american,
            "away_odds_american": pick.market.away_odds_american,
            "fair_home_prob": pick.market.fair_home_prob,
            "fair_away_prob": pick.market.fair_away_prob,
            "vig_percent": pick.market.vig_percent,
            "pick_side": pick.pick_side,
            "pick_american_odds": pick.pick_american_odds,
            "expected_value": pick.expected_value,
            "kelly_fraction_half": pick.kelly_fraction_half,
        }
        df = pd.DataFrame([flat])
        df.to_parquet(partition_dir / f"{pick.pick_id}.parquet", index=False)

    return json_path
