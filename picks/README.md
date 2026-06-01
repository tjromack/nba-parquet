# Published picks

> **⚠️ EDUCATIONAL / METHODOLOGY DEMONSTRATION ONLY.**
>
> The picks in this directory are the output of a documented prediction
> model with **known edge limits** — see the README's [Phase 4b honest
> results](../README.md#prediction-model-phase-4b--honest-results)
> section. The model currently lands at baseline parity (≈ -0.2pp vs.
> "pick the team with the better trailing win pct" on a 993-game OOF
> test set). **The picks here do NOT constitute betting advice or a
> recommendation to wager.**
>
> Sports betting can become a problem. If you or someone you know
> needs help: **1-800-GAMBLER** (US), **Gam-Anon** ([gam-anon.org](https://gam-anon.org)),
> **GamCare** ([gamcare.org.uk](https://gamcare.org.uk)) (UK). Bet only
> with money you can afford to lose. Past performance does not predict
> future results.

## Why this directory exists

The legitimate way to demonstrate that a prediction model has edge is
to publish picks **before tipoff** and track them against the **closing
line value** (CLV) over a large enough sample (typically 200+ bets) for
the result to be statistically meaningful.

Every file here is a JSON snapshot of a single pick at the moment it
was generated. The **git commit timestamp** on each file is the
cryptographic proof that the pick existed *before* the game started —
no after-the-fact editing is possible without leaving an obvious git
trace. Anyone can verify with:

```bash
git log --follow picks/<game_id>.json
```

## What's in each JSON file

| Field | What it is |
|---|---|
| `pick_id` | The Odds API's hash id for the game |
| `published_at` | UTC timestamp at pick generation |
| `game_date` | NBA-canonical ET game date |
| `commence_time` | UTC tipoff |
| `home_team_abbr` / `away_team_abbr` | 3-letter codes matching nba_api |
| `home_team_full` / `away_team_full` | Full team names as The Odds API returns them |
| `model_version` | Short git SHA of the model code at pick time |
| `model_prob_home_win` | Model's predicted probability the home team wins |
| `model_features` | The lagged rolling-feature vector that drove the prediction (forensic record) |
| `market` | Pinnacle (sharp anchor) snapshot: American odds both sides, vig-inclusive implied probs, de-vigged fair probs, total vig |
| `pick_side` | `"home"`, `"away"`, or `"no_bet"` |
| `pick_american_odds` | Offered price on the picked side, `null` for `no_bet` |
| `expected_value` | EV per $1 stake at the offered price, `null` for `no_bet` |
| `kelly_fraction_half` | Half-Kelly bankroll fraction, `null` for `no_bet` |
| `notes` | Disclaimer + edge caveats (same for every pick) |

## How picks are generated

1. The model (`models/artifacts/winner_hgb.joblib`) is trained on
   leak-free walk-forward CV against the processed + features layers.
2. For a given game date, the model's `predict_matchup` produces a
   home-win probability from each team's latest rolling features.
3. `models.market.devig_two_way` converts the Pinnacle h2h price into
   the sharp-anchored "fair" probability for each side.
4. `models.market.expected_value` computes EV for both sides at the
   offered prices. Whichever side has the higher *positive* EV is the
   pick; if both are negative, the pick is `no_bet`.
5. `models.market.kelly_fraction` computes a half-Kelly recommendation
   on the picked side (half-Kelly is the practical-bankroll default;
   full Kelly has too much variance for unverified edge).

The reference book is **Pinnacle**. It's the canonical "sharp" book
because its hold is among the lowest in the industry, so its prices
are the closest to a fair market. De-vigging a softer book (e.g.,
DraftKings, FanDuel) would give a skewed "fair" probability.

## How to interpret a pick

- **`pick_side: "no_bet"`** is information. It means the model thinks
  the market price reflects reality (within the vig). That's the
  honest answer most of the time — most NBA moneylines are
  well-priced and contain no edge.

- **`pick_side: "home"` or `"away"`** with `expected_value: 0.04`
  means: *if* the model is correctly calibrated, this bet has a 4%
  edge over many repetitions. **Single-bet outcomes are noise.** A
  +4% EV bet still loses ~48% of the time.

- **The track record (CLV) is the only thing that proves edge.** A
  pick that wins doesn't prove the model has edge. A pick that loses
  doesn't disprove it either. What proves edge is closing-line value
  *averaged across hundreds of picks*: did the line move toward our
  pick after we published? That's the question this directory exists
  to eventually answer.

## How to run

```powershell
# Make sure the latest odds are ingested and a model is trained.
$env:LOCAL_OUTPUT_DIR = "$PWD\out"
$env:ODDS_API_KEY = "your-key"
.venv\Scripts\python.exe scripts\publish_pick.py --game-date 2026-06-04 --refresh-odds
```

Then commit:

```powershell
git add picks/
git commit -m "Picks: 2026-06-04"
git push origin main
```

The push timestamp on GitHub is the verifiability anchor.
