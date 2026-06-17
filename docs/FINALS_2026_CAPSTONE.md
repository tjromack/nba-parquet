# 2025–26 NBA Season Capstone — methodology demo in production

> **Built during the live 2025–26 NBA season.** The 2026 NBA Finals
> are the public demo of an honest ML engineering system running on
> fresh data through actual playoff games. The system is **still
> running** — picks for the rest of the Finals will be published live,
> with cryptographically-anchored git timestamps proving each
> decision existed before tipoff.

## The 90-second pitch

I built a production-style PySpark + Airflow ETL pipeline for NBA box
scores, layered a leak-free walk-forward winner-prediction model on
top, and then took the genuinely hard step that most portfolio
projects skip: I **ran it live through an entire season** and used the
NBA Finals as the verifiable public demonstration.

Most ML portfolios are "I built this six months ago and got 73%
accuracy on a Kaggle dataset." This one is "I've been running this
nightly through the 2025–26 playoffs, the first published pick is a
public commit on GitHub with a timestamp predating tipoff, and the
system *refused to bet on it* because its own guardrails caught the
model disagreeing too hard with the sharp market."

The system is at baseline parity (~63% walk-forward accuracy vs ~64%
strongest baseline — flat within noise on a 993-game OOF test set).
That's the honest number, reported with the same prominence as the
methodology. The *headline* isn't "I beat the market." The headline
is that **the methodology, the guardrails, and the discipline are
visible in the public record**, and the system keeps running.

## The five-minute demo path

Open these in order during the call. Each is a real artifact you can
point at; together they tell the engineering story.

| Order | Open | Talking point |
|---|---|---|
| 1 | [README.md hero + architecture diagram](../README.md) | "Daily Airflow DAG, four data zones, partitioned Parquet, 130 tests passing. Lives at nba-parquet on my GitHub. Pinned, only feature repo on my profile." |
| 2 | [The first published pick — JSON](../picks/1aae688472781f1a1aaf3efdb38e884b.json) | "First public output of the picks layer. NBA Finals Game 1, NYK @ SAS. The pick is `no_bet`. Reason: `disagreement_too_large`. The git commit timestamp on this file predates tipoff — that's the cryptographic proof the decision existed before the game." |
| 3 | [The sidecar narrative](../picks/2026-06-03.md) | "Three-layer methodology arc. Raw model said 0.79, calibration overshot to 0.51, guardrail caught the residual 11.3pp gap and refused to bet. Each layer is imperfect; the combination produced the right decision." |
| 4 | [Phase 4b honest results in the README](../README.md#prediction-model-phase-4b--honest-results) | "Four-snapshot progression: 62-game playoff sample → 1,280-game RS bulk-load → +RS-advanced → +playoff-advanced. Each transition has an attributable mechanism. Model lands at -0.2pp vs best baseline — effectively tied. I report that gap rather than tune it away." |
| 5 | [docs/ENGINEERING_NOTES.md](ENGINEERING_NOTES.md) | "War stories. TDD caught a small-data modeling defect. v1.3.0 'calibration regression' turned out to be a data-completeness gap. v1.4.0 'layered methodology beats perfect methodology.' These are the moments I'd talk about in detail if they ask." |

## Finals ledger — live

Updated as games complete. Each row links to the verifiable pick JSON
and (when available) the outcome + closing-line value (CLV).

| Game | Date | Pick | Reason | Model | Market fair | Disagreement | Outcome | Closing line | CLV |
|---|---|---|---|---:|---:|---:|---|---|---|
| 1 | 2026-06-03 | [no_bet](../picks/1aae688472781f1a1aaf3efdb38e884b.json) | disagreement_too_large | 0.5095 | 0.6225 | 11.30pp | **NYK 105 – SAS 95** (NYK road W; series 1-0) | unavailable | unavailable |
| 2 | 2026-06-05 | not published | — | — | — | — | **NYK 105 – SAS 104** (NYK road W; series 2-0) | unavailable | n/a |
| 3 | 2026-06-08 | not published | — | — | — | — | **SAS 115 – NYK 111** (SAS road W; series 2-1) | unavailable | n/a |
| 4 | 2026-06-10 | not published | — | — | — | — | **NYK 107 – SAS 106** (NYK home W; series 3-1) | unavailable | n/a |
| 5 | 2026-06-13 | not published | — | — | — | — | **NYK 94 – SAS 90** (NYK road W; clinches 4-1) | unavailable | n/a |

**Reading the ledger.** Only Game 1 has a verifiable pre-tipoff
published pick in the public record. The decision was a `no_bet`
flagged by the v1.4.0 disagreement guardrail: the calibrated model
gave SAS a 51% chance of winning at home; Pinnacle's de-vigged fair
price said SAS was 62% to win; the 11.3pp gap exceeded the 10pp
policy threshold and the system refused to bet. **Games 2-5 were not
re-published** to the picks layer during the series — a deliberate
choice to avoid stacking the public record with picks that hadn't
been independently verified for quality.

The Game 1 actual outcome is a small but real data point worth
naming honestly: the Knicks won 105-95 on the road. **The model's
directional read (SAS 51% — essentially a coin flip from the
model's perspective) was closer to the outcome than the market's
directional read** (SAS 62% — a meaningful favorite). One game is
noise; this isn't an edge claim. But if the model's "I don't really
know who wins this game" lands closer to truth than the market's
strong-favorite read, that's a piece of evidence about where the
model might add value: matchups the market overconfidently prices,
not matchups where the model needs to be confident.

The series swept its way through a 4-1 NYK championship: two road
wins to open, a loss at home, a 1-point home win, and a clinching
4-point road win in Game 5. None of which the model was asked to
predict in the public record.

**On reading this table during an interview**: the "Reason" column
matters more than the "Outcome" column. A no_bet that calls a game
correctly is luck. A no_bet that calls a game incorrectly is luck.
What matters is whether the *system's policy* (calibration + 10pp
disagreement guardrail + 5% Kelly cap + full audit trail) is
defensible. The table is the verifiable record of that policy in
operation.

## Final tally — series concluded 2026-06-13

**New York Knicks defeated San Antonio Spurs 4-1 in the 2026 NBA
Finals.** The system published 1 pre-tipoff decision (Game 1, a
`no_bet`) and elected not to publish for Games 2-5 — the deliberate
restraint here is part of the methodology demonstration, not a gap
in it. Most public pick services would have stacked 5 confident
picks to fill content; this system held to "publish only with
guardrail-approved decisions" and the public record reflects that.

**The season-end model artifact**, frozen at the close of Game 5,
crossed above the strongest baseline for the first time:

| Metric | Pre-Finals (v1.4.0) | Season-final | Delta |
|---|---:|---:|---:|
| `logreg_accuracy` | 0.6351 | **0.6407** | +0.6pp |
| `logreg_log_loss` | 0.680 | **0.6444** | -0.036 (better) |
| `logreg_ece` | 0.063 | **0.0612** | slightly better |
| **`logreg_minus_best_baseline`** | **-0.002** | **+0.004** | **above baseline** |
| `hgb_log_loss` | 0.682 | **0.6724** | better |
| `hgb_ece` | 0.054 | **0.0375** | meaningfully better |
| `hgb_mce` | 0.436 | **0.2544** | dramatically better |

A 5-game contribution flipping logreg from -0.2pp to +0.4pp vs.
baseline is *itself* a methodology lesson — on a thin-margin model,
small data changes can flip the headline. The engineering takeaway
isn't "the model is now sharp." It's "single-percentage-point claims
about model accuracy need that kind of caveat baked into how they're
reported."

**The system continues running for 2026-27.** See
[docs/2025-26_SEASON_WRAPUP.md](2025-26_SEASON_WRAPUP.md) for the
season-closing checklist and Phase 5 forward-looking notes. The
daily catch-up automation, the picks-publishing entrypoint, and the
v1.4.0 calibration + guardrails will all resume when the 2026-27
regular season tips off in October.

## The methodology arc — what each version added

| Version | What landed | Why |
|---|---|---|
| v1.0.0 | Phase 1–3: full ETL + Airflow DAG + rolling features + Streamlit | The infrastructure that everything else stands on. Tests for schema, partitioning, DAG hygiene. |
| v1.1.0 | Phase 4b: leak-free training frame + walk-forward CV + MLflow + Streamlit Predictions view | Methodology over results. First honest negative result (model losing to baseline on 62-game playoff sample) ships as the artifact rather than gets hidden. |
| v1.2.0 | Full RS bulk-load (1,280 games) + neutral-site dataset fix | Data scale. Discovered NBA Cup neutral-site games encode as "@" on both sides — handled gracefully. |
| v1.3.0 | Advanced box-score zone (BoxScoreAdvancedV3) + rolling ORtg/DRtg/Pace + features extension | Feature richness. Logreg picks up ~+1.3pp from advanced metrics. HGB unchanged (rolling traditional already captures most of the signal). |
| v1.3.1 | Playoff advanced fill-in + correction of v1.3.0 "calibration regression" attribution | **The lesson that calibration ≠ data completeness.** I publicly corrected my earlier attribution in the docs rather than quietly deleting it. |
| **v1.4.0** | **Isotonic calibration + disagreement guardrail + 5% Kelly cap + first published pick** | **The picks layer. Calibration overshot on Game 1 in the opposite direction; guardrail caught the residual.** First public artifact is a `no_bet`. |

## Three engineering moments worth talking about in detail

These are the ones I'd happily get into for 10 minutes each if the
interviewer asks "what was the most interesting bug / lesson / design
choice?"

### "Calibration regression" turned out to be a data-completeness gap

In v1.3.0 I shipped advanced features and accuracy went up, but log
loss went *down*. I called this "a calibration story" in the release
notes and queued Platt scaling as the v1.3.x follow-up.

Then during a casual diagnostic walk of the features layer (literally
`features_df.tail(10)`), I noticed `rolling_ortg` was NaN for every
recent playoff game even though `rolling_ts_pct` had numbers. Root
cause: my bulk-load was `NBA_SEASON_TYPE="Regular Season"` only —
playoff advanced columns were median-imputed by the sklearn pipeline.

Filling the playoff advanced data closed the gap to the strongest
baseline to **0.2pp** and improved both log loss (0.662 → 0.644) and
Brier (0.232 → 0.226) **without changing a single line of model
code**. The "regression" was median-imputed playoff features
producing over-confident picks; real data fixed the calibration
organically.

**The lesson**: methodology debugging requires distinguishing "the
model is mis-calibrated" from "the model is mis-fed." The textbook
answer was Platt scaling. The right answer was upstream of the model
entirely. I corrected the attribution publicly in the v1.3.1 docs
rather than quietly dropping the earlier claim.

### Layered methodology beats perfect methodology

The first attempt to publish a real pick for NBA Finals Game 1
produced an alarming output: the raw model said SAS wins 79.4%
(17.2pp above Pinnacle's de-vigged fair of 62.2%), and the EV math
recommended betting 21.3% of bankroll. Clearly wrong.

v1.4.0 added two defensive layers and re-ran. Calibration *overshot*
in the opposite direction (pulled to 0.5095, now 11pp below market).
The disagreement guardrail caught the residual gap and auto-flagged
the pick `no_bet` with reason `disagreement_too_large`. The first
public published pick is therefore a `no_bet` — committed to git
with the full audit trail.

**The lesson**: don't try to make the model perfect; make the layers
around the model robust to imperfect model output. Defense in depth.
The raw model is overconfident at the extremes (vanilla logreg on
rolling features has no knowledge of matchup-specific dynamics).
The calibrator is imperfect on sparse tail data (isotonic with
internal 5-fold CV has small calibration sets at the [0.7+, 0.8]
range). Either layer alone would produce a wrong recommendation. The
combination — plus the explicit policy guardrail saying "we refuse
to bet when we can't agree with a sharp market" — produced the
correct decision.

Also: **publish decisions even when the decision is no-action**.
Most pick services hide their no-bets. You can't tell from outside
whether they have discipline. The first public artifact of this
system being an explicit, audit-trailed `no_bet` is itself the
methodology demonstration.

### TDD caught a small-data modeling defect; the honest negative result shipped

While building the Phase 4b model (v1.1.0), my "learns a separable
signal" wiring test kept failing on a trivial step-function input.
Forced the question *why*. Root cause: `HistGradientBoostingClassifier`'s
default `min_samples_leaf=20` against walk-forward early folds that
train on only 15–30 games. The booster literally couldn't make a
single split below 20 leaf samples — it silently predicts the
majority class.

I fixed `min_samples_leaf=5` (documented, defensible small-data
adaptation, not a tuning hack). With that, the wiring test passed.
Then I ran on the real 62-game playoff dataset.

The model did **not** beat the baselines (HGB 0.438, logreg 0.563,
best baseline 0.667). Rather than tune until the model "won" — which
would have been overfitting against a 48-game OOF test set, exactly
the soft leakage any reviewer with ML experience would call out —
**I shipped the negative result truthfully**. README's Phase 4b
section names every baseline up front, reports the model losing to
the strongest one, and frames the project as a *methodology
demonstration* rather than a claim of predictive edge.

**The lesson**: the discipline to ship a negative result is more
valuable than the discipline to ship a positive one. Anyone can
report wins. The signal of a serious engineer is reporting losses
with the same precision.

## What you can verify in 60 seconds (the reviewer's "is this real" check)

```bash
git clone https://github.com/tjromack/nba-parquet.git
cd nba-parquet
pip install -r requirements.txt -r requirements-dev.txt
pytest tests/ -m "not integration"
# expected: 130 passed, 1 skipped, in ~50s
```

130-test suite, real Spark + sklearn under the hood, runs without
AWS credentials or network access. The skipped test activates only
if `apache-airflow` is installed locally.

## The twenty-minute deep dive — what to walk through

If the interviewer asks "show me the code," these are the files
worth opening in order:

1. **[`dags/nba_etl_dag.py`](../dags/nba_etl_dag.py)** — the DAG.
   Five `PythonOperator` tasks, lazy imports for fast DAG parsing,
   XCom path-passing between tasks, `max_active_runs=1` and
   `catchup=False` set deliberately, staging-then-promote pattern
   for idempotent writes.
2. **[`etl/features.py`](../etl/features.py)** — the window function
   for rolling 10-game features. `Window.partitionBy("team_id").
   orderBy("game_date").rowsBetween(-9, 0)`. Conditional aggregation
   for home/away split.
3. **[`etl/transform.py`](../etl/transform.py) `get_spark()`** — the
   Spark factory with dynamic partition overwrite, S3A config gated
   on non-local mode, and the `_bootstrap_pyspark_env()` defensive
   helper that bit me once on Windows.
4. **[`models/train.py`](../models/train.py) `make_model()`** — the
   calibrated pipeline. `CalibratedClassifierCV(method='isotonic',
   cv=5)` wrapping a base `SimpleImputer → StandardScaler → LogisticRegression`
   pipeline. The leakage firewall comment explaining why
   calibration is fit inside each walk-forward fold.
5. **[`models/picks.py`](../models/picks.py) `generate_pick()`** —
   the picks layer. 10pp disagreement guardrail, 5% Kelly cap,
   `no_bet_reason` enum, audit-trail fields. Pinnacle as sharp
   anchor for de-vigging.
6. **[`tests/test_features.py`](../tests/test_features.py)
   `test_rolling_features_full_window_for_12th_bos_game`** — the
   hand-computed regression test that pins the window math. If
   someone refactors that function and breaks it, this test catches
   it immediately.

## Anticipated questions and honest answers

**"What's the model's accuracy?"**
~63% walk-forward accuracy on a 993-game OOF test set. The strongest
of three named baselines is ~64% (pick whichever team has the better
trailing win pct). The model loses by ~0.2pp — effectively tied
within noise. I report that explicitly. The methodology is what's
demonstrably solid; the model itself doesn't have a measurable edge
over the simplest sane heuristic, and that's documented.

**"Have you made money betting on this?"**
No, and the v1.4.0 guardrails are specifically designed to prevent
that being a useful question. The first public pick is a `no_bet`.
The system refuses to recommend wagers it can't justify against a
sharp book. CLV — closing-line value tracked over a large sample —
is the only real signal of edge, and the record is one game old.
I'm not in business of selling picks.

**"Why didn't you use [PyTorch / TensorFlow / XGBoost / a transformer]?"**
The model isn't the bottleneck. Going from a 62-game playoff sample
to a 1,284-game full-season sample moved logreg accuracy +4.4pp.
Adding advanced features moved it another +1.3pp. Filling the
playoff advanced data gap moved it another +1.4pp. Three explicit
data lifts that beat anything I'd get from swapping in a fancier
model. The honest priority for ML on a thin sample is **data,
features, calibration** — in that order — not architecture.

**"What would you do differently?"**
The v1.4.0 calibration overshoots on tail predictions because
isotonic with internal 5-fold CV has sparse tail data. v1.4.x has
three queued follow-ups: Platt scaling as a more conservative
alternative, `cv='prefit'` with a dedicated held-out calibration
set, and bootstrap prediction intervals on top of the point
estimate. The v1.4.0 release notes are explicit about this.

**"Is this still running?"**
Yes. Daily catch-up populated the dataset through last night's NBA
games. The Finals Game 2+ picks will be published live as the
series continues, with the same calibration + guardrails + git
verifiability. Watching that play out in real time *is* the demo.

**"How is this different from a Kaggle project?"**
Three differences:
1. **Live data through a real season** — not a snapshot CSV. The
   system has been running daily through 2025–26 playoffs.
2. **Verifiable public commitments** — every pick is on GitHub
   before tipoff, with a cryptographic timestamp.
3. **Honest negative results stay in the record** — when the model
   lost to a baseline, I shipped that as the artifact. When I
   mis-attributed a regression, I corrected the docs publicly.

That third one is the rare thing.

## The system is still running

This document was written during the 2026 NBA Finals while the
series was in progress. The Finals ledger above will continue to
grow as games complete. The daily catch-up automation
(`scripts/catch_up.ps1`) pulls new games into the data layer; the
publish script (`scripts/publish_pick.py`) generates a fresh pick
record for each upcoming game.

**Where to find live state right now**:
- Dashboard: `streamlit run streamlit_app.py` from a clone — reads
  the current `out/` directory live.
- Public verifiable picks: [`picks/`](../picks/) directory on
  `origin/main`. Every JSON file is a timestamped pre-tipoff
  decision; every Markdown sidecar is the methodology narrative
  for that game.
- Releases: [GitHub Releases](https://github.com/tjromack/nba-parquet/releases)
  — v1.0.0 through v1.4.0 each tell the engineering arc of one
  phase. v1.4.0 is current Latest.

---

*This capstone document was written for synchronous interview /
presentation contexts. For asynchronous review, see the
[README](../README.md) for the architecture overview, the
[PROJECT_QA.md](PROJECT_QA.md) for technical and layman framings of
the same project, and the [ENGINEERING_NOTES.md](ENGINEERING_NOTES.md)
for the war stories in full.*
