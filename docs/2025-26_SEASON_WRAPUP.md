# 2025–26 NBA Season Wrap-Up Checklist

> **Status**: 2026 NBA Finals concluded — New York Knicks defeated
> San Antonio Spurs 4-1. The system ran live through the playoffs
> and Finals. This document is the actionable checklist for closing
> out the season as a "completed chapter" of the project while
> leaving everything runnable for 2026-27 and beyond.

## How to use this doc

Each phase is a self-contained chunk of work you can do in one
sitting. Check items off as you complete them — the checklist is
designed to be edited in-place and committed when done. Phases 1-4
are sequential; Phase 5 is forward-looking and can happen anytime
after.

---

## Phase 1 — Final-state verification (1-2 hours) — ✅ COMPLETE 2026-06-17

> **Season-end retrain headline**: **logreg edged the strongest
> baseline for the first time** — 0.6407 vs 0.6366 (+0.4pp on a
> 988-game OOF test set). Logreg log_loss 0.6444, Brier 0.2266,
> ECE 0.0612, MCE 0.1350. HGB ECE collapsed 0.054 → 0.0375 and
> HGB MCE collapsed 0.436 → 0.2544 — the worst-bucket calibration
> got dramatically better with Finals games + retraining. **Frozen
> as the 2025-26 season-final model artifact.** This is the v1.5.0
> release-notes headline and the blog post's opening hook.

Make sure every Finals game is in the data and the model reflects
the complete season.

- [x] **Confirm final catchup ran through Finals Game 5**
  ```powershell
  .\scripts\catch_up.ps1
  ```
  Then verify the latest game_date is the Game 5 date (mid-June):
  ```powershell
  .venv\Scripts\python.exe -c "import pandas as pd; df = pd.read_parquet('out/processed/nba/team_game_stats'); print('latest game_date:', df['game_date'].astype(str).max()); print('Finals rows:'); finals = df[df['game_date'].astype(str) >= '2026-06-03']; print(finals[['game_date','team_abbreviation','opponent_abbreviation','is_home','win','pts']].to_string(index=False))"
  ```

- [x] **Backfill any missing playoff advanced data**
  ```powershell
  $env:LOCAL_OUTPUT_DIR = "$PWD\out"
  $env:NBA_SEASON_TYPE = "Playoffs"
  .venv\Scripts\python.exe scripts\bulk_load_advanced_only.py
  ```
  Necessary if you added playoff games via daily catchup (which
  only ingests traditional). Picks up advanced ORtg/DRtg/Pace for
  any missing playoff partitions.

- [x] **Rebuild processed + features from raw**
  ```powershell
  .venv\Scripts\python.exe scripts\rebuild_from_raw.py
  ```
  ~20-30 seconds. Ensures all derived layers reflect the final raw
  state including the Finals.

- [x] **Final season-end retrain**
  ```powershell
  .venv\Scripts\python.exe -m models.train
  ```
  Capture the output. This is the season-final model artifact +
  metrics. Compare to v1.4.0 numbers (logreg ~0.635, hgb ~0.59).
  The Finals adds 5 games to training data; numbers shouldn't move
  meaningfully but you want the artifact frozen at season-end state.

- [x] **Run the full test suite one more time**
  ```powershell
  .venv\Scripts\python.exe -m pytest tests/ -m "not integration"
  ```
  Should be 130 passed, 1 skipped. If anything regresses, fix
  before tagging the season-end release.

  **Outcome 2026-06-17**: 130 passed, 1 skipped, 2 warnings in
  51.05s. One snag during the wrap-up itself: `LOCAL_OUTPUT_DIR`
  set in the developer shell poisoned 5 `tests/test_ingest.py`
  tests and caused the 6th to hang on a real `nba_api` call.
  Unsetting the env var (`Remove-Item Env:\LOCAL_OUTPUT_DIR`)
  before running pytest resolved it. Test-hygiene fix queued as
  a v1.5.x backlog item in TODO.md (autouse conftest fixture
  to make tests robust to shell state).

- [x] **Commit any data updates that ended up touched** (mostly
  none — `out/` is gitignored — but `models/artifacts/` is also
  gitignored so the new joblib stays local. The point of this step
  is to verify nothing leaked into git accidentally):
  ```powershell
  git status
  # expect: nothing changed unless you regenerated picks
  ```
  **Outcome**: working tree clean (only this checklist file and
  TODO.md updated for record-keeping).

---

## Phase 2 — Documentation finalization (1-2 hours)

The system is in its final 2025-26 state. Documentation should
reflect that.

- [ ] **Fill in the FINALS_2026_CAPSTONE.md ledger placeholders**

  Replace each `_<TODO>_` row in [`docs/FINALS_2026_CAPSTONE.md`](FINALS_2026_CAPSTONE.md)
  with the actual game data:

  ```markdown
  | 1 | 2026-06-03 | [no_bet](../picks/1aae688472781f1a1aaf3efdb38e884b.json) | disagreement_too_large | 0.5095 | 0.6225 | 11.30pp | <home/away winner + score> | <Pinnacle closing line> | <CLV>  |
  | 2 | 2026-06-05 | <pick or not> | ... | <score> | <closing> | <CLV> |
  | 3 | 2026-06-XX | ...
  | 4 | 2026-06-XX | ...
  | 5 | 2026-06-XX | NYK clinches 4-1 ...
  ```

  For each game: outcome (winner + score), and if you have closing
  lines, the Pinnacle SAS closing odds. Even partial data is fine
  — write `unavailable` for cells you don't have.

- [ ] **Add a closing summary to the capstone**

  Append a brief section after the ledger:

  ```markdown
  ## Final tally — series concluded 2026-06-XX

  New York Knicks defeated San Antonio Spurs 4-1 in the 2026 NBA
  Finals. The system published [N] pre-tipoff decisions across the
  series; [N] were no_bet under the v1.4.0 disagreement guardrail,
  [N] fired as `pick_side: home/away`. CLV tracking: <available |
  partial | unavailable; note specifically>.

  The system continues running for the 2026-27 regular season
  starting in October 2026. See
  [docs/SEASON_2026_27_PREP.md](SEASON_2026_27_PREP.md) for the
  resumption procedure.
  ```

- [ ] **Update README hero to mark "season completed"**

  Add a season-end note to the hero block in [`README.md`](../README.md):

  ```markdown
  > 🏆 **2025-26 NBA season concluded** — system ran live through
  > playoffs + Finals (NYK over SAS, 4-1). See
  > [docs/FINALS_2026_CAPSTONE.md](docs/FINALS_2026_CAPSTONE.md)
  > for the full series ledger + methodology arc. Resumes for
  > 2026-27 in October.
  ```

- [ ] **Refresh dashboard screenshots**

  ```powershell
  .venv\Scripts\python.exe -m streamlit run streamlit_app.py
  ```

  Capture two updated screenshots reflecting the season-end state:
  - **Leaderboard**: save as `demo screenshots/Leaderboard_season_end.png`
  - **Predictions**: save as `demo screenshots/Predictions_season_end.png`

  Update the README image references to use the new files. The
  current images show the conference-finals state and should be
  superseded.

- [ ] **Add a final ENGINEERING_NOTES entry**

  Append to [`docs/ENGINEERING_NOTES.md`](ENGINEERING_NOTES.md) —
  any operational learnings from the actual Finals run that
  weren't already captured. Even "no operational issues during the
  Finals; daily catchup ran cleanly each morning" is valuable.

- [ ] **Mark TODO.md backlog items appropriately**

  In [`TODO.md`](../TODO.md), move the v1.4.x follow-ups (better
  tail calibration, bootstrap CIs, CLV automation, dynamic
  threshold) to a clearly-labeled "Future builds (2026-27 or
  later)" section so a reader can see what's queued for the next
  active period.

---

## Phase 3 — The retrospective write-up (3-5 hours)

This is the artifact most useful for portfolio/interviews. The
inputs are already enumerated in the earlier conversation; this
checklist makes sure they get gathered.

- [ ] **Gather these inputs** (you have most of these already from
  Phase 2):
  - [ ] Finals game outcomes (winner + score per game)
  - [ ] Pinnacle closing lines if available
  - [ ] Whether picks were published for Games 2+
  - [ ] 3-5 sentences in your voice: what surprised you, what
        worked, what was annoying, what to take away
  - [ ] Any operational issues encountered
  - [ ] Latest model.train summary

- [ ] **Draft the blog post** (handed to me; I'll write 1500-2500
  words against the outline in the last conversation turn). Save
  as `docs/RETROSPECTIVE_2025-26.md`.

- [ ] **Review the draft for voice**. Specifically: anywhere it
  sounds too AI-polished, swap in your own phrasing. The most
  valuable parts are direct quotes from you.

- [ ] **Pull derivative artifacts from the canonical doc**:
  - LinkedIn post (~300 words) — paste excerpts
  - Resume bullet (1-2 sentences) — extract from the opening
  - Interview STAR-format answers (4-5 questions) — derive from
    the engineering stories

- [ ] **Publish on Medium / Substack / personal site** (whichever
  you use). Link back to the GitHub repo and the FINALS_2026
  capstone doc.

- [ ] **Add the published URL to the README** so the post is
  discoverable from the repo:

  ```markdown
  > 📝 **Retrospective post**: [Title](https://medium.com/...) —
  > the full 2025-26 NBA season as a case study in honest ML
  > engineering. ~2,000 words.
  ```

---

## Phase 4 — Season-end release tag (30 minutes)

A clean punctuation mark so the v1.4.x → v1.5.0 progression makes
sense and v1.5.0 represents "season completed, ready to resume."

- [ ] **Final lint + test gate**
  ```powershell
  .venv\Scripts\python.exe -m ruff check .
  .venv\Scripts\python.exe -m black --check .
  .venv\Scripts\python.exe -m pytest tests/ -m "not integration"
  ```
  All three clean.

- [ ] **Commit all Phase 2 + 3 doc changes**
  ```powershell
  git add docs/ README.md TODO.md "demo screenshots/"
  git commit -m "Season wrap-up: 2025-26 finalized; ready for 2026-27"
  git push origin main
  ```

- [ ] **Tag v1.5.0 — "Season Completed"**
  ```powershell
  git tag -a v1.5.0 -m "$(cat <<'EOF'
  v1.5.0 — 2025-26 NBA season completed; system ready to resume for 2026-27

  Punctuates the active live-data period of the project. The system
  ran daily through the 2025-26 NBA playoffs and the 2026 NBA Finals
  (NYK over SAS, 4-1). Every methodology layer demonstrated in
  production — leak-free walk-forward CV, calibration, disagreement
  guardrails, public pre-tipoff verifiable picks via git commit
  timestamps.

  Final season metrics:
    <paste from final python -m models.train summary>

  Documentation snapshots taken at season-end:
    - docs/FINALS_2026_CAPSTONE.md — interview-ready walkthrough
    - docs/RETROSPECTIVE_2025-26.md — long-form blog version
    - README hero updated with "season completed" callout
    - Dashboard screenshots refreshed to season-end state

  System is preserved as-is for resumption in October 2026 (start
  of 2026-27 regular season). See docs/SEASON_2026_27_PREP.md for
  the resumption procedure.

  130 passed, 1 skipped, ruff + black clean.
  EOF
  )"
  git push origin v1.5.0
  gh release create v1.5.0 --verify-tag --latest --notes-from-tag --title "v1.5.0 — 2025-26 NBA Season Completed"
  ```

- [ ] **Verify the release page shows v1.5.0 as Latest** at
  https://github.com/tjromack/nba-parquet/releases

- [ ] **Update the Streamlit Predictions banner** to reflect the
  off-season state. Current banner cites the v1.3.1/v1.4.0 metrics;
  add a sentence noting the 2025-26 season has ended and
  predictions are for retrospective demonstration only until the
  2026-27 season begins.

---

## Phase 5 — Future-proofing (1-2 hours, one-time)

Document what changes for resumption + maintain the backlog so
picking the project up again is friction-free.

- [ ] **Create `docs/SEASON_2026_27_PREP.md`** with the resumption
  procedure. Skeleton:

  ```markdown
  # 2026-27 NBA Season Resumption Procedure

  How to resume the system when the 2026-27 regular season begins
  (October 2026).

  ## Configuration changes
  - .env: NBA_SEASON="2026-27"
  - .env: NBA_SEASON_TYPE="Regular Season"
  - .env: keep ODDS_API_KEY (rotate if expired)

  ## First-day checklist
  - [ ] Update .env per above
  - [ ] Run docker compose up + unpause the DAG
  - [ ] Wait for first games (regular season tips off ~Oct 21)
  - [ ] Verify daily catchup runs and produces processed/features rows
  - [ ] After ~10 games per team: first retrain w/ new season's data
  - [ ] After ~30 games: first new-season pick (if disagreement-
        guardrail allows)

  ## Things that should NOT change
  - Schema definitions in etl/schema.py
  - The picks/ directory structure
  - The guardrail thresholds (defaults in models/picks.py)
  - The CalibratedClassifierCV configuration

  ## Backlog of things WORTH addressing before resuming
  - [ ] Better tail calibration (try Platt / cv='prefit')
  - [ ] CLV-tracking automation (capture closing line pre-tipoff)
  - [ ] Bootstrap prediction intervals on model output
  - [ ] Dynamic disagreement threshold based on accumulated CLV
  - [ ] Player-level rolling features (data is available via
        BoxScoreAdvancedV3 already, just not aggregated)
  ```

- [ ] **Add a "Future builds" callout to README**

  Near the bottom of the README, add a short forward-looking
  section so reviewers understand the project is paused not
  abandoned:

  ```markdown
  ## Future builds (2026-27 NBA season + beyond)

  The system runs daily during the NBA regular season + playoffs.
  Between seasons it's paused; data ingestion catches up on the
  first day of the new season per the
  [resumption procedure](docs/SEASON_2026_27_PREP.md).

  Queued for the next active period: tail-calibration improvements
  (Platt / prefit), CLV-tracking automation, bootstrap prediction
  intervals, player-level rolling features. See [TODO.md](TODO.md)
  for the full backlog with rationale.

  Beyond NBA: the data layer + Airflow pipeline pattern transfers
  cleanly to NFL / MLB / other sports — the ingest module is the
  only layer that needs replacement; the rest reusable as-is.
  ```

- [ ] **Optionally: archive the 2025-26 mlruns**

  ```powershell
  Move-Item mlruns mlruns_2025-26
  New-Item -ItemType Directory -Path mlruns
  ```

  Preserves the 2025-26 MLflow runs as a frozen record while
  giving the 2026-27 season a clean tracking dir. The archived
  dir is gitignored either way.

- [ ] **Update PROJECT_QA.md final pitches**

  The 2-minute pitch in [`docs/PROJECT_QA.md`](PROJECT_QA.md)
  currently uses present-tense "the system runs daily." Update
  past tense for the 2025-26 season + future tense for next:

  ```
  The system ran daily through the 2025-26 NBA season including
  the Finals. The methodology — Spark, Airflow, leak-free walk-
  forward CV, layered calibration + guardrails, public verifiable
  picks — is documented in the repo. It resumes for 2026-27 in
  October.
  ```

---

## Phase 6 — Optional polish (anytime)

Nice-to-haves that aren't required for "season completed" but
elevate the portfolio piece if you have time.

- [ ] **GitHub Pages deployment** of the dashboard or capstone
  doc, so reviewers don't need to clone the repo to see the live
  state.

- [ ] **LinkedIn carousel post** derived from the retrospective —
  4-6 slide images with the key engineering moments + numbers.

- [ ] **Talk-track / slide deck** for in-person presentations
  (10-min and 25-min versions). The capstone document is the
  source of truth; the deck is a visual derivation.

- [ ] **Cross-reference from your personal site / portfolio
  landing page** so anyone landing on tjromack.com sees this
  project highlighted.

---

## Forward-looking optional builds (any future active period)

Things to consider if you decide to keep growing the project rather
than just running it daily for next season. These are in rough
priority order based on what would teach you the most + showcase
the most additional skills.

### Higher-leverage technical additions

- **CLV-tracking automation** (~half a day). Cron-style script that
  captures closing lines from The Odds API ~5 minutes pre-tipoff
  and updates the picks parquet zone. Without this, CLV is manual
  and won't accumulate the 200+ sample needed for real edge
  signal.

- **Tail calibration improvements** (~1-2 sessions). Experiment
  with `method='sigmoid'` (Platt scaling), `cv='prefit'` with a
  dedicated held-out calibration set from recent games, or full
  conformal prediction. The v1.4.0 isotonic overcorrection on the
  Finals Game 1 prediction is a known limitation.

- **Bootstrap prediction intervals** (~2-3 hours). N=100
  bootstrap-trained models give point estimates + 95% CI. JSON
  records get `model_prob_home_win_p5` and `_p95` alongside the
  point. Makes the model's uncertainty visible rather than hidden
  in the point estimate.

- **Player-level rolling features** (~1 week). The advanced
  BoxScoreV3 data already has per-player ORtg/DRtg/USG/PIE.
  Build the rolling-per-player layer. Opens up player props
  modeling — a market with softer prices than moneyline.

### Broader-scope additions

- **Multi-sport expansion** (~2-3 weeks per sport). The ETL +
  Airflow + features pattern is sport-agnostic. NFL and MLB have
  free APIs analogous to nba_api. The "lessons learned in one
  sport apply to others" angle is portfolio gold if you can
  demonstrate it.

- **Live Streamlit Cloud deployment** (~2 days). Currently the
  dashboard requires a local clone + Spark. Bundle a recent
  snapshot of `out/processed/` and `out/features/` into the repo,
  set up Streamlit Cloud, and the dashboard becomes publicly
  accessible at a URL — much lower friction for reviewers than
  "clone and run."

- **Pluggable model interface** (~1 week). Currently
  `make_model()` hardcodes logreg and HGB. Refactor to support
  any sklearn-compatible classifier (XGBoost, LightGBM, plain
  random forest) selected by config. Lets future seasons try new
  models without code surgery.

### Portfolio-strengthening additions

- **Public CLV dashboard / sharp-tracking page**. Once CLV
  automation exists and accumulates picks, a public page showing
  the running CLV % is the strongest single "edge proof" possible.
  Sharp betters do this manually; you'd be doing it with a real
  automated pipeline.

- **Comparison runs with alternate model classes**. Train an XGBoost
  + a small neural net on the same data + walk-forward splits.
  Compare metrics. Tells a "I evaluated multiple approaches" story
  without pretending one of them was the canonical choice.

- **Cost-benefit analysis writeup**: a doc that compares the
  ~$0/mo running cost of this system (free Odds API tier, free
  nba_api, local Spark) against what a commercial pick service
  would charge for the same artifact. Strong portfolio framing for
  data-engineering / fintech roles.

---

## Done?

When every box in Phases 1-5 is checked, the 2025-26 season is
formally closed. The system sits at v1.5.0, fully documented, with
clean resumption instructions. Future-you (or anyone evaluating
this project) can pick it back up in October 2026 with about 15
minutes of setup.

Commit this checklist itself when complete so the historical
record of the wrap-up is preserved.
