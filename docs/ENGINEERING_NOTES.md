# Engineering notes — nba-parquet

> Curated log of notable moments from building and operating this project —
> bugs caught, design decisions defended, recurring operational patterns
> automated away. The goal is a permanent record of *why* the codebase looks
> the way it does, beyond what individual commit messages capture.
>
> Companion doc: [PROJECT_QA.md](PROJECT_QA.md) — the same project explained
> end-to-end. PROJECT_QA covers *what the project is*; this file covers
> *interesting moments that happened while building it*.

## Format

Each note follows the same five-field shape:

- **Headline** — one-line summary
- **When** — date or phase, so it can be placed on a timeline
- **What happened** — the actual technical story, 2–4 sentences
- **What it demonstrates** — the engineering principle, design choice, or operational pattern it illustrates
- **Where to look** — file paths or commit SHAs so the story is verifiable

---

## Notes

### TDD caught a small-data modeling defect; shipped the honest negative result

- **When**: 2026-05-19 (Phase 4b session 2b)
- **What happened**: The model harness was built test-first. A
  "learns a separable signal" wiring test kept failing even on a
  trivial single-feature step function, which forced the question
  *why*. Root cause: `HistGradientBoostingClassifier`'s default
  `min_samples_leaf=20`, against walk-forward early folds that train
  on only ~15–30 games — the booster literally cannot make a single
  split below 20 leaf samples, so it silently predicts the majority
  class. Fixed by setting `min_samples_leaf=5` (a documented,
  defensible small-data adaptation, not a tuning hack). With that, the
  wiring test passed. Then the model was run on the real 62-game
  playoff set: it does **not** beat the baselines (HGB 0.438, logreg
  0.563, best baseline "better trailing TS%" 0.667). Rather than tune
  until it "won" — which on a 48-game test set is just overfitting —
  the negative result was written plainly into the README with the
  reason and the credible scaling path.
- **What it demonstrates**: TDD doing its actual job — a wiring test
  surfaced a real defect (default leaf size silently disabling the
  model on small folds) that would otherwise have shipped as garbage
  predictions with no error. And the discipline to ship a truthful
  negative result: simple heuristics beat learned models in low-data
  regimes, the honest move is to say so, and a suspiciously-good
  accuracy on a thin sample reads as leakage to anyone who knows the
  field. Methodology over results.
- **Where to look**: [`models/train.py`](../models/train.py)
  (`make_model`, the `min_samples_leaf=5` rationale comment);
  `test_evaluate_walk_forward_learns_separable_signal` in
  [`tests/test_models.py`](../tests/test_models.py); the "Prediction
  model (Phase 4b) — honest results" table in the README.

### Leakage firewall verified against real data by exact reconciliation

- **When**: 2026-05-18 (Phase 4b session 1)
- **What happened**: The prediction model's training-frame builder was
  written test-first — the target-leakage guard test was authored and
  confirmed red before any implementation, then `build_training_frame`
  was built until green. But unit tests run on synthetic fixtures, and
  this project has twice been bitten by "passes synthetic, real data
  exposes a mismatch" (the `TO`→`tov` rename, the no-games off-day). So
  before moving on, the function was run against the live `out/` Parquet
  as a deliberate gap-closer. It reconciled exactly: 69 games → 61
  training rows, and the 8 dropped rows are precisely the 8 first-round
  series openers where both teams had no prior rolling window. The
  leak-free "drop a game if either team lacks history" rule, plus the
  visible fact that the training set starts 4/20 rather than the 4/18
  playoff tip-off, confirmed the lag-1 firewall behaves correctly on
  real multi-series playoff structure — not just the hand-built fixture.
- **What it demonstrates**: Test-first discipline on the one piece where
  a silent bug (leakage) would invalidate every downstream metric — and
  the habit of explicitly verifying synthetic-tested logic against real
  data instead of trusting green unit tests, given a track record of
  real-data mismatches. The exact row-count reconciliation is the kind
  of independent check that catches off-by-one and join errors a
  schema-only assertion would miss.
- **Where to look**: [`models/dataset.py`](../models/dataset.py)
  (`build_training_frame`, the per-team lag-1 shift + inner-join drop);
  leakage guard `test_build_training_frame_is_leak_free` in
  [`tests/test_models.py`](../tests/test_models.py); commit `00b0de9`.

### No-games playoff off-day surfaced an empty-partition schema bug

- **When**: 2026-05-14 backfill (caught 2026-05-15)
- **What happened**: The daily catch-up failed on 2026-05-14 and kept
  failing identically on every re-run. NBA playoff schedules have
  off-days — 5/14 had zero games. `ingest_raw` handled that correctly
  (it writes an empty but schema'd raw snapshot), but
  `transform_and_aggregate` then wrote an empty *partitioned* DataFrame
  to staging, which produces zero Parquet data files, and
  `write_processed`'s bare `spark.read.parquet(staging_uri)` on that
  empty directory raised `AnalysisException: [UNABLE_TO_INFER_SCHEMA]`.
  The task failed both retries (deterministic — same empty input every
  time), blocked `write_features`, and the backfill orchestrator raised
  `BackfillUnfinished`. Root-caused by pulling the actual task log out
  of the Airflow logs volume (the orchestrator output only showed the
  generic "unfinished" summary, not the underlying Spark error). Fixed
  by treating a zero-game date as a *skip*, not a failure:
  `transform_and_aggregate` now raises `AirflowSkipException` when the
  raw frame is empty, which skip-propagates to the downstream writes;
  `notify_done` still runs via `trigger_rule=all_done`; the backfill
  orchestrator treats skipped as success. A regression test locks the
  precondition (empty raw → valid empty aggregation, not a crash).
- **What it demonstrates**: A real production edge case found by
  operating the pipeline daily, not by synthetic testing — and the
  discipline to root-cause from task logs rather than guess. Also using
  the *correct* primitive: a no-data day is semantically "skipped," not
  "failed," and Airflow's skip-propagation + `all_done` trigger rule
  model that exactly. The fix makes the pipeline correct for the full
  playoff calendar, off-days included.
- **Where to look**: [`dags/nba_etl_dag.py`](../dags/nba_etl_dag.py)
  `_transform_and_aggregate` (the `raw_df.rdd.isEmpty()` guard) and
  `_write_processed` (defensive `UNABLE_TO_INFER_SCHEMA` → skip
  translation); regression test
  `test_aggregate_on_empty_raw_yields_empty_not_error` in
  [`tests/test_transform.py`](../tests/test_transform.py); commit
  `144a3b4`.

### Auto-recovery from a transient API failure via Airflow's retry policy

- **When**: 2026-05-05 backfill
- **What happened**: The `ingest_raw` task for 2026-05-05 hit a transient
  `nba_api` failure on first attempt and exited at the 26-second mark.
  Airflow's `retries=1, retry_delay=5min` policy from the DAG's
  `default_args` kicked in, waited 5 minutes, and re-attempted. Try 2
  succeeded in 26 seconds and all four downstream tasks ran cleanly. Total
  data loss: zero. Manual intervention required: zero.
- **What it demonstrates**: Operational maturity — knowing that flaky external
  APIs are a fact of life and designing for them upfront rather than reacting
  after the fact. Also knowing when *not* to set retries (we use 1, not 3,
  because the failure modes nba_api shows are either transient blips or
  hard-blocked rate limits, and there's no point hammering on the latter).
- **Where to look**: [`dags/nba_etl_dag.py`](../dags/nba_etl_dag.py)
  `DEFAULT_ARGS`. The retry happened automatically on the live pipeline; no
  code change needed.

### Caught a real-data column-name regression that all our unit tests would have missed

- **When**: Phase 1, first real `make run-local` against today's playoff slate
- **What happened**: The processed-layer `tov` (turnovers) column came back
  null for every team. Investigation: `nba_api`'s `BoxScoreTraditionalV2`
  returns the turnover column as `TO`, not `TOV`. After lowercasing in the
  ingest normalizer, we got `to`, which didn't match the schema field `tov`,
  so it got nulled out silently. Fix: added an explicit alias map in
  `_normalize_player_rows`. Then added a regression test asserting `tov` is
  non-null for every row in the fake-data fixture, so this can never sneak
  back.
- **What it demonstrates**: The discipline of running real data through a
  pipeline before declaring it done — unit tests against synthetic fixtures
  wouldn't have caught a column-name mismatch with the live API. Also the
  reflex to add a regression test the moment you fix something, so the bug
  can't recur.
- **Where to look**: [`etl/ingest.py`](../etl/ingest.py) `_API_COLUMN_ALIASES`,
  [`tests/test_ingest.py`](../tests/test_ingest.py)
  `test_ingest_writes_parquet_and_respects_rate_limit` (final assertion
  block).

### Designed for safe daily backfills by setting `partitionOverwriteMode=dynamic`

- **When**: Phase 3, just before the first 14-day playoff backfill
- **What happened**: I realized that with Spark's default
  `partitionOverwriteMode=static`, every daily backfill would clobber the
  *entire* `processed/` prefix instead of just the day's
  `(season, game_date)` partition. So a 14-day backfill in static mode would
  leave only the last day's data on disk. Switched to `dynamic` mode in
  `get_spark()` before running the backfill. Validated by spot-checking that
  9 days of data accumulated, not just one.
- **What it demonstrates**: Understanding Spark's write semantics deeply
  enough to anticipate a footgun *before* destroying data, instead of
  diagnosing it afterward. Also paying off the same architectural choice
  later — when I had to patch in 4/18 separately after spotting the NYK
  series gap, dynamic mode meant the patch only touched the 4/18 partition
  without disturbing the existing 14 days.
- **Where to look**: [`etl/transform.py`](../etl/transform.py) `get_spark()`,
  the `spark.sql.sources.partitionOverwriteMode=dynamic` line. Phase 3
  commit message has the full backstory.

### Cross-reconciled pipeline output against ESPN, caught a backfill-window gap

- **When**: Phase 3, validating the rolling-features leaderboard
- **What happened**: When sanity-checking the leaderboard against real-world
  series results, I noticed our pipeline showed NYK at 3-2 over their last 5
  games — but their actual round-1 series result vs ATL was 4-2. Investigated:
  the backfill ran 4/19 → 5/2, but Game 1 of the NYK-ATL series was on 4/18,
  one day before our window started. Patched with a single-day backfill of
  4/18; thanks to dynamic partition overwrite this was a clean
  add-without-disturb. Final NYK row reconciled to 4 wins of 6 games,
  matching the real series.
- **What it demonstrates**: Skepticism toward pipeline output — even after
  seeing the data, asking "does this match reality?" Also why having an
  external source of truth (ESPN, in this case) matters: a pipeline that
  agrees with itself but disagrees with the real world is worse than a
  pipeline that crashes loudly.
- **Where to look**: Phase 3 commit message, the "patched with single-day
  backfill of 4/18" paragraph.

### Solved a Docker Compose parallel-build race via the single-build-owner pattern

- **When**: Phase 2, first `docker compose up`
- **What happened**: The initial compose file declared `build:` on three
  services that all shared the same image tag (`airflow-init`,
  `airflow-webserver`, `airflow-scheduler`). Compose tried to build all
  three in parallel and they collided at the export step with
  `image already exists`. Fixed it by making `airflow-init` the *sole* owner
  of the `build:` directive and having the other two services use
  `image:` + `pull_policy: never` to consume the already-built image.
  Subsequent `up -d` calls now have exactly one build attempt, no race.
- **What it demonstrates**: Reading Docker Compose internals carefully enough
  to spot a parallel-execution race condition, not just running into it
  repeatedly and bouncing the stack. Also picking the cleanest fix from
  multiple options (we could have used `depends_on` to serialize, or
  separate image tags per service — single-build-owner is more elegant
  because it matches the actual *intent*: one image used everywhere).
- **Where to look**: [`infra/docker-compose.yml`](../infra/docker-compose.yml)
  — `airflow-init` has the `build:` block; `airflow-webserver` and
  `airflow-scheduler` use `pull_policy: never`.

### Designed an operational playbook that survived real-world failure modes

- **When**: Throughout the 2025–26 NBA postseason (April 18 onward)
- **What happened**: I committed to running this pipeline daily against
  fresh data for the duration of the playoffs — not as a one-off demo but
  as a real operational obligation. Over the first ~3+ weeks of operation
  the pipeline survived: a transient `nba_api` failure (auto-recovered via
  retry policy), a Docker Desktop crash mid-backfill (recovered by
  restarting the stack and using the UI to mark the orphaned DagRun
  failed), a stale-DagRun blocking pattern that bit twice (eventually
  automated away with the `-CleanStale` flag in `catch_up.ps1`), and a
  one-day backfill-window gap caught only by manual reconciliation
  against ESPN. End result through 2026-05-13: **132 team-game rows
  across 26 distinct game dates (66 games captured), zero data loss, zero
  unsynced days** at any point after a catch-up.
- **What it demonstrates**: The difference between *building* a pipeline
  and *operating* one. Real-world failure modes only surface when you live
  with the system day-to-day — each one here produced a fix that's still
  in the codebase: dynamic partition overwrite, the retry policy,
  single-build-owner Docker pattern, `-CleanStale` automation, and the
  catch-up script's auto-gap detection.
- **Where to look**: [`scripts/catch_up.ps1`](../scripts/catch_up.ps1) is
  the single command that operationalizes this; the README's "Daily
  catch-up during the season" section is the runbook;
  `demo screenshots/backfill_success.png` shows what 70 / 70 task
  instances green looks like when the system is healthy.

### Built a self-healing daily catch-up script after hitting the same `max_active_runs` wall twice

- **When**: After Phase 3, during ongoing daily ops
- **What happened**: My DAG sets `max_active_runs=1` (intentionally — to be
  kind to `nba_api` rate limits). Twice during daily catch-up runs, a stale
  DagRun left in `running` state from a prior crash blocked all new runs
  with the indefinite "max_active_runs limit has been reached" log loop.
  After fixing manually via the UI both times, I added a `-CleanStale` flag
  to `scripts/catch_up.ps1` that detects stale running DagRuns *before*
  submitting work and offers to mark them failed via Airflow's REST API.
  The daily ritual is now self-healing for this specific failure mode.
- **What it demonstrates**: Recognizing a recurring operational pattern and
  automating the recovery, instead of just remembering the manual fix. Also
  using Airflow's REST API (rather than direct DB manipulation or a Python
  shim) to keep the recovery path supported and version-stable.
- **Where to look**: [`scripts/catch_up.ps1`](../scripts/catch_up.ps1) — the
  `Get-StaleRunningRuns` and `Mark-DagRunFailed` functions, and the
  `-CleanStale` parameter.

---

## When to add a new note

Add an entry when something happens that future-me (or anyone reading the
codebase six months from now) couldn't reconstruct from the commit alone.
The bar isn't "every change" — most commits explain themselves. The bar is
"this moment teaches something about the system or the engineering process."

Strong candidates:

- A bug whose resolution required a non-obvious fix (architectural or
  process-level), especially if the same class of bug could recur in
  another project
- A design choice where multiple plausible options existed and the picked
  one was non-obvious in retrospect
- A recurring operational pattern that got automated away
- A regression caught via testing or cross-reconciliation against external
  truth
- A failure mode that surfaced only under real-world load

Weak candidates (skip these):

- Pure debugging where the resolution was a typo or one-character fix
- Routine refactors with no design content
- Anything the commit message already covers in full

### Discovered nba_api endpoint soft-deprecation via a 32-MB bulk-load failure

- **When**: 2026-05-23 (Phase A advanced ingest, first real-data run)
- **What happened**: Phase A built the advanced box-score ingest layer
  using `nba_api.stats.endpoints.BoxScoreAdvancedV2`. All tests passed
  against mocked V2-shaped fixtures. First real bulk-load: the
  traditional pass succeeded for all 1,230 games in ~28 min, then the
  advanced pass blew up on the very first call with
  `KeyError: 'resultSet'` deep inside `nba_api`'s parser. A single
  isolated call to V2 reproduced the failure — ruling out rate
  limiting. Direct `requests.get()` to the V2 URL returned HTTP 200
  with body literally `{}` — stats.nba.com had soft-deprecated the V2
  advanced endpoint, accepting requests but returning empty payloads.
  Swapped to `BoxScoreAdvancedV3` (newer columns, camelCase instead
  of UPPER_SNAKE) with an explicit `_V3_ADVANCED_COLUMN_MAP`. Second
  bulk run: 32,179 rows in ~28 min, clean.
- **What it demonstrates**: External-API contracts drift silently —
  the soft-deprecation pattern (200 + empty body, no 404) is
  specifically designed to look like "your data is just missing"
  rather than "you're using a dead endpoint." The triage was three
  diagnostic steps: (1) verify it isn't rate limiting via isolated
  retry, (2) inspect the actual response shape with `requests`
  directly, (3) check whether a newer endpoint version exists. The
  explicit column-mapping dict is the defensive payoff — V3 ships new
  fields (`pacePer40`, `possessions`) that we intentionally drop
  rather than silently include, so the schema stays auditable.
- **Where to look**: [etl/ingest.py:357-410](../etl/ingest.py) for
  `_fetch_advanced_box_score` + `_V3_ADVANCED_COLUMN_MAP`; commit
  `cb20c13` for the swap diff and detailed reasoning.

### `get_spark()` bootstraps PYSPARK_PYTHON + HADOOP_HOME defensively

- **When**: 2026-05-23 (Phase A advanced ingest, second real-data run)
- **What happened**: The advanced bulk-load was kicked off via a
  PowerShell one-liner that imported `etl.transform.get_spark` and
  `etl.ingest.ingest_advanced_box_scores_bulk` directly — bypassing
  the `scripts/bulk_load_season.py` wrapper that had been quietly
  setting `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`, and `HADOOP_HOME`
  at module top. ~12 minutes into the run, Spark tried to spawn its
  Python workers via `python3` (Linux convention) and every task
  failed with `CreateProcess error=2, The system cannot find the file
  specified`. Burned the API budget; lost the in-memory data because
  the write step never executed. Fixed by making `get_spark()` itself
  call a `_bootstrap_pyspark_env()` helper the first time it's
  invoked, setting both Python env vars to `sys.executable` and
  pointing `HADOOP_HOME` at the vendored `.hadoop/` directory. The
  wrapper scripts still set these at module top — belt-and-suspenders;
  `get_spark()` is now the safety net for direct callers.
- **What it demonstrates**: A "convenience entrypoint" (here:
  `get_spark()`) should be self-sufficient, not rely on the existence
  of a particular wrapper script to set environment up. The original
  design had the wrapper scripts do the setup, which worked fine until
  someone (me, in this case) reached past the wrapper and hit the bare
  function. Lifting the platform-specific incantations *into* the
  factory function — guarded by `os.environ.setdefault` so wrappers
  that already set them don't get clobbered — eliminates the
  "wrapper-required" footgun without changing the wrappers' behavior.
  Also a real cost: the failure cost ~25 minutes of wall-clock time
  and forced a re-run, which is exactly the kind of incident worth
  preventing with 6 lines of defensive code.
- **Where to look**: [etl/transform.py:14-44](../etl/transform.py) for
  `_bootstrap_pyspark_env`; [scripts/bulk_load_advanced_only.py](../scripts/bulk_load_advanced_only.py)
  for the dedicated re-runnable script that came out of this incident;
  commit `512d757` for the diff.

### Phase B feature richness → +1.3pp logreg accuracy, HGB unchanged — reported honestly

- **When**: 2026-05-24 (Phase B retrain after rebuild_from_raw)
- **What happened**: Phase B blended `BoxScoreAdvancedV3` aggregates
  (minutes-weighted team ORtg / DRtg / NetRtg / Pace) into processed
  and added matching rolling features. Retrained on N=1,284 games. The
  story is mixed and reported as such: logreg accuracy moved 0.607 →
  **0.620** (+1.3pp), closing the gap to the strongest baseline from
  -2.8pp to **-1.6pp**. HGB barely moved (+0.2pp, noise). Log loss got
  slightly *worse* for both models (logreg 0.654 → 0.662, HGB 1.019 →
  1.070) — the model is making more confident picks whose confidence
  isn't always justified. A genuine small regression hidden inside an
  accuracy improvement.
- **What it demonstrates**: Adding domain-informed features ≠
  automatic improvement. The linear model picked up signal from the
  advanced metrics that wasn't in the rolling traditional stats; the
  gradient booster did not, almost certainly because rolling pts /
  eFG% / TS% are already near-monotonic transforms of ORtg/DRtg, so
  the trees couldn't carve out additional decision regions. The
  accuracy lift came packaged with a log-loss regression — the kind of
  trade-off that gets buried in real ML projects when only the
  favorable metric is reported. Calibration (Platt / isotonic) is the
  honest v1.3.x follow-up. Reporting both the win and the small loss
  is the methodology this whole phase has been about.
- **Where to look**: README's "Phase 4b — honest results" table for
  the three-snapshot comparison; commit `d86a13e` for the Phase B
  pipeline change; `TODO.md` "Phase B follow-up" for the calibration
  plan.

## Template

```markdown
### Headline (one line)

- **When**: phase / date
- **What happened**: 2–4 sentences, technical specifics
- **What it demonstrates**: engineering principle, design choice, or operational pattern
- **Where to look**: file paths or commit SHA
```
