# Portfolio Anecdotes — nba-parquet

> Real moments from building this project that demonstrate specific skills or
> tell a useful story in interviews. Each entry is calibrated to be concise
> enough to drop into a conversation without notes — usually 30 seconds spoken.
>
> Companion doc: [PROJECT_QA.md](PROJECT_QA.md) — the same project explained
> end-to-end. PROJECT_QA covers *what the project is*; this file covers
> *interesting moments that happened while building it*.

## Format

Each anecdote follows the same five-field shape:

- **Headline** — interview-ready one-liner ("I caught a..." / "I noticed that..." / "I designed for...")
- **When** — date or phase, so I can place it on a timeline if asked
- **What happened** — the actual technical story, 2–4 sentences
- **What it demonstrates** — the skill, competency, or decision-making it proves
- **Where to look** — file paths or commit SHAs so the story is verifiable

---

## Anecdotes

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
- **What it demonstrates**: The difference between "I built a pipeline"
  and "I built a pipeline I actually run." Most portfolio data
  engineering stops at the first one — the second exposes failure modes
  you only learn by living with the system. Each of those failure modes
  produced a fix that's still in the codebase: dynamic partition
  overwrite, the retry policy, single-build-owner Docker pattern,
  `-CleanStale` automation, and the catch-up script's auto-gap detection.
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

## How to use this doc

- **Before an interview**: skim the headlines and pick 2–3 that match the
  job's stated stack. The retry-recovery and dynamic-partition-overwrite
  stories are the strongest "production-readiness" anecdotes; the
  TO/tov regression and ESPN reconciliation are the strongest "data
  correctness mindset" anecdotes; the Docker race is a "infrastructure
  literacy" story.
- **When asked an open-ended "tell me about a time"**: lead with the
  headline, give the 2-sentence "what happened," then pivot to "what it
  demonstrates." Have the file path ready in case the interviewer wants to
  see the code.
- **When updating this doc**: add new anecdotes as they happen — don't try
  to manufacture them. The most credible stories are the ones where the
  problem genuinely surprised me. If the resolution required a real fix
  (code, architecture, process), it's worth recording. Pure debugging
  ("turned out it was a typo") usually isn't unless the typo had
  educational value.

## Adding a new anecdote — quick template

```markdown
### Headline (one line, interview-ready)

- **When**: phase / date
- **What happened**: 2–4 sentences, technical specifics
- **What it demonstrates**: skill, competency, or decision-making proven
- **Where to look**: file paths or commit SHA
```
