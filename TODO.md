# TODO.md — Phased Build Plan

## Status snapshot (as of 2026-05-13)

| Phase | State | Commit |
|---|---|---|
| **Phase 1 — Core ETL** | ✅ shipped | [`f51d0bb`](https://github.com/tjromack/nba-parquet/commit/f51d0bb) |
| **Phase 2 — Airflow DAG + Docker Compose** | ✅ shipped | [`46377d1`](https://github.com/tjromack/nba-parquet/commit/46377d1) |
| **Phase 3 — Rolling features + dynamic partition overwrite** | ✅ shipped | [`efa1c3b`](https://github.com/tjromack/nba-parquet/commit/efa1c3b) |
| Phase 4 — Real AWS deploy | ⏳ optional, low priority | — |
| **Phase 4b — Prediction model** (parked, fully scoped) | ⏳ next major lift — open the Backlog section, start test-first | see Backlog |
| **Phase 5 — Polish / CI / docs / dashboard** | ✅ mostly shipped | multiple commits, see below |

**Phase 5 sub-status:**
- ✅ GitHub Actions CI (`eb71ac2`) — lint + tests + Docker build verify on every push
- ✅ Streamlit dashboard (`ff0e222`, polish `e70c0c2`, `bb1abcb`) — 4 views, reads live pipeline output, includes auto-generated "What's notable" commentary + series-elimination tracking with ACTIVE/OUT status
- ✅ `docs/PROJECT_QA.md` (`b379be4`) — technical + layman Q&A reference
- ✅ `docs/ENGINEERING_NOTES.md` (`62f5dd2`, `d728aab`, renamed from `PORTFOLIO_ANECDOTES.md` before going public) — curated log of notable engineering moments
- ✅ README rewrite (`1cb1438`) — Mermaid architecture diagram, Results & Metrics, Verify-in-60s
- ✅ Daily catch-up automation (`812ba6c`, `62f5dd2`) — `scripts/catch_up.ps1` with `-CleanStale`
- ⏳ `notebooks/exploratory_eda.ipynb` — narrative companion to Streamlit
- ⏳ `docs/architecture.md` + `docs/runbook.md` — written-out playbook
- ⏳ Tag `v1.0.0` release
- ⏳ LinkedIn / portfolio post

**Test gate**: 30 passed, 1 skipped in ~33s (Airflow-load test runs only when `apache-airflow` is installed locally; the new test exercises series-elimination logic against a synthetic 14-row fixture).
**Real-data validation**: 132 team-game rows from 26 distinct game dates (66 games captured), 2026-04-18 → 2026-05-13. Cross-reconciles to ESPN — NYK 8-2 over their last 10 with .630 TS%, OKC perfect 8-0 with .628 TS%, ten teams already eliminated.
**Operational milestone**: pipeline has run daily through the 2025–26 NBA playoffs with zero data loss across three+ weeks; one transient `nba_api` blip was auto-recovered via Airflow's retry policy. Dashboard now surfaces series-elimination state (10 of 16 teams out, 6 still active) computed live from the processed layer.

---

## Phase 1 — Core ETL: Ingest → Transform → S3 Write — ✅ shipped
> Goal: Full pipeline runs locally via `python scripts/run_local.py` with real NBA data writing real Parquet to S3 / LocalStack / local disk.

- [x] Scaffold repo structure: dirs, `__init__.py` files, `Makefile`, `.env.example`, `pyproject.toml`
- [x] Write `requirements.txt`: `pyspark==3.5.*`, `nba_api`, `boto3`, `python-dotenv`, `pandas`, `pyarrow`
- [x] Write `requirements-dev.txt`: `pytest`, `pytest-mock`, `black`, `ruff`
- [x] Implement `etl/schema.py`: `RAW_BOX_SCORE_SCHEMA`, `PROCESSED_SCHEMA`, `FEATURE_SCHEMA`
- [x] Implement `etl/ingest.py`:
  - [x] `nba_api.stats.endpoints.LeagueGameLog` to list games for a date
  - [x] `BoxScoreTraditionalV2` per game for player rows; sleep 0.6 s between calls (rate-limit guard)
  - [x] Rename `TO` → `tov` (real-`nba_api` regression caught and fixed)
  - [x] Float→int hardening for IntegerType columns (Spark schema mismatch fix)
  - [x] Write raw Parquet to `raw/nba/box_scores/season={Y}/game_date={D}/`
- [x] Implement `etl/transform.py`:
  - [x] `get_spark()` factory function (S3A config gated on non-local mode, `partitionOverwriteMode=dynamic`)
  - [x] `aggregate_team_game(df)` — pts, reb, ast, tov, eFG%, TS%, assist:turnover, win flag, opponent abbreviation, home/away parsing
  - [x] `get_top_player(df, stat_col)` — Window function for top scorer / rebounder / playmaker per team / game
- [x] Implement `etl/write.py`: partitioned by `(season, game_date)` to `processed/` prefix; rejects raw/ paths
- [x] Implement `etl/paths.py`: `LOCAL_OUTPUT_DIR` mode for zero-S3 dev runs
- [x] Implement `scripts/run_local.py`: ingest → transform → write → features, default ingest date = yesterday, dual S3/local destination
- [x] `tests/fixtures/sample_box_scores.csv`: 48 player rows × 4 teams × 2 dates with realistic shot/turnover variance
- [x] `tests/conftest.py`: shared `SparkSession` fixture (`local[2]`), auto-bootstraps `HADOOP_HOME` (Windows winutils.exe) and `PYSPARK_PYTHON`
- [x] `tests/test_ingest.py`: mocks `nba_api`, asserts shape, columns, rate-limit sleep, `tov` regression
- [x] `tests/test_transform.py`: schema match, hand-computed eFG%/TS%, AST/TOV null guard, top-player correctness, home/away parse
- [x] `tests/test_write.py`: partition layout, prefix rejection
- [x] `tests/test_paths.py`: local-mode URI resolution, S3A fallback
- [x] Lint + tests pass with zero AWS credentials and zero network access
- [x] Manual smoke test: real-data run for 4/29 playoff slate produces realistic eFG%, TS%, top scorers (Banchero, Cunningham, Mobley, Barrett, Jokić, Doncić)

---

## Phase 2 — Airflow DAG + Docker Compose Orchestration — ✅ shipped
> Goal: Same pipeline runs as a daily Airflow DAG inside Docker, picking up the prior day's games on its own.

- [x] `infra/Dockerfile.airflow`: extends `apache/airflow:2.9.3-python3.11`, installs OpenJDK 17 + project requirements
- [x] `infra/docker-compose.yml`: postgres + airflow-init + webserver + scheduler. **`LocalExecutor`** (no Celery / Redis — simpler stack, same orchestration semantics for our scale)
- [x] Single-build-owner pattern (`airflow-init` owns `build:`, others use `pull_policy: never`) — avoids the parallel image-export race
- [x] `infra/airflow.env.example`: templated stack env, real `airflow.env` gitignored
- [x] Bind-mounted `dags/`, `etl/`, `scripts/`, `out/` so code changes propagate without a rebuild
- [x] `dags/nba_etl_dag.py`:
  - [x] DAG id `nba_etl_pipeline`, `schedule="@daily"`, `catchup=False`, `max_active_runs=1`
  - [x] Five `PythonOperator` tasks: `ingest_raw → transform_and_aggregate → write_processed → write_features → notify_done`
  - [x] All heavy imports (`pyspark`, `nba_api`, `pandas`, `etl.*`) lazy-loaded inside callables
  - [x] `transform_and_aggregate` writes to `staging/run_date={ds}/`, `write_processed` promotes to canonical `processed/` (real staging-promotion pattern)
  - [x] S3/local paths threaded between tasks via XCom
- [x] `tests/test_dag.py`: AST-level guard rails (no heavy module-level imports), all 5 callables present, full Airflow-load smoke (skipped without `apache-airflow`)
- [x] DAG validated end-to-end: autonomous run on unpause ingested 4/30 playoff games and wrote partitioned Parquet — `demo screenshots/dag_screenshot.png`

---

## Phase 3 — Feature Engineering + Rolling Window Stats — ✅ shipped
> Goal: Feature layer writes rolling 10-game aggregations to `features/` prefix, ready for prediction models.

- [x] `etl/features.py`: `build_rolling_features(df, window=10)` using `Window.partitionBy("team_id").orderBy("game_date").rowsBetween(-9, 0)`
- [x] Rolling metrics: `rolling_pts`, `rolling_efg_pct`, `rolling_ts_pct`, `rolling_ast_to_tov`, `rolling_win_pct`, `games_in_window`
- [x] Home/away split: `rolling_pts_home` / `rolling_pts_away` via conditional-average within the same window
- [x] `etl/schema.py`: `FEATURE_SCHEMA` (13 fields)
- [x] `etl/write.py`: `write_features()` partitioned by `season`; rejects raw/ and processed/ prefixes
- [x] `dags/nba_etl_dag.py`: real `_write_features` task replaces Phase 2 stub; reads full processed history, rebuilds features layer end-to-end
- [x] `scripts/run_local.py`: extended with the features step
- [x] `tests/test_features.py`: 7 tests against a hand-computed 12-game BOS sequence + 4-game LAL counter-team. Asserts exact rolling values, partial-window edges, partitioning, validation, prefix rejection
- [x] **Bonus fix**: `spark.sql.sources.partitionOverwriteMode=dynamic` in `get_spark()` — without this, daily backfills clobber the whole `processed/` prefix instead of touching just the day's partition
- [x] Real-data validation: 14-day Airflow backfill (4/19 → 5/2), then patched 4/18 single-day backfill after spotting the NYK series-record discrepancy. Final state: 92 rows, NYK reconciles to 4-2 — `demo screenshots/backfill_success.png`, `demo screenshots/thru5_3_26_leaderboard.png`
- [x] README updated with feature layer schema and demo screenshots

---

## Phase 4 — Cloud Deploy: EC2 + S3 + IAM Hardening — ⏳ optional
> Goal: Pipeline runs on real AWS infrastructure with proper IAM, no hardcoded credentials anywhere.
>
> Note: the code already supports real S3 today via the same `S3_BUCKET` / AWS env vars. Phase 4 is purely about provisioning + screenshotting "it ran in the cloud". Skippable for portfolio purposes — most reviewers value the architecture proof in Phases 1-3 over a literal AWS console screenshot.

- [ ] Create IAM policy JSON (`infra/iam_policy.json`): S3 `GetObject`, `PutObject`, `ListBucket` on target bucket only
- [ ] Create IAM role for EC2 instance profile (documented in `infra/README_infra.md`)
- [ ] Provision EC2 instance (t3.medium+) with bootstrap script: Java 17, Python 3.11, Docker, repo clone
- [ ] Write `scripts/bootstrap.sh`: idempotent EC2 install script
- [ ] Configure Airflow on EC2 with `LocalExecutor` + postgres RDS or SQLite (documented trade-off)
- [ ] Test: run `nba_etl_pipeline` DAG on EC2, files land in real S3 bucket
- [ ] Verify: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are **not** in any file on the instance
- [ ] Add S3 bucket policy blocking public access
- [ ] CloudWatch log group for Airflow task logs (optional, documented)
- [ ] Cost estimate: document expected AWS monthly spend at daily cadence in README

---

## Phase 5 — Docs, CI, Demo, Ship It — ✅ mostly shipped
> Goal: Repo is portfolio-ready, discoverable, and demo-able without running any code.

- [x] **Top-of-README rewrite** (`1cb1438`): 60-second elevator pitch — tagline, demo screenshot, What/Why/How/For-Whom block, Skills Demonstrated table with file refs.
- [x] **GitHub Actions CI** (`eb71ac2`): `.github/workflows/ci.yml` runs `pytest` + `ruff` + `black --check` + Docker image build on every push to `main`. Green "CI passing" badge in README.
- [x] **Skills demonstrated table** (`1cb1438`): embedded in README rather than its own file. 12-row table mapping competencies to clickable file references.
- [x] **Mermaid architecture diagram** (`011c7c3`, `883ced5`): renders natively on GitHub, shows data flow + DAG + zones in one TD layout.
- [x] **`docs/PROJECT_QA.md`** (`b379be4`): technical + layman Q&A reference, six questions + tiered pitches (30 sec / 2 min / 30 min).
- [x] **`docs/ENGINEERING_NOTES.md`** (`62f5dd2`, `d728aab`; renamed from `PORTFOLIO_ANECDOTES.md` before flipping the repo public): seven curated notes with headline / when / what / demonstrates / where-to-look format.
- [x] **`scripts/catch_up.ps1`** (`812ba6c`, `545fd3c`, `62f5dd2`): self-healing daily catch-up with `-CleanStale` flag for stuck-DagRun recovery.
- [x] **Streamlit dashboard** (`ff0e222`, polish `e70c0c2`): four views (Leaderboard, Team detail, Head-to-head, Data explorer) reading live pipeline output.
- [x] **Results & Metrics** section in README (`1ba21a1`): concrete numbers from the validation run + top-of-leaderboard table.

Phase 5 leftovers (not blocking; do as energy allows):

- [ ] **`docs/architecture.md`**: longer-form architecture writeup — what's in the README is already sufficient for most readers, this would only matter if someone really wants depth
- [ ] **`docs/data_dictionary.md`**: field-by-field column docs with example values
- [ ] **`docs/runbook.md`**: operational playbook (how to re-trigger a single date, debug a failed task, switch destinations)
- [ ] **`notebooks/exploratory_eda.ipynb`**: pandas read + matplotlib chart of rolling TS% trajectory for the top 4 contenders. Visual storytelling that complements Streamlit (static, embedded in repo for browse-without-running).
- [ ] **`CONTRIBUTING.md`**: how to add a new data source (NFL, MLB) following the ingest → transform → write pattern
- [ ] **Tag `v1.0.0` release** on GitHub once any of the above ship that you want as the "v1 line"
- [ ] **LinkedIn / portfolio post** with the leaderboard screenshot, the DAG graph, and one observation from the data

---

## Backlog / future ideas (not phased, just parked)

### Phase 4b — Prediction model (the project's stated raison d'être)

> The features layer was built to feed a downstream prediction model. Closing
> the loop turns this from "I built a feature store" into "I built a feature
> store, the model that consumes it, and an honest evaluation of how it does."
> Weekend-scale. Open this section and start from the top when ready.

**Goal (v1):** predict the **winner** of an NBA playoff game (binary
classification) from each team's trailing rolling features as they stood
*entering* that game. Ship: a leak-free training-set builder, a baseline +
one real model, time-series evaluation with honest baselines, MLflow
tracking, a Streamlit "Predictions" view, and the regression test that
guarantees no target leakage.

**Non-goals for v1 (park as Phase 4c if wanted):**
- Spread (point-margin) and total-points regression — winner-only first
- Hyperparameter-tuning sagas — one baseline + one tree model, sensible defaults
- Beating Vegas / any claim of predictive supremacy — see "Data-volume honesty"
- Real-time / in-game prediction — pre-game only

**The #1 correctness gate — no target leakage (read before writing any code):**
The `features/` rolling columns for game N are computed over a window that
*includes game N itself*. Using them to predict game N is leakage and makes
every metric a lie. The training-set builder MUST use each team's features
**as of the game strictly before** the one being predicted (lag by one game
per team, or recompute the window excluding the target game). This is the
single most important thing in the whole phase and gets a dedicated test
before anything else is trusted.

**Build order:**

- [x] `models/__init__.py` + module layout decided (session 1, commit pending):
  - [x] `models/dataset.py` — pure pandas `build_training_frame(features,
    processed) -> DataFrame`. One row per game: home team's lagged rolling
    features (`home_*`), away team's lagged rolling features (`away_*`),
    `label` = 1 if home won. **Leak-free by construction** — per-team
    lag-1 shift; a team's first game (no prior window) is dropped via
    inner-join semantics. Pure pandas (decoupled from Spark — the model
    layer is sklearn/mlflow-world; game-level frame is tiny).
  - [ ] `models/train.py` — fits baseline + model, runs time-series
    evaluation, logs everything to MLflow, persists the chosen model
    artifact. *(session 2)*
  - [ ] `models/predict.py` — loads the persisted model, scores upcoming /
    recent games, returns predicted winner + probability + the feature
    vector that drove it (for the Streamlit view). *(session 2/3)*
- [x] Labels: home/away orientation + `label` recovered from `processed`
  (`is_home` + `win`); data-quality guard raises `ValueError` unless each
  game has exactly one home row and one away row.

> **Session 1 real-data verification (2026-05-18):** ran
> `build_training_frame` against the live `out/` Parquet. 69 games →
> 61 training rows; the 8 dropped reconcile exactly to the 8 first-round
> series openers where both teams had no prior window — leak-free drop
> logic confirmed on real multi-series playoff structure, not just the
> synthetic fixture. Two facts captured for session 2:
>
> - **Measured home-win baseline = 0.541** over the 61 games. That is
>   the "always pick home" accuracy the model must beat. No need to
>   re-derive it — start the baseline comparison from this number.
> - **NaN in the split features is expected and correct.**
>   `home_rolling_pts_home` / `home_rolling_pts_away` / `away_*` carry
>   ~8 NaNs each — teams whose lagged window had no home (or no away)
>   game yet. The builder preserves NaN rather than fabricating values
>   (right call). Consequence: `train.py` MUST impute before fitting.
>   `HistGradientBoostingClassifier` handles NaN natively; logistic
>   regression does NOT — so the baseline-vs-model comparison needs a
>   consistent imputation step (e.g. a `SimpleImputer(strategy="median")`
>   inside an sklearn `Pipeline`) applied identically to every model so
>   the comparison stays fair.

- [ ] **Baselines first, before any model.** Report accuracy / log-loss for:
  (a) always pick home team (measured: 0.541 — see note above),
  (b) always pick the team with the better trailing `rolling_win_pct`,
  (c) always pick the better trailing `rolling_ts_pct`. The model has to
  beat these to be worth anything — naming them up front is the honest
  framing.
- [ ] Model: logistic-regression baseline, then `sklearn`
  `HistGradientBoostingClassifier` (or `xgboost`). Fixed `random_state`;
  deterministic and reproducible.
- [ ] **Time-series evaluation only.** Expanding-window walk-forward: train
  on games up to date D, test on games after D, step forward. NEVER random
  k-fold (leakage). Metrics: accuracy, log loss, Brier score, a calibration
  curve, and the model-vs-baseline delta.
- [ ] **MLflow** experiment tracking: each run logs params, metrics, the
  baseline deltas, and the model artifact. Add `mlflow` to
  `requirements.txt`; document `mlflow ui` in the README. Runs must be
  reproducible from a clean clone.
- [ ] New `streamlit_app.py` view — **"Predictions"**: for recent/upcoming
  games show predicted winner, win probability, and the rolling features
  that drove it. Closes the narrative loop visually. Reads the persisted
  model + `predict.py`; degrades gracefully if no model artifact present.
- [ ] Tests (`tests/test_models.py`):
  - [x] **Leakage guard (written first, red→green):** synthetic 2-team
    4-game sequence with decodable rolling values; asserts the training
    row for game G carries each team's game G-1 values (not G's own) and
    `label` = game G's actual result.
  - [x] `build_training_frame` produces one row per game, exactly one
    label, expected column set; empty input → empty frame (mirrors the
    off-day handling elsewhere); a game where one team lacks prior
    history is dropped, not half-populated.
  - [x] Data-quality guard: corrupt orientation (two home rows for one
    game) raises `ValueError`.
  - [ ] Baseline functions return sane accuracies on a hand-built
    fixture. *(session 2)*
  - [ ] Walk-forward splitter never puts a test game chronologically
    before any training game. *(session 2)*
- [ ] Optional `notebooks/model_eval.ipynb` — calibration plot, feature
  importance, error analysis by situation (home/away, series game number).
- [ ] Conventions to preserve (same discipline as the ETL layer): pure
  dataset function, no `.collect()` outside the final materialization,
  schema-first where practical, lint-clean, CI green, lazy imports if any
  of this is ever wired into the DAG.

**Definition of done (v1):** `python -m models.train` runs from a clean
clone, logs an MLflow run, persists a model that beats all three baselines
on walk-forward eval (or the README honestly states it doesn't and why),
the leakage test passes, and the Streamlit "Predictions" view renders for
recent games.

**Data-volume honesty (state this in the README, don't hide it):** playoff-
only data is thin (~65 games). Phase 4b is a demonstration of *correct ML
engineering methodology on real data* — leak-free features, honest
baselines, time-series evaluation, reproducible tracking — not a claim of
predictive edge. The credible scaling path is the regular-season bulk-load
(see "Other parked ideas"): ~1,200+ games is enough to make the model
numbers meaningful. Saying this plainly is itself a portfolio strength —
it signals you know the difference between methodology and results.

**Suggested first session when you pick this up:** create `models/`, write
the leakage test against a synthetic sequence FIRST (red), then
`build_training_frame` until it's green. That single test-first step
de-risks the entire phase — everything downstream is only trustworthy if
the training frame is leak-free.

### Streamlit Cloud public deployment (deferred)

The local-run model (`streamlit run streamlit_app.py`, screen-share in
meetings) is genuinely fine for the current "personal dev + occasional show
during calls" use case. Deferred until: (a) repo goes public, AND
(b) a snapshot of `out/processed/` + `out/features/` is bundled into the
repo so the cloud-hosted app has data to render, OR (c) we hook the
dashboard up to a real S3 bucket with daily writes.

### Other parked ideas

- LocalStack integration test that runs the DAG end-to-end against a fake S3 (`pytest -m integration`) — proves the S3A code path works without a real AWS account
- Switch raw layer from `BoxScoreTraditionalV2` to `BoxScoreAdvancedV2` for additional advanced metrics (offensive rating, defensive rating, pace) → more model-ready features
- Player-level rolling features (next to team-level): trailing pts/reb/ast per player for usage / minutes models
- Add a `season_over_season_delta` feature column: team's current rolling EPA-equivalent vs same week prior season — useful for survivor / spread models
- Schema migration story: how to evolve `PROCESSED_SCHEMA` without breaking existing partitioned reads (Iceberg or Delta Lake substitution)
