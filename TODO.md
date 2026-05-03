# TODO.md — Phased Build Plan

## Phase 1 — Core ETL: Ingest → Transform → S3 Write
> Goal: Full pipeline runs locally via `python scripts/run_local.py` with real NBA data writing real Parquet to S3 or LocalStack.

- [ ] Scaffold repo structure: all dirs, `__init__.py` files, `Makefile`, `.env.example`, `pyproject.toml`
- [ ] Write `requirements.txt`: `pyspark==3.5.*`, `nba_api`, `boto3`, `python-dotenv`, `pandas`
- [ ] Write `requirements-dev.txt`: `pytest`, `pytest-mock`, `black`, `ruff`
- [ ] Implement `etl/schema.py`: `StructType` for raw box-score rows + processed team-game output
- [ ] Implement `etl/ingest.py`:
  - [ ] `nba_api.stats.endpoints.LeagueGameLog` to list games for a date
  - [ ] `BoxScoreTraditionalV2` per game for player rows; sleep 0.6s between calls
  - [ ] Write raw Parquet to S3 `raw/nba/box_scores/season={Y}/game_date={D}/`
- [ ] Implement `etl/transform.py`:
  - [ ] `get_spark()` factory function (S3A config, credentials from env)
  - [ ] `aggregate_team_game(df)` — pts, reb, ast, tov, eFG%, TS%, assist:turnover, win flag
  - [ ] `get_top_player(df, stat_col)` — window function to find top scorer/rebounder/playmaker per team/game
- [ ] Implement `etl/write.py`: write processed DataFrame as Parquet partitioned by `(season, game_date)` to S3 `processed/` prefix
- [ ] Implement `scripts/run_local.py`: wire ingest → transform → write, load config from `.env`, default ingest date = yesterday
- [ ] Create `tests/fixtures/`: small (≤200-row) sample of player-level box-score rows covering at least 4 teams across 2 game dates, with realistic shot/assist/turnover variance
- [ ] Write `tests/conftest.py`: shared `SparkSession` fixture (`local[2]`)
- [ ] Write `tests/test_ingest.py`: mock `nba_api` endpoints, assert DataFrame shape + key columns present + sleep was called between game pulls
- [ ] Write `tests/test_transform.py`: load fixture, run `aggregate_team_game`, assert schema + eFG% / TS% math + win flag correctness
- [ ] Write `tests/test_write.py`: write to temp local path, assert Parquet files created with correct partition dirs (`season=*/game_date=*`)
- [ ] `make test` passes with zero AWS credentials and zero network access
- [ ] `make lint` passes (black + ruff clean)
- [ ] Manual smoke test: `make run-local` for yesterday's playoff games writes Parquet to LocalStack or real S3, files visible in bucket

---

## Phase 2 — Airflow DAG + Docker Compose Orchestration
> Goal: Full pipeline runs as an Airflow DAG triggered from the web UI, picking up the prior day's games on a daily schedule.

- [ ] Write `infra/docker-compose.yml`: Airflow webserver, scheduler, worker, postgres, redis (official `apache/airflow:2.9` image)
- [ ] Write `infra/airflow.env`: non-secret Airflow config (executor, DB conn, fernet key placeholder)
- [ ] Add Airflow to `Makefile`: `airflow-up`, `airflow-down`, `trigger-dag` targets
- [ ] Write `dags/nba_etl_dag.py`:
  - [ ] DAG id `nba_etl_pipeline`, `schedule="@daily"`, `catchup=False`
  - [ ] `ingest_raw` task pulls games for `{{ ds }}` (Airflow execution date)
  - [ ] `transform_and_aggregate` task → `etl.transform`
  - [ ] `write_processed` task → `etl.write`
  - [ ] `write_features` task (stub for Phase 3)
  - [ ] `notify_done` task (`PythonOperator` printing summary stats from XCom)
  - [ ] XCom pass of S3 paths between tasks
- [ ] Mount `etl/` and `dags/` into Airflow containers via volume
- [ ] Configure `aws_default` Airflow Connection via env var injection (not hardcoded)
- [ ] Test: trigger DAG from Airflow UI, all tasks green, files appear in S3/LocalStack
- [ ] Test: confirm DAG import time < 2s (no heavy imports at module level)
- [ ] Add Airflow DAG graph screenshot to `docs/dag_screenshot.png`

---

## Phase 3 — Feature Engineering + Rolling Window Stats
> Goal: Feature layer writes rolling 10-game aggregations to S3 `features/` prefix, ready for prediction models.

- [x] Implement `etl/features.py`:
  - [x] `build_rolling_features(df, window=10)` using PySpark `Window.partitionBy("team_id").orderBy("game_date").rowsBetween(-9, 0)`
  - [x] Rolling metrics: `rolling_pts`, `rolling_efg_pct`, `rolling_ts_pct`, `rolling_ast_to_tov`, `rolling_win_pct`
  - [x] Output schema in `etl/schema.py`
- [x] Extend `etl/write.py`: `write_features()` writes to `features/` prefix partitioned by `season`
- [x] Wire `write_features` Airflow task in DAG (replace stub from Phase 2)
- [x] Write `tests/test_features.py`: assert rolling values are correct for a known 12-game fixture sequence
- [x] Extend `scripts/run_local.py` to include feature build step
- [x] Add `home_away_split` rolling metric (`rolling_pts_home` / `rolling_pts_away`)
- [x] Validate output: 14-day playoff backfill (4/19→5/2) + patch backfill of 4/18 to capture series openers; NYK row reconciles to 4-2 vs ATL, OKC leads at .614 TS% over a 4-0 stretch
- [x] Update README output schema table with feature layer columns
- [x] Bonus fix: set `spark.sql.sources.partitionOverwriteMode=dynamic` in `get_spark()` so daily partitions accumulate during backfills instead of clobbering the whole prefix

---

## Phase 4 — Cloud Deploy: EC2 + S3 + IAM Hardening
> Goal: Pipeline runs on real AWS infrastructure with proper IAM, no hardcoded credentials anywhere.

- [ ] Create IAM policy JSON (`infra/iam_policy.json`): S3 `GetObject`, `PutObject`, `ListBucket` on target bucket only
- [ ] Create IAM role for EC2 instance profile (documented in `infra/README_infra.md`)
- [ ] Provision EC2 instance (t3.medium+) with bootstrap script: Java 11, Python 3.11, Spark, repo clone
- [ ] Write `scripts/bootstrap.sh`: idempotent install script for EC2 (Java, pip, env setup)
- [ ] Configure Airflow on EC2 with `LocalExecutor` + postgres RDS or SQLite (documented trade-off)
- [ ] Test: run `nba_etl_pipeline` DAG on EC2, files land in real S3 bucket
- [ ] Verify: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are **not** in any file on the instance
- [ ] Add S3 bucket policy to block public access (documented in `infra/README_infra.md`)
- [ ] Add CloudWatch log group for Airflow task logs (optional but documented)
- [ ] Cost estimate: document expected AWS monthly spend at daily cadence in README

---

## Phase 5 — Docs, CI, Demo, Ship It
> Goal: Repo is portfolio-ready, discoverable, and demo-able without running any code.

- [ ] Write full `README.md`: architecture diagram (ASCII or Mermaid), full setup instructions, output schema, cost estimate
- [ ] Add `docs/` folder: `architecture.md`, `data_dictionary.md`, `runbook.md` (how to re-trigger, how to backfill a date range)
- [ ] Add GitHub Actions CI (`.github/workflows/ci.yml`): `make lint` + `make test` on every push
- [ ] Record or screenshot Airflow DAG successful run — add to `docs/`
- [ ] Add S3 output tree screenshot (or `aws s3 ls` output) to `docs/`
- [ ] Write `notebooks/exploratory_eda.ipynb`: quick pandas read of processed Parquet, one visualization showing rolling true-shooting by team across the playoffs
- [ ] Add `CONTRIBUTING.md`: how to add a new data source (NFL, MLB) following existing ingest pattern
- [ ] Tag `v1.0.0` release on GitHub
- [ ] **Ship it**: post repo link to LinkedIn/portfolio with the Airflow DAG screenshot and one interesting playoff insight from the data
