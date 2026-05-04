# nba-parquet

> **A daily PySpark + Airflow pipeline that turns NBA box scores into model-ready trailing-window features.**

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)
![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apacheairflow)
![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazons3)
[![CI](https://github.com/tjromack/nba-parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/tjromack/nba-parquet/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-MIT-green)

## Demo at a glance

Validated end-to-end against the 2025–26 NBA playoffs. The leaderboard below was produced by the live pipeline running daily through 2026-05-03, sorted by trailing 10-game true-shooting %:

![Rolling features leaderboard](demo%20screenshots/thru5_3_26_leaderboard.png)

NYK at 4-2 matches their real series result vs ATL; OKC leads at .614 TS% on a 4-0 stretch; Phoenix sits at 0-4 after a sweep. These are exactly the trailing-window signals a survivor / spread / total prediction model consumes downstream.

## What / Why / How / For Whom

- **What it does.** A daily Airflow DAG ingests NBA box scores from `nba_api`, aggregates them with PySpark into team-game stats (eFG%, true shooting %, AST/TOV, win flag), and writes partitioned Parquet to S3 — then engineers rolling 10-game features (`rolling_ts_pct`, `rolling_win_pct`, home/away split) ready for downstream prediction models.
- **Why it exists.** Sports-analytics prediction models (survivor pools, spreads, totals) need clean, aggregated, time-windowed signal. This pipeline replaces ad-hoc pandas notebooks with a real data platform: schema-typed, idempotent, partition-aware, daily-orchestrated, retry-safe.
- **How it's built.** Five-task Airflow DAG (`ingest_raw → transform_and_aggregate → write_processed → write_features → notify_done`), `LocalExecutor` on Postgres, staging-then-promote Parquet writes with **dynamic partition overwrite**, dual-mode destination (S3A or local disk via `LOCAL_OUTPUT_DIR`), and 25 unit tests covering schema, math, partitioning, and DAG load-time guard rails.
- **For whom.** Sports-analytics teams who want a model-ready feature layer fed nightly; data-engineering hiring managers reviewing portfolio work; future-me who needs to remember why the staging-then-promote pattern is there. Also a reusable template for any "ingest API → transform → partitioned warehouse" use case (NFL, MLB, fantasy, etc.).

## Skills demonstrated

Each row points at a specific file or function so reviewers can verify the claim, not just take my word for it.

| Skill | Where to look |
|---|---|
| PySpark `Window` functions over `partitionBy + orderBy + rowsBetween` for trailing-window features | [`etl/features.py`](etl/features.py) |
| Conditional aggregation within a window (home/away split) | [`etl/features.py`](etl/features.py) `pts_home_only` / `pts_away_only` |
| Idempotent partitioned Parquet writes via `partitionOverwriteMode=dynamic` | [`etl/transform.py`](etl/transform.py) `get_spark()` |
| Airflow DAG design with lazy imports, XCom path-passing, `max_active_runs=1`, `catchup=False` | [`dags/nba_etl_dag.py`](dags/nba_etl_dag.py) |
| Production staging → canonical promotion pattern | DAG `transform_and_aggregate` → `write_processed` tasks |
| Schema-first ingestion with `StructType` (no `inferSchema=True` on production paths) | [`etl/schema.py`](etl/schema.py), [`etl/ingest.py`](etl/ingest.py) |
| API rate-limit handling (`stats.nba.com`) | [`etl/ingest.py`](etl/ingest.py) `_rate_limit_sleep` |
| Real-data correctness reconciliation | Phase 3 commit message — caught NYK 4-2 vs ATL gap, patched with single-day backfill |
| Docker Compose multi-service stack with single-build-owner pattern (avoids parallel image-export race) | [`infra/docker-compose.yml`](infra/docker-compose.yml) |
| Custom Airflow image extending `apache/airflow` with OpenJDK 17 for PySpark local mode | [`infra/Dockerfile.airflow`](infra/Dockerfile.airflow) |
| Static guard-rail tests for DAG hygiene (no heavy module-level imports) | [`tests/test_dag.py`](tests/test_dag.py) |
| Cross-platform dev (Windows + Linux containers) — bind-mounted code, vendored Hadoop winutils, dual S3/local destination | [`tests/conftest.py`](tests/conftest.py), [`etl/paths.py`](etl/paths.py) |

---

## Architecture

```
nba_api (stats.nba.com)
      │
      ▼
  [ingest.py]  ──→  s3://{BUCKET}/raw/nba/box_scores/season={Y}/game_date={D}/
      │
      ▼
[transform.py] ──→  PySpark aggregations + rolling window features
      │
      ├──→  s3://{BUCKET}/processed/nba/team_game_stats/season={Y}/game_date={D}/
      └──→  s3://{BUCKET}/features/nba/rolling_team_stats/season={Y}/

All steps wired together in:  dags/nba_etl_dag.py  (Airflow DAG, @daily)
```

---

## Quick Start

### Prerequisites
- Python 3.11+
- Docker + Docker Compose (for Airflow)
- Java 11+ (for PySpark local mode)
- AWS account with an S3 bucket — or [LocalStack](https://localstack.cloud/) for fully offline dev

### 1. Clone & configure
```bash
git clone https://github.com/you/nba-parquet.git
cd nba-parquet
cp .env.example .env
# Edit .env: set S3_BUCKET, AWS credentials (or AWS_ENDPOINT_URL for LocalStack)
```

### 2. Install dependencies
```bash
make setup
```

### 3. Run the ETL locally (no Airflow needed)
```bash
make run-local
# Reads NBA_SEASON + NBA_INGEST_DATE from .env, runs full pipeline, writes to S3/LocalStack
```

### 4. Run tests
```bash
make test
# Zero AWS credentials and zero network access required — uses local fixtures
```

### 5. Start Airflow (Docker Compose)
First-time setup:
```bash
cp infra/airflow.env.example infra/airflow.env
# (optional) generate a real Fernet key and paste it into airflow.env:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

make airflow-up        # builds the image, starts postgres + scheduler + webserver
# First boot takes 5-10 minutes (image pull + pip install + db migrate).
# Subsequent boots are seconds.
```
Open http://localhost:8080 and log in with `admin` / `admin`. The
`nba_etl_pipeline` DAG appears paused — toggle it on, then trigger a run
from the UI or via:
```bash
make trigger-dag
```
The DAG runs against the same `./out/` directory the local pipeline
writes to (mounted into the container as `/opt/airflow/out`), so you can
inspect output with:
```bash
.venv/Scripts/python.exe -c "import pandas as pd; print(pd.read_parquet('./out/processed/nba/team_game_stats/'))"
```

Useful commands:
```bash
make airflow-logs      # tail the scheduler + webserver logs
make dag-list          # list any DAG import errors
make airflow-rebuild   # rebuild the image after editing requirements.txt or Dockerfile
make airflow-down      # stop the stack
```

### Daily catch-up during the season

The DAG is `@daily` with `catchup=False`, so if the scheduler is down at trigger
time (laptop off, Docker stopped) it won't auto-fill missed days. To stay
current, run the catch-up helper each morning:

```powershell
.\scripts\catch_up.ps1
```

It auto-detects the latest `game_date` partition under `out/processed/`, brings
up the Airflow stack if needed, and backfills every day from `(latest + 1)` through
yesterday. Idempotent — safe to re-run any time. Override the range explicitly with
`-From 2026-04-18 -To 2026-05-15` for cold starts or wider catch-ups.

---

## Output Schema

**Processed layer** — `team_game_stats` (one row per team per game):
| Column | Type | Description |
|---|---|---|
| season | int | NBA season start year (e.g. 2025 for 2025-26) |
| game_date | date | Date the game was played |
| game_id | string | nba_api game identifier |
| season_type | string | "Regular Season" or "Playoffs" |
| team_abbreviation | string | e.g. "BOS", "LAL" |
| opponent_abbreviation | string | Opponent team abbreviation |
| is_home | boolean | True if team played at home |
| win | boolean | True if team won |
| pts | int | Points scored |
| effective_fg_pct | double | (FGM + 0.5 * 3PM) / FGA |
| true_shooting_pct | double | PTS / (2 * (FGA + 0.44 * FTA)) |
| assist_to_turnover | double | AST / TOV |
| top_scorer | string | Player with most points for the team in this game |

**Feature layer** — `rolling_team_stats` (one row per (team, game), partitioned by `season`):
| Column | Type | Description |
|---|---|---|
| games_in_window | int | Actual lookback size (1–10; smaller for early-season games) |
| rolling_pts | double | Avg points over last 10 games |
| rolling_efg_pct | double | Avg effective FG% over last 10 games |
| rolling_ts_pct | double | Avg true shooting % over last 10 games |
| rolling_ast_to_tov | double | Avg AST/TOV ratio over last 10 games |
| rolling_win_pct | double | Win fraction over last 10 games |
| rolling_pts_home | double | Avg points in home games within the window (NULL if none) |
| rolling_pts_away | double | Avg points in away games within the window (NULL if none) |

---

## Tech Stack

| | |
|---|---|
| **PySpark 3.5** | Distributed batch processing (local mode for dev, EMR-compatible) |
| **Apache Airflow 2.9** | DAG orchestration via Docker Compose |
| **AWS S3** | Parquet storage (Hadoop S3A connector) |
| **nba_api** | Free, official-endpoint NBA stats source |
| **pytest** | Unit tests with local SparkSession |
| **LocalStack** | Optional: fully offline S3 emulation |

---

## How the pipeline behaves under load

Three more views from the validation run, each showing a different part of the architecture working:

**Per-game ETL — scripted local run for the 4/29 playoff slate.** Proves the schema math (eFG%, TS%, AST/TOV, top scorer per team) holds against real `nba_api` data:
![Box-score smoke test](demo%20screenshots/4_29_26_nba_games.png)

**Airflow DAG graph — five tasks green on an autonomous run** triggered by the scheduler (no manual click):
![DAG graph](demo%20screenshots/dag_screenshot.png)

**14-day playoff backfill via `airflow dags backfill 2026-04-19 2026-05-02`** — 70/70 task instances succeeded, mean run duration 1:11. This is what convinced me dynamic partition overwrite was working: each daily run touched only its own `(season, game_date)` partition without clobbering the rest:
![Backfill grid](demo%20screenshots/backfill_success.png)

---

## Project Status

- [x] Phase 1 — Core ETL (ingest → transform → S3 write)
- [x] Phase 2 — Airflow DAG + Docker Compose
- [x] Phase 3 — Feature engineering + rolling windows
- [ ] Phase 4 — Cloud deploy (EC2/EMR) + IAM hardening
- [ ] Phase 5 — Docs, demo, CI/CD

---

## License

MIT — see [LICENSE](LICENSE)
