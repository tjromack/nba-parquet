# nba-parquet

> **Portfolio-grade PySpark ETL pipeline — NBA box scores → S3 Parquet, orchestrated by Airflow.**

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)
![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apacheairflow)
![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazons3)
![License](https://img.shields.io/badge/license-MIT-green)

---

## What It Is

A production-style batch ETL pipeline that:
1. **Ingests** NBA box scores via [`nba_api`](https://github.com/swar/nba_api) — pulling completed games for a given date
2. **Transforms** them with PySpark — engineering team-level per-game stats (eFG%, true shooting, assist:turnover) and rolling 10-game windowed features for downstream prediction models
3. **Writes** Parquet output to AWS S3 in a partitioned layout (`season/game_date`)
4. **Orchestrates** all of it with an Apache Airflow 2.9 DAG that runs on a daily schedule, picking up the prior day's games

Built during the 2025–26 NBA playoffs so the pipeline is exercised against **fresh in-season data** every night.

## Why It Exists

The developer is building sports-analytics tooling and prediction models that rely on aggregated, well-shaped game-level signal. This repo is the data-platform foundation underneath that work: instead of ad-hoc pandas scripts, it's a proper batch pipeline a real analytics team would ship.

It demonstrates three skills that are hard to fake in a portfolio:
- **Spark** (real transformations on multi-thousand-row box-score data per night, scalable to full-season volumes)
- **Airflow** (DAG-as-code with task dependencies, XCom, and environment-driven config)
- **AWS** (S3 writes via Hadoop S3A connector, IAM-safe credential handling)

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

**Feature layer** — `rolling_team_stats`: 10-game rolling averages of the above, designed to plug directly into spread / total / win-probability prediction models.

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

## Demo / Screenshot

> _Airflow DAG graph and S3 output tree screenshots — coming after Phase 2_
>
> ![DAG screenshot placeholder](docs/dag_screenshot.png)

---

## Project Status

- [x] Phase 1 — Core ETL (ingest → transform → S3 write)
- [x] Phase 2 — Airflow DAG + Docker Compose
- [ ] Phase 3 — Feature engineering + rolling windows
- [ ] Phase 4 — Cloud deploy (EC2/EMR) + IAM hardening
- [ ] Phase 5 — Docs, demo, CI/CD

---

## License

MIT — see [LICENSE](LICENSE)
