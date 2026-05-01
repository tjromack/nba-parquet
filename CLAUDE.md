# CLAUDE.md — AI Coding Assistant Source of Truth

## Project Purpose
A production-style PySpark ETL pipeline that ingests NBA data via `nba_api` (box scores + play-by-play for completed games), runs meaningful aggregations and feature engineering, and writes Parquet output to S3 — orchestrated end-to-end with an Apache Airflow DAG. The goal is a portfolio-grade repo that demonstrates Spark, Airflow, and AWS (S3, IAM) working together in a plausible sports-analytics context, with **fresh in-season data** flowing through the pipeline (built during the 2025–26 NBA playoffs).

## Tech Stack & Rationale
| Layer | Choice | Why |
|---|---|---|
| Batch processing | PySpark 3.5 (local mode + EMR-compatible) | Demonstrates distributed compute; local mode keeps dev cost = $0 |
| Orchestration | Apache Airflow 2.9 (Docker Compose) | Industry standard; DAG-as-code is auditable |
| Storage output | AWS S3 (via `boto3` + Hadoop S3A) | Real destination; can swap to LocalStack for offline dev |
| Data source | `nba_api` (primary) | Free, well-maintained, official stats.nba.com endpoints |
| Language | Python 3.11 | Airflow operators + PySpark both Python-native |
| Infra local | Docker Compose | Airflow + Postgres metastore in one command |
| Infra cloud | AWS IAM role (instance profile or env vars) | Avoids hardcoded secrets |
| Testing | `pytest` + `pyspark` local SparkSession | Unit-testable without a cluster |
| Formatting | `black`, `ruff` | Enforced in CI |

## Project Structure
```
nba-parquet/
├── dags/
│   └── nba_etl_dag.py          # Airflow DAG definition
├── etl/
│   ├── __init__.py
│   ├── ingest.py               # Pull raw data via nba_api, write to raw/ on S3
│   ├── transform.py            # PySpark aggregations + feature engineering
│   ├── write.py                # Write Parquet to S3 processed/ prefix
│   └── schema.py               # StructType schemas for raw + output DataFrames
├── tests/
│   ├── conftest.py             # Shared SparkSession fixture
│   ├── test_ingest.py
│   ├── test_transform.py
│   └── test_write.py
├── infra/
│   ├── docker-compose.yml      # Airflow webserver + scheduler + postgres + redis
│   └── airflow.env             # Non-secret Airflow config (gitignored secrets)
├── scripts/
│   ├── bootstrap.sh            # One-shot env setup (pip install, spark check)
│   └── run_local.py            # Run the full ETL locally without Airflow
├── .env.example                # Template for required env vars
├── requirements.txt            # Python deps for ETL code
├── requirements-dev.txt        # pytest, black, ruff
├── pyproject.toml              # black + ruff config
├── Makefile                    # Shortcut targets
├── README.md
├── TODO.md
├── CLAUDE.md
└── PROMPT.md
```

## Data Model

### Raw Layer (`s3://{BUCKET}/raw/nba/box_scores/season={YEAR}/game_date={YYYY-MM-DD}/`)
Unmodified Parquet dump from `nba_api.stats.endpoints.LeagueGameLog` + `BoxScoreTraditionalV2` for each game. Key columns:
- `game_id`, `game_date`, `season`, `season_type` (Regular Season / Playoffs)
- `team_id`, `team_abbreviation`, `team_city`, `matchup`, `wl` (W/L)
- `player_id`, `player_name`, `start_position`, `min` (minutes)
- `pts`, `reb`, `ast`, `stl`, `blk`, `tov`, `pf`
- `fgm`, `fga`, `fg3m`, `fg3a`, `ftm`, `fta`, `plus_minus`

### Processed Layer (`s3://{BUCKET}/processed/nba/team_game_stats/season={YEAR}/game_date={YYYY-MM-DD}/`)
Parquet output partitioned by season + game_date. One row per (team, game). Schema:
```
root
 |-- season: integer
 |-- game_date: date
 |-- game_id: string
 |-- season_type: string
 |-- team_id: integer
 |-- team_abbreviation: string
 |-- opponent_abbreviation: string
 |-- is_home: boolean
 |-- win: boolean
 |-- pts: integer
 |-- reb: integer
 |-- ast: integer
 |-- tov: integer
 |-- fg_pct: double
 |-- fg3_pct: double
 |-- ft_pct: double
 |-- effective_fg_pct: double
 |-- true_shooting_pct: double
 |-- assist_to_turnover: double
 |-- top_scorer: string
 |-- top_rebounder: string
 |-- top_playmaker: string
```

### Feature Layer (`s3://{BUCKET}/features/nba/rolling_team_stats/season={YEAR}/`)
Rolling 10-game window aggregations per team — designed to feed a downstream prediction model:
- `rolling_pts`, `rolling_efg_pct`, `rolling_ts_pct`, `rolling_ast_to_tov`, `rolling_win_pct`

## Key Conventions

### PySpark
- Always create `SparkSession` via `etl/transform.py::get_spark()` — never inline.
- Use `StructType` schemas from `etl/schema.py` when reading raw CSVs/JSON; never infer schema in production paths.
- Partition output by `(season, game_date)` — do NOT use `repartition(1)` (we want real-world file layout).
- Use `spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol` for S3 writes.
- All transformations must be pure functions: `transform(df: DataFrame, spark: SparkSession) -> DataFrame`.
- No `.collect()` inside transform functions — only allowed in tests or final write step.

### Airflow
- DAG id: `nba_etl_pipeline`. Schedule: `@daily` (NBA games happen most nights of the season).
- Tasks: `ingest_raw` → `transform_and_aggregate` → `write_processed` → `write_features` → `notify_done`.
- Use `PythonOperator` wrapping `etl/` module functions. No inline logic in the DAG file.
- Pass S3 paths between tasks via `XCom` (return path strings from operators).
- All connections stored as Airflow Connections, not env vars read inside tasks.
- `catchup=False` by default.
- Each run pulls games from the previous calendar day (via `{{ ds }}` macro) — incremental, not full refresh.

### nba_api
- Respect rate limits: stats.nba.com throttles aggressive callers. Sleep 0.6s between calls in `ingest.py`.
- Always pass an explicit `timeout=` to endpoints (default 30s).
- Treat `nba_api` calls as the only network boundary — never call it from `transform.py` or `write.py`.

### AWS / S3
- Bucket name from env var `S3_BUCKET`. Never hardcode.
- Prefix structure: `raw/`, `processed/`, `features/` — enforced by `write.py`.
- IAM: read via instance profile in prod; use `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` locally (never commit).
- LocalStack endpoint override: if `AWS_ENDPOINT_URL` is set, use it (enables offline testing).

### Testing
- All tests use a shared `SparkSession` with `master=local[2]`, created once per session via `conftest.py`.
- Tests read from `tests/fixtures/` (small Parquet/CSV/JSON snapshots), never from real S3 or stats.nba.com.
- `pytest -m "not integration"` must pass with zero AWS credentials and zero network access.

## Scripts
```bash
make setup          # pip install -r requirements.txt -r requirements-dev.txt
make airflow-up     # docker compose -f infra/docker-compose.yml up -d
make airflow-down   # docker compose -f infra/docker-compose.yml down
make test           # pytest tests/ -m "not integration" -v
make lint           # ruff check . && black --check .
make format         # black . && ruff check --fix .
make run-local      # python scripts/run_local.py (full ETL, local Spark, real or LocalStack S3)
make trigger-dag    # airflow dags trigger nba_etl_pipeline (requires airflow-up)
```

## Environment Variables
```
# .env.example
S3_BUCKET=your-nba-etl-bucket
AWS_ACCESS_KEY_ID=...          # local dev only; use IAM role in prod
AWS_SECRET_ACCESS_KEY=...      # local dev only
AWS_DEFAULT_REGION=us-east-1
AWS_ENDPOINT_URL=              # set to http://localhost:4566 for LocalStack
NBA_SEASON=2025-26             # nba_api season string format (e.g. "2025-26")
NBA_INGEST_DATE=               # optional YYYY-MM-DD; if empty, defaults to yesterday
NBA_SEASON_TYPE=Playoffs       # "Regular Season" or "Playoffs"
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
```

## What NOT To Do
- **Do NOT** hardcode `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY` anywhere in source files.
- **Do NOT** use `spark.read.csv(..., inferSchema=True)` or `spark.read.json(..., inferSchema=True)` on production paths — always use `schema.py`.
- **Do NOT** call `.collect()` or `.toPandas()` inside transformation functions — only at write time or in tests.
- **Do NOT** put business logic inside the Airflow DAG file — it must stay thin (import + call only).
- **Do NOT** use `repartition(1)` — this defeats the purpose of demonstrating Spark output.
- **Do NOT** skip `catchup=False` — we don't want Airflow backfilling the entire season on first deploy.
- **Do NOT** write raw data to `processed/` prefix or vice versa — the prefix contract is strict.
- **Do NOT** add Jupyter notebooks to the repo root — keep analysis in a separate `notebooks/` dir if needed, excluded from linting.
- **Do NOT** import `pyspark` at the top level of DAG files — Airflow workers may not have PySpark installed; use `PythonOperator` with lazy imports.
- **Do NOT** call `nba_api` from inside Spark UDFs or `transform.py` — all network I/O happens in `ingest.py` only.
- **Do NOT** hammer stats.nba.com without a sleep between calls — you will get rate-limited and IP-blocked.
