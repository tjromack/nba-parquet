# PROMPT.md — Claude Code Kickoff: Phase 1

You are building **Phase 1** of `nba-parquet`, a production-style PySpark ETL pipeline on NBA box-score data with S3 output.

**Before writing any code**, read these three files in full:
1. `CLAUDE.md` — architecture, conventions, guardrails
2. `README.md` — what this project is and why
3. `TODO.md` — find Phase 1 and treat it as your exact task list

Do not proceed until you have read all three.

---

## Your Mission: Build Phase 1

Build the core ETL pipeline so that `python scripts/run_local.py` ingests NBA box scores via `nba_api`, transforms them with PySpark, and writes Parquet to S3 (or LocalStack). `make test` must pass with zero AWS credentials and zero network access.

Build in this exact order:

### Step 1 — Scaffold the repo
- Create every directory and file in the project structure tree from `CLAUDE.md`
- Create `Makefile` with all targets listed in `CLAUDE.md` (`setup`, `test`, `lint`, `format`, `run-local`, `airflow-up`, `airflow-down`, `trigger-dag`)
- Create `pyproject.toml` with `[tool.black]` (line-length=88) and `[tool.ruff]` (target-version="py311", select=["E","F","I"])
- Create `.env.example` with all vars from `CLAUDE.md`
- Create `requirements.txt`: `pyspark==3.5.*`, `nba_api`, `boto3`, `python-dotenv`, `pandas`
- Create `requirements-dev.txt`: `pytest`, `pytest-mock`, `black`, `ruff`

### Step 2 — Schema definitions
- Create `etl/schema.py` with two `StructType` objects:
  - `RAW_BOX_SCORE_SCHEMA`: covers `game_id`, `game_date`, `season`, `season_type`, `team_id`, `team_abbreviation`, `matchup`, `wl`, `player_id`, `player_name`, `start_position`, `min`, `pts`, `reb`, `ast`, `stl`, `blk`, `tov`, `pf`, `fgm`, `fga`, `fg3m`, `fg3a`, `ftm`, `fta`, `plus_minus` — all correct Spark types
  - `PROCESSED_SCHEMA`: matches the processed layer schema in `CLAUDE.md` (one row per team per game)

### Step 3 — Ingest module
- Create `etl/ingest.py`:
  - `ingest_box_scores(season: str, game_date: str, season_type: str, s3_bucket: str, spark: SparkSession) -> str`
  - Use `nba_api.stats.endpoints.LeagueGameLog(season=season, season_type_all_star=season_type, date_from_nullable=game_date, date_to_nullable=game_date)` to list game IDs for that date
  - For each game id, call `BoxScoreTraditionalV2(game_id=...)` — sleep `0.6s` between calls — and collect player rows
  - Convert combined pandas frame to Spark DataFrame using `RAW_BOX_SCORE_SCHEMA`
  - Write Parquet to `s3://{s3_bucket}/raw/nba/box_scores/season={season_year}/game_date={game_date}/`, return the S3 path
  - Handle `AWS_ENDPOINT_URL` env var to support LocalStack (pass as `fs.s3a.endpoint` Hadoop config)
  - Never hardcode bucket name or credentials

### Step 4 — Transform module
- Create `etl/transform.py`:
  - `get_spark(app_name: str = "nba-etl") -> SparkSession` — configures S3A connector, reads AWS credentials and endpoint from env vars, sets `PathOutputCommitProtocol`
  - `aggregate_team_game(df: DataFrame) -> DataFrame` — produces the processed layer schema:
    - group by `(season, game_date, game_id, season_type, team_id, team_abbreviation)`
    - sums for `pts`, `reb`, `ast`, `tov`, `fgm`, `fga`, `fg3m`, `fg3a`, `ftm`, `fta`
    - derive `effective_fg_pct = (fgm + 0.5 * fg3m) / fga`
    - derive `true_shooting_pct = pts / (2 * (fga + 0.44 * fta))`
    - derive `assist_to_turnover = ast / nullif(tov, 0)`
    - derive `is_home` from the `matchup` column ("vs." = home, "@" = away)
    - derive `opponent_abbreviation` by parsing `matchup`
    - derive `win` from `wl` column ("W" → True, "L" → False)
  - `get_top_player(df: DataFrame, stat_col: str, group_cols: list[str], out_col: str) -> DataFrame`:
    - uses `Window.partitionBy(*group_cols).orderBy(col(stat_col).desc())` to rank players within each group
    - returns one row per group with the top player's name in `out_col`
  - Join `top_scorer` (max `pts`), `top_rebounder` (max `reb`), `top_playmaker` (max `ast`) onto the aggregated result
  - All functions are pure: `(df, ...) -> df`, no side effects

### Step 5 — Write module
- Create `etl/write.py`:
  - `write_processed(df: DataFrame, s3_bucket: str) -> str` — writes Parquet partitioned by `(season, game_date)` to `s3://{s3_bucket}/processed/nba/team_game_stats/`, returns path
  - Do NOT use `repartition(1)`
  - Do NOT write to `raw/` prefix from this function

### Step 6 — Local runner script
- Create `scripts/run_local.py`:
  - Load `.env` with `python-dotenv`
  - Read `NBA_SEASON`, `NBA_INGEST_DATE` (default to yesterday if empty), `NBA_SEASON_TYPE`, `S3_BUCKET` from env
  - Call `get_spark()` → `ingest_box_scores()` → read raw back → `aggregate_team_game()` → join top players → `write_processed()`
  - Print: rows processed, S3 path written, elapsed time
  - Catch and log exceptions without swallowing them

### Step 7 — Test fixtures
- Create `tests/fixtures/sample_box_scores.csv`: ≤200 rows of realistic-looking player-level box-score rows covering at least 4 teams across 2 game dates, with enough variance to test scoring leaders, rebound leaders, eFG/TS math, and turnover handling
- Include at least one game per date with a clear top scorer (>25 pts), a player with 0 attempts (eFG edge case), and at least one game with a turnover-heavy team

### Step 8 — Tests
- Create `tests/conftest.py`: `SparkSession` fixture scoped to session, `local[2]`, no S3 config
- Create `tests/test_ingest.py`:
  - mock `nba_api.stats.endpoints.LeagueGameLog` and `BoxScoreTraditionalV2` with canned pandas frames
  - assert returned Spark DataFrame has correct columns and non-zero row count
  - assert `time.sleep` was called between game pulls (rate-limit guardrail)
  - assert S3 write is called with correct prefix (mock `boto3` or test with temp local path)
- Create `tests/test_transform.py`:
  - Load `sample_box_scores.csv` as Spark DataFrame
  - Call `aggregate_team_game`, assert output schema matches `PROCESSED_SCHEMA`
  - Assert `effective_fg_pct` and `true_shooting_pct` math is correct for a hand-computed row
  - Assert `assist_to_turnover` is null (not Inf/NaN) when `tov = 0`
  - Assert `top_scorer` is non-null for every (team, game) row
  - Assert `is_home` and `opponent_abbreviation` are parsed correctly from `matchup`
- Create `tests/test_write.py`: write fixture DataFrame to `tmp_path`, assert partition dirs exist (`season=*/game_date=*`), assert at least one `.parquet` file per partition

### Step 9 — Lint and verify
- Run `make format` (fix all black/ruff issues)
- Run `make lint` — must exit 0
- Run `make test` — must exit 0 with no AWS credentials and no network access
- Fix any failures before reporting done

---

## Guardrails (read before every file you write)
- Never hardcode `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or any bucket name
- Never call `.collect()` inside `transform.py` functions
- Never use `inferSchema=True` on production read paths
- Never use `repartition(1)` in write functions
- Never put business logic inside `dags/nba_etl_dag.py` (DAG file is Phase 2, but don't pre-pollute it)
- Never import `pyspark` at module level in DAG files
- Never call `nba_api` from inside `transform.py`, `write.py`, or any Spark UDF — network I/O lives only in `ingest.py`
- Always sleep ≥0.6s between consecutive `nba_api` endpoint calls

---

## When Phase 1 Is Complete

Stop and tell me:
1. **What was built** — list every file created with a one-line description
2. **How to run it locally** — exact commands from a clean clone (assume `.env` is filled in)
3. **Test results** — paste the `pytest` summary line
4. **Any assumptions made** — especially around `nba_api` return frame columns, season/date format quirks, or S3A JAR availability
5. **What Phase 2 will need** — anything Phase 1 exposed that I should know before building the Airflow DAG (e.g. how `{{ ds }}` should be threaded through, rate-limit behavior under daily scheduled runs)
