# Project Q&A — nba-parquet

> A reference doc for me — and anyone reading this repo — that captures how I'd
> describe `nba-parquet` to different audiences. Each question has two answers:
> a **technical** version for an engineer or interviewer, and a **layman**
> version for a recruiter, family member, or anyone non-technical.
>
> If you're skimming, jump to the [TL;DR pitches](#tldr-pitches) at the bottom.

## 1. What is it?

**Technical:** A daily PySpark + Airflow data pipeline that ingests NBA box
scores from `nba_api`, aggregates them into team-game stats and rolling 10-game
features, and writes partitioned Parquet to S3 (or local disk). Five-task DAG,
four data zones (raw → staging → processed → features), 25 unit tests, GitHub
Actions CI, dynamic-partition-overwrite for safe daily backfills.

**Layman:** A program that automatically grabs every NBA game's box score from
the night before, crunches the numbers into team-level performance stats, and
tracks how each team's been trending over their last 10 games. It runs itself
once a day, every day, and keeps a clean historical record I can use to feed
prediction models or just spot trends.

---

## 2. How does it work?

**Technical:** An Airflow DAG runs on `@daily` schedule with `catchup=False`
and `max_active_runs=1`. Five `PythonOperator` tasks —
`ingest_raw → transform_and_aggregate → write_processed → write_features →
notify_done` — pass S3 paths between each other via XCom. Heavy imports
(`pyspark`, `nba_api`, `pandas`) are lazy-loaded inside callables to keep DAG
parse time fast. The transform step uses Spark
`Window.partitionBy("team_id").orderBy("game_date").rowsBetween(-9, 0)` to
compute trailing 10-game features, with conditional aggregation for home/away
split. Writes are partitioned by `(season, game_date)` with
`partitionOverwriteMode=dynamic` so daily runs only touch the day's partition.
The Airflow stack runs on Docker Compose with `LocalExecutor` and a Postgres
metadata DB — no Celery or Redis.

**Layman:** Every day, the pipeline:

1. **Asks the NBA's official stats site** for yesterday's games and pulls down
   the player-level box scores.
2. **Cleans and aggregates** the raw player data into one row per team per game
   with metrics like points, true-shooting percentage, and assist-to-turnover
   ratio.
3. **Computes rolling averages** over each team's last 10 games — so I can see
   who's been hot or cold lately, not just who won last night.
4. **Saves it all to disk** in a structured format that other programs (like a
   prediction model) can read efficiently.
5. **Logs whether it succeeded.** If something breaks, I get a clear error
   instead of bad data quietly piling up.

The orchestration is handled by Airflow, which is essentially a "smart
scheduler" — it knows what order the steps need to run in, retries failed
steps, and shows me a graph view of every run so I can spot problems at a
glance.

---

## 3. Why does it exist?

**Technical:** Sports prediction models (survivor pools, spreads, totals) need
clean, time-windowed, model-ready features. Most sports-analytics work lives in
ad-hoc pandas notebooks that break when you scale up or try to share. This
project is the opposite: schema-typed, idempotent, partition-aware,
daily-orchestrated, retry-safe. It also exists as a portfolio piece to
demonstrate three skills together — distributed compute (PySpark), workflow
orchestration (Airflow), and cloud-style storage patterns (partitioned Parquet
on S3) — that are hard to fake without an actual working artifact.

**Layman:** Two reasons:

1. **It's a foundation for prediction models.** I want to build models that
   predict NBA spreads, totals, or survivor-pool picks. Those models need
   clean, consistent input data — exactly the kind of historical signal this
   pipeline produces. Without something like this, you end up writing
   throwaway scripts every time and the data is never quite the same shape
   twice.
2. **It proves I can do this kind of work.** A lot of data-engineering job
   postings ask for experience with Spark, Airflow, and AWS S3. Talking about
   those is one thing; pointing at a working repo with a green CI badge, a
   real architecture diagram, and 16+ days of validated NBA data flowing
   through it is a much stronger answer.

---

## 4. Specific uses

**Technical:**

- Feature store for downstream prediction models (spread, total, survivor)
- Reproducible historical research — eFG%, TS%, AST/TOV, win streaks,
  home/away splits, all computable for any (team, game) tuple
- A reference template for adding new sports / data sources following the same
  ingest → transform → features pattern (the layered S3 layout and DAG shape
  generalize cleanly)
- Demo for technical interviews — concrete file references for window
  functions, idempotent writes, lazy DAG imports, schema-first ingestion

**Layman:**

- **Predicting outcomes.** The rolling features are exactly what a betting /
  fantasy / survivor-pool model wants as input.
- **Trend analysis.** Want to know which playoff teams have shot above 60%
  true-shooting over their last 10 games? Two lines of pandas against the
  features layer answer that.
- **A reusable blueprint.** Once the shape works for NBA, I can clone the
  structure for any other "ingest data → transform → time-windowed features"
  use case — the actual code that pulls NBA box scores is small; most of the
  value is in the architecture around it.
- **Portfolio proof.** I can hand someone the GitHub link and they can verify
  it works in 60 seconds without setting anything up.

---

## 5. Transferability

**Technical:** The architecture is sport-agnostic and even domain-agnostic. To
repoint it at a different data source, you swap three things:

1. **The ingestion module** (`etl/ingest.py`) — replace the `nba_api` calls
   with whatever API or file source you have. The 0.6s rate-limit pattern, the
   schema-first normalization, and the float→int hardening generalize directly.
2. **The schemas** (`etl/schema.py`) — define `RAW_*_SCHEMA` and
   `PROCESSED_*_SCHEMA` for the new domain.
3. **The transform / feature logic** (`etl/transform.py`, `etl/features.py`) —
   the `aggregate_team_game` and `build_rolling_features` functions are pure
   DataFrame transforms; replace the math with whatever's appropriate.

The DAG shape, partition layout, dynamic-overwrite write pattern,
staging-then-promote idiom, lazy-import discipline, and CI workflow are all
completely reusable. Concrete examples this could become tomorrow with ~1 day
of work each: NFL play-by-play (`nfl_data_py`), MLB Statcast (`pybaseball`),
NHL play-by-play, soccer match data, fantasy-football projections — anything
where data lands in periodic batches and you want trailing-window signals out
of it. **Not limited to sports**: same architecture applies to e-commerce
daily sales aggregations, ad-platform spend rollups, IoT sensor batches, or
financial market end-of-day snapshots.

**Layman:** Almost any "data shows up daily, I want to clean it up and look at
trends" problem can use this exact same shape. The NBA part is the example
data — the actual reusable thing is the *pattern* of: pull → clean →
aggregate → feature-engineer → store, orchestrated automatically every day.
Concrete tomorrow-projects:

- **NFL or college football play-by-play** — same pipeline, different
  ingestion source. Probably one full day of work to repoint.
- **Stock market end-of-day data** — daily price + volume, rolling momentum /
  volatility features. Identical pattern.
- **E-commerce sales** — daily order data, rolling 7/30/90-day customer cohort
  metrics.
- **Sensor telemetry** — IoT devices that report every hour, you want
  trailing-window anomaly detection.

The point is: the *valuable* part of this project isn't "I learned how to call
the NBA API." It's the operational infrastructure around the data — schema
enforcement, idempotent writes, retryable orchestration, automated testing,
observable failures. That part transfers to any domain.

---

## 6. Tools used and what they're good for

| Tool | What it is | What it's good for | Why I picked it for this project |
|---|---|---|---|
| **PySpark 3.5** | Python interface to Apache Spark — a distributed data-processing engine | Crunching large amounts of structured data in parallel; expressing complex transformations (joins, window functions, aggregations) in code that scales from one laptop to a cluster | Window functions are the core of rolling features. Spark also gives me proper schema enforcement, partitioned Parquet writes, and the option to scale to a cluster if data grows |
| **Apache Airflow 2.9** | A workflow orchestrator — "Cron with a brain" | Running a series of dependent tasks on a schedule, retrying failures, passing data between steps, visualizing success/failure history | Daily pipelines need a scheduler. Airflow's the industry standard, has a great UI, and the DAG-as-code pattern is auditable in git. I'm using `LocalExecutor` to keep the stack simple — same orchestration semantics without Celery / Redis overhead |
| **Docker Compose** | A way to run multiple containerized services together on one machine | Bundling Airflow + Postgres + a custom Python image into one `up` command; reproducible local environments without polluting the host | Lets anyone clone the repo and have Airflow running in 5 minutes. The custom Dockerfile bakes Java 17 + PySpark + nba_api into the image so containers are self-contained |
| **Postgres** | Relational database | Airflow's metadata store — DAG runs, task instances, XCom values, connection configs | Required by Airflow's `LocalExecutor`. I'm not using it for application data — strictly metadata |
| **AWS S3 + Parquet** | Object storage + columnar file format | Cheap durable storage; Parquet is column-oriented so queries that touch only some columns read only those bytes; partitioning lets you skip whole subtrees at read time | Industry-standard pattern for analytics workloads. The S3A connector lets Spark read / write S3 paths just like local files |
| **`nba_api`** | A Python wrapper around stats.nba.com's official endpoints | Free, well-maintained access to box scores, play-by-play, schedule data, etc. | Cheaper than buying a sports-data API; data quality is good for our use case (completed games) |
| **pytest** | Python testing framework | Unit + integration testing with fixtures, parametrization, and clear failure output | The 25-test suite proves correctness on a real `SparkSession` against bundled fixtures — no AWS, no network. Catches regressions before they reach prod |
| **ruff + black** | Python linter + formatter | Automatic code-style enforcement; ruff catches bugs (unused imports, undefined names) and black keeps formatting consistent without bikeshedding | Both run in CI on every push, so anything that lands on `main` is automatically lint-clean |
| **GitHub Actions** | CI/CD platform built into GitHub | Running tests, builds, and deployments on every push / PR; gives a green / red badge proving the test suite passes | Two-job workflow: lint+tests + Docker image build verify. Without CI, "tests pass" means "tests pass on my machine"; with CI, anyone can verify |
| **Mermaid** | Text-based diagramming syntax | Architecture diagrams that live next to your code in markdown; renders natively on GitHub | Architecture diagram in the README updates with the repo — no static images that drift out of date |

---

## TL;DR pitches

### 30-second elevator pitch

> "I built a daily data pipeline that pulls NBA box scores from the league's
> official API, cleans them up with PySpark, and computes rolling 10-game
> features for each team. The whole thing runs automatically through Airflow
> and saves the results to S3-style partitioned Parquet. It's the foundation
> I'd feed into a betting or survivor-pool prediction model — and it's also
> a portfolio piece that demonstrates Spark, Airflow, and AWS storage
> patterns working together on real data."

### 2-minute deeper pitch (for a phone screen)

> "Architecture is four data zones — raw, staging, processed, features —
> with a five-task Airflow DAG moving data between them on a daily schedule.
> The transform step uses Spark window functions partitioned by team and
> ordered by game date to compute trailing 10-game averages of true-shooting
> percentage, assist-to-turnover ratio, win rate, and a home / away split.
> Writes use dynamic partition overwrite so daily backfills are idempotent.
> The whole stack runs locally in Docker Compose with LocalExecutor — no
> Celery, no Redis — and the test suite has 25 unit tests covering schema
> math, partitioning, and DAG-import hygiene. CI on every push.
>
> The interesting part of this project isn't the NBA-specific code, which is
> small. It's the operational infrastructure around the data — schema
> enforcement, retry-safe writes, observable failures. The same architecture
> would repoint at NFL play-by-play or stock market data with about a day of
> work."

### 30-minute technical deep-dive

Reach for the [README.md](../README.md) — Skills Demonstrated table, Mermaid
architecture diagram, Results & Metrics section. Then walk through:

- [`dags/nba_etl_dag.py`](../dags/nba_etl_dag.py) — DAG structure, lazy imports,
  XCom path-passing, staging-then-promote pattern
- [`etl/features.py`](../etl/features.py) — window function for rolling
  features, conditional aggregation for home / away split
- [`etl/transform.py`](../etl/transform.py) — `get_spark()` factory with
  dynamic partition overwrite, S3A config gated on non-local mode
- [`etl/ingest.py`](../etl/ingest.py) — rate-limit pattern, schema-first
  normalization, the `TO`→`tov` regression fix
- [`infra/docker-compose.yml`](../infra/docker-compose.yml) — single-build-owner
  pattern, bind-mounted code, healthchecks
- [`tests/test_features.py`](../tests/test_features.py) — hand-computed expected
  values against a 12-game fixture sequence
- [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) — two-job CI: lint +
  tests, plus Docker image build verify

---

## How to use this doc

- **Going into an interview** → re-read sections 1, 2, 3, and the 2-minute
  pitch. Have the README open in another tab so you can pull up the diagram
  and Skills table on demand.
- **Explaining to non-technical friends / family** → use the layman halves of
  each question and the 30-second pitch. Skip section 6.
- **Helping someone clone this for another sport** → section 5 is the
  blueprint; pair it with the README architecture diagram.
- **Future you, six months from now, trying to remember why** → sections 2 and
  3 are the load-bearing ones. The "why we picked it" column in section 6 is
  the second-most-load-bearing.

This document is meant to be re-read whenever the project comes up in
conversation. The technical landscape will evolve; the *shape* of how to
explain a real data platform shouldn't.
