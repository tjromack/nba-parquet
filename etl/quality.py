"""Data-quality assertions for the features layer.

The rolling features are the pipeline's contract with everything
downstream -- the model, the dashboard, any future consumer. A silent
defect here (a duplicated team-game, a partition that half-wrote, a
column that went all-null when an upstream endpoint changed shape) does
not crash anything. It quietly degrades every number computed from it,
and the first symptom is a model metric moving for reasons nobody can
explain.

So the features write is gated. ``check_feature_quality`` runs a fixed
set of assertions and returns a report; ``raise_for_status`` turns any
violation into a failed task, which in Airflow means a visible red run
rather than a successful one that published bad data.

Three families of check, matching the ways this layer has actually been
wrong or could plausibly go wrong:

1. **Grain.** Exactly one row per (game_id, team_id). A duplicate here
   double-counts a team in every rolling average downstream.
2. **Window completeness.** ``games_in_window`` must be populated and
   within ``[1, window]``. A zero or a null means the window function
   produced a row with no history behind it; a value above ``window``
   means the frame bounds are wrong.
3. **Null rates and ranges.** Some columns are legitimately nullable and
   some are not, and the difference is the whole point -- see
   ``NULLABLE_COLUMNS``. Ranges catch arithmetic going wrong: a
   percentage outside [0, 1], non-positive points.

Row-count reconciliation against the processed layer runs when the
caller can supply that frame, which the DAG can and a bare write cannot.

One ``.collect()`` happens here, on a single aggregated row. That is the
sanctioned pattern: this runs at write time, not inside a transform, and
it pulls back one row of counts rather than any data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Columns that must never be null in a well-formed features row. Every
# team-game has points, shooting and a win/loss, so every rolling average
# over a non-empty window has a value.
NON_NULL_COLUMNS: tuple[str, ...] = (
    "season",
    "game_date",
    "game_id",
    "team_id",
    "team_abbreviation",
    "games_in_window",
    "rolling_pts",
    "rolling_efg_pct",
    "rolling_ts_pct",
    "rolling_win_pct",
)

# Columns that CAN be null, each for a stated reason. Listing them
# explicitly is what makes the non-null set above meaningful -- without
# this, "some nulls are fine" degrades into "nulls are never checked".
NULLABLE_COLUMNS: dict[str, str] = {
    "rolling_ast_to_tov": "undefined when a team had zero turnovers in the window",
    "rolling_pts_home": "null until the team has played a home game in-window",
    "rolling_pts_away": "null until the team has played an away game in-window",
    "rolling_ortg": "null for partitions ingested before the advanced-stats phase",
    "rolling_drtg": "null for partitions ingested before the advanced-stats phase",
    "rolling_net_rtg": "null for partitions ingested before the advanced-stats phase",
    "rolling_pace": "null for partitions ingested before the advanced-stats phase",
}

# (column, lower, upper) -- inclusive bounds checked on non-null values.
RANGE_CHECKS: tuple[tuple[str, float, float], ...] = (
    ("rolling_win_pct", 0.0, 1.0),
    ("rolling_efg_pct", 0.0, 1.5),
    ("rolling_ts_pct", 0.0, 1.5),
    ("rolling_pts", 1.0, 200.0),
)


class FeatureQualityError(ValueError):
    """Raised when the features layer violates its own contract."""


@dataclass
class QualityReport:
    """Outcome of a features-layer quality run.

    ``metrics`` is populated even on a pass, so a green run still leaves
    the numbers in the Airflow log -- "0 violations" is less useful six
    months later than "2,630 rows, 30 teams, 0.0% null on core columns".
    """

    rows: int
    violations: list[str] = field(default_factory=list)
    metrics: dict[str, object] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.violations

    def raise_for_status(self) -> QualityReport:
        """Raise if anything failed; return self so calls can chain."""
        if self.violations:
            detail = "\n  - ".join(self.violations)
            raise FeatureQualityError(
                f"features layer failed {len(self.violations)} quality "
                f"check(s):\n  - {detail}"
            )
        return self

    def summary(self) -> str:
        state = "PASS" if self.ok else f"FAIL ({len(self.violations)})"
        body = ", ".join(f"{k}={v}" for k, v in self.metrics.items())
        return f"[features quality {state}] {body}"


def check_feature_quality(
    features_df: DataFrame,
    *,
    window: int = 10,
    processed_df: DataFrame | None = None,
) -> QualityReport:
    """Assert the features layer's contract. Returns a report.

    ``processed_df`` enables row-count reconciliation: the features layer
    is one row per (team, game), same grain as processed, so the counts
    must match exactly. A mismatch means the rolling build dropped or
    duplicated rows.
    """
    columns = set(features_df.columns)

    # One aggregation pass for everything expressible as an aggregate, so
    # the whole check costs a single scan.
    aggs = [
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("team_id").alias("distinct_teams"),
        F.countDistinct("game_id").alias("distinct_games"),
        F.countDistinct("season").alias("distinct_seasons"),
        F.countDistinct(F.concat_ws("|", F.col("game_id"), F.col("team_id"))).alias(
            "distinct_grain"
        ),
    ]
    for col in (*NON_NULL_COLUMNS, *NULLABLE_COLUMNS):
        if col in columns:
            aggs.append(F.sum(F.col(col).isNull().cast("long")).alias(f"null__{col}"))
    if "games_in_window" in columns:
        giw = F.col("games_in_window")
        aggs.append(
            F.sum(((giw < 1) | (giw > window)).cast("long")).alias("giw_out_of_range")
        )
        aggs.append(F.max(giw).alias("giw_max"))
    for col, lo, hi in RANGE_CHECKS:
        if col in columns:
            c = F.col(col)
            aggs.append(
                F.sum((c.isNotNull() & ((c < lo) | (c > hi))).cast("long")).alias(
                    f"range__{col}"
                )
            )

    row = features_df.agg(*aggs).collect()[0].asDict()

    rows = int(row["rows"])
    violations: list[str] = []
    metrics: dict[str, object] = {"rows": rows}

    if rows == 0:
        violations.append("features layer is empty")
        return QualityReport(rows=rows, violations=violations, metrics=metrics)

    metrics["teams"] = int(row["distinct_teams"])
    metrics["games"] = int(row["distinct_games"])
    metrics["seasons"] = int(row["distinct_seasons"])

    # 1. Grain: one row per (game_id, team_id).
    distinct_grain = int(row["distinct_grain"])
    if distinct_grain != rows:
        violations.append(
            f"grain is not one row per (game_id, team_id): {rows} rows but "
            f"{distinct_grain} distinct pairs ({rows - distinct_grain} duplicated)"
        )

    # 2. Window completeness.
    if "games_in_window" in columns:
        metrics["max_games_in_window"] = int(row["giw_max"])
        out_of_range = int(row["giw_out_of_range"])
        if out_of_range:
            violations.append(
                f"games_in_window outside [1, {window}] on {out_of_range} row(s)"
            )

    # 3a. Null rates.
    missing = [c for c in NON_NULL_COLUMNS if c not in columns]
    if missing:
        violations.append(f"features layer is missing required column(s): {missing}")
    for col in NON_NULL_COLUMNS:
        key = f"null__{col}"
        if key in row and int(row[key]):
            violations.append(
                f"{col} must never be null, found {int(row[key])} null(s)"
            )
    for col, reason in NULLABLE_COLUMNS.items():
        key = f"null__{col}"
        if key in row and int(row[key]):
            nulls = int(row[key])
            metrics[f"null_rate__{col}"] = f"{nulls / rows:.1%}"
            logger.info(
                "features: %s is %.1f%% null (%s)", col, 100 * nulls / rows, reason
            )

    # 3b. Ranges.
    for col, lo, hi in RANGE_CHECKS:
        key = f"range__{col}"
        if key in row and int(row[key]):
            violations.append(f"{col} outside [{lo}, {hi}] on {int(row[key])} row(s)")

    # 4. Reconciliation against the source layer.
    if processed_df is not None:
        processed_rows = processed_df.count()
        metrics["processed_rows"] = processed_rows
        if processed_rows != rows:
            violations.append(
                f"row-count reconciliation failed: processed has {processed_rows} "
                f"rows, features has {rows} (delta {rows - processed_rows})"
            )

    report = QualityReport(rows=rows, violations=violations, metrics=metrics)
    logger.info(report.summary())
    return report


def partition_row_counts(features_df: DataFrame) -> dict[str, int]:
    """Rows per ``season`` partition, for logging after a write.

    Not an assertion. A partition count that looks wrong is a judgement
    call, and a hard threshold here would fail legitimately on the first
    day of a season. Logged so the number is in the run history when
    someone needs to compare against it.
    """
    rows = (
        features_df.groupBy("season")
        .agg(F.count(F.lit(1)).alias("rows"))
        .orderBy("season")
        .collect()
    )
    return {str(r["season"]): int(r["rows"]) for r in rows}
