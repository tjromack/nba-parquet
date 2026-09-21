"""Build the bundled demo snapshot that the public dashboard reads.

The live dashboard reads ``$LOCAL_OUTPUT_DIR`` (or ``./out``), which is a
gitignored build output — fine locally, useless on Streamlit Community
Cloud, which only ever sees what is committed. This script freezes the
current ``out/`` zones into ``data/sample/``, which IS committed, so the
hosted app has something to render.

Two deliberate differences from the pipeline's own writes:

- **Coalesced.** The real ``processed/`` zone is partitioned by
  ``(season, game_date)`` — 194 directories for a full season, which is
  the right layout for a warehouse and the wrong one for a git repo.
  Here we partition by ``season`` only, so each zone is one file. This
  does not violate the "no ``repartition(1)``" rule in CLAUDE.md: that
  rule is about the *pipeline's* output layout, and this is a pandas
  export of a demo artifact, not a Spark write on a production path.
- **pandas, not Spark.** Reading 2,600 rows to rewrite them does not
  need a SparkSession, and keeping Spark out means this script runs
  anywhere, including CI.

Usage::

    python scripts/build_sample_snapshot.py
    python scripts/build_sample_snapshot.py --source out --dest data/sample
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]

# (zone path relative to the data root) -> human label for the manifest.
ZONES = {
    "processed/nba/team_game_stats": "processed",
    "features/nba/rolling_team_stats": "features",
}


def _freeze_zone(source_zone: Path, dest_zone: Path) -> dict:
    """Read one partitioned zone and rewrite it coalesced. Returns stats."""
    df = pd.read_parquet(source_zone)
    if df.empty:
        raise SystemExit(f"{source_zone} is empty — run the pipeline first")

    # Partition columns come back as pandas Categoricals. Materialize them
    # so the rewrite doesn't carry a category index that depends on which
    # partitions happened to exist in the source.
    for col in df.select_dtypes("category").columns:
        df[col] = df[col].astype(str)

    game_dates = pd.to_datetime(df["game_date"].astype(str))

    if dest_zone.exists():
        shutil.rmtree(dest_zone)
    dest_zone.mkdir(parents=True)
    # Stable filenames: pyarrow's default is a random UUID per write, which
    # would make every regeneration a delete + add pair in git instead of a
    # readable diff.
    df.to_parquet(
        dest_zone,
        partition_cols=["season"],
        index=False,
        basename_template="snapshot-{i}.parquet",
    )

    return {
        "rows": int(len(df)),
        "teams": int(df["team_abbreviation"].nunique()),
        "game_dates": int(game_dates.dt.date.nunique()),
        "first_game_date": str(game_dates.min().date()),
        "last_game_date": str(game_dates.max().date()),
        "seasons": sorted(df["season"].unique().tolist()),
        "files": sorted(p.name for p in dest_zone.rglob("*.parquet")),
    }


def build(source: Path, dest: Path) -> dict:
    manifest: dict = {
        "generated_on": str(date.today()),
        "source": str(source),
        "note": (
            "Frozen snapshot of the pipeline's own output, committed so the "
            "hosted dashboard has data. Regenerate with "
            "scripts/build_sample_snapshot.py."
        ),
        "zones": {},
    }
    for zone_path, label in ZONES.items():
        source_zone = source / zone_path
        if not source_zone.exists():
            raise SystemExit(f"missing source zone: {source_zone}")
        manifest["zones"][label] = _freeze_zone(source_zone, dest / zone_path)

    (dest / "MANIFEST.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="out", help="pipeline data root")
    parser.add_argument("--dest", default="data/sample", help="snapshot root")
    args = parser.parse_args()

    source = (REPO_ROOT / args.source).resolve()
    dest = (REPO_ROOT / args.dest).resolve()
    manifest = build(source, dest)

    print(f"snapshot written to {dest}")
    for label, stats in manifest["zones"].items():
        print(
            f"  {label:<10} {stats['rows']:>5} rows  "
            f"{stats['teams']:>2} teams  "
            f"{stats['first_game_date']} -> {stats['last_game_date']}"
        )


if __name__ == "__main__":
    main()
