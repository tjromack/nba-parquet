"""Tests for the committed demo snapshot and the dashboard's use of it.

The snapshot in ``data/sample/`` is the only data a hosted deploy of the
dashboard can see — ``out/`` is a gitignored build output. That makes it
load-bearing for the public demo, so it gets the same treatment as the
rest of the pipeline: assert its shape, and assert the app actually
reaches for it under the conditions a hosted deploy creates.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_FILE = REPO_ROOT / "streamlit_app.py"
SAMPLE_ROOT = REPO_ROOT / "data" / "sample"
PROCESSED_ZONE = "processed/nba/team_game_stats"
FEATURES_ZONE = "features/nba/rolling_team_stats"

# Columns the dashboard reads by name. If the snapshot loses one of these
# the hosted app breaks at render time, which is the worst place to find
# out about it.
REQUIRED_PROCESSED_COLUMNS = {
    "game_id",
    "game_date",
    "season",
    "season_type",
    "team_abbreviation",
    "opponent_abbreviation",
    "is_home",
    "win",
    "pts",
    "true_shooting_pct",
}
REQUIRED_FEATURE_COLUMNS = {
    "game_date",
    "season",
    "team_abbreviation",
    "games_in_window",
    "rolling_pts",
    "rolling_ts_pct",
    "rolling_win_pct",
}


def _load(zone: str) -> pd.DataFrame:
    path = SAMPLE_ROOT / zone
    if not path.exists():
        pytest.skip(
            f"no demo snapshot at {path} — "
            "run `python scripts/build_sample_snapshot.py`"
        )
    return pd.read_parquet(path)


def test_sample_snapshot_zones_exist():
    assert (SAMPLE_ROOT / PROCESSED_ZONE).exists(), "processed zone missing"
    assert (SAMPLE_ROOT / FEATURES_ZONE).exists(), "features zone missing"
    assert (SAMPLE_ROOT / "MANIFEST.json").is_file(), "manifest missing"


def test_sample_snapshot_has_columns_the_dashboard_reads():
    processed = _load(PROCESSED_ZONE)
    features = _load(FEATURES_ZONE)
    assert REQUIRED_PROCESSED_COLUMNS <= set(processed.columns), (
        f"processed snapshot missing "
        f"{REQUIRED_PROCESSED_COLUMNS - set(processed.columns)}"
    )
    assert REQUIRED_FEATURE_COLUMNS <= set(features.columns), (
        f"features snapshot missing "
        f"{REQUIRED_FEATURE_COLUMNS - set(features.columns)}"
    )


def test_sample_snapshot_is_non_trivial_and_aligned():
    """A snapshot with three rows in it would render a demo that looks
    broken. Both zones are one row per (team, game), so they should also
    agree on row count — a mismatch means the export caught the two zones
    mid-pipeline-run."""
    processed = _load(PROCESSED_ZONE)
    features = _load(FEATURES_ZONE)
    assert len(processed) > 500, f"snapshot too thin: {len(processed)} rows"
    assert len(processed) == len(
        features
    ), f"zones disagree: processed={len(processed)} features={len(features)}"
    assert processed["team_abbreviation"].nunique() >= 16


def test_sample_snapshot_manifest_matches_the_parquet():
    """The manifest is what a reader trusts for 'how fresh is this demo'.
    It should not be able to drift from the files beside it."""
    manifest = json.loads((SAMPLE_ROOT / "MANIFEST.json").read_text(encoding="utf-8"))
    for label, zone in (("processed", PROCESSED_ZONE), ("features", FEATURES_ZONE)):
        df = _load(zone)
        dates = pd.to_datetime(df["game_date"].astype(str))
        stats = manifest["zones"][label]
        assert stats["rows"] == len(df)
        assert stats["first_game_date"] == str(dates.min().date())
        assert stats["last_game_date"] == str(dates.max().date())


def _resolve_data_root_fn(live: Path, sample: Path):
    """Extract ``_resolve_data_root`` from the app and bind it to the
    given roots.

    The app can't be imported for this: it calls ``st.set_page_config``
    and loads data at module scope, and its roots are module constants
    resolved at import time. Lifting just this one function out of the
    AST lets us drive it with real directories instead of reasoning
    about it by eye.
    """
    tree = ast.parse(APP_FILE.read_text(encoding="utf-8"))
    fn = next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "_resolve_data_root"
        ),
        None,
    )
    assert fn is not None, "streamlit_app.py no longer defines _resolve_data_root"
    namespace: dict = {
        "Path": Path,
        "LIVE_DATA_ROOT": live,
        "SAMPLE_DATA_ROOT": sample,
        "PROCESSED_ZONE": PROCESSED_ZONE,
        "FEATURES_ZONE": FEATURES_ZONE,
    }
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "<app>", "exec"), namespace)
    return namespace["_resolve_data_root"]


def _make_zones(root: Path, *zones: str) -> Path:
    for zone in zones:
        (root / zone).mkdir(parents=True, exist_ok=True)
    return root


def test_resolve_data_root_prefers_live_pipeline_output(tmp_path):
    live = _make_zones(tmp_path / "out", PROCESSED_ZONE, FEATURES_ZONE)
    sample = _make_zones(tmp_path / "sample", PROCESSED_ZONE, FEATURES_ZONE)
    root, is_sample = _resolve_data_root_fn(live, sample)()
    assert root == live
    assert is_sample is False


def test_resolve_data_root_falls_back_to_snapshot_when_out_is_absent(tmp_path):
    """This is the hosted-deploy case: `out/` is gitignored, so Streamlit
    Community Cloud never sees it."""
    live = tmp_path / "out"  # never created
    sample = _make_zones(tmp_path / "sample", PROCESSED_ZONE, FEATURES_ZONE)
    root, is_sample = _resolve_data_root_fn(live, sample)()
    assert root == sample
    assert is_sample is True


def test_resolve_data_root_falls_back_when_live_root_is_half_built(tmp_path):
    """A run that ingested but never wrote features shouldn't render a
    half-empty dashboard — both zones are required to claim 'live'."""
    live = _make_zones(tmp_path / "out", PROCESSED_ZONE)  # no features zone
    sample = _make_zones(tmp_path / "sample", PROCESSED_ZONE, FEATURES_ZONE)
    root, is_sample = _resolve_data_root_fn(live, sample)()
    assert root == sample
    assert is_sample is True


def test_resolve_data_root_reports_live_when_nothing_exists(tmp_path):
    """With neither root populated the app must report the live path, so
    its error message names the directory the user was expecting."""
    live = tmp_path / "out"
    sample = tmp_path / "sample"
    root, is_sample = _resolve_data_root_fn(live, sample)()
    assert root == live
    assert is_sample is False
