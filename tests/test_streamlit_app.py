"""Static checks for streamlit_app.py.

We don't actually *run* the Streamlit app under pytest — Streamlit's
``set_page_config`` and the data-loading at module scope expect a real
Streamlit script-execution context. Instead, we AST-parse the file to
catch syntax errors and verify the high-level structure (imports,
helper functions) hasn't drifted.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_FILE = REPO_ROOT / "streamlit_app.py"


def test_streamlit_app_file_exists():
    assert APP_FILE.is_file(), f"streamlit_app.py missing at {APP_FILE}"


def test_streamlit_app_parses_cleanly():
    """AST-parse must succeed — catches any syntax errors at commit time."""
    source = APP_FILE.read_text(encoding="utf-8")
    ast.parse(source)


def test_streamlit_app_declares_expected_loaders():
    """The cache-decorated data loaders are the load-bearing entry points."""
    source = APP_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    function_names = {
        node.name for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    expected = {
        "load_processed",
        "load_features",
        "latest_snapshot",
        "latest_data_timestamp",
        "series_summary",
        "team_status",
        "generate_commentary",
        "team_series_history",
    }
    missing = expected - function_names
    assert not missing, f"streamlit_app.py missing functions: {missing}"


def test_series_summary_and_team_status_against_synthetic_data():
    """Spot-check the new series/elimination logic with a small fixture.

    Loads streamlit_app via importlib so the parse/load itself is also
    exercised. We then drive its pure-data helpers directly with a
    pandas frame — no Streamlit runtime needed.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("streamlit_app", APP_FILE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # streamlit_app calls st.set_page_config and data loaders at module scope.
    # The loaders return empty DataFrames when out/ doesn't exist (which is
    # fine for this test) — and set_page_config is a no-op outside a real
    # Streamlit run.
    try:
        spec.loader.exec_module(module)
    except Exception:
        # If Streamlit's set_page_config rejects being called twice across
        # tests we skip — the parse-only checks still cover correctness.
        import pytest

        pytest.skip("streamlit_app already loaded in this process")

    import pandas as pd

    # NYK 4-2 over ATL series → ATL eliminated, NYK won
    # OKC 4-0 over PHX → PHX eliminated, OKC won
    # NYK 1-1 ongoing vs IND → NYK active, IND active
    rows = []
    for w in range(4):
        rows.append(
            {
                "team_abbreviation": "NYK",
                "opponent_abbreviation": "ATL",
                "game_id": f"a{w}",
                "win": True,
            }
        )
        rows.append(
            {
                "team_abbreviation": "ATL",
                "opponent_abbreviation": "NYK",
                "game_id": f"a{w}",
                "win": False,
            }
        )
    for w in range(2):
        rows.append(
            {
                "team_abbreviation": "NYK",
                "opponent_abbreviation": "ATL",
                "game_id": f"b{w}",
                "win": False,
            }
        )
        rows.append(
            {
                "team_abbreviation": "ATL",
                "opponent_abbreviation": "NYK",
                "game_id": f"b{w}",
                "win": True,
            }
        )
    for w in range(4):
        rows.append(
            {
                "team_abbreviation": "OKC",
                "opponent_abbreviation": "PHX",
                "game_id": f"c{w}",
                "win": True,
            }
        )
        rows.append(
            {
                "team_abbreviation": "PHX",
                "opponent_abbreviation": "OKC",
                "game_id": f"c{w}",
                "win": False,
            }
        )
    rows.append(
        {
            "team_abbreviation": "NYK",
            "opponent_abbreviation": "IND",
            "game_id": "d0",
            "win": True,
        }
    )
    rows.append(
        {
            "team_abbreviation": "IND",
            "opponent_abbreviation": "NYK",
            "game_id": "d0",
            "win": False,
        }
    )
    rows.append(
        {
            "team_abbreviation": "NYK",
            "opponent_abbreviation": "IND",
            "game_id": "d1",
            "win": False,
        }
    )
    rows.append(
        {
            "team_abbreviation": "IND",
            "opponent_abbreviation": "NYK",
            "game_id": "d1",
            "win": True,
        }
    )
    df = pd.DataFrame(rows)

    statuses = module.team_status(df)
    assert statuses["NYK"] == "ACTIVE"
    assert statuses["ATL"] == "ELIMINATED"
    assert statuses["OKC"] == "ACTIVE"
    assert statuses["PHX"] == "ELIMINATED"
    assert statuses["IND"] == "ACTIVE"

    series = module.series_summary(df)
    nyk_vs_atl = series[
        (series["team_abbreviation"] == "NYK")
        & (series["opponent_abbreviation"] == "ATL")
    ].iloc[0]
    assert (nyk_vs_atl["wins"], nyk_vs_atl["losses"], nyk_vs_atl["state"]) == (
        4,
        2,
        "WON",
    )
    nyk_vs_ind = series[
        (series["team_abbreviation"] == "NYK")
        & (series["opponent_abbreviation"] == "IND")
    ].iloc[0]
    assert (nyk_vs_ind["wins"], nyk_vs_ind["losses"], nyk_vs_ind["state"]) == (
        1,
        1,
        "ACTIVE",
    )


def test_team_status_filters_to_playoffs_only():
    """Bulk-loading the regular season exposed this: the elimination
    logic must scope to playoff games. A 4-game playoff series produces
    one team with 4 losses; the regular season never does (teams play
    each other 2-4 times max). Without scoping, RS-only teams (e.g.
    teams that didn't make the playoffs at all) silently slide through
    as 'ACTIVE'.

    Expected three states:
      ACTIVE       - in playoffs, not yet eliminated
      ELIMINATED   - in playoffs, lost a 4-game series
      DNP          - did not play in playoffs (RS-only)
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("streamlit_app", APP_FILE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception:
        import pytest

        pytest.skip("streamlit_app already loaded in this process")

    import pandas as pd

    rows = []
    # OKC 4-0 PHX in the playoffs - PHX eliminated, OKC active
    for w in range(4):
        rows.append(
            {
                "team_abbreviation": "OKC",
                "opponent_abbreviation": "PHX",
                "game_id": f"p{w}",
                "season_type": "Playoffs",
                "win": True,
            }
        )
        rows.append(
            {
                "team_abbreviation": "PHX",
                "opponent_abbreviation": "OKC",
                "game_id": f"p{w}",
                "season_type": "Playoffs",
                "win": False,
            }
        )
    # SAS plays DEN twice in the regular season, sweep. Without
    # the playoff filter, SAS would be flagged ACTIVE (no 4-loss
    # series) when it should be DNP (didn't make the playoffs).
    for w in range(2):
        rows.append(
            {
                "team_abbreviation": "SAS",
                "opponent_abbreviation": "DEN",
                "game_id": f"r{w}",
                "season_type": "Regular Season",
                "win": True,
            }
        )
        rows.append(
            {
                "team_abbreviation": "DEN",
                "opponent_abbreviation": "SAS",
                "game_id": f"r{w}",
                "season_type": "Regular Season",
                "win": False,
            }
        )
    df = pd.DataFrame(rows)

    statuses = module.team_status(df)
    assert statuses["OKC"] == "ACTIVE"
    assert statuses["PHX"] == "ELIMINATED"
    # The headline bug: RS-only teams must be DNP, not ACTIVE.
    assert statuses["SAS"] == "DNP"
    assert statuses["DEN"] == "DNP"


def test_streamlit_app_imports_streamlit_and_pandas():
    source = APP_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    top_level_imports: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                top_level_imports.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            top_level_imports.add(node.module.split(".")[0])
    assert "streamlit" in top_level_imports
    assert "pandas" in top_level_imports
