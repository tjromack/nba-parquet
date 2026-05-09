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
    expected = {"load_processed", "load_features", "latest_snapshot"}
    missing = expected - function_names
    assert not missing, f"streamlit_app.py missing functions: {missing}"


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
