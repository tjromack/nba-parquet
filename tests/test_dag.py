from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = REPO_ROOT / "dags" / "nba_etl_dag.py"

# Heavy imports that MUST NOT appear at module scope in the DAG file.
# Airflow re-parses every DAG file on every scheduler tick, so paying
# the import cost for pyspark/nba_api on each parse would make scheduling
# unusable. These imports belong inside task callables.
FORBIDDEN_TOP_LEVEL_IMPORTS = {
    "pyspark",
    "nba_api",
    "pandas",
}
FORBIDDEN_TOP_LEVEL_PACKAGES = {
    "etl",
}


def _top_level_imports(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_dag_file_exists():
    assert DAG_FILE.is_file(), f"DAG file missing: {DAG_FILE}"


def test_dag_has_no_heavy_module_level_imports():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    top_level = _top_level_imports(tree)

    leaked_modules = top_level & FORBIDDEN_TOP_LEVEL_IMPORTS
    assert not leaked_modules, (
        f"DAG file imports {leaked_modules} at module scope; "
        "move into task callables (Airflow re-parses this file constantly)."
    )

    leaked_packages = top_level & FORBIDDEN_TOP_LEVEL_PACKAGES
    assert not leaked_packages, (
        f"DAG file imports our internal {leaked_packages} package at module "
        "scope; lazy-import inside task callables."
    )


def test_dag_declares_expected_callables():
    """Static check: each task callable referenced by the DAG must exist."""
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    function_names = {
        node.name for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    expected_callables = {
        "_ingest_raw",
        "_transform_and_aggregate",
        "_write_processed",
        "_write_features",
        "_notify_done",
    }
    missing = expected_callables - function_names
    assert not missing, f"DAG missing task callables: {missing}"


def test_dag_loads_with_airflow():
    """If Airflow is installed locally, verify the DAG parses cleanly."""
    pytest.importorskip("airflow")

    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location("nba_etl_dag", DAG_FILE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["nba_etl_dag"] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop("nba_etl_dag", None)

    dag = module.dag
    assert dag.dag_id == "nba_etl_pipeline"
    assert dag.schedule_interval == "@daily" or str(dag.timetable) == "@daily"
    assert dag.catchup is False

    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "ingest_raw",
        "transform_and_aggregate",
        "write_processed",
        "write_features",
        "notify_done",
    }

    # Verify the linear ordering ingest -> transform -> write -> features -> notify.
    by_id = {t.task_id: t for t in dag.tasks}
    assert {t.task_id for t in by_id["ingest_raw"].downstream_list} == {
        "transform_and_aggregate"
    }
    assert {t.task_id for t in by_id["transform_and_aggregate"].downstream_list} == {
        "write_processed"
    }
    assert {t.task_id for t in by_id["write_processed"].downstream_list} == {
        "write_features"
    }
    assert {t.task_id for t in by_id["write_features"].downstream_list} == {
        "notify_done"
    }
