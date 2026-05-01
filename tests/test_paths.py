from __future__ import annotations

from pathlib import Path

import pytest

from etl.paths import LOCAL_OUTPUT_ENV, is_local_mode, resolve_output_uri


def test_resolve_output_uri_returns_s3a_when_no_local_dir(monkeypatch):
    monkeypatch.delenv(LOCAL_OUTPUT_ENV, raising=False)
    uri = resolve_output_uri("my-bucket", "raw/nba/box_scores/season=2025")
    assert uri == "s3a://my-bucket/raw/nba/box_scores/season=2025/"


def test_resolve_output_uri_returns_file_uri_when_local_dir_set(
    monkeypatch, tmp_path: Path
):
    monkeypatch.setenv(LOCAL_OUTPUT_ENV, str(tmp_path))
    uri = resolve_output_uri(None, "raw/nba/box_scores/season=2025")
    assert uri.startswith("file:")
    assert uri.endswith("/")
    assert "raw/nba/box_scores/season=2025" in uri.replace("\\", "/")
    assert (tmp_path / "raw" / "nba" / "box_scores" / "season=2025").is_dir()


def test_resolve_output_uri_requires_bucket_when_not_local(monkeypatch):
    monkeypatch.delenv(LOCAL_OUTPUT_ENV, raising=False)
    with pytest.raises(ValueError):
        resolve_output_uri("", "raw/nba/box_scores")


def test_is_local_mode_reflects_env(monkeypatch, tmp_path: Path):
    monkeypatch.delenv(LOCAL_OUTPUT_ENV, raising=False)
    assert is_local_mode() is False
    monkeypatch.setenv(LOCAL_OUTPUT_ENV, str(tmp_path))
    assert is_local_mode() is True
