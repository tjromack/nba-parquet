from __future__ import annotations

from pathlib import Path

import pytest

from etl.transform import aggregate_team_game, join_top_players
from etl.write import write_processed_to_path


def test_write_creates_partition_layout(raw_df, tmp_path: Path):
    team_game = aggregate_team_game(raw_df)
    enriched = join_top_players(team_game, raw_df)

    output = tmp_path / "processed"
    write_processed_to_path(enriched, str(output))

    season_dirs = list(output.glob("season=*"))
    assert season_dirs, "expected at least one season=* partition dir"

    date_dirs = list(output.glob("season=*/game_date=*"))
    assert len(date_dirs) >= 2, f"expected ≥2 game_date partitions, got {date_dirs}"

    for d in date_dirs:
        parquet_files = list(d.glob("*.parquet"))
        assert parquet_files, f"no parquet files in {d}"


def test_write_rejects_raw_prefix(raw_df, tmp_path: Path):
    team_game = aggregate_team_game(raw_df)
    enriched = join_top_players(team_game, raw_df)

    bad_path = str(tmp_path / "raw" / "leaked")
    with pytest.raises(ValueError):
        write_processed_to_path(enriched, bad_path)
