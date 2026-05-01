from __future__ import annotations

import math

from etl.schema import PROCESSED_SCHEMA
from etl.transform import aggregate_team_game, get_top_player, join_top_players


def _row(df, **filters):
    rows = df.filter(
        " AND ".join(
            f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
            for k, v in filters.items()
        )
    ).collect()
    assert len(rows) == 1, f"Expected 1 row for {filters}, got {len(rows)}"
    return rows[0]


def test_aggregate_team_game_schema_matches_processed(raw_df):
    team_game = aggregate_team_game(raw_df)
    enriched = join_top_players(team_game, raw_df)

    expected = {f.name for f in PROCESSED_SCHEMA.fields}
    actual = set(enriched.columns)
    assert expected == actual, f"missing={expected - actual} extra={actual - expected}"


def test_aggregate_team_totals_match_hand_computed(raw_df):
    team_game = aggregate_team_game(raw_df).cache()

    # BOS in game 0042500101: sum across 6 players
    # pts: 32+24+15+12+8+3 = 94
    # fgm: 11+9+5+4+3+1 = 33; fga: 22+18+11+10+8+3 = 72
    # fg3m: 4+2+3+2+2+1 = 14; fg3a: 9+6+6+5+4+3 = 33
    # fta: 7+5+2+2+0+0 = 16
    bos_g1 = _row(
        team_game,
        game_id="0042500101",
        team_abbreviation="BOS",
    )
    assert bos_g1["pts"] == 94
    expected_efg = (33 + 0.5 * 14) / 72
    assert math.isclose(bos_g1["effective_fg_pct"], expected_efg, rel_tol=1e-9)
    expected_ts = 94 / (2 * (72 + 0.44 * 16))
    assert math.isclose(bos_g1["true_shooting_pct"], expected_ts, rel_tol=1e-9)
    assert bos_g1["is_home"] is True
    assert bos_g1["opponent_abbreviation"] == "NYK"
    assert bos_g1["win"] is True


def test_away_team_parsed_correctly(raw_df):
    team_game = aggregate_team_game(raw_df)
    nyk_g1 = _row(team_game, game_id="0042500101", team_abbreviation="NYK")
    assert nyk_g1["is_home"] is False
    assert nyk_g1["opponent_abbreviation"] == "BOS"
    assert nyk_g1["win"] is False


def test_assist_to_turnover_handles_zero(spark, raw_df):
    # Build a fake row where tov = 0 to verify division guard
    from pyspark.sql import Row

    minimal = spark.createDataFrame(
        [
            Row(
                game_id="0042599999",
                game_date=raw_df.first()["game_date"],
                season=2025,
                season_type="Playoffs",
                team_id=999,
                team_abbreviation="ZZZ",
                team_city="Nowhere",
                matchup="ZZZ vs. AAA",
                wl="W",
                player_id=1,
                player_name="Test Player",
                start_position="G",
                min="20:00",
                pts=10,
                reb=2,
                ast=5,
                stl=0,
                blk=0,
                tov=0,
                pf=0,
                fgm=4,
                fga=8,
                fg3m=1,
                fg3a=2,
                ftm=1,
                fta=1,
                plus_minus=0,
            )
        ],
        schema=raw_df.schema,
    )
    aggregated = aggregate_team_game(minimal).collect()
    assert len(aggregated) == 1
    row = aggregated[0]
    assert row["tov"] == 0
    assert row["assist_to_turnover"] is None


def test_top_players_are_correct(raw_df):
    team_game = aggregate_team_game(raw_df)
    enriched = join_top_players(team_game, raw_df).cache()

    # BOS g1 leaders: Tatum 32 pts, Horford 9 reb, Holiday 9 ast
    bos_g1 = _row(enriched, game_id="0042500101", team_abbreviation="BOS")
    assert bos_g1["top_scorer"] == "Jayson Tatum"
    assert bos_g1["top_rebounder"] == "Al Horford"
    assert bos_g1["top_playmaker"] == "Jrue Holiday"

    # DEN game 1: Jokic leads in pts/reb/ast
    den_g1 = _row(enriched, game_id="0042500102", team_abbreviation="DEN")
    assert den_g1["top_scorer"] == "Nikola Jokic"
    assert den_g1["top_rebounder"] == "Nikola Jokic"
    assert den_g1["top_playmaker"] == "Nikola Jokic"


def test_get_top_player_filters_null_names(spark):
    from pyspark.sql import Row

    data = spark.createDataFrame(
        [
            Row(game_id="g1", team_id=1, player_name=None, pts=40),
            Row(game_id="g1", team_id=1, player_name="Real Player", pts=20),
            Row(game_id="g1", team_id=1, player_name="Bench Guy", pts=5),
        ]
    )
    result = get_top_player(data, "pts", ["game_id", "team_id"], "top_scorer").collect()
    assert len(result) == 1
    assert result[0]["top_scorer"] == "Real Player"
