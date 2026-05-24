from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

RAW_BOX_SCORE_ADVANCED_SCHEMA = StructType(
    [
        # Partition + identity keys: same shape as the traditional layer
        # so the two zones join cleanly on (game_id, team_id, player_id).
        StructField("game_id", StringType(), nullable=False),
        StructField("game_date", DateType(), nullable=False),
        StructField("season", IntegerType(), nullable=False),
        StructField("season_type", StringType(), nullable=False),
        StructField("team_id", IntegerType(), nullable=False),
        StructField("team_abbreviation", StringType(), nullable=False),
        StructField("player_id", IntegerType(), nullable=True),
        StructField("player_name", StringType(), nullable=True),
        StructField("min", StringType(), nullable=True),
        # Core advanced metrics. nba_api ships both "predicted" (E_*) and
        # measured variants for most ratings; we keep both so future
        # feature engineering can experiment without re-ingesting.
        StructField("e_off_rating", DoubleType(), nullable=True),
        StructField("off_rating", DoubleType(), nullable=True),
        StructField("e_def_rating", DoubleType(), nullable=True),
        StructField("def_rating", DoubleType(), nullable=True),
        StructField("e_net_rating", DoubleType(), nullable=True),
        StructField("net_rating", DoubleType(), nullable=True),
        StructField("ast_pct", DoubleType(), nullable=True),
        StructField("ast_tov", DoubleType(), nullable=True),
        StructField("ast_ratio", DoubleType(), nullable=True),
        StructField("oreb_pct", DoubleType(), nullable=True),
        StructField("dreb_pct", DoubleType(), nullable=True),
        StructField("reb_pct", DoubleType(), nullable=True),
        StructField("tm_tov_pct", DoubleType(), nullable=True),
        StructField("efg_pct", DoubleType(), nullable=True),
        StructField("ts_pct", DoubleType(), nullable=True),
        StructField("usg_pct", DoubleType(), nullable=True),
        StructField("e_usg_pct", DoubleType(), nullable=True),
        StructField("pace", DoubleType(), nullable=True),
        StructField("e_pace", DoubleType(), nullable=True),
        StructField("pie", DoubleType(), nullable=True),
    ]
)


RAW_BOX_SCORE_SCHEMA = StructType(
    [
        StructField("game_id", StringType(), nullable=False),
        StructField("game_date", DateType(), nullable=False),
        StructField("season", IntegerType(), nullable=False),
        StructField("season_type", StringType(), nullable=False),
        StructField("team_id", IntegerType(), nullable=False),
        StructField("team_abbreviation", StringType(), nullable=False),
        StructField("team_city", StringType(), nullable=True),
        StructField("matchup", StringType(), nullable=True),
        StructField("wl", StringType(), nullable=True),
        StructField("player_id", IntegerType(), nullable=True),
        StructField("player_name", StringType(), nullable=True),
        StructField("start_position", StringType(), nullable=True),
        StructField("min", StringType(), nullable=True),
        StructField("pts", IntegerType(), nullable=True),
        StructField("reb", IntegerType(), nullable=True),
        StructField("ast", IntegerType(), nullable=True),
        StructField("stl", IntegerType(), nullable=True),
        StructField("blk", IntegerType(), nullable=True),
        StructField("tov", IntegerType(), nullable=True),
        StructField("pf", IntegerType(), nullable=True),
        StructField("fgm", IntegerType(), nullable=True),
        StructField("fga", IntegerType(), nullable=True),
        StructField("fg3m", IntegerType(), nullable=True),
        StructField("fg3a", IntegerType(), nullable=True),
        StructField("ftm", IntegerType(), nullable=True),
        StructField("fta", IntegerType(), nullable=True),
        StructField("plus_minus", IntegerType(), nullable=True),
    ]
)

FEATURE_SCHEMA = StructType(
    [
        StructField("season", IntegerType(), nullable=False),
        StructField("game_date", DateType(), nullable=False),
        StructField("game_id", StringType(), nullable=False),
        StructField("team_id", IntegerType(), nullable=False),
        StructField("team_abbreviation", StringType(), nullable=False),
        StructField("games_in_window", IntegerType(), nullable=True),
        StructField("rolling_pts", DoubleType(), nullable=True),
        StructField("rolling_efg_pct", DoubleType(), nullable=True),
        StructField("rolling_ts_pct", DoubleType(), nullable=True),
        StructField("rolling_ast_to_tov", DoubleType(), nullable=True),
        StructField("rolling_win_pct", DoubleType(), nullable=True),
        StructField("rolling_pts_home", DoubleType(), nullable=True),
        StructField("rolling_pts_away", DoubleType(), nullable=True),
    ]
)

PROCESSED_SCHEMA = StructType(
    [
        StructField("season", IntegerType(), nullable=False),
        StructField("game_date", DateType(), nullable=False),
        StructField("game_id", StringType(), nullable=False),
        StructField("season_type", StringType(), nullable=False),
        StructField("team_id", IntegerType(), nullable=False),
        StructField("team_abbreviation", StringType(), nullable=False),
        StructField("opponent_abbreviation", StringType(), nullable=True),
        StructField("is_home", BooleanType(), nullable=True),
        StructField("win", BooleanType(), nullable=True),
        StructField("pts", LongType(), nullable=True),
        StructField("reb", LongType(), nullable=True),
        StructField("ast", LongType(), nullable=True),
        StructField("tov", LongType(), nullable=True),
        StructField("fg_pct", DoubleType(), nullable=True),
        StructField("fg3_pct", DoubleType(), nullable=True),
        StructField("ft_pct", DoubleType(), nullable=True),
        StructField("effective_fg_pct", DoubleType(), nullable=True),
        StructField("true_shooting_pct", DoubleType(), nullable=True),
        StructField("assist_to_turnover", DoubleType(), nullable=True),
        StructField("top_scorer", StringType(), nullable=True),
        StructField("top_rebounder", StringType(), nullable=True),
        StructField("top_playmaker", StringType(), nullable=True),
    ]
)
