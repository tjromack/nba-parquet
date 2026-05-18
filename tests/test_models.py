"""Tests for the Phase 4b prediction-model data layer.

The first and most important test is the **leakage guard**. The
``features/`` rolling columns for game N include game N itself; using
them to predict game N is target leakage and silently invalidates every
metric. ``build_training_frame`` must use each team's features as they
stood *entering* the game (lag-1 within the team's chronological
sequence). This test pins that with hand-computable numbers: if the
implementation ever uses a game's own features instead of the prior
game's, the asserted values change and the test fails loudly.
"""

from __future__ import annotations

import pandas as pd
import pytest

from models.dataset import OUTPUT_COLUMNS, build_training_frame


def _synthetic_features_and_processed():
    """Two teams (HOME, AWAY) play each other on 4 consecutive dates.

    Rolling values are deliberately decodable:
      HOME.rolling_pts = 100 + game_index  -> 101,102,103,104
      AWAY.rolling_pts = 200 + game_index  -> 201,202,203,204
    HOME is always the home team; HOME wins games 1 & 3, loses 2 & 4.

    Leak-free expectation: the training row for game G must carry each
    team's rolling values from game G-1, never game G. Game 1 has no
    prior history for either team and must be dropped.
    """
    feat_rows = []
    proc_rows = []
    for idx in range(1, 5):
        game_id = f"G{idx}"
        game_date = pd.Timestamp("2026-04-01") + pd.Timedelta(days=idx)
        home_win = idx % 2 == 1  # HOME wins odd-indexed games

        for team, base in (("HOME", 100), ("AWAY", 200)):
            feat_rows.append(
                {
                    "season": 2025,
                    "game_date": game_date,
                    "game_id": game_id,
                    "team_abbreviation": team,
                    "games_in_window": idx,
                    "rolling_pts": float(base + idx),
                    "rolling_efg_pct": 0.50 + 0.01 * idx,
                    "rolling_ts_pct": 0.55 + 0.01 * idx,
                    "rolling_ast_to_tov": 1.5 + 0.1 * idx,
                    "rolling_win_pct": 0.10 * idx,
                    "rolling_pts_home": float(base + idx + 1),
                    "rolling_pts_away": float(base + idx - 1),
                }
            )
        proc_rows.append(
            {
                "game_id": game_id,
                "game_date": game_date,
                "team_abbreviation": "HOME",
                "opponent_abbreviation": "AWAY",
                "is_home": True,
                "win": home_win,
            }
        )
        proc_rows.append(
            {
                "game_id": game_id,
                "game_date": game_date,
                "team_abbreviation": "AWAY",
                "opponent_abbreviation": "HOME",
                "is_home": False,
                "win": not home_win,
            }
        )
    return pd.DataFrame(feat_rows), pd.DataFrame(proc_rows)


def test_build_training_frame_is_leak_free():
    features, processed = _synthetic_features_and_processed()
    frame = build_training_frame(features, processed)

    # Game 1 has no prior history for either team -> dropped.
    # Usable training rows are games 2, 3, 4.
    assert sorted(frame["game_id"].tolist()) == ["G2", "G3", "G4"]

    by_game = frame.set_index("game_id")

    # --- The leakage assertion ---
    # Game 3's row must carry each team's rolling values from game 2
    # (prior game), NOT game 3's own values. HOME g2 rolling_pts = 102
    # (not 103); AWAY g2 = 202 (not 203).
    g3 = by_game.loc["G3"]
    assert g3["home_rolling_pts"] == 102.0, (
        f"LEAKAGE: home_rolling_pts for G3 is {g3['home_rolling_pts']}, "
        "expected 102 (HOME's game-2 value). Got 103 => the game's own "
        "features were used."
    )
    assert g3["away_rolling_pts"] == 202.0, (
        f"LEAKAGE: away_rolling_pts for G3 is {g3['away_rolling_pts']}, "
        "expected 202 (AWAY's game-2 value)."
    )

    # Game 2's row carries game-1 values.
    g2 = by_game.loc["G2"]
    assert g2["home_rolling_pts"] == 101.0
    assert g2["away_rolling_pts"] == 201.0

    # Game 4's row carries game-3 values.
    g4 = by_game.loc["G4"]
    assert g4["home_rolling_pts"] == 103.0
    assert g4["away_rolling_pts"] == 203.0

    # --- Label correctness (the one thing that IS the current game) ---
    # HOME wins odd games: G3 -> 1, G2 -> 0, G4 -> 0.
    assert g3["label"] == 1
    assert g2["label"] == 0
    assert g4["label"] == 0


def test_one_row_per_game_and_expected_schema():
    features, processed = _synthetic_features_and_processed()
    frame = build_training_frame(features, processed)

    # Exactly one row per game, no duplication from the two team rows.
    assert frame["game_id"].is_unique
    assert list(frame.columns) == OUTPUT_COLUMNS
    # Sorted chronologically.
    assert frame["game_date"].is_monotonic_increasing
    # Label is integer 0/1.
    assert set(frame["label"].unique()).issubset({0, 1})


def test_empty_input_returns_empty_frame_with_schema():
    """Mirrors the off-day discipline elsewhere: empty in -> empty out,
    schema intact, no crash."""
    empty = pd.DataFrame()
    frame = build_training_frame(empty, empty)
    assert frame.empty
    assert list(frame.columns) == OUTPUT_COLUMNS


def test_game_is_dropped_when_one_team_has_no_prior_history():
    """If only one side has a prior window, the game can't be a training
    row — inner-join semantics must drop it, not emit a half-populated row."""
    features, processed = _synthetic_features_and_processed()

    # Inject a brand-new team NEW that plays exactly one game (G5) vs HOME.
    # NEW has zero prior history, so G5 must NOT appear in the output.
    g5_date = pd.Timestamp("2026-04-01") + pd.Timedelta(days=5)
    extra_feat = pd.DataFrame(
        [
            {
                "season": 2025,
                "game_date": g5_date,
                "game_id": "G5",
                "team_abbreviation": t,
                "games_in_window": 1,
                "rolling_pts": 110.0,
                "rolling_efg_pct": 0.55,
                "rolling_ts_pct": 0.60,
                "rolling_ast_to_tov": 2.0,
                "rolling_win_pct": 0.5,
                "rolling_pts_home": 111.0,
                "rolling_pts_away": 109.0,
            }
            for t in ("HOME", "NEW")
        ]
    )
    extra_proc = pd.DataFrame(
        [
            {
                "game_id": "G5",
                "game_date": g5_date,
                "team_abbreviation": "HOME",
                "opponent_abbreviation": "NEW",
                "is_home": True,
                "win": True,
            },
            {
                "game_id": "G5",
                "game_date": g5_date,
                "team_abbreviation": "NEW",
                "opponent_abbreviation": "HOME",
                "is_home": False,
                "win": False,
            },
        ]
    )
    features = pd.concat([features, extra_feat], ignore_index=True)
    processed = pd.concat([processed, extra_proc], ignore_index=True)

    frame = build_training_frame(features, processed)
    # G5 dropped: NEW has no prior history even though HOME does.
    assert "G5" not in frame["game_id"].tolist()


def test_bad_orientation_raises_value_error():
    """Two is_home=True rows for one game is corrupt orientation —
    the builder must fail loudly, not silently emit garbage."""
    features, processed = _synthetic_features_and_processed()
    # Corrupt G3: flip AWAY's is_home to True so the game has two home rows.
    mask = (processed["game_id"] == "G3") & (processed["team_abbreviation"] == "AWAY")
    processed.loc[mask, "is_home"] = True

    with pytest.raises(ValueError, match="exactly one home row"):
        build_training_frame(features, processed)
