"""Live dashboard for the nba-parquet pipeline.

Reads the ``processed/`` and ``features/`` Parquet zones produced by the
ETL DAG (or by ``scripts/run_local.py``) and surfaces them in four views:

  - Leaderboard     - latest rolling-feature snapshot per team
  - Team detail     - one team's rolling TS%, win rate, pts over time
  - Head-to-head    - side-by-side comparison of two teams
  - Data explorer   - filterable view of the raw processed layer

Run locally::

    streamlit run streamlit_app.py

Set ``LOCAL_OUTPUT_DIR`` to point at a different data root::

    $env:LOCAL_OUTPUT_DIR = "C:/dev/nba-parquet/out"
    streamlit run streamlit_app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="nba-parquet", layout="wide", page_icon="🏀")

DATA_ROOT = Path(os.environ.get("LOCAL_OUTPUT_DIR", "./out"))
PROCESSED_PATH = DATA_ROOT / "processed/nba/team_game_stats"
FEATURES_PATH = DATA_ROOT / "features/nba/rolling_team_stats"


@st.cache_data
def load_processed() -> pd.DataFrame:
    if not PROCESSED_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(PROCESSED_PATH)
    df["game_date"] = pd.to_datetime(df["game_date"].astype(str))
    return df


@st.cache_data
def load_features() -> pd.DataFrame:
    if not FEATURES_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(FEATURES_PATH)
    df["game_date"] = pd.to_datetime(df["game_date"].astype(str))
    return df


def latest_snapshot(features: pd.DataFrame) -> pd.DataFrame:
    """Latest row per team, sorted by trailing TS%."""
    if features.empty:
        return features
    latest = features.sort_values("game_date").groupby("team_abbreviation").tail(1)
    return latest.sort_values("rolling_ts_pct", ascending=False).reset_index(drop=True)


# --- Sidebar ---

st.sidebar.title("🏀 nba-parquet")
st.sidebar.caption("Live playoff trailing-window features")

processed = load_processed()
features = load_features()

if processed.empty or features.empty:
    st.error(
        "No data found at "
        f"`{DATA_ROOT}`. Run the ETL pipeline first via "
        "`python scripts/run_local.py` or set `LOCAL_OUTPUT_DIR` to point at "
        "a populated data root."
    )
    st.stop()

st.sidebar.metric("Total team-game rows", f"{len(processed):,}")
st.sidebar.metric("Distinct game dates", processed["game_date"].nunique())
st.sidebar.metric(
    "Latest game date",
    processed["game_date"].max().strftime("%Y-%m-%d"),
)
date_range_str = (
    f"{processed['game_date'].min().strftime('%m/%d')}"
    f" → {processed['game_date'].max().strftime('%m/%d')}"
)
st.sidebar.metric("Date range", date_range_str)

view = st.sidebar.radio(
    "View",
    ["Leaderboard", "Team detail", "Head-to-head", "Data explorer"],
)

st.sidebar.markdown("---")
st.sidebar.caption(
    "Data flow: nba_api → PySpark transforms → partitioned Parquet → this "
    "dashboard. See [docs/PROJECT_QA.md](docs/PROJECT_QA.md) for the "
    "architecture story."
)

# --- Main view router ---

if view == "Leaderboard":
    st.title("Leaderboard — trailing 10-game window")
    st.caption(
        "Latest snapshot per team, sorted by rolling true-shooting %. "
        "Inline bars on TS% (green) and win% (blue) scale to the leader of "
        "each column, so the gap between the top team and the rest is "
        "visible at a glance."
    )

    snap = latest_snapshot(features)
    snap_display = snap.rename(
        columns={
            "team_abbreviation": "team",
            "games_in_window": "games",
            "rolling_pts": "pts",
            "rolling_efg_pct": "eFG%",
            "rolling_ts_pct": "TS%",
            "rolling_ast_to_tov": "AST/TOV",
            "rolling_win_pct": "win%",
            "rolling_pts_home": "pts (home)",
            "rolling_pts_away": "pts (away)",
        }
    )[
        [
            "team",
            "games",
            "pts",
            "eFG%",
            "TS%",
            "AST/TOV",
            "win%",
            "pts (home)",
            "pts (away)",
        ]
    ]

    # Inline bars on TS% and win% scale to each column's actual min/max,
    # so the visually-leading team has the longest bar. Pure CSS — avoids
    # the matplotlib dependency that .background_gradient() pulls in.
    st.dataframe(
        snap_display.style.bar(subset=["TS%"], color="#2e8b57")
        .bar(subset=["win%"], color="#3a7ca5")
        .format(
            {
                "pts": "{:.1f}",
                "eFG%": "{:.3f}",
                "TS%": "{:.3f}",
                "AST/TOV": "{:.2f}",
                "win%": "{:.3f}",
                "pts (home)": "{:.1f}",
                "pts (away)": "{:.1f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "Reading the table: a team near the top with a high win% is firing on "
        "all cylinders; a team near the top with a low win% is shooting well "
        "but losing close games (luck or opponent quality); a team near the "
        "bottom with a high win% is grinding out wins despite cold shooting "
        "(unsustainable, model-flag-worthy)."
    )

elif view == "Team detail":
    st.title("Team detail")

    team = st.selectbox(
        "Team",
        sorted(features["team_abbreviation"].unique()),
    )
    team_features = features[features["team_abbreviation"] == team].sort_values(
        "game_date"
    )
    team_processed = processed[processed["team_abbreviation"] == team].sort_values(
        "game_date"
    )

    if team_features.empty:
        st.warning(f"No data for {team}")
        st.stop()

    latest = team_features.iloc[-1]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Rolling TS%", f"{latest['rolling_ts_pct']:.3f}")
    col2.metric("Rolling win%", f"{latest['rolling_win_pct']:.3f}")
    col3.metric("Rolling pts", f"{latest['rolling_pts']:.1f}")
    col4.metric("Games in window", int(latest["games_in_window"]))

    st.subheader("Trailing-window trajectory")

    pts_chart = team_features.set_index("game_date")[["rolling_pts"]]
    st.caption("Rolling points per game")
    st.line_chart(pts_chart, height=260)

    pct_chart = team_features.set_index("game_date")[
        ["rolling_ts_pct", "rolling_efg_pct", "rolling_win_pct"]
    ]
    st.caption("Rolling shooting % and win rate")
    st.line_chart(pct_chart, height=260)

    st.subheader("Game-by-game results")
    games = team_processed[
        [
            "game_date",
            "opponent_abbreviation",
            "is_home",
            "win",
            "pts",
            "effective_fg_pct",
            "true_shooting_pct",
            "assist_to_turnover",
        ]
    ].rename(
        columns={
            "opponent_abbreviation": "opp",
            "is_home": "home",
            "effective_fg_pct": "eFG%",
            "true_shooting_pct": "TS%",
            "assist_to_turnover": "AST/TOV",
        }
    )
    games["game_date"] = games["game_date"].dt.strftime("%Y-%m-%d")
    st.dataframe(
        games.sort_values("game_date", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

elif view == "Head-to-head":
    st.title("Head-to-head comparison")
    st.caption("Pick any two teams to compare their latest rolling-window snapshots.")

    teams = sorted(features["team_abbreviation"].unique())
    cols = st.columns(2)
    team_a = cols[0].selectbox("Team A", teams, index=0)
    team_b = cols[1].selectbox(
        "Team B",
        teams,
        index=min(1, len(teams) - 1),
    )

    if team_a == team_b:
        st.warning("Pick two different teams.")
        st.stop()

    snap_all = latest_snapshot(features)
    snap_a = snap_all[snap_all["team_abbreviation"] == team_a]
    snap_b = snap_all[snap_all["team_abbreviation"] == team_b]

    if snap_a.empty or snap_b.empty:
        st.warning("No data for one of the selected teams.")
        st.stop()

    st.subheader("Latest snapshot")
    metric_rows = [
        "games_in_window",
        "rolling_pts",
        "rolling_efg_pct",
        "rolling_ts_pct",
        "rolling_ast_to_tov",
        "rolling_win_pct",
        "rolling_pts_home",
        "rolling_pts_away",
    ]
    side_by_side = pd.DataFrame(
        {
            team_a: snap_a.iloc[0][metric_rows],
            team_b: snap_b.iloc[0][metric_rows],
        }
    )
    pretty_index = {
        "games_in_window": "games",
        "rolling_pts": "pts",
        "rolling_efg_pct": "eFG%",
        "rolling_ts_pct": "TS%",
        "rolling_ast_to_tov": "AST/TOV",
        "rolling_win_pct": "win%",
        "rolling_pts_home": "pts (home)",
        "rolling_pts_away": "pts (away)",
    }
    st.dataframe(
        side_by_side.rename(index=pretty_index),
        use_container_width=True,
    )

    st.subheader("Trajectories overlaid")

    a_traj = features[features["team_abbreviation"] == team_a].sort_values("game_date")
    b_traj = features[features["team_abbreviation"] == team_b].sort_values("game_date")

    ts_overlay = pd.DataFrame(
        {
            f"{team_a} TS%": a_traj.set_index("game_date")["rolling_ts_pct"],
            f"{team_b} TS%": b_traj.set_index("game_date")["rolling_ts_pct"],
        }
    )
    st.caption("Rolling true-shooting %")
    st.line_chart(ts_overlay, height=280)

    win_overlay = pd.DataFrame(
        {
            f"{team_a} win%": a_traj.set_index("game_date")["rolling_win_pct"],
            f"{team_b} win%": b_traj.set_index("game_date")["rolling_win_pct"],
        }
    )
    st.caption("Rolling win rate")
    st.line_chart(win_overlay, height=280)

elif view == "Data explorer":
    st.title("Data explorer — processed layer")
    st.caption(
        "Raw filterable view of the team-game DataFrame. One row per "
        "(team, game). Use this to spot-check pipeline output against "
        "external sources like ESPN."
    )

    teams = ["(all)"] + sorted(processed["team_abbreviation"].unique())
    selected_team = st.selectbox("Filter by team", teams)

    df = processed.copy()
    if selected_team != "(all)":
        df = df[df["team_abbreviation"] == selected_team]
    df = df.sort_values("game_date", ascending=False).reset_index(drop=True)
    df["game_date"] = df["game_date"].dt.strftime("%Y-%m-%d")

    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"{len(df):,} rows shown")
