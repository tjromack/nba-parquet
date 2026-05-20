"""Live dashboard for the nba-parquet pipeline.

Reads the ``processed/`` and ``features/`` Parquet zones produced by the
ETL DAG (or by ``scripts/run_local.py``) and surfaces them in five views:

  - Leaderboard     - latest rolling-feature snapshot per team
  - Team detail     - one team's rolling TS%, win rate, pts over time
  - Head-to-head    - side-by-side comparison of two teams
  - Predictions     - Phase 4b winner model: matchup explorer + scorecard
  - Data explorer   - filterable view of the raw processed layer

Run locally::

    streamlit run streamlit_app.py

Set ``LOCAL_OUTPUT_DIR`` to point at a different data root::

    $env:LOCAL_OUTPUT_DIR = "C:/dev/nba-parquet/out"
    streamlit run streamlit_app.py
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import altair as alt
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


def latest_data_timestamp() -> datetime | None:
    """Most recent parquet file mtime under the processed prefix, or None."""
    if not PROCESSED_PATH.exists():
        return None
    parquet_files = list(PROCESSED_PATH.rglob("*.parquet"))
    if not parquet_files:
        return None
    return datetime.fromtimestamp(max(f.stat().st_mtime for f in parquet_files))


def _color_wl(val: str) -> str:
    """Cell-level color for W/L tokens. Used by Styler.map on the games table."""
    if val == "W":
        return "background-color: #1f4d2e; color: #b6e6c5; font-weight: 600"
    if val == "L":
        return "background-color: #4d1f1f; color: #e6b6b6; font-weight: 600"
    return ""


def _color_status(val: str) -> str:
    """Cell-level color for ACTIVE / OUT / DNP team-status tokens."""
    if val == "ACTIVE":
        return "background-color: #1f4d2e; color: #b6e6c5; font-weight: 600"
    if val == "OUT":
        return "background-color: #4d1f1f; color: #e6b6b6; font-weight: 600"
    if val == "DNP":
        return "background-color: #2a2a2a; color: #8a8a8a; font-weight: 600"
    return ""


def _playoff_only(processed: pd.DataFrame) -> pd.DataFrame:
    """Scope a processed frame to playoff games only.

    A 4-game playoff series produces one team with 4 losses against a
    single opponent; the regular season never does (teams play each
    other 2-4 times max). Without this filter, every helper that looks
    for "4 losses to one opponent" would treat RS-only teams as alive.
    Back-compat: if ``season_type`` isn't present (older tests, minimal
    fixtures), the frame is returned unchanged.
    """
    if "season_type" not in processed.columns:
        return processed
    return processed[processed["season_type"] == "Playoffs"]


def series_summary(processed: pd.DataFrame) -> pd.DataFrame:
    """Per-(team, opponent) series tally with WON / LOST / ACTIVE state.

    A series is detected by grouping all playoff games between two teams
    within the dataset. State transitions when one side reaches 4 wins
    (NBA playoff series length). Returns one row per (team, opponent)
    pair — so a single series between A and B produces two rows (A's
    view and B's view).
    """
    processed = _playoff_only(processed)
    if processed.empty:
        return pd.DataFrame(
            columns=[
                "team_abbreviation",
                "opponent_abbreviation",
                "wins",
                "losses",
                "games",
                "state",
            ]
        )
    grouped = (
        processed.groupby(["team_abbreviation", "opponent_abbreviation"])
        .agg(
            wins=("win", lambda s: int(s.astype(bool).sum())),
            losses=("win", lambda s: int((~s.astype(bool)).sum())),
            games=("game_id", "count"),
        )
        .reset_index()
    )

    def _state(row: pd.Series) -> str:
        if row["wins"] >= 4:
            return "WON"
        if row["losses"] >= 4:
            return "LOST"
        return "ACTIVE"

    grouped["state"] = grouped.apply(_state, axis=1)
    return grouped


def team_status(processed: pd.DataFrame) -> dict[str, str]:
    """Map team abbreviation -> 'ACTIVE' / 'ELIMINATED' / 'DNP'.

    Bulk-loading the regular season exposed that the old two-state
    logic ('any series with 4 losses = ELIMINATED, else ACTIVE') is
    only valid for playoff data. A team that never played in the
    playoffs (didn't qualify) trivially has zero series of 4 losses,
    so it would slide through as 'ACTIVE' — a wrong claim.

    Three states now:
      ACTIVE       - played in the playoffs, no 4-loss series yet
      ELIMINATED   - played in the playoffs, lost a 4-game series
      DNP          - present in processed but never played a playoff game
    """
    if processed.empty:
        return {}
    playoff_df = _playoff_only(processed)
    playoff_teams = set(playoff_df["team_abbreviation"].unique())
    series = series_summary(processed)
    eliminated = set(
        series.loc[series["state"] == "LOST", "team_abbreviation"].unique()
    )
    statuses: dict[str, str] = {}
    for team in processed["team_abbreviation"].unique():
        if team not in playoff_teams:
            statuses[team] = "DNP"
        elif team in eliminated:
            statuses[team] = "ELIMINATED"
        else:
            statuses[team] = "ACTIVE"
    return statuses


def generate_commentary(features: pd.DataFrame, processed: pd.DataFrame) -> list[str]:
    """Template-driven natural-language notes about current state.

    Returned as a list of markdown strings — caller is expected to
    render them as bullets or join with spacing. Notes are emitted in
    priority order; first item is always the leaderboard top.
    """
    notes: list[str] = []
    if features.empty:
        return notes

    latest = latest_snapshot(features)
    if latest.empty:
        return notes

    top = latest.iloc[0]
    notes.append(
        f"**{top['team_abbreviation']}** leads at "
        f"{top['rolling_ts_pct']:.3f} TS% over "
        f"{int(top['games_in_window'])} games "
        f"(win rate {top['rolling_win_pct']:.0%}, "
        f"avg {top['rolling_pts']:.1f} pts)."
    )

    full = latest[latest["games_in_window"] == 10]
    if not full.empty:
        names = ", ".join(full["team_abbreviation"].tolist())
        notes.append(
            f"Hit a full 10-game window (lookback is now saturated): **{names}**."
        )

    # Restrict undefeated / winless to teams with ≥4 games — a 1-game
    # window is technically 100% win rate but tells you nothing.
    sample = latest[latest["games_in_window"] >= 4]
    undefeated = sample[sample["rolling_win_pct"] == 1.0]
    if not undefeated.empty:
        names = ", ".join(undefeated["team_abbreviation"].tolist())
        notes.append(f"Undefeated in current rolling window: **{names}**.")
    winless = sample[sample["rolling_win_pct"] == 0.0]
    if not winless.empty:
        names = ", ".join(winless["team_abbreviation"].tolist())
        notes.append(f"Winless in current rolling window: **{names}**.")

    statuses = team_status(processed)
    eliminated = sorted(t for t, s in statuses.items() if s == "ELIMINATED")
    active_count = sum(1 for s in statuses.values() if s == "ACTIVE")
    playoff_total = sum(1 for s in statuses.values() if s in ("ACTIVE", "ELIMINATED"))
    if eliminated:
        notes.append(f"Eliminated from playoffs: **{', '.join(eliminated)}**.")
    if active_count and playoff_total:
        notes.append(
            f"**{active_count}** of {playoff_total} playoff teams still active."
        )

    return notes


def team_series_history(processed: pd.DataFrame, team: str) -> pd.DataFrame:
    """Series history for a single team — one row per opponent."""
    if processed.empty:
        return pd.DataFrame()
    team_view = series_summary(processed)
    return team_view[team_view["team_abbreviation"] == team].reset_index(drop=True)


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

_statuses_sidebar = team_status(processed)
if _statuses_sidebar:
    _active = sum(1 for s in _statuses_sidebar.values() if s == "ACTIVE")
    # Denominator is playoff teams only (ACTIVE + ELIMINATED); DNP teams
    # never qualified and shouldn't count against the "still alive" ratio.
    _playoff_total = sum(
        1 for s in _statuses_sidebar.values() if s in ("ACTIVE", "ELIMINATED")
    )
    if _playoff_total:
        st.sidebar.metric("Playoff teams still active", f"{_active} / {_playoff_total}")

_ts = latest_data_timestamp()
if _ts is not None:
    st.sidebar.caption(f"Data last refreshed: **{_ts.strftime('%Y-%m-%d %H:%M')}**")

view = st.sidebar.radio(
    "View",
    ["Leaderboard", "Team detail", "Head-to-head", "Predictions", "Data explorer"],
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

    # --- What's notable callout ---
    _notes = generate_commentary(features, processed)
    if _notes:
        _ts_label = processed["game_date"].max().strftime("%Y-%m-%d")
        st.info(
            f"**What's notable through {_ts_label}**\n\n"
            + "\n\n".join(f"- {n}" for n in _notes)
        )

    snap = latest_snapshot(features)
    _statuses = team_status(processed)

    def _display_status(team: str) -> str:
        s = _statuses.get(team)
        if s == "ELIMINATED":
            return "OUT"
        if s == "DNP":
            return "DNP"
        return "ACTIVE"

    snap = snap.assign(status=snap["team_abbreviation"].map(_display_status))
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
            "status",
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
    # ACTIVE / OUT status column gets green/red cell coloring via _color_status.
    st.dataframe(
        snap_display.style.bar(subset=["TS%"], color="#2e8b57")
        .bar(subset=["win%"], color="#3a7ca5")
        .map(_color_status, subset=["status"])
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

    # Default to the team currently leading by trailing TS% — the most
    # interesting first impression vs. landing on alphabetical-first.
    team_options = sorted(features["team_abbreviation"].unique())
    snap = latest_snapshot(features)
    if snap.empty:
        default_team = team_options[0]
    else:
        default_team = snap.iloc[0]["team_abbreviation"]
    default_idx = (
        team_options.index(default_team) if default_team in team_options else 0
    )
    team = st.selectbox("Team", team_options, index=default_idx)
    team_features = features[features["team_abbreviation"] == team].sort_values(
        "game_date"
    )
    team_processed = processed[processed["team_abbreviation"] == team].sort_values(
        "game_date"
    )

    if team_features.empty:
        st.warning(f"No data for {team}")
        st.stop()

    # --- Series history banner ---
    _series = team_series_history(processed, team)
    _team_status = team_status(processed).get(team)
    if _team_status == "DNP":
        st.info(f"{team}: did not play in the 2025–26 playoffs.")
    elif not _series.empty:
        if _team_status == "ELIMINATED":
            st.error(f"{team}: **ELIMINATED** from the 2025–26 playoffs.")
        else:
            st.success(f"{team}: **ACTIVE** in the 2025–26 playoffs.")
        # Render one bullet per opponent series
        bullets = []
        for _, row in _series.iterrows():
            label = {
                "WON": "won",
                "LOST": "lost",
                "ACTIVE": "in progress",
            }.get(row["state"], row["state"])
            bullets.append(
                f"- vs **{row['opponent_abbreviation']}**: "
                f"{int(row['wins'])}-{int(row['losses'])} ({label})"
            )
        st.markdown("**Series history:**\n" + "\n".join(bullets))

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
    games["win"] = games["win"].map({True: "W", False: "L"})
    games = games.sort_values("game_date", ascending=False)
    st.dataframe(
        games.style.map(_color_wl, subset=["win"]).format(
            {
                "eFG%": "{:.3f}",
                "TS%": "{:.3f}",
                "AST/TOV": "{:.2f}",
            }
        ),
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

    # Stack the two teams into one long-format DataFrame, then let Altair
    # encode `team` as the color channel. Locking y-axis domains makes
    # the comparison fair across renders — switching teams won't auto-
    # rescale and silently mislead about magnitude.
    def _trajectory_chart(metric: str, title: str, ymin: float, ymax: float):
        combined = pd.concat(
            [
                a_traj[["game_date", metric]].assign(team=team_a),
                b_traj[["game_date", metric]].assign(team=team_b),
            ],
            ignore_index=True,
        )
        return (
            alt.Chart(combined)
            .mark_line(point=True, strokeWidth=2.5)
            .encode(
                x=alt.X("game_date:T", title="Game date"),
                y=alt.Y(
                    f"{metric}:Q",
                    title=title,
                    scale=alt.Scale(domain=[ymin, ymax]),
                ),
                color=alt.Color("team:N", title="Team"),
                tooltip=[
                    alt.Tooltip("game_date:T", title="Date"),
                    alt.Tooltip("team:N", title="Team"),
                    alt.Tooltip(f"{metric}:Q", title=title, format=".3f"),
                ],
            )
            .properties(height=280)
            .interactive()
        )

    st.altair_chart(
        _trajectory_chart("rolling_ts_pct", "Rolling TS%", 0.40, 0.70),
        use_container_width=True,
    )
    st.altair_chart(
        _trajectory_chart("rolling_win_pct", "Rolling win rate", 0.0, 1.0),
        use_container_width=True,
    )

elif view == "Predictions":
    from models.predict import latest_team_features, load_model, predict_matchup
    from models.train import oof_scored_frame

    st.title("Predictions — winner model (Phase 4b)")
    st.warning(
        "**Honest framing.** On this playoff-only sample the model "
        "*underperforms* the simple 'better trailing TS%' baseline "
        "(see the Results & Metrics section of the README). This view "
        "demonstrates the prediction *interface* and the model's actual "
        "track record — treat the picks as illustrative of methodology, "
        "not as a betting edge. The credible path to meaningful accuracy "
        "is more data (regular-season backfill), not tuning this thin "
        "sample until it looks good."
    )

    model = load_model()
    if model is None:
        st.info(
            "No trained model artifact found. Generate it with:\n\n"
            "```\npython -m models.train\n```\n\n"
            "(The `.joblib` is a gitignored build output — reproducible "
            "from a clean clone, not committed.)"
        )
        st.stop()

    st.subheader("Matchup explorer")
    st.caption(
        "Pick two teams; the model predicts a hypothetical next game "
        "from each team's *latest* rolling-feature snapshot. Forward-"
        "looking, so no leakage — there's no actual outcome to compare."
    )
    latest = latest_team_features(features)
    teams = sorted(latest["team_abbreviation"].unique())
    cols = st.columns(2)
    home_team = cols[0].selectbox("Home team", teams, index=0)
    away_team = cols[1].selectbox("Away team", teams, index=min(1, len(teams) - 1))

    if home_team == away_team:
        st.warning("Pick two different teams.")
    else:
        home_row = latest[latest["team_abbreviation"] == home_team].iloc[0]
        away_row = latest[latest["team_abbreviation"] == away_team].iloc[0]
        pred = predict_matchup(model, home_row, away_row)
        prob = pred["home_win_prob"]
        winner = home_team if pred["predicted_home_win"] else away_team
        m1, m2 = st.columns(2)
        m1.metric("Model pick", winner)
        m2.metric(f"{home_team} (home) win probability", f"{prob:.1%}")
        drivers = pd.DataFrame(
            {
                "feature": [
                    c.replace("home_rolling_", "").replace("away_rolling_", "")
                    for c in pred["features"]
                    if c.startswith("home_")
                ],
            }
        )
        drivers["home"] = [
            pred["features"][c] for c in pred["features"] if c.startswith("home_")
        ]
        drivers["away"] = [
            pred["features"][c] for c in pred["features"] if c.startswith("away_")
        ]
        st.caption("Latest rolling features driving this prediction")
        st.dataframe(drivers, use_container_width=True, hide_index=True)

    st.subheader("Out-of-fold scorecard — model vs. reality")
    st.caption(
        "Each game scored by a model that did **not** train on it "
        "(walk-forward test folds). This is the honest track record — "
        "in-sample scoring would trivially read ~100% and contradict "
        "the real number. The accuracy here matches the README's "
        "Phase 4b table by construction."
    )
    from models.dataset import build_training_frame

    frame = build_training_frame(features, processed)
    if frame.empty or len(frame["game_date"].unique()) < 5:
        st.info(
            "Not enough game history yet to form walk-forward folds "
            "(need ≥ 5 distinct game dates)."
        )
    else:
        scored = oof_scored_frame(frame)
        acc = scored["correct"].mean()
        st.metric(
            "Out-of-fold model accuracy",
            f"{acc:.1%}",
            help="On thin playoff data this trails the best baseline "
            "(~0.67) — that's the honest, expected result, reported "
            "rather than hidden.",
        )
        show = scored.sort_values("game_date", ascending=False).head(20).copy()
        show["game_date"] = show["game_date"].dt.strftime("%Y-%m-%d")
        show["result"] = show.apply(lambda r: ("✓" if r["correct"] else "✗"), axis=1)
        show["actual"] = show["label"].map({1: "home won", 0: "away won"})
        show["model_pick"] = show["model_pick"].map({1: "home", 0: "away"})
        st.dataframe(
            show[
                [
                    "game_date",
                    "home_team",
                    "away_team",
                    "model_pick",
                    "model_home_win_prob",
                    "actual",
                    "result",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

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
