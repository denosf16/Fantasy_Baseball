from pathlib import Path
import sys

import pandas as pd
import matplotlib.pyplot as plt

from src.db_connect import engine
from src.queries.statcast_queries import get_hitter_season
from src.utils.pitch_config import PITCH_NAME_MAP, get_pitch_colors
from src.utils.plot_helpers import draw_spray_chart
from src.utils.report_helpers import (
    classify_in_zone,
    build_pitch_summary_hitter,
    enrich_chase_by_pitch_type,
    format_table_df,
    add_pitchtype_heatmap_grid,
    build_game_trend_summary_hitter,
)
from src.utils.output_router import get_output_path


BASE_DIR = Path(__file__).resolve().parents[2]


def infer_hitter_team_code(df: pd.DataFrame) -> str:
    """
    Infer the hitter's team code across a season sample.

    For a batter:
      - Top inning  -> away team is batting
      - Bottom inning -> home team is batting

    We take the most common inferred batting team in the dataframe.
    """
    required_cols = {"inning_half", "home_team", "away_team"}
    if not required_cols.issubset(df.columns):
        return "UNK"

    valid = df[
        df["inning_half"].notna()
        & df["home_team"].notna()
        & df["away_team"].notna()
    ].copy()

    if valid.empty:
        return "UNK"

    def map_team(row):
        inning_half = str(row["inning_half"]).strip().lower()
        if inning_half == "top":
            return str(row["away_team"]).upper()
        if inning_half == "bot":
            return str(row["home_team"]).upper()
        return None

    valid["batting_team_code"] = valid.apply(map_team, axis=1)
    valid = valid[valid["batting_team_code"].notna()]

    if valid.empty:
        return "UNK"

    return valid["batting_team_code"].mode().iloc[0]


def main():
    if len(sys.argv) >= 3:
        batter_id = int(sys.argv[1])
        season = int(sys.argv[2])
    else:
        batter_id = 677800
        season = 2025

    df = get_hitter_season(engine, batter_id, season)

    if df.empty:
        print("No data found.")
        return

    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["pitch_type_plot"] = df["pitch_type"].fillna("UNK")
    df["in_zone"] = df["zone"].apply(classify_in_zone).astype(int)
    df["out_of_zone"] = (1 - df["in_zone"]).astype(int)
    df["is_chase_swing"] = (
        (df["in_zone"] == 0) & (df["is_swing"].fillna(0) == 1)
    ).astype(int)

    pitch_types = sorted(df["pitch_type_plot"].unique())
    color_map = get_pitch_colors(pitch_types)

    batter_name = (
        df["batter_name"].dropna().iloc[0]
        if "batter_name" in df.columns and df["batter_name"].notna().any()
        else str(batter_id)
    )

    team_code = infer_hitter_team_code(df)

    total_pitches_seen = len(df)
    total_games = df["game_pk"].nunique()
    swings = int(df["is_swing"].fillna(0).sum())
    whiffs = int(df["is_whiff"].fillna(0).sum())
    in_zone_total = int(df["in_zone"].fillna(0).sum())
    out_zone_total = total_pitches_seen - in_zone_total
    chase_swings_total = int(df["is_chase_swing"].fillna(0).sum())
    balls_in_play = int(df["is_in_play"].fillna(0).sum())

    zone_pct_total = round(100 * in_zone_total / total_pitches_seen, 1) if total_pitches_seen else 0
    swing_pct_total = round(100 * swings / total_pitches_seen, 1) if total_pitches_seen else 0
    whiff_pct_total = round(100 * whiffs / swings, 1) if swings else 0
    contact_pct_total = round(100 * (swings - whiffs) / swings, 1) if swings else 0
    chase_pct_total = round(100 * chase_swings_total / out_zone_total, 1) if out_zone_total else 0

    avg_ev = (
        round(df["launch_speed"].dropna().mean(), 1)
        if df["launch_speed"].notna().any()
        else None
    )
    max_ev = (
        round(df["launch_speed"].dropna().max(), 1)
        if df["launch_speed"].notna().any()
        else None
    )
    avg_la = (
        round(df["launch_angle"].dropna().mean(), 1)
        if df["launch_angle"].notna().any()
        else None
    )
    avg_velo_seen = (
        round(df["release_speed"].dropna().mean(), 1)
        if df["release_speed"].notna().any()
        else None
    )
    avg_xba = (
        round(df["estimated_ba_using_speedangle"].dropna().mean(), 3)
        if "estimated_ba_using_speedangle" in df.columns
        and df["estimated_ba_using_speedangle"].notna().any()
        else None
    )
    avg_xwoba = (
        round(df["estimated_woba_using_speedangle"].dropna().mean(), 3)
        if "estimated_woba_using_speedangle" in df.columns
        and df["estimated_woba_using_speedangle"].notna().any()
        else None
    )

    bip_df = df[df["is_in_play"] == 1].copy()

    pitch_summary = build_pitch_summary_hitter(df).sort_values(
        "pitches_seen",
        ascending=False,
    )
    pitch_summary = enrich_chase_by_pitch_type(df, pitch_summary)

    df_rhp = (
        df[df["pitcher_hand"] == "R"].copy()
        if "pitcher_hand" in df.columns
        else pd.DataFrame()
    )
    df_lhp = (
        df[df["pitcher_hand"] == "L"].copy()
        if "pitcher_hand" in df.columns
        else pd.DataFrame()
    )

    pitch_order_rhp = (
        df_rhp["pitch_type_plot"].value_counts().loc[lambda s: s >= 5].index.tolist()
        if not df_rhp.empty else []
    )
    pitch_order_lhp = (
        df_lhp["pitch_type_plot"].value_counts().loc[lambda s: s >= 5].index.tolist()
        if not df_lhp.empty else []
    )

    game_summary = build_game_trend_summary_hitter(df)

    fig = plt.figure(figsize=(18, 17))
    gs = fig.add_gridspec(
        5,
        2,
        height_ratios=[1.0, 1.05, 1.15, 1.15, 1.0],
        hspace=0.35,
        wspace=0.18,
    )

    ax_usage = fig.add_subplot(gs[0, 0])
    ax_evla = fig.add_subplot(gs[0, 1])
    ax_spray = fig.add_subplot(gs[1, 0])
    ax_trend = fig.add_subplot(gs[1, 1])
    rhp_spec = gs[2:4, 0]
    lhp_spec = gs[2:4, 1]
    ax_tbl = fig.add_subplot(gs[4, :])

    # Pitch mix seen
    ax_usage.bar(
        pitch_summary["pitch_type"],
        pitch_summary["pitches_seen"],
        color=[color_map.get(pt, "#666666") for pt in pitch_summary["pitch_type"]],
        edgecolor="black",
        linewidth=0.5,
    )
    ax_usage.set_title("Season Pitch Mix Seen")
    ax_usage.set_ylabel("Pitches Seen")
    ax_usage.set_xlabel("Pitch Type")
    ax_usage.grid(axis="y", alpha=0.15)

    # EV / LA
    ax_evla.set_title("Season Exit Velocity vs Launch Angle")
    valid_evla = bip_df[
        bip_df["launch_speed"].notna() & bip_df["launch_angle"].notna()
    ].copy()

    if not valid_evla.empty:
        ax_evla.scatter(
            valid_evla["launch_angle"],
            valid_evla["launch_speed"],
            s=42,
            alpha=0.65,
            edgecolors="black",
            linewidths=0.25,
        )
        ax_evla.set_xlim(-30, 60)
        ax_evla.set_ylim(50, 115)
    else:
        ax_evla.text(
            0.5,
            0.5,
            "No batted-ball EV/LA data",
            transform=ax_evla.transAxes,
            ha="center",
            va="center",
        )

    ax_evla.set_xlabel("Launch Angle")
    ax_evla.set_ylabel("Exit Velocity")
    ax_evla.grid(alpha=0.15)

    # Spray chart
    draw_spray_chart(ax_spray, bip_df, title="Season Spray Chart")

    # Rolling contact trend
    ax_trend.set_title("Rolling Contact by Game")
    ax_trend.set_xlabel("Game Sequence")
    ax_trend.set_ylabel("Contact %")

    if not game_summary.empty:
        ax_trend.plot(
            range(len(game_summary)),
            game_summary["contact_pct"],
            marker="o",
            linewidth=1.5,
            label="Game Contact%",
        )
        ax_trend.plot(
            range(len(game_summary)),
            game_summary["rolling_contact_5"],
            linewidth=2.5,
            label="Rolling 5G Contact%",
        )
        ax_trend.set_ylim(0, 100)
        ax_trend.grid(alpha=0.15)
        ax_trend.legend(loc="upper right")
    else:
        ax_trend.text(
            0.5,
            0.5,
            "No game trend data",
            transform=ax_trend.transAxes,
            ha="center",
            va="center",
        )
        ax_trend.grid(alpha=0.15)

    # Heatmaps
    add_pitchtype_heatmap_grid(fig, rhp_spec, df_rhp, pitch_order_rhp, "vs RHP", min_points=5)
    add_pitchtype_heatmap_grid(fig, lhp_spec, df_lhp, pitch_order_lhp, "vs LHP", min_points=5)

    # Table
    ax_tbl.axis("off")
    tbl_df = pitch_summary[
        [
            "pitch_type",
            "pitches_seen",
            "avg_velo_seen",
            "avg_ev",
            "max_ev",
            "avg_la",
            "swing_pct",
            "whiff_pct",
            "contact_pct",
            "chase_pct",
            "called_strike_pct",
            "in_play_pct",
            "zone_pct",
        ]
    ].copy()

    tbl_df = format_table_df(
        tbl_df,
        round_cols=[
            "avg_velo_seen",
            "avg_ev",
            "max_ev",
            "avg_la",
            "swing_pct",
            "whiff_pct",
            "contact_pct",
            "chase_pct",
            "called_strike_pct",
            "in_play_pct",
            "zone_pct",
        ],
    )

    col_labels = [
        "Type",
        "# Seen",
        "Avg Velo",
        "Avg EV",
        "Max EV",
        "Avg LA",
        "Swing%",
        "Whiff%",
        "Contact%",
        "Chase%",
        "Called K%",
        "In Play%",
        "Zone%",
    ]
    table = ax_tbl.table(
        cellText=tbl_df.values,
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.1, 1.45)
    ax_tbl.set_title("Season Pitch Type Results", pad=12)

    for row_idx, pitch_type in enumerate(tbl_df["pitch_type"], start=1):
        table[(row_idx, 0)].set_text_props(
            color=color_map.get(pitch_type, "#666666"),
            weight="bold",
        )

    title = f"{batter_name} Season Report | {season}"
    fig.suptitle(title, fontsize=22, y=0.985)

    summary_parts = [
        f"Games: {total_games}",
        f"Pitches Seen: {total_pitches_seen}",
        f"Swings: {swings}",
        f"Whiffs: {whiffs}",
        f"Balls in Play: {balls_in_play}",
        f"Swing%: {swing_pct_total}",
        f"Whiff%: {whiff_pct_total}",
        f"Contact%: {contact_pct_total}",
        f"Chase%: {chase_pct_total}",
        f"Zone% Seen: {zone_pct_total}",
    ]
    if avg_velo_seen is not None:
        summary_parts.append(f"Avg Velo Seen: {avg_velo_seen}")
    if avg_ev is not None:
        summary_parts.append(f"Avg EV: {avg_ev}")
    if max_ev is not None:
        summary_parts.append(f"Max EV: {max_ev}")
    if avg_la is not None:
        summary_parts.append(f"Avg LA: {avg_la}")
    if avg_xba is not None:
        summary_parts.append(f"Avg xBA: {avg_xba}")
    if avg_xwoba is not None:
        summary_parts.append(f"Avg xwOBA: {avg_xwoba}")

    summary_text = " | ".join(summary_parts)

    fig.text(
        0.5,
        0.948,
        summary_text,
        ha="center",
        va="center",
        fontsize=10.5,
        bbox=dict(
            boxstyle="round,pad=0.35",
            facecolor="white",
            edgecolor="gray",
            alpha=0.95,
        ),
    )

    fig.subplots_adjust(
        top=0.91,
        bottom=0.05,
        left=0.05,
        right=0.98,
        hspace=0.34,
        wspace=0.20,
    )

    output_file = get_output_path(
        report_family="player_overview",
        time_grain="season",
        side="hitting",
        team_code=team_code,
        season=season,
        player_name=batter_name,
        player_id=batter_id,
        output_format="png",
    )

    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print("Chart saved:", output_file)


if __name__ == "__main__":
    main()