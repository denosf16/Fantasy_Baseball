from pathlib import Path
import sys

import pandas as pd
import matplotlib.pyplot as plt

from src.db_connect import engine
from src.queries.statcast_queries import get_pitcher_season
from src.utils.pitch_config import PITCH_NAME_MAP, get_pitch_colors
from src.utils.report_helpers import (
    classify_in_zone,
    build_pitch_summary_pitcher,
    format_table_df,
    add_pitchtype_heatmap_grid,
)
from src.utils.output_router import get_output_path


BASE_DIR = Path(__file__).resolve().parents[2]


def infer_pitcher_team_code(df: pd.DataFrame) -> str:
    """
    Infer the pitcher's team code across a season sample.

    For a pitcher:
      - Top inning  -> home team is pitching
      - Bottom inning -> away team is pitching

    We take the most common inferred pitching team in the dataframe.
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
            return str(row["home_team"]).upper()
        if inning_half == "bot":
            return str(row["away_team"]).upper()
        return None

    valid["pitching_team_code"] = valid.apply(map_team, axis=1)
    valid = valid[valid["pitching_team_code"].notna()]

    if valid.empty:
        return "UNK"

    return valid["pitching_team_code"].mode().iloc[0]


def main():
    if len(sys.argv) >= 3:
        pitcher_id = int(sys.argv[1])
        season = int(sys.argv[2])
    else:
        pitcher_id = 543135
        season = 2025

    df = get_pitcher_season(engine, pitcher_id, season)

    if df.empty:
        print("No data found.")
        return

    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["pitch_type_plot"] = df["pitch_type"].fillna("UNK")
    df["in_zone"] = df["zone"].apply(classify_in_zone).astype(int)
    df["pfx_x_in"] = pd.to_numeric(df["pfx_x"], errors="coerce") * 12
    df["pfx_z_in"] = pd.to_numeric(df["pfx_z"], errors="coerce") * 12

    pitch_summary = build_pitch_summary_pitcher(df)
    pitch_order = pitch_summary["pitch_type"].tolist()

    pitch_types = sorted(df["pitch_type_plot"].unique())
    color_map = get_pitch_colors(pitch_types)

    pitcher_name = (
        df["pitcher_name"].dropna().iloc[0]
        if "pitcher_name" in df.columns and df["pitcher_name"].notna().any()
        else str(pitcher_id)
    )

    team_code = infer_pitcher_team_code(df)

    total_pitches = len(df)
    total_games = df["game_pk"].nunique()
    whiffs = int(df["is_whiff"].fillna(0).sum())
    called_strikes = int(df["is_called_strike"].fillna(0).sum())
    balls_in_play = int(df["is_in_play"].fillna(0).sum())
    csw = whiffs + called_strikes
    csw_pct = round(100 * csw / total_pitches, 1) if total_pitches else 0
    avg_velo = (
        round(df["release_speed"].dropna().mean(), 1)
        if df["release_speed"].notna().any()
        else None
    )
    avg_spin = (
        round(df["release_spin_rate"].dropna().mean(), 0)
        if "release_spin_rate" in df.columns and df["release_spin_rate"].notna().any()
        else None
    )
    avg_extension = (
        round(df["release_extension"].dropna().mean(), 2)
        if "release_extension" in df.columns and df["release_extension"].notna().any()
        else None
    )
    avg_spin_axis = (
        round(df["spin_axis"].dropna().mean(), 0)
        if "spin_axis" in df.columns and df["spin_axis"].notna().any()
        else None
    )
    zone_pct = round(100 * df["in_zone"].mean(), 1) if total_pitches else 0

    primary_pitch = pitch_summary.iloc[0]["pitch_type"] if not pitch_summary.empty else "UNK"

    df_lhb = df[df["batter_stand"] == "L"].copy()
    df_rhb = df[df["batter_stand"] == "R"].copy()

    game_summary = (
        df.groupby("game_pk")
        .agg(
            game_date=("game_date", "min"),
            pitches=("pitch_event_id", "size"),
            whiffs=("is_whiff", "sum"),
            called_strikes=("is_called_strike", "sum"),
        )
        .reset_index()
        .sort_values(["game_date", "game_pk"])
    )
    game_summary["csw_pct"] = 100 * (
        (game_summary["whiffs"] + game_summary["called_strikes"])
        / game_summary["pitches"]
    )
    game_summary["rolling_csw_5"] = game_summary["csw_pct"].rolling(5, min_periods=1).mean()

    fig = plt.figure(figsize=(18, 16))
    gs = fig.add_gridspec(
        4,
        2,
        height_ratios=[1.0, 1.15, 1.15, 0.95],
        hspace=0.35,
        wspace=0.18,
    )

    ax_usage = fig.add_subplot(gs[0, 0])
    ax_move = fig.add_subplot(gs[0, 1])
    lhb_spec = gs[1:3, 0]
    rhb_spec = gs[1:3, 1]
    ax_trend = fig.add_subplot(gs[3, 0])
    ax_tbl = fig.add_subplot(gs[3, 1])

    # Pitch mix
    ax_usage.bar(
        pitch_summary["pitch_type"],
        pitch_summary["usage_pct"],
        color=[color_map.get(pt, "#666666") for pt in pitch_summary["pitch_type"]],
        edgecolor="black",
        linewidth=0.5,
    )
    ax_usage.set_title("Season Pitch Mix")
    ax_usage.set_ylabel("Usage %")
    ax_usage.set_xlabel("Pitch Type")
    ax_usage.grid(axis="y", alpha=0.15)

    # Movement
    for pt in pitch_order:
        sub = df[df["pitch_type_plot"] == pt]
        ax_move.scatter(
            sub["pfx_x_in"],
            sub["pfx_z_in"],
            color=color_map.get(pt, "#666666"),
            s=45,
            alpha=0.55,
            edgecolors="black",
            linewidths=0.2,
        )

    centroids = df.groupby("pitch_type_plot")[["pfx_x_in", "pfx_z_in"]].mean()
    for pt, row in centroids.iterrows():
        ax_move.text(
            row["pfx_x_in"],
            row["pfx_z_in"],
            pt,
            fontsize=9,
            weight="bold",
            ha="center",
            va="center",
            color="black",
        )

    ax_move.axhline(0, color="black", linewidth=1)
    ax_move.axvline(0, color="black", linewidth=1)
    ax_move.set_title("Movement Clusters")
    ax_move.set_xlabel("Horizontal Break (in.)")
    ax_move.set_ylabel("Induced Vertical Break (in.)")
    ax_move.set_xlim(-25, 25)
    ax_move.set_ylim(-25, 25)
    ax_move.set_aspect("equal", adjustable="box")
    ax_move.grid(alpha=0.15)

    # Heatmaps
    add_pitchtype_heatmap_grid(fig, lhb_spec, df_lhb, pitch_order, "vs LHB", min_points=3)
    add_pitchtype_heatmap_grid(fig, rhb_spec, df_rhb, pitch_order, "vs RHB", min_points=3)

    # Rolling CSW
    ax_trend.plot(
        range(len(game_summary)),
        game_summary["csw_pct"],
        marker="o",
        linewidth=1.5,
        label="Game CSW%",
    )
    ax_trend.plot(
        range(len(game_summary)),
        game_summary["rolling_csw_5"],
        linewidth=2.5,
        label="Rolling 5G CSW%",
    )
    ax_trend.set_title("Rolling CSW% by Game")
    ax_trend.set_xlabel("Game Sequence")
    ax_trend.set_ylabel("CSW%")
    ax_trend.grid(alpha=0.15)
    ax_trend.legend()

    # Table
    ax_tbl.axis("off")
    tbl_df = pitch_summary[
        [
            "pitch_type",
            "pitches",
            "usage_pct",
            "avg_velo",
            "max_velo",
            "avg_ivb",
            "avg_hb",
            "zone_pct",
            "strike_pct",
            "whiff_pct",
            "csw_pct",
        ]
    ].copy()

    tbl_df = format_table_df(
        tbl_df,
        round_cols=[
            "usage_pct",
            "avg_velo",
            "max_velo",
            "avg_ivb",
            "avg_hb",
            "zone_pct",
            "strike_pct",
            "whiff_pct",
            "csw_pct",
        ],
    )

    col_labels = [
        "Type",
        "#",
        "Usage%",
        "AvgV",
        "MaxV",
        "IVB",
        "HB",
        "Zone%",
        "Strike%",
        "Whiff%",
        "CSW%",
    ]
    table = ax_tbl.table(
        cellText=tbl_df.values,
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.05, 1.5)
    ax_tbl.set_title("Season Pitch Metrics", pad=12)

    for row_idx, pitch_type in enumerate(tbl_df["pitch_type"], start=1):
        table[(row_idx, 0)].set_text_props(
            color=color_map.get(pitch_type, "#666666"),
            weight="bold",
        )

    title = f"{pitcher_name} Season Report | {season}"
    fig.suptitle(title, fontsize=22, y=0.985)

    fig.text(
        0.5,
        0.955,
        f"Primary Pitch: {PITCH_NAME_MAP.get(primary_pitch, primary_pitch)}",
        ha="center",
        va="center",
        fontsize=12,
    )

    header_parts = [
        f"Games: {total_games}",
        f"Pitches: {total_pitches}",
        f"Whiffs: {whiffs}",
        f"Called Strikes: {called_strikes}",
        f"BIP: {balls_in_play}",
        f"CSW%: {csw_pct}",
        f"Zone%: {zone_pct}",
    ]
    if avg_velo is not None:
        header_parts.append(f"Avg Velo: {avg_velo}")
    if avg_spin is not None:
        header_parts.append(f"Avg Spin: {int(avg_spin)}")
    if avg_extension is not None:
        header_parts.append(f"Ext: {avg_extension}")
    if avg_spin_axis is not None:
        header_parts.append(f"Axis: {int(avg_spin_axis)}")

    header_text = " | ".join(header_parts)

    fig.text(
        0.5,
        0.925,
        header_text,
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
        top=0.90,
        bottom=0.05,
        left=0.05,
        right=0.98,
        hspace=0.34,
        wspace=0.22,
    )

    output_file = get_output_path(
        report_family="player_overview",
        time_grain="season",
        side="pitching",
        team_code=team_code,
        season=season,
        player_name=pitcher_name,
        player_id=pitcher_id,
        output_format="png",
    )

    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print("Chart saved:", output_file)


if __name__ == "__main__":
    main()