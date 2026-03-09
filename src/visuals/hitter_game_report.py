from pathlib import Path
import sys

import pandas as pd
import matplotlib.pyplot as plt

from src.db_connect import engine
from src.queries.statcast_queries import get_hitter_game
from src.utils.pitch_config import PITCH_NAME_MAP, get_pitch_colors
from src.utils.plot_helpers import draw_zone, draw_spray_chart
from src.utils.report_helpers import (
    classify_in_zone,
    build_title_date,
    build_pitch_summary_hitter,
    format_table_df,
)


BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "outputs" / "png" / "hitters" / "game"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    if len(sys.argv) >= 3:
        batter_id = int(sys.argv[1])
        game_pk = int(sys.argv[2])
    else:
        batter_id = 677800
        game_pk = 778553

    df = get_hitter_game(engine, batter_id, game_pk)

    if df.empty:
        print("No data found.")
        return

    df = df.copy()
    df["pitch_type_plot"] = df["pitch_type"].fillna("UNK")
    df["in_zone"] = df["zone"].apply(classify_in_zone).astype(int)

    pitch_types = sorted(df["pitch_type_plot"].unique())
    color_map = get_pitch_colors(pitch_types)

    batter_name = (
        df["batter_name"].dropna().iloc[0]
        if "batter_name" in df.columns and df["batter_name"].notna().any()
        else str(batter_id)
    )
    game_date = build_title_date(df["game_date"].iloc[0])

    total_pitches_seen = len(df)
    swings = int(df["is_swing"].fillna(0).sum())
    whiffs = int(df["is_whiff"].fillna(0).sum())
    called_strikes = int(df["is_called_strike"].fillna(0).sum())
    balls_in_play = int(df["is_in_play"].fillna(0).sum())
    zone_pct_total = round(100 * df["in_zone"].mean(), 1) if total_pitches_seen else 0
    swing_pct_total = round(100 * swings / total_pitches_seen, 1) if total_pitches_seen else 0
    whiff_pct_total = round(100 * whiffs / swings, 1) if swings else 0
    contact_pct_total = round(100 * (swings - whiffs) / swings, 1) if swings else 0

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
    pitch_summary = build_pitch_summary_hitter(df)

    fig = plt.figure(figsize=(15, 11))
    gs = fig.add_gridspec(3, 2, height_ratios=[1.0, 1.0, 0.95])

    ax_loc = fig.add_subplot(gs[0, 0])
    ax_evla = fig.add_subplot(gs[0, 1])
    ax_spray = fig.add_subplot(gs[1, 0])
    ax_summary = fig.add_subplot(gs[1, 1])
    ax_tbl = fig.add_subplot(gs[2, :])

    # Pitch location seen
    for pt in pitch_summary["pitch_type"]:
        sub = df[df["pitch_type_plot"] == pt]
        ax_loc.scatter(
            sub["plate_x"],
            sub["plate_z"],
            label=f"{PITCH_NAME_MAP.get(pt, pt)} ({len(sub)})",
            color=color_map.get(pt, "#666666"),
            s=70,
            alpha=0.8,
            edgecolors="black",
            linewidths=0.3,
        )

    draw_zone(ax_loc)
    ax_loc.set_title("Pitch Location Seen")
    ax_loc.set_xlabel("Plate X")
    ax_loc.set_ylabel("Plate Z")
    ax_loc.legend(title="Pitch Type", fontsize=8, loc="upper right")

    # EV / LA
    ax_evla.set_title("Exit Velocity vs Launch Angle")
    valid_evla = bip_df[
        bip_df["launch_speed"].notna() & bip_df["launch_angle"].notna()
    ].copy()

    if not valid_evla.empty:
        ax_evla.scatter(
            valid_evla["launch_angle"],
            valid_evla["launch_speed"],
            s=80,
            alpha=0.8,
            edgecolors="black",
            linewidths=0.3,
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

    # Real Statcast spray chart if hc_x / hc_y are present
    draw_spray_chart(ax_spray, bip_df, title="Spray Chart")

    # Game summary
    summary_box = pd.DataFrame(
        {
            "Metric": [
                "Pitches Seen",
                "Swings",
                "Swing%",
                "Whiffs",
                "Whiff%",
                "Contact%",
                "Called Strikes",
                "Balls in Play",
                "Zone%",
                "Avg xBA",
                "Avg xwOBA",
            ],
            "Value": [
                total_pitches_seen,
                swings,
                swing_pct_total,
                whiffs,
                whiff_pct_total,
                contact_pct_total,
                called_strikes,
                balls_in_play,
                zone_pct_total,
                avg_xba if avg_xba is not None else "-",
                avg_xwoba if avg_xwoba is not None else "-",
            ],
        }
    )

    ax_summary.axis("off")
    summary_table = ax_summary.table(
        cellText=summary_box.values,
        colLabels=summary_box.columns,
        loc="center",
        cellLoc="center",
    )
    summary_table.auto_set_font_size(False)
    summary_table.set_fontsize(10.5)
    summary_table.scale(1, 1.6)
    ax_summary.set_title("Game Summary")

    # Pitch-type results
    ax_tbl.axis("off")
    tbl_df = pitch_summary[
        [
            "pitch_type",
            "pitches_seen",
            "avg_velo_seen",
            "avg_ev",
            "avg_la",
            "swing_pct",
            "whiff_pct",
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
            "avg_la",
            "swing_pct",
            "whiff_pct",
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
        "Avg LA",
        "Swing%",
        "Whiff%",
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
    table.set_fontsize(10)
    table.scale(1, 1.6)
    ax_tbl.set_title("Pitch Type Results", pad=12)

    for row_idx, pitch_type in enumerate(tbl_df["pitch_type"], start=1):
        table[(row_idx, 0)].set_text_props(
            color=color_map.get(pitch_type, "#666666"),
            weight="bold",
        )

    title = f"{batter_name} Hitter Report\nGame {game_pk} | {game_date}"
    fig.suptitle(title, fontsize=22, y=0.985)

    header_parts = [
        f"Pitches Seen: {total_pitches_seen}",
        f"Swings: {swings}",
        f"Whiffs: {whiffs}",
        f"Called Strikes: {called_strikes}",
        f"Balls in Play: {balls_in_play}",
    ]
    if avg_ev is not None:
        header_parts.append(f"Avg EV: {avg_ev}")
    if max_ev is not None:
        header_parts.append(f"Max EV: {max_ev}")
    if avg_la is not None:
        header_parts.append(f"Avg LA: {avg_la}")

    header_text = " | ".join(header_parts)

    fig.text(
        0.5,
        0.93,
        header_text,
        ha="center",
        va="center",
        fontsize=11,
        bbox=dict(
            boxstyle="round,pad=0.35",
            facecolor="white",
            edgecolor="gray",
            alpha=0.95,
        ),
    )

    fig.subplots_adjust(
        top=0.88,
        bottom=0.05,
        left=0.05,
        right=0.98,
        hspace=0.35,
        wspace=0.20,
    )

    output_file = OUT_DIR / f"hitter_report_{batter_id}_{game_pk}.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print("Chart saved:", output_file)


if __name__ == "__main__":
    main()