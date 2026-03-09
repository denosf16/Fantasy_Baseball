from pathlib import Path
import sys

import pandas as pd
import matplotlib.pyplot as plt

from src.db_connect import engine
from src.queries.statcast_queries import get_pitcher_game
from src.utils.pitch_config import PITCH_NAME_MAP, get_pitch_colors
from src.utils.plot_helpers import draw_zone
from src.utils.report_helpers import (
    classify_in_zone,
    build_title_date,
    build_pitch_summary_pitcher,
    format_table_df,
    add_pitchtype_heatmap_grid,
)


BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "outputs" / "png" / "pitchers" / "game"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    if len(sys.argv) >= 3:
        pitcher_id = int(sys.argv[1])
        game_pk = int(sys.argv[2])
    else:
        pitcher_id = 543135
        game_pk = 778553

    df = get_pitcher_game(engine, pitcher_id, game_pk)

    if df.empty:
        print("No data found.")
        return

    df = df.copy()
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
    game_date = build_title_date(df["game_date"].iloc[0])

    total_pitches = len(df)
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

    df_lhb = df[df["batter_stand"] == "L"].copy()
    df_rhb = df[df["batter_stand"] == "R"].copy()

    usage_split = (
        df.groupby(["pitch_type_plot", "batter_stand"])
        .size()
        .unstack(fill_value=0)
        .reindex(index=pitch_summary["pitch_type"].tolist(), fill_value=0)
    )
    for col in ["L", "R"]:
        if col not in usage_split.columns:
            usage_split[col] = 0

    primary_pitch = pitch_summary.iloc[0]["pitch_type"] if not pitch_summary.empty else "UNK"

    fig = plt.figure(figsize=(16, 16))
    gs = fig.add_gridspec(
        4,
        2,
        height_ratios=[1.0, 1.15, 1.15, 0.95],
        hspace=0.35,
        wspace=0.18,
    )

    ax_loc = fig.add_subplot(gs[0, 0])
    ax_move = fig.add_subplot(gs[0, 1])
    lhb_spec = gs[1:3, 0]
    rhb_spec = gs[1:3, 1]
    ax_tbl = fig.add_subplot(gs[3, :])

    # Pitch location
    for pt in pitch_order:
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
    ax_loc.set_title("Pitch Location")
    ax_loc.set_xlabel("Plate X")
    ax_loc.set_ylabel("Plate Z")
    ax_loc.legend(title="Pitch Type", fontsize=8, loc="upper right")

    # Movement clusters
    for pt in pitch_order:
        sub = df[df["pitch_type_plot"] == pt]
        ax_move.scatter(
            sub["pfx_x_in"],
            sub["pfx_z_in"],
            color=color_map.get(pt, "#666666"),
            s=70,
            alpha=0.8,
            edgecolors="black",
            linewidths=0.3,
        )

    ax_move.axhline(0, color="black", linewidth=1)
    ax_move.axvline(0, color="black", linewidth=1)
    ax_move.set_title("Movement")
    ax_move.set_xlabel("Horizontal Break (in.)")
    ax_move.set_ylabel("Induced Vertical Break (in.)")
    ax_move.set_xlim(-25, 25)
    ax_move.set_ylim(-25, 25)
    ax_move.set_aspect("equal", adjustable="box")
    ax_move.grid(alpha=0.15)

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

    # Heatmaps by handedness
    add_pitchtype_heatmap_grid(fig, lhb_spec, df_lhb, pitch_order, "vs LHB", min_points=3)
    add_pitchtype_heatmap_grid(fig, rhb_spec, df_rhb, pitch_order, "vs RHB", min_points=3)

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
    table.set_fontsize(9.5)
    table.scale(1, 1.55)
    ax_tbl.set_title("Pitch Type Metrics", pad=12)

    for row_idx, pitch_type in enumerate(tbl_df["pitch_type"], start=1):
        table[(row_idx, 0)].set_text_props(
            color=color_map.get(pitch_type, "#666666"),
            weight="bold",
        )

    usage_lines = []
    for pitch_type in pitch_summary["pitch_type"]:
        l_count = int(usage_split.loc[pitch_type, "L"]) if pitch_type in usage_split.index else 0
        r_count = int(usage_split.loc[pitch_type, "R"]) if pitch_type in usage_split.index else 0
        l_total = max(len(df_lhb), 1)
        r_total = max(len(df_rhb), 1)
        l_pct = round(100 * l_count / l_total, 1) if len(df_lhb) else 0.0
        r_pct = round(100 * r_count / r_total, 1) if len(df_rhb) else 0.0
        usage_lines.append(f"{pitch_type}: vs LHB {l_pct}% | vs RHB {r_pct}%")

    title = f"{pitcher_name} Report\nGame {game_pk} | {game_date}"
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

    fig.text(
        0.5,
        0.895,
        "   ".join(usage_lines[:4]),
        ha="center",
        va="center",
        fontsize=9,
    )

    fig.subplots_adjust(
        top=0.88,
        bottom=0.05,
        left=0.05,
        right=0.98,
        hspace=0.38,
        wspace=0.20,
    )

    output_file = OUT_DIR / f"pitcher_report_{pitcher_id}_{game_pk}.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print("Chart saved:", output_file)


if __name__ == "__main__":
    main()