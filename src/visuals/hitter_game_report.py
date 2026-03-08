from pathlib import Path
import sys

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, Polygon, Arc

from src.db_connect import engine
from src.queries.statcast_queries import get_hitter_game


BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "outputs" / "png" / "hitters" / "game"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PITCH_COLORS = {
    "FF": "#e41a1c",
    "SI": "#ff7f00",
    "FC": "#a65628",
    "SL": "#377eb8",
    "ST": "#6baed6",
    "CU": "#4daf4a",
    "CH": "#984ea3",
    "FS": "#f781bf",
    "KC": "#33a02c",
    "SV": "#1f78b4",
    "CS": "#b2df8a",
    "EP": "#999999",
    "KN": "#17becf",
    "FO": "#bc80bd",
    "SC": "#8dd3c7",
    "UNK": "#666666",
}

PITCH_NAME_MAP = {
    "FF": "4-Seam",
    "SI": "Sinker",
    "FC": "Cutter",
    "SL": "Slider",
    "ST": "Sweeper",
    "CU": "Curveball",
    "CH": "Changeup",
    "FS": "Splitter",
    "KC": "Knuckle Curve",
    "SV": "Slurve",
    "CS": "Slow Curve",
    "EP": "Eephus",
    "KN": "Knuckleball",
    "FO": "Forkball",
    "SC": "Screwball",
    "UNK": "Unknown",
}


def get_pitch_colors(pitch_types: list[str]) -> dict[str, str]:
    return {pt: PITCH_COLORS.get(pt, "#666666") for pt in pitch_types}


def build_title_date(value) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def classify_in_zone(zone_value) -> int:
    try:
        return int(zone_value) in range(1, 10)
    except Exception:
        return 0


def draw_zone(ax) -> None:
    zone_left = -0.83
    zone_bottom = 1.5
    zone_width = 1.66
    zone_height = 2.0

    strike_zone = Rectangle(
        (zone_left, zone_bottom),
        zone_width,
        zone_height,
        fill=False,
        linewidth=1.8,
        edgecolor="black",
    )
    ax.add_patch(strike_zone)

    for i in range(1, 3):
        ax.plot(
            [zone_left + i * (zone_width / 3), zone_left + i * (zone_width / 3)],
            [zone_bottom, zone_bottom + zone_height],
            color="black",
            linewidth=0.6,
            alpha=0.45,
        )
        ax.plot(
            [zone_left, zone_left + zone_width],
            [zone_bottom + i * (zone_height / 3), zone_bottom + i * (zone_height / 3)],
            color="black",
            linewidth=0.6,
            alpha=0.45,
        )

    home_plate = Polygon(
        [
            (-0.708, 0.1),
            (0.708, 0.1),
            (0.5, -0.15),
            (0.0, -0.30),
            (-0.5, -0.15),
        ],
        closed=True,
        fill=False,
        linewidth=1.6,
        edgecolor="black",
    )
    ax.add_patch(home_plate)

    ax.set_xlim(2.2, -2.2)
    ax.set_ylim(-0.5, 4.8)
    ax.grid(alpha=0.12)


def draw_spray_chart(ax, df_bip: pd.DataFrame) -> None:
    ax.set_title("Spray Chart")
    ax.set_aspect("equal", adjustable="box")

    infield_arc = Arc(
        (0, 0),
        180,
        180,
        theta1=45,
        theta2=135,
        linewidth=1.2,
        color="black",
    )
    outfield_arc = Arc(
        (0, 0),
        320,
        320,
        theta1=45,
        theta2=135,
        linewidth=1.2,
        color="black",
        alpha=0.6,
    )
    ax.add_patch(infield_arc)
    ax.add_patch(outfield_arc)

    ax.plot([0, -113], [0, 113], color="black", linewidth=1)
    ax.plot([0, 113], [0, 113], color="black", linewidth=1)

    if not df_bip.empty:
        has_hc = {"hc_x", "hc_y"}.issubset(df_bip.columns)
        if has_hc and df_bip["hc_x"].notna().any() and df_bip["hc_y"].notna().any():
            ax.scatter(
                df_bip["hc_x"],
                df_bip["hc_y"],
                s=70,
                alpha=0.75,
                edgecolors="black",
                linewidths=0.3,
            )
        elif "hit_distance_sc" in df_bip.columns and df_bip["hit_distance_sc"].notna().any():
            temp = df_bip.copy()
            n = len(temp)
            temp["plot_x"] = pd.Series(range(n)).apply(
                lambda i: (-1) ** i * (20 + (i % 8) * 10)
            )
            temp["plot_y"] = temp["hit_distance_sc"].fillna(0)
            ax.scatter(
                temp["plot_x"],
                temp["plot_y"],
                s=70,
                alpha=0.75,
                edgecolors="black",
                linewidths=0.3,
            )
        else:
            ax.text(
                0.5,
                0.5,
                "No spray coordinates available",
                transform=ax.transAxes,
                ha="center",
                va="center",
            )

    ax.set_xlim(-130, 130)
    ax.set_ylim(-10, 330)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.grid(alpha=0.1)


def build_pitch_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("pitch_type_plot", dropna=False)
        .agg(
            pitches_seen=("pitch_type_plot", "size"),
            swings=("is_swing", "sum"),
            whiffs=("is_whiff", "sum"),
            called_strikes=("is_called_strike", "sum"),
            balls_in_play=("is_in_play", "sum"),
            avg_velo_seen=("release_speed", "mean"),
            avg_ev=("launch_speed", "mean"),
            avg_la=("launch_angle", "mean"),
            zone_seen=("in_zone", "sum"),
        )
        .reset_index()
        .rename(columns={"pitch_type_plot": "pitch_type"})
    )

    summary["swing_pct"] = 100 * summary["swings"] / summary["pitches_seen"]
    summary["whiff_pct"] = 100 * summary["whiffs"] / summary["swings"].replace(0, pd.NA)
    summary["called_strike_pct"] = 100 * summary["called_strikes"] / summary["pitches_seen"]
    summary["in_play_pct"] = 100 * summary["balls_in_play"] / summary["pitches_seen"]
    summary["zone_pct"] = 100 * summary["zone_seen"] / summary["pitches_seen"]
    summary["pitch_name"] = (
        summary["pitch_type"].map(PITCH_NAME_MAP).fillna(summary["pitch_type"])
    )

    return summary.sort_values(
        ["pitches_seen", "pitch_type"], ascending=[False, True]
    ).reset_index(drop=True)


def format_table_df(tbl_df: pd.DataFrame) -> pd.DataFrame:
    out = tbl_df.copy()
    round_cols = [
        "avg_velo_seen",
        "avg_ev",
        "avg_la",
        "swing_pct",
        "whiff_pct",
        "called_strike_pct",
        "in_play_pct",
        "zone_pct",
    ]
    for col in round_cols:
        out[col] = out[col].apply(lambda x: "-" if pd.isna(x) else round(float(x), 1))
    return out


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

    bip_df = df[df["is_in_play"] == 1].copy()
    pitch_summary = build_pitch_summary(df)

    fig = plt.figure(figsize=(15, 11))
    gs = fig.add_gridspec(3, 2, height_ratios=[1.0, 1.0, 0.95])

    ax_loc = fig.add_subplot(gs[0, 0])
    ax_evla = fig.add_subplot(gs[0, 1])
    ax_spray = fig.add_subplot(gs[1, 0])
    ax_zone = fig.add_subplot(gs[1, 1])
    ax_tbl = fig.add_subplot(gs[2, :])

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

    ax_evla.set_title("Exit Velocity vs Launch Angle")
    if (
        not bip_df.empty
        and bip_df["launch_speed"].notna().any()
        and bip_df["launch_angle"].notna().any()
    ):
        ax_evla.scatter(
            bip_df["launch_angle"],
            bip_df["launch_speed"],
            s=80,
            alpha=0.8,
            edgecolors="black",
            linewidths=0.3,
        )
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

    draw_spray_chart(ax_spray, bip_df)

    summary_box = pd.DataFrame(
        {
            "Metric": [
                "Pitches Seen",
                "Swings",
                "Whiffs",
                "Called Strikes",
                "Balls in Play",
                "Zone%",
            ],
            "Value": [
                total_pitches_seen,
                swings,
                whiffs,
                called_strikes,
                balls_in_play,
                zone_pct_total,
            ],
        }
    )
    ax_zone.axis("off")
    zone_table = ax_zone.table(
        cellText=summary_box.values,
        colLabels=summary_box.columns,
        loc="center",
        cellLoc="center",
    )
    zone_table.auto_set_font_size(False)
    zone_table.set_fontsize(11)
    zone_table.scale(1, 1.8)
    ax_zone.set_title("Game Summary")

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
    tbl_df = format_table_df(tbl_df)

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