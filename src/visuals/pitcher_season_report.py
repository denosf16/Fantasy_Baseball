from pathlib import Path
import sys
import urllib

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, Polygon
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "outputs" / "png"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SERVER = r"RAMSEY_BOLTON\SQLEXPRESS"
DATABASE = "fantasy_baseball"
DRIVER = "ODBC Driver 17 for SQL Server"

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

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
        linewidth=2.0,
        edgecolor="black",
    )
    ax.add_patch(strike_zone)

    for i in range(1, 3):
        ax.plot(
            [zone_left + i * (zone_width / 3), zone_left + i * (zone_width / 3)],
            [zone_bottom, zone_bottom + zone_height],
            color="black",
            linewidth=0.8,
            alpha=0.5,
        )
        ax.plot(
            [zone_left, zone_left + zone_width],
            [zone_bottom + i * (zone_height / 3), zone_bottom + i * (zone_height / 3)],
            color="black",
            linewidth=0.8,
            alpha=0.5,
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
        linewidth=2.0,
        edgecolor="black",
    )
    ax.add_patch(home_plate)

    ax.set_xlim(2.2, -2.2)
    ax.set_ylim(-0.5, 4.8)
    ax.set_xlabel("Plate X")
    ax.set_ylabel("Plate Z")
    ax.grid(alpha=0.15)


def build_pitch_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("pitch_type_plot", dropna=False)
        .agg(
            pitches=("pitch_type_plot", "size"),
            avg_velo=("release_speed", "mean"),
            max_velo=("release_speed", "max"),
            avg_ivb=("pfx_z_in", "mean"),
            avg_hb=("pfx_x_in", "mean"),
            whiffs=("is_whiff", "sum"),
            called_strikes=("is_called_strike", "sum"),
            fouls=("is_foul", "sum"),
            in_play=("is_in_play", "sum"),
            swings=("is_swing", "sum"),
            zone_pitches=("in_zone", "sum"),
        )
        .reset_index()
        .rename(columns={"pitch_type_plot": "pitch_type"})
    )

    summary["csw"] = summary["whiffs"] + summary["called_strikes"]
    summary["strike_events"] = (
        summary["whiffs"]
        + summary["called_strikes"]
        + summary["fouls"]
        + summary["in_play"]
    )
    summary["usage_pct"] = 100 * summary["pitches"] / summary["pitches"].sum()
    summary["strike_pct"] = 100 * summary["strike_events"] / summary["pitches"]
    summary["whiff_pct"] = 100 * summary["whiffs"] / summary["swings"].replace(0, pd.NA)
    summary["csw_pct"] = 100 * summary["csw"] / summary["pitches"]
    summary["zone_pct"] = 100 * summary["zone_pitches"] / summary["pitches"]
    summary["pitch_name"] = summary["pitch_type"].map(PITCH_NAME_MAP).fillna(summary["pitch_type"])

    return summary.sort_values(["pitches", "pitch_type"], ascending=[False, True]).reset_index(drop=True)


def format_table_df(tbl_df: pd.DataFrame) -> pd.DataFrame:
    out = tbl_df.copy()
    round_cols = [
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
    for col in round_cols:
        out[col] = out[col].apply(lambda x: "-" if pd.isna(x) else round(float(x), 1))
    return out


def main():
    if len(sys.argv) >= 3:
        pitcher_id = int(sys.argv[1])
        season = int(sys.argv[2])
    else:
        pitcher_id = 543135
        season = 2025

    query = f"""
    SELECT
        p.*
    FROM clean.pitches p
    WHERE p.pitcher_id = {pitcher_id}
      AND YEAR(p.game_date) = {season}
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        print("No data found.")
        return

    df = df.copy()
    df["pitch_type_plot"] = df["pitch_type"].fillna("UNK")
    df["in_zone"] = df["zone"].apply(classify_in_zone).astype(int)
    df["pfx_x_in"] = df["pfx_x"] * 12
    df["pfx_z_in"] = df["pfx_z"] * 12

    pitch_types = sorted(df["pitch_type_plot"].unique())
    color_map = get_pitch_colors(pitch_types)

    pitcher_name = df["pitcher_name"].dropna().iloc[0]
    total_pitches = len(df)
    total_games = df["game_pk"].nunique()
    whiffs = int(df["is_whiff"].fillna(0).sum())
    called_strikes = int(df["is_called_strike"].fillna(0).sum())
    csw = whiffs + called_strikes
    csw_pct = round(100 * csw / total_pitches, 1) if total_pitches else 0
    avg_velo = round(df["release_speed"].dropna().mean(), 1) if df["release_speed"].notna().any() else None
    zone_pct = round(100 * df["in_zone"].mean(), 1) if total_pitches else 0

    pitch_summary = build_pitch_summary(df)
    primary_pitch = pitch_summary.iloc[0]["pitch_type"] if not pitch_summary.empty else "UNK"

    df_lhb = df[df["batter_stand"] == "L"].copy()
    df_rhb = df[df["batter_stand"] == "R"].copy()

    # Rolling CSW by game
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
        (game_summary["whiffs"] + game_summary["called_strikes"]) / game_summary["pitches"]
    )
    game_summary["rolling_csw_5"] = game_summary["csw_pct"].rolling(5, min_periods=1).mean()

    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(3, 2, height_ratios=[1.0, 1.0, 1.0])

    ax_usage = fig.add_subplot(gs[0, 0])
    ax_move = fig.add_subplot(gs[0, 1])
    ax_lhb = fig.add_subplot(gs[1, 0])
    ax_rhb = fig.add_subplot(gs[1, 1])
    ax_trend = fig.add_subplot(gs[2, 0])
    ax_tbl = fig.add_subplot(gs[2, 1])

    # Pitch mix / usage
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
    for pt in pitch_types:
        sub = df[df["pitch_type_plot"] == pt]
        ax_move.scatter(
            sub["pfx_x_in"],
            sub["pfx_z_in"],
            color=color_map[pt],
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

    # Location vs LHB
    for pt in sorted(df_lhb["pitch_type_plot"].dropna().unique()):
        sub = df_lhb[df_lhb["pitch_type_plot"] == pt]
        ax_lhb.scatter(
            sub["plate_x"],
            sub["plate_z"],
            color=color_map[pt],
            s=45,
            alpha=0.55,
            edgecolors="black",
            linewidths=0.2,
        )
    draw_zone(ax_lhb)
    ax_lhb.set_title("Season Location vs LHB")

    # Location vs RHB
    for pt in sorted(df_rhb["pitch_type_plot"].dropna().unique()):
        sub = df_rhb[df_rhb["pitch_type_plot"] == pt]
        ax_rhb.scatter(
            sub["plate_x"],
            sub["plate_z"],
            color=color_map[pt],
            s=45,
            alpha=0.55,
            edgecolors="black",
            linewidths=0.2,
        )
    draw_zone(ax_rhb)
    ax_rhb.set_title("Season Location vs RHB")

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
    tbl_df = format_table_df(tbl_df)

    col_labels = ["Type", "#", "Usage%", "AvgV", "MaxV", "IVB", "HB", "Zone%", "Strike%", "Whiff%", "CSW%"]
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
        table[(row_idx, 0)].set_text_props(color=color_map.get(pitch_type, "#666666"), weight="bold")

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
        f"CSW%: {csw_pct}",
        f"Zone%: {zone_pct}",
    ]
    if avg_velo is not None:
        header_parts.append(f"Avg Velo: {avg_velo}")
    header_text = " | ".join(header_parts)

    fig.text(
        0.5,
        0.925,
        header_text,
        ha="center",
        va="center",
        fontsize=11,
        bbox=dict(boxstyle="round,pad=0.35", facecolor="white", edgecolor="gray", alpha=0.95),
    )

    plt.tight_layout(rect=[0, 0, 1, 0.90])

    output_file = OUT_DIR / f"pitcher_season_report_{pitcher_id}_{season}.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print("Chart saved:", output_file)


if __name__ == "__main__":
    main()