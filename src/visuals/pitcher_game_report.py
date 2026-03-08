from pathlib import Path
import sys
import urllib

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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


def draw_kde(ax, df: pd.DataFrame, title: str) -> None:
    if df.empty or df["plate_x"].dropna().empty or df["plate_z"].dropna().empty:
        draw_zone(ax)
        ax.set_title(title)
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        return

    sns.kdeplot(
        data=df,
        x="plate_x",
        y="plate_z",
        fill=True,
        thresh=0.05,
        levels=20,
        cmap="coolwarm",
        alpha=0.9,
        bw_adjust=1.0,
        ax=ax,
    )

    draw_zone(ax)
    ax.set_title(title)


def build_title_date(value) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def classify_in_zone(zone_value) -> int:
    try:
        return int(zone_value) in range(1, 10)
    except Exception:
        return 0


def build_pitch_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("pitch_type_plot", dropna=False)
        .agg(
            pitches=("pitch_type_plot", "size"),
            avg_velo=("release_speed", "mean"),
            avg_ivb=("pfx_z_in", "mean"),
            avg_hb=("pfx_x_in", "mean"),
            whiffs=("is_whiff", "sum"),
            called_strikes=("is_called_strike", "sum"),
            balls=("is_ball", "sum"),
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
        game_pk = int(sys.argv[2])
    else:
        pitcher_id = 543135
        game_pk = 778553

    query = f"""
        SELECT *
        FROM mart.pitcher_game_chart_input
        WHERE pitcher_id = {pitcher_id}
          AND game_pk = {game_pk}
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
    game_date = build_title_date(df["game_date"].iloc[0])

    total_pitches = len(df)
    whiffs = int(df["is_whiff"].fillna(0).sum())
    called_strikes = int(df["is_called_strike"].fillna(0).sum())
    csw = whiffs + called_strikes
    csw_pct = round(100 * csw / total_pitches, 1) if total_pitches else 0
    avg_velo = round(df["release_speed"].dropna().mean(), 1) if df["release_speed"].notna().any() else None
    zone_pct = round(100 * df["in_zone"].mean(), 1) if total_pitches else 0

    pitch_summary = build_pitch_summary(df)

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

    fig = plt.figure(figsize=(15, 11))
    gs = fig.add_gridspec(3, 2, height_ratios=[1.05, 1.05, 0.95])

    ax_loc = fig.add_subplot(gs[0, 0])
    ax_move = fig.add_subplot(gs[0, 1])
    ax_lhb = fig.add_subplot(gs[1, 0])
    ax_rhb = fig.add_subplot(gs[1, 1])
    ax_tbl = fig.add_subplot(gs[2, :])

    # Overall location scatter stays as scatter
    for pt in pitch_types:
        sub = df[df["pitch_type_plot"] == pt]
        ax_loc.scatter(
            sub["plate_x"],
            sub["plate_z"],
            label=f"{PITCH_NAME_MAP.get(pt, pt)} ({len(sub)})",
            color=color_map[pt],
            s=80,
            alpha=0.8,
            edgecolors="black",
            linewidths=0.3,
        )
    draw_zone(ax_loc)
    ax_loc.set_title("Pitch Location")
    ax_loc.legend(title="Pitch Type", fontsize=9, loc="upper right")

    # Movement
    for pt in pitch_types:
        sub = df[df["pitch_type_plot"] == pt]
        ax_move.scatter(
            sub["pfx_x_in"],
            sub["pfx_z_in"],
            color=color_map[pt],
            s=80,
            alpha=0.8,
            edgecolors="black",
            linewidths=0.3,
            label=pt,
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

    # Replace vs handedness scatterplots with KDE heatmaps
    draw_kde(ax_lhb, df_lhb, "Location Density vs LHB")
    draw_kde(ax_rhb, df_rhb, "Location Density vs RHB")

    ax_tbl.axis("off")
    tbl_df = pitch_summary[
        [
            "pitch_type",
            "pitches",
            "usage_pct",
            "avg_velo",
            "avg_ivb",
            "avg_hb",
            "zone_pct",
            "strike_pct",
            "whiff_pct",
            "csw_pct",
        ]
    ].copy()
    tbl_df = format_table_df(tbl_df)

    col_labels = ["Type", "#", "Usage%", "Velo", "IVB", "HB", "Zone%", "Strike%", "Whiff%", "CSW%"]
    table = ax_tbl.table(
        cellText=tbl_df.values,
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.6)
    ax_tbl.set_title("Pitch Type Metrics", pad=12)

    for row_idx, pitch_type in enumerate(tbl_df["pitch_type"], start=1):
        table[(row_idx, 0)].set_text_props(color=color_map.get(pitch_type, "#666666"), weight="bold")

    usage_lines = []
    for pitch_type in pitch_summary["pitch_type"]:
        l_count = int(usage_split.loc[pitch_type, "L"]) if pitch_type in usage_split.index else 0
        r_count = int(usage_split.loc[pitch_type, "R"]) if pitch_type in usage_split.index else 0
        l_total = max(len(df_lhb), 1)
        r_total = max(len(df_rhb), 1)
        l_pct = round(100 * l_count / l_total, 1) if len(df_lhb) else 0.0
        r_pct = round(100 * r_count / r_total, 1) if len(df_rhb) else 0.0
        usage_lines.append(f"{pitch_type}: vs LHB {l_pct}% | vs RHB {r_pct}%")

    primary_pitch = pitch_summary.iloc[0]["pitch_type"] if not pitch_summary.empty else "UNK"

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

    fig.text(
        0.5,
        0.895,
        "   ".join(usage_lines[:4]),
        ha="center",
        va="center",
        fontsize=9,
    )

    plt.tight_layout(rect=[0, 0, 1, 0.87])

    output_file = OUT_DIR / f"pitcher_report_{pitcher_id}_{game_pk}.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print("Chart saved:", output_file)


if __name__ == "__main__":
    main()