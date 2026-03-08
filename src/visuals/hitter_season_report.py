from pathlib import Path
import math
import sys

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Rectangle, Polygon

from src.db_connect import engine
from src.queries.statcast_queries import get_hitter_season


BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "outputs" / "png"
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
        linewidth=1.6,
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
        linewidth=1.4,
        edgecolor="black",
    )
    ax.add_patch(home_plate)

    ax.set_xlim(2.2, -2.2)
    ax.set_ylim(-0.5, 4.8)
    ax.grid(alpha=0.12)


def draw_kde(ax, df: pd.DataFrame, title: str) -> None:
    draw_zone(ax)

    if df.empty or df["plate_x"].dropna().empty or df["plate_z"].dropna().empty:
        ax.set_title(title, fontsize=10)
        ax.text(
            0.5,
            0.5,
            "No data",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=9,
        )
        return

    if len(df) < 5 or df["plate_x"].nunique() < 2 or df["plate_z"].nunique() < 2:
        ax.set_title(title, fontsize=10)
        ax.text(
            0.5,
            0.5,
            "Not enough data",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=9,
        )
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
        warn_singular=False,
        ax=ax,
    )

    draw_zone(ax)
    ax.set_title(title, fontsize=10)


def build_pitch_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("pitch_type_plot", dropna=False)
        .agg(
            pitches_seen=("pitch_type_plot", "size"),
            swings=("is_swing", "sum"),
            whiffs=("is_whiff", "sum"),
            called_strikes=("is_called_strike", "sum"),
            balls_in_play=("is_in_play", "sum"),
            zone_seen=("in_zone", "sum"),
            avg_velo_seen=("release_speed", "mean"),
            avg_ev=("launch_speed", "mean"),
            max_ev=("launch_speed", "max"),
            avg_la=("launch_angle", "mean"),
        )
        .reset_index()
        .rename(columns={"pitch_type_plot": "pitch_type"})
    )

    summary["swing_pct"] = 100 * summary["swings"] / summary["pitches_seen"]
    summary["whiff_pct"] = 100 * summary["whiffs"] / summary["swings"].replace(0, float("nan"))
    summary["called_strike_pct"] = 100 * summary["called_strikes"] / summary["pitches_seen"]
    summary["in_play_pct"] = 100 * summary["balls_in_play"] / summary["pitches_seen"]
    summary["zone_pct"] = 100 * summary["zone_seen"] / summary["pitches_seen"]
    summary["contact_pct"] = 100 * (
        (summary["swings"] - summary["whiffs"])
        / summary["swings"].replace(0, float("nan"))
    )
    summary["out_zone_seen"] = summary["pitches_seen"] - summary["zone_seen"]
    summary["pitch_name"] = summary["pitch_type"].map(PITCH_NAME_MAP).fillna(summary["pitch_type"])

    return summary.sort_values(
        ["pitches_seen", "pitch_type"], ascending=[False, True]
    ).reset_index(drop=True)


def enrich_chase_by_pitch_type(df: pd.DataFrame, pitch_summary: pd.DataFrame) -> pd.DataFrame:
    chase_df = (
        df.groupby("pitch_type_plot", dropna=False)
        .agg(
            chase_swings=("is_chase_swing", "sum"),
            out_zone_seen=("out_of_zone", "sum"),
        )
        .reset_index()
        .rename(columns={"pitch_type_plot": "pitch_type"})
    )

    merged = pitch_summary.merge(chase_df, on="pitch_type", how="left")
    merged["chase_swings"] = merged["chase_swings"].fillna(0)
    merged["out_zone_seen"] = merged["out_zone_seen_y"].fillna(
        merged["out_zone_seen_x"]
    ) if "out_zone_seen_y" in merged.columns else merged["out_zone_seen"]
    if "out_zone_seen_x" in merged.columns:
        merged = merged.drop(columns=["out_zone_seen_x"])
    if "out_zone_seen_y" in merged.columns:
        merged = merged.drop(columns=["out_zone_seen_y"])

    merged["chase_pct"] = 100 * merged["chase_swings"] / merged["out_zone_seen"].replace(0, float("nan"))
    return merged


def format_table_df(tbl_df: pd.DataFrame) -> pd.DataFrame:
    out = tbl_df.copy()
    round_cols = [
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
    for col in round_cols:
        out[col] = out[col].apply(lambda x: "-" if pd.isna(x) else round(float(x), 1))
    return out


def add_pitchtype_heatmap_grid(
    fig,
    subspec,
    handed_df: pd.DataFrame,
    pitch_order: list[str],
    side_label: str,
) -> None:
    if not pitch_order:
        ax = fig.add_subplot(subspec)
        ax.axis("off")
        ax.set_title(f"Pitch Location Seen {side_label} by Pitch Type", fontsize=12, pad=8)
        ax.text(0.5, 0.5, "Not enough data", ha="center", va="center", transform=ax.transAxes)
        return

    n = len(pitch_order)
    ncols = min(3, max(1, n))
    nrows = math.ceil(n / ncols)
    subgs = subspec.subgridspec(nrows, ncols, wspace=0.18, hspace=0.30)

    for idx, pitch_type in enumerate(pitch_order):
        r = idx // ncols
        c = idx % ncols
        ax = fig.add_subplot(subgs[r, c])

        sub = handed_df[handed_df["pitch_type_plot"] == pitch_type].copy()
        title = f"{pitch_type} | {PITCH_NAME_MAP.get(pitch_type, pitch_type)}\nN={len(sub)}"
        draw_kde(ax, sub, title)

        if r < nrows - 1:
            ax.set_xlabel("")
        else:
            ax.set_xlabel("plate_x", fontsize=9)

        if c > 0:
            ax.set_ylabel("")
        else:
            ax.set_ylabel("plate_z", fontsize=9)

    total_slots = nrows * ncols
    for idx in range(n, total_slots):
        r = idx // ncols
        c = idx % ncols
        ax = fig.add_subplot(subgs[r, c])
        ax.axis("off")

    title_ax = fig.add_subplot(subspec)
    title_ax.axis("off")
    title_ax.set_title(
        f"Pitch Location Seen {side_label} by Pitch Type",
        fontsize=12,
        pad=8,
    )


def build_game_trend_summary(df: pd.DataFrame) -> pd.DataFrame:
    game_summary = (
        df.groupby(["game_pk", "game_date"], dropna=False)
        .agg(
            pitches_seen=("pitch_event_id", "size"),
            swings=("is_swing", "sum"),
            whiffs=("is_whiff", "sum"),
            avg_ev=("launch_speed", "mean"),
        )
        .reset_index()
        .sort_values(["game_date", "game_pk"])
    )

    game_summary["swings"] = pd.to_numeric(game_summary["swings"], errors="coerce")
    game_summary["whiffs"] = pd.to_numeric(game_summary["whiffs"], errors="coerce")
    game_summary["avg_ev"] = pd.to_numeric(game_summary["avg_ev"], errors="coerce")
    game_summary["contact_pct"] = (
        100 * (game_summary["swings"] - game_summary["whiffs"])
        / game_summary["swings"].replace(0, float("nan"))
    )
    game_summary["contact_pct"] = pd.to_numeric(game_summary["contact_pct"], errors="coerce")
    game_summary["rolling_contact_5"] = game_summary["contact_pct"].rolling(5, min_periods=1).mean()

    return game_summary


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

    total_pitches_seen = len(df)
    total_games = df["game_pk"].nunique()
    swings = int(df["is_swing"].fillna(0).sum())
    whiffs = int(df["is_whiff"].fillna(0).sum())
    in_zone_total = int(df["in_zone"].fillna(0).sum())
    out_zone_total = total_pitches_seen - in_zone_total
    chase_swings_total = int(df["is_chase_swing"].fillna(0).sum())

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

    bip_df = df[df["is_in_play"] == 1].copy()

    pitch_summary = build_pitch_summary(df).sort_values("pitches_seen", ascending=False)
    pitch_summary = enrich_chase_by_pitch_type(df, pitch_summary)

    df_rhp = df[df["pitcher_hand"] == "R"].copy() if "pitcher_hand" in df.columns else pd.DataFrame()
    df_lhp = df[df["pitcher_hand"] == "L"].copy() if "pitcher_hand" in df.columns else pd.DataFrame()

    pitch_order_rhp = (
        df_rhp["pitch_type_plot"].value_counts().loc[lambda s: s >= 5].index.tolist()
        if not df_rhp.empty else []
    )
    pitch_order_lhp = (
        df_lhp["pitch_type_plot"].value_counts().loc[lambda s: s >= 5].index.tolist()
        if not df_lhp.empty else []
    )

    game_summary = build_game_trend_summary(df)

    fig = plt.figure(figsize=(18, 16))
    gs = fig.add_gridspec(
        4,
        2,
        height_ratios=[1.0, 1.15, 1.15, 0.95],
        hspace=0.35,
        wspace=0.18,
    )

    ax_usage = fig.add_subplot(gs[0, 0])
    ax_evla = fig.add_subplot(gs[0, 1])
    rhp_spec = gs[1:3, 0]
    lhp_spec = gs[1:3, 1]
    ax_trend = fig.add_subplot(gs[3, 0])
    ax_tbl = fig.add_subplot(gs[3, 1])

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

    add_pitchtype_heatmap_grid(fig, rhp_spec, df_rhp, pitch_order_rhp, "vs RHP")
    add_pitchtype_heatmap_grid(fig, lhp_spec, df_lhp, pitch_order_lhp, "vs LHP")

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
    tbl_df = format_table_df(tbl_df)

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
    table.scale(1.1, 1.5)
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

    summary_text = " | ".join(summary_parts)

    fig.text(
        0.5,
        0.945,
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
        top=0.90,
        bottom=0.05,
        left=0.05,
        right=0.98,
        hspace=0.34,
        wspace=0.20,
    )

    output_file = OUT_DIR / f"hitter_season_report_{batter_id}_{season}.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print("Chart saved:", output_file)


if __name__ == "__main__":
    main()