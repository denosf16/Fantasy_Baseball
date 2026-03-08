import math

import pandas as pd

from src.utils.pitch_config import PITCH_NAME_MAP
from src.utils.plot_helpers import draw_kde


def classify_in_zone(zone_value) -> int:
    try:
        return int(zone_value) in range(1, 10)
    except Exception:
        return 0


def build_title_date(value) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def format_table_df(tbl_df: pd.DataFrame, round_cols: list[str]) -> pd.DataFrame:
    out = tbl_df.copy()
    for col in round_cols:
        out[col] = out[col].apply(lambda x: "-" if pd.isna(x) else round(float(x), 1))
    return out


def build_pitch_summary_pitcher(df: pd.DataFrame) -> pd.DataFrame:
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
    summary["whiff_pct"] = 100 * summary["whiffs"] / summary["swings"].replace(0, float("nan"))
    summary["csw_pct"] = 100 * summary["csw"] / summary["pitches"]
    summary["zone_pct"] = 100 * summary["zone_pitches"] / summary["pitches"]
    summary["pitch_name"] = summary["pitch_type"].map(PITCH_NAME_MAP).fillna(summary["pitch_type"])

    return summary.sort_values(
        ["pitches", "pitch_type"], ascending=[False, True]
    ).reset_index(drop=True)


def build_pitch_summary_hitter(df: pd.DataFrame) -> pd.DataFrame:
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

    if "out_zone_seen_y" in merged.columns:
        merged["out_zone_seen"] = merged["out_zone_seen_y"].fillna(
            merged.get("out_zone_seen_x", 0)
        )
        drop_cols = [c for c in ["out_zone_seen_x", "out_zone_seen_y"] if c in merged.columns]
        merged = merged.drop(columns=drop_cols)

    merged["chase_pct"] = 100 * merged["chase_swings"] / merged["out_zone_seen"].replace(0, float("nan"))
    return merged


def build_game_trend_summary_hitter(df: pd.DataFrame) -> pd.DataFrame:
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


def add_pitchtype_heatmap_grid(
    fig,
    subspec,
    handed_df: pd.DataFrame,
    pitch_order: list[str],
    side_label: str,
    min_points: int = 5,
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
        draw_kde(ax, sub, title, min_points=min_points)

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