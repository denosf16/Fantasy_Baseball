import sys

import pandas as pd
import matplotlib.pyplot as plt

from src.db_connect import engine
from src.utils.plot_helpers import draw_spray_chart
from src.utils.report_helpers import format_table_df, build_title_date
from src.utils.output_router import get_output_path


def parse_args():
    """
    Expected CLI:
        python -m src.visuals.batted_ball_profile_report <entity_type> <time_grain> <entity_id> <value>

    Examples:
        hitter game 677594 778496
        pitcher season 695549 2025
    """
    if len(sys.argv) >= 5:
        entity_type = str(sys.argv[1]).strip().lower()
        time_grain = str(sys.argv[2]).strip().lower()
        entity_id = int(sys.argv[3])
        value = int(sys.argv[4])
    else:
        entity_type = "hitter"
        time_grain = "season"
        entity_id = 677594
        value = 2025

    if entity_type not in {"hitter", "pitcher"}:
        raise ValueError("entity_type must be 'hitter' or 'pitcher'")

    if time_grain not in {"game", "season"}:
        raise ValueError("time_grain must be 'game' or 'season'")

    return entity_type, time_grain, entity_id, value


def get_batted_ball_data(entity_type: str, time_grain: str, entity_id: int, value: int) -> pd.DataFrame:
    id_col = "batter_id" if entity_type == "hitter" else "pitcher_id"

    if time_grain == "game":
        filter_sql = f"""
            WHERE {id_col} = {entity_id}
              AND game_pk = {value}
        """
    else:
        filter_sql = f"""
            WHERE {id_col} = {entity_id}
              AND season = {value}
        """

    sql = f"""
        SELECT
            *
        FROM clean.batted_balls
        {filter_sql}
        ORDER BY game_date, game_pk, at_bat_number, pitch_number;
    """

    return pd.read_sql(sql, engine)


def infer_team_code(df: pd.DataFrame, entity_type: str, time_grain: str) -> str:
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

        if entity_type == "hitter":
            if inning_half == "top":
                return str(row["away_team"]).upper()
            if inning_half == "bot":
                return str(row["home_team"]).upper()

        if entity_type == "pitcher":
            if inning_half == "top":
                return str(row["home_team"]).upper()
            if inning_half == "bot":
                return str(row["away_team"]).upper()

        return None

    valid["entity_team_code"] = valid.apply(map_team, axis=1)
    valid = valid[valid["entity_team_code"].notna()]

    if valid.empty:
        return "UNK"

    if time_grain == "game":
        return valid["entity_team_code"].iloc[0]

    return valid["entity_team_code"].mode().iloc[0]


def build_bb_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("bb_type", dropna=False)
        .agg(
            batted_balls=("pitch_event_id", "count"),
            avg_ev=("launch_speed", "mean"),
            max_ev=("launch_speed", "max"),
            avg_la=("launch_angle", "mean"),
            avg_xba=("estimated_ba_using_speedangle", "mean"),
            avg_xwoba=("estimated_woba_using_speedangle", "mean"),
            hard_hit_rate=("is_hard_hit", "mean"),
            barrel_rate=("is_barrel", "mean"),
        )
        .reset_index()
        .rename(columns={"bb_type": "batted_ball_type"})
        .sort_values(["batted_balls", "batted_ball_type"], ascending=[False, True])
    )

    if not summary.empty:
        summary["hard_hit_rate"] *= 100
        summary["barrel_rate"] *= 100

    return summary


def build_game_trend_summary(df: pd.DataFrame) -> pd.DataFrame:
    game_summary = (
        df.groupby(["game_date", "game_pk"])
        .agg(
            batted_balls=("pitch_event_id", "count"),
            avg_ev=("launch_speed", "mean"),
            avg_xwoba=("estimated_woba_using_speedangle", "mean"),
            hard_hit_rate=("is_hard_hit", "mean"),
        )
        .reset_index()
        .sort_values(["game_date", "game_pk"])
    )

    if game_summary.empty:
        return game_summary

    game_summary["hard_hit_rate"] *= 100
    game_summary["rolling_avg_ev_5"] = game_summary["avg_ev"].rolling(5, min_periods=1).mean()
    game_summary["rolling_xwoba_5"] = game_summary["avg_xwoba"].rolling(5, min_periods=1).mean()

    return game_summary


def main():
    entity_type, time_grain, entity_id, value = parse_args()

    df = get_batted_ball_data(entity_type, time_grain, entity_id, value)

    if df.empty:
        print("No batted-ball data found.")
        return

    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])

    name_col = "batter_name" if entity_type == "hitter" else "pitcher_name"
    side = "hitting" if entity_type == "hitter" else "pitching"

    player_name = (
        df[name_col].dropna().iloc[0]
        if name_col in df.columns and df[name_col].notna().any()
        else str(entity_id)
    )

    team_code = infer_team_code(df, entity_type, time_grain)

    total_bb = len(df)
    total_games = df["game_pk"].nunique()
    avg_ev = round(df["launch_speed"].dropna().mean(), 1) if df["launch_speed"].notna().any() else None
    max_ev = round(df["launch_speed"].dropna().max(), 1) if df["launch_speed"].notna().any() else None
    avg_la = round(df["launch_angle"].dropna().mean(), 1) if df["launch_angle"].notna().any() else None
    avg_xba = round(df["estimated_ba_using_speedangle"].dropna().mean(), 3) if df["estimated_ba_using_speedangle"].notna().any() else None
    avg_xwoba = round(df["estimated_woba_using_speedangle"].dropna().mean(), 3) if df["estimated_woba_using_speedangle"].notna().any() else None
    hard_hit_rate = round(100 * df["is_hard_hit"].fillna(0).mean(), 1) if total_bb else 0
    barrel_rate = round(100 * df["is_barrel"].fillna(0).mean(), 1) if total_bb else 0

    gb_rate = round(100 * df["is_ground_ball"].fillna(0).mean(), 1) if total_bb else 0
    ld_rate = round(100 * df["is_line_drive"].fillna(0).mean(), 1) if total_bb else 0
    fb_rate = round(100 * df["is_fly_ball"].fillna(0).mean(), 1) if total_bb else 0
    pu_rate = round(100 * df["is_popup"].fillna(0).mean(), 1) if total_bb else 0

    bb_summary = build_bb_summary(df)
    trend_summary = build_game_trend_summary(df)

    fig = plt.figure(figsize=(18, 16))
    gs = fig.add_gridspec(
        4,
        2,
        height_ratios=[1.0, 1.0, 1.0, 1.05],
        hspace=0.35,
        wspace=0.20,
    )

    ax_evla = fig.add_subplot(gs[0, 0])
    ax_spray = fig.add_subplot(gs[0, 1])
    ax_mix = fig.add_subplot(gs[1, 0])
    ax_summary = fig.add_subplot(gs[1, 1])
    ax_trend = fig.add_subplot(gs[2, :])
    ax_tbl = fig.add_subplot(gs[3, :])

    # EV / LA scatter
    valid_evla = df[df["launch_speed"].notna() & df["launch_angle"].notna()].copy()
    ax_evla.set_title("Exit Velocity vs Launch Angle")

    if not valid_evla.empty:
        ax_evla.scatter(
            valid_evla["launch_angle"],
            valid_evla["launch_speed"],
            s=55,
            alpha=0.7,
            edgecolors="black",
            linewidths=0.25,
        )
        ax_evla.set_xlim(-30, 60)
        ax_evla.set_ylim(45, 115)
    else:
        ax_evla.text(
            0.5,
            0.5,
            "No EV / LA data",
            transform=ax_evla.transAxes,
            ha="center",
            va="center",
        )

    ax_evla.set_xlabel("Launch Angle")
    ax_evla.set_ylabel("Exit Velocity")
    ax_evla.grid(alpha=0.15)

    # Spray chart
    draw_spray_chart(ax_spray, df, title="Spray Chart")

    # Batted-ball type mix
    bb_mix = (
        df["bb_type"]
        .fillna("unknown")
        .value_counts()
        .sort_values(ascending=False)
    )

    ax_mix.bar(
        bb_mix.index.astype(str),
        bb_mix.values,
        edgecolor="black",
        linewidth=0.5,
    )
    ax_mix.set_title("Batted Ball Type Mix")
    ax_mix.set_xlabel("Batted Ball Type")
    ax_mix.set_ylabel("Count")
    ax_mix.grid(axis="y", alpha=0.15)

    # Summary box
    summary_box = pd.DataFrame(
        {
            "Metric": [
                "Batted Balls",
                "Games",
                "Avg EV",
                "Max EV",
                "Avg LA",
                "Avg xBA",
                "Avg xwOBA",
                "Hard-Hit%",
                "Barrel%",
                "GB%",
                "LD%",
                "FB%",
                "PU%",
            ],
            "Value": [
                total_bb,
                total_games,
                avg_ev if avg_ev is not None else "-",
                max_ev if max_ev is not None else "-",
                avg_la if avg_la is not None else "-",
                avg_xba if avg_xba is not None else "-",
                avg_xwoba if avg_xwoba is not None else "-",
                hard_hit_rate,
                barrel_rate,
                gb_rate,
                ld_rate,
                fb_rate,
                pu_rate,
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
    summary_table.set_fontsize(10)
    summary_table.scale(1, 1.5)
    ax_summary.set_title("Contact Summary")

    # Rolling trend
    ax_trend.set_title("Rolling Contact Trend by Game")
    ax_trend.set_xlabel("Game Sequence")

    if not trend_summary.empty:
        x_vals = range(len(trend_summary))
        ax_trend.plot(
            x_vals,
            trend_summary["avg_ev"],
            marker="o",
            linewidth=1.5,
            label="Game Avg EV",
        )
        ax_trend.plot(
            x_vals,
            trend_summary["rolling_avg_ev_5"],
            linewidth=2.5,
            label="Rolling 5G Avg EV",
        )

        if trend_summary["avg_xwoba"].notna().any():
            ax_trend_2 = ax_trend.twinx()
            ax_trend_2.plot(
                x_vals,
                trend_summary["rolling_xwoba_5"],
                linestyle="--",
                linewidth=2.0,
                label="Rolling 5G xwOBA",
            )
            ax_trend_2.set_ylabel("xwOBA")
        ax_trend.set_ylabel("Exit Velocity")
        ax_trend.grid(alpha=0.15)
        ax_trend.legend(loc="upper left")
    else:
        ax_trend.text(
            0.5,
            0.5,
            "No trend data",
            transform=ax_trend.transAxes,
            ha="center",
            va="center",
        )
        ax_trend.grid(alpha=0.15)

    # Results table
    ax_tbl.axis("off")

    tbl_df = bb_summary[
        [
            "batted_ball_type",
            "batted_balls",
            "avg_ev",
            "max_ev",
            "avg_la",
            "avg_xba",
            "avg_xwoba",
            "hard_hit_rate",
            "barrel_rate",
        ]
    ].copy()

    tbl_df = format_table_df(
        tbl_df,
        round_cols=[
            "avg_ev",
            "max_ev",
            "avg_la",
            "avg_xba",
            "avg_xwoba",
            "hard_hit_rate",
            "barrel_rate",
        ],
    )

    col_labels = [
        "BB Type",
        "#",
        "Avg EV",
        "Max EV",
        "Avg LA",
        "Avg xBA",
        "Avg xwOBA",
        "Hard-Hit%",
        "Barrel%",
    ]

    table = ax_tbl.table(
        cellText=tbl_df.values,
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9.5)
    table.scale(1.05, 1.5)
    ax_tbl.set_title("Batted Ball Results", pad=12)

    # Title + subtitle
    if time_grain == "game":
        game_pk = int(df["game_pk"].iloc[0])
        game_date_title = build_title_date(df["game_date"].iloc[0])
        title = f"{player_name} Batted Ball Profile\n{entity_type.title()} | Game {game_pk} | {game_date_title}"
    else:
        season = int(df["season"].iloc[0])
        title = f"{player_name} Batted Ball Profile\n{entity_type.title()} | Season {season}"

    fig.suptitle(title, fontsize=22, y=0.985)

    header_parts = [
        f"Batted Balls: {total_bb}",
        f"Games: {total_games}",
        f"Hard-Hit%: {hard_hit_rate}",
        f"Barrel%: {barrel_rate}",
    ]
    if avg_ev is not None:
        header_parts.append(f"Avg EV: {avg_ev}")
    if max_ev is not None:
        header_parts.append(f"Max EV: {max_ev}")
    if avg_la is not None:
        header_parts.append(f"Avg LA: {avg_la}")
    if avg_xwoba is not None:
        header_parts.append(f"Avg xwOBA: {avg_xwoba}")

    header_text = " | ".join(header_parts)

    fig.text(
        0.5,
        0.945,
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
        top=0.91,
        bottom=0.05,
        left=0.05,
        right=0.98,
        hspace=0.35,
        wspace=0.20,
    )

    output_kwargs = {
        "report_family": "batted_ball_profile",
        "time_grain": time_grain,
        "side": side,
        "team_code": team_code,
        "player_name": player_name,
        "player_id": entity_id,
        "output_format": "png",
    }

    if time_grain == "game":
        output_kwargs["game_date"] = str(df["game_date"].dt.date.iloc[0])
    else:
        output_kwargs["season"] = int(df["season"].iloc[0])

    output_file = get_output_path(**output_kwargs)

    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print("Chart saved:", output_file)


if __name__ == "__main__":
    main()