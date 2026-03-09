import sys

import pandas as pd
import matplotlib.pyplot as plt

from src.db_connect import engine
from src.utils.output_router import get_output_path
from src.utils.report_helpers import format_table_df, build_title_date


def parse_args():
    """
    Expected CLI:
        python -m src.visuals.plate_appearance_profile_report <entity_type> <time_grain> <entity_id> <value>
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


def get_pa_data(entity_type: str, time_grain: str, entity_id: int, value: int) -> pd.DataFrame:
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
        FROM clean.at_bats
        {filter_sql}
        ORDER BY game_date, game_pk, inning, inning_half, at_bat_number;
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


def build_terminal_event_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("terminal_event", dropna=False)
        .agg(
            plate_appearances=("at_bat_key", "count"),
            avg_pitches_per_pa=("pitch_count", "mean"),
            avg_xwoba=("estimated_woba_using_speedangle", "mean"),
            avg_run_value=("delta_run_exp", "mean"),
        )
        .reset_index()
        .sort_values(["plate_appearances", "terminal_event"], ascending=[False, True])
    )

    return summary


def build_pitch_count_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("pitch_count", dropna=False)
        .agg(
            plate_appearances=("at_bat_key", "count"),
            hit_rate=("is_hit", "mean"),
            walk_rate=("is_walk", "mean"),
            strikeout_rate=("is_strikeout", "mean"),
        )
        .reset_index()
        .sort_values("pitch_count")
    )

    if not summary.empty:
        summary["hit_rate"] *= 100
        summary["walk_rate"] *= 100
        summary["strikeout_rate"] *= 100

    return summary


def build_game_trend_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["game_date", "game_pk"])
        .agg(
            plate_appearances=("at_bat_key", "count"),
            hit_rate=("is_hit", "mean"),
            walk_rate=("is_walk", "mean"),
            strikeout_rate=("is_strikeout", "mean"),
            avg_pitches_per_pa=("pitch_count", "mean"),
        )
        .reset_index()
        .sort_values(["game_date", "game_pk"])
    )

    if not summary.empty:
        summary["hit_rate"] *= 100
        summary["walk_rate"] *= 100
        summary["strikeout_rate"] *= 100
        summary["rolling_hit_rate_5"] = summary["hit_rate"].rolling(5, min_periods=1).mean()

    return summary


def build_chronological_game_table(df: pd.DataFrame, entity_type: str) -> pd.DataFrame:
    ordered = df.sort_values(["inning", "inning_half", "at_bat_number"]).copy()

    counterpart_col = "pitcher_name" if entity_type == "hitter" else "batter_name"

    out = ordered[
        [
            "inning",
            "inning_half",
            "at_bat_number",
            counterpart_col,
            "pitch_count",
            "terminal_event",
            "terminal_description",
            "launch_speed",
            "launch_angle",
            "delta_run_exp",
        ]
    ].copy()

    out = out.rename(
        columns={
            counterpart_col: "opponent",
            "inning": "inn",
            "inning_half": "half",
            "at_bat_number": "pa_no",
            "pitch_count": "pitches",
            "terminal_event": "result",
            "terminal_description": "desc",
            "launch_speed": "ev",
            "launch_angle": "la",
            "delta_run_exp": "run_value",
        }
    )

    return out


def main():
    entity_type, time_grain, entity_id, value = parse_args()

    df = get_pa_data(entity_type, time_grain, entity_id, value)

    if df.empty:
        print("No plate appearance data found.")
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

    total_pa = len(df)
    total_games = df["game_pk"].nunique()
    total_ab = int(df["is_ab"].fillna(0).sum())
    hits = int(df["is_hit"].fillna(0).sum())
    singles = int(df["is_single"].fillna(0).sum())
    doubles = int(df["is_double"].fillna(0).sum())
    triples = int(df["is_triple"].fillna(0).sum())
    home_runs = int(df["is_home_run"].fillna(0).sum())
    walks = int(df["is_walk"].fillna(0).sum())
    strikeouts = int(df["is_strikeout"].fillna(0).sum())
    hbp = int(df["is_hbp"].fillna(0).sum())
    in_play_pa = int(df["is_in_play_pa"].fillna(0).sum())

    avg_pitches_per_pa = round(df["pitch_count"].dropna().mean(), 2) if df["pitch_count"].notna().any() else None
    avg_xwoba = round(df["estimated_woba_using_speedangle"].dropna().mean(), 3) if df["estimated_woba_using_speedangle"].notna().any() else None
    avg_run_value = round(df["delta_run_exp"].dropna().mean(), 3) if df["delta_run_exp"].notna().any() else None

    hit_rate = round(100 * hits / total_pa, 1) if total_pa else 0
    walk_rate = round(100 * walks / total_pa, 1) if total_pa else 0
    strikeout_rate = round(100 * strikeouts / total_pa, 1) if total_pa else 0
    in_play_rate = round(100 * in_play_pa / total_pa, 1) if total_pa else 0

    terminal_summary = build_terminal_event_summary(df)
    pitch_count_summary = build_pitch_count_summary(df)
    trend_summary = build_game_trend_summary(df)
    game_table = build_chronological_game_table(df, entity_type) if time_grain == "game" else pd.DataFrame()

    fig = plt.figure(figsize=(18, 16))
    gs = fig.add_gridspec(
        4,
        2,
        height_ratios=[1.0, 1.0, 1.0, 1.1],
        hspace=0.35,
        wspace=0.22,
    )

    ax_outcomes = fig.add_subplot(gs[0, 0])
    ax_pitch_count = fig.add_subplot(gs[0, 1])
    ax_summary = fig.add_subplot(gs[1, 0])
    ax_trend = fig.add_subplot(gs[1, 1])
    ax_tbl1 = fig.add_subplot(gs[2, :])
    ax_tbl2 = fig.add_subplot(gs[3, :])

    # Outcome mix
    outcome_labels = ["Hit", "Walk", "Strikeout", "HBP", "In-Play PA", "Other"]
    other_count = total_pa - hits - walks - strikeouts - hbp - in_play_pa
    outcome_values = [hits, walks, strikeouts, hbp, in_play_pa, max(other_count, 0)]

    ax_outcomes.bar(outcome_labels, outcome_values, edgecolor="black", linewidth=0.5)
    ax_outcomes.set_title("Plate Appearance Outcome Mix")
    ax_outcomes.set_ylabel("Count")
    ax_outcomes.grid(axis="y", alpha=0.15)

    # Pitch count distribution
    ax_pitch_count.bar(
        pitch_count_summary["pitch_count"].astype(str),
        pitch_count_summary["plate_appearances"],
        edgecolor="black",
        linewidth=0.5,
    )
    ax_pitch_count.set_title("Pitch Count per PA")
    ax_pitch_count.set_xlabel("Pitches in PA")
    ax_pitch_count.set_ylabel("Plate Appearances")
    ax_pitch_count.grid(axis="y", alpha=0.15)

    # Summary box
    summary_box = pd.DataFrame(
        {
            "Metric": [
                "Plate Appearances",
                "At-Bats",
                "Hits",
                "Singles",
                "Doubles",
                "Triples",
                "Home Runs",
                "Walks",
                "Strikeouts",
                "HBP",
                "In-Play PAs",
                "Hit%",
                "Walk%",
                "Strikeout%",
                "Avg Pitches / PA",
                "Avg xwOBA",
                "Avg Run Value",
            ],
            "Value": [
                total_pa,
                total_ab,
                hits,
                singles,
                doubles,
                triples,
                home_runs,
                walks,
                strikeouts,
                hbp,
                in_play_pa,
                hit_rate,
                walk_rate,
                strikeout_rate,
                avg_pitches_per_pa if avg_pitches_per_pa is not None else "-",
                avg_xwoba if avg_xwoba is not None else "-",
                avg_run_value if avg_run_value is not None else "-",
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
    summary_table.set_fontsize(9.5)
    summary_table.scale(1, 1.4)
    ax_summary.set_title("PA Summary")

    # Rolling trend / game trend
    ax_trend.set_title("Rolling PA Trend by Game")
    ax_trend.set_xlabel("Game Sequence")

    if not trend_summary.empty:
        x_vals = range(len(trend_summary))
        ax_trend.plot(
            x_vals,
            trend_summary["hit_rate"],
            marker="o",
            linewidth=1.5,
            label="Game Hit%",
        )
        ax_trend.plot(
            x_vals,
            trend_summary["rolling_hit_rate_5"],
            linewidth=2.5,
            label="Rolling 5G Hit%",
        )
        ax_trend.set_ylabel("Hit %")
        ax_trend.grid(alpha=0.15)
        ax_trend.legend(loc="upper right")
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

    # Table 1: terminal outcomes
    ax_tbl1.axis("off")
    tbl1 = terminal_summary[
        [
            "terminal_event",
            "plate_appearances",
            "avg_pitches_per_pa",
            "avg_xwoba",
            "avg_run_value",
        ]
    ].copy()

    tbl1 = format_table_df(
        tbl1,
        round_cols=["avg_pitches_per_pa", "avg_xwoba", "avg_run_value"],
    )

    col_labels_1 = ["Result", "#", "Avg Pitches", "Avg xwOBA", "Avg Run Value"]
    table1 = ax_tbl1.table(
        cellText=tbl1.values,
        colLabels=col_labels_1,
        loc="center",
        cellLoc="center",
    )
    table1.auto_set_font_size(False)
    table1.set_fontsize(9.5)
    table1.scale(1.0, 1.45)
    ax_tbl1.set_title("Terminal Outcome Summary", pad=12)

    # Table 2: game chronology or season pitch-count summary
    ax_tbl2.axis("off")

    if time_grain == "game" and not game_table.empty:
        tbl2 = game_table.head(14).copy()
        tbl2 = format_table_df(
            tbl2,
            round_cols=["ev", "la", "run_value"],
        )
        col_labels_2 = ["Inn", "Half", "PA #", "Opponent", "Pitches", "Result", "Desc", "EV", "LA", "Run Value"]
        title_2 = "Chronological Game PA Summary"
    else:
        tbl2 = pitch_count_summary[
            ["pitch_count", "plate_appearances", "hit_rate", "walk_rate", "strikeout_rate"]
        ].copy()
        tbl2 = format_table_df(
            tbl2,
            round_cols=["hit_rate", "walk_rate", "strikeout_rate"],
        )
        col_labels_2 = ["Pitch Ct", "# PAs", "Hit%", "Walk%", "Strikeout%"]
        title_2 = "Pitch Count Outcome Summary"

    table2 = ax_tbl2.table(
        cellText=tbl2.values,
        colLabels=col_labels_2,
        loc="center",
        cellLoc="center",
    )
    table2.auto_set_font_size(False)
    table2.set_fontsize(8.8)
    table2.scale(1.0, 1.45)
    ax_tbl2.set_title(title_2, pad=12)

    # Title + subtitle
    if time_grain == "game":
        game_pk = int(df["game_pk"].iloc[0])
        game_date_title = build_title_date(df["game_date"].iloc[0])
        title = f"{player_name} Plate Appearance Profile\n{entity_type.title()} | Game {game_pk} | {game_date_title}"
    else:
        season = int(df["season"].iloc[0])
        title = f"{player_name} Plate Appearance Profile\n{entity_type.title()} | Season {season}"

    fig.suptitle(title, fontsize=22, y=0.985)

    header_parts = [
        f"PAs: {total_pa}",
        f"Hits: {hits}",
        f"Walks: {walks}",
        f"Strikeouts: {strikeouts}",
        f"In-Play PAs: {in_play_pa}",
        f"Hit%: {hit_rate}",
        f"Walk%: {walk_rate}",
        f"K%: {strikeout_rate}",
    ]
    if avg_pitches_per_pa is not None:
        header_parts.append(f"Avg Pitches/PA: {avg_pitches_per_pa}")
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
        wspace=0.22,
    )

    output_kwargs = {
        "report_family": "plate_appearance_profile",
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