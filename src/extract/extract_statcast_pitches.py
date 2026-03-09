from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from pybaseball import statcast


BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "raw"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def daterange_chunks(start_date: str, end_date: str, chunk_days: int = 7) -> list[tuple[str, str]]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    ranges: list[tuple[str, str]] = []
    cur = start

    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        ranges.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(days=1)

    return ranges


def print_basic_summary(df: pd.DataFrame) -> None:
    print("\nRows extracted:", len(df))
    print("Total columns:", len(df.columns))

    if "game_pk" in df.columns:
        print("Distinct games:", df["game_pk"].nunique(dropna=True))

    if "pitcher" in df.columns:
        print("Distinct pitchers:", df["pitcher"].nunique(dropna=True))

    if "batter" in df.columns:
        print("Distinct batters:", df["batter"].nunique(dropna=True))

    if "game_date" in df.columns:
        print("Min game_date:", pd.to_datetime(df["game_date"]).min())
        print("Max game_date:", pd.to_datetime(df["game_date"]).max())


def print_priority_field_audit(df: pd.DataFrame) -> None:
    check_cols = [
        "game_pk",
        "game_date",
        "game_type",
        "at_bat_number",
        "pitch_number",
        "inning",
        "inning_topbot",
        "pitcher",
        "player_name",
        "batter",
        "stand",
        "p_throws",
        "balls",
        "strikes",
        "outs_when_up",
        "pitch_type",
        "pitch_name",
        "release_speed",
        "release_spin_rate",
        "release_extension",
        "spin_axis",
        "release_pos_x",
        "release_pos_y",
        "release_pos_z",
        "pfx_x",
        "pfx_z",
        "plate_x",
        "plate_z",
        "zone",
        "description",
        "events",
        "bb_type",
        "launch_speed",
        "launch_angle",
        "launch_speed_angle",
        "hit_distance_sc",
        "hc_x",
        "hc_y",
        "estimated_ba_using_speedangle",
        "estimated_slg_using_speedangle",
        "estimated_woba_using_speedangle",
        "vx0",
        "vy0",
        "vz0",
        "ax",
        "ay",
        "az",
        "sz_top",
        "sz_bot",
        "on_1b",
        "on_2b",
        "on_3b",
        "fielder_2",
        "fielder_3",
        "fielder_4",
        "fielder_5",
        "fielder_6",
        "fielder_7",
        "fielder_8",
        "fielder_9",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "bat_score",
        "fld_score",
        "delta_run_exp",
        "delta_home_win_exp",
        "woba_value",
        "woba_denom",
    ]

    print("\nPriority Field Audit")
    print("-" * 100)
    print(f"{'column':35} {'present':8} {'non_null':10} {'pct_non_null':12}")

    total_rows = len(df)

    for col in check_cols:
        if col in df.columns:
            non_null = int(df[col].notna().sum())
            pct_non_null = round(100 * non_null / total_rows, 2) if total_rows else 0.0
            print(f"{col:35} {'True':8} {non_null:<10} {pct_non_null:<12}")
        else:
            print(f"{col:35} {'False':8} {'MISSING':10} {'MISSING':12}")


def print_batted_ball_summary(df: pd.DataFrame) -> None:
    print("\nBatted Ball Summary")

    if "description" in df.columns:
        desc = df["description"].fillna("").astype(str).str.lower()

        in_play_desc = [
            "hit_into_play",
            "hit_into_play_no_out",
            "hit_into_play_score",
        ]

        swing_desc = [
            "swinging_strike",
            "swinging_strike_blocked",
            "foul",
            "foul_tip",
            "foul_bunt",
            "hit_into_play",
            "hit_into_play_no_out",
            "hit_into_play_score",
            "missed_bunt",
        ]

        print("Estimated swings from description:", int(desc.isin(swing_desc).sum()))
        print("Estimated balls in play from description:", int(desc.isin(in_play_desc).sum()))
        print("Estimated fouls from description:", int(desc.str.contains("foul", na=False).sum()))

    for col in [
        "launch_speed",
        "launch_angle",
        "hit_distance_sc",
        "bb_type",
        "hc_x",
        "hc_y",
        "estimated_ba_using_speedangle",
        "estimated_slg_using_speedangle",
        "estimated_woba_using_speedangle",
    ]:
        if col in df.columns:
            print(f"{col} non-null:", int(df[col].notna().sum()))

    if "events" in df.columns:
        print("\nTop events:")
        print(df["events"].fillna("NULL").value_counts().head(20).to_string())

    if "description" in df.columns:
        print("\nTop descriptions:")
        print(df["description"].fillna("NULL").value_counts().head(20).to_string())

    if {"description", "events", "launch_speed", "launch_angle"}.issubset(df.columns):
        desc_events = (
            df.assign(
                description=df["description"].fillna("NULL"),
                events=df["events"].fillna("NULL"),
                has_launch_speed=df["launch_speed"].notna().astype(int),
                has_launch_angle=df["launch_angle"].notna().astype(int),
            )
            .groupby(["description", "events"], dropna=False)
            .agg(
                rows=("description", "size"),
                launch_speed_rows=("has_launch_speed", "sum"),
                launch_angle_rows=("has_launch_angle", "sum"),
            )
            .reset_index()
            .sort_values(["rows", "launch_speed_rows"], ascending=[False, False])
            .head(25)
        )

        print("\nTop description/events combinations:")
        print(desc_events.to_string(index=False))


def print_pitch_type_summary(df: pd.DataFrame) -> None:
    if "pitch_type" not in df.columns:
        return

    print("\nTop pitch types:")
    print(df["pitch_type"].fillna("NULL").value_counts().head(20).to_string())

    if "pitch_name" in df.columns:
        pitch_name_summary = (
            df.groupby(["pitch_type", "pitch_name"], dropna=False)
            .size()
            .reset_index(name="rows")
            .sort_values("rows", ascending=False)
            .head(20)
        )
        print("\nPitch type / pitch name combinations:")
        print(pitch_name_summary.to_string(index=False))


def print_missingness_summary(df: pd.DataFrame, top_n: int = 40) -> None:
    miss_df = pd.DataFrame(
        {
            "column": df.columns,
            "non_null": [int(df[c].notna().sum()) for c in df.columns],
        }
    )
    miss_df["pct_non_null"] = (100 * miss_df["non_null"] / len(df)).round(2)
    miss_df = miss_df.sort_values(["pct_non_null", "column"], ascending=[False, True])

    print(f"\nTop {top_n} columns by completeness:")
    print(miss_df.head(top_n).to_string(index=False))

    print(f"\nBottom {top_n} columns by completeness:")
    print(miss_df.tail(top_n).to_string(index=False))


def print_column_list(df: pd.DataFrame) -> None:
    print("\nStatcast Columns:")
    for col in sorted(df.columns):
        print(col)


def extract_statcast_range(start_date: str, end_date: str, chunk_days: int = 7) -> pd.DataFrame:
    chunks = daterange_chunks(start_date, end_date, chunk_days=chunk_days)

    all_frames: list[pd.DataFrame] = []

    print("\nExtraction chunks:")
    for chunk_start, chunk_end in chunks:
        print(f"  {chunk_start} -> {chunk_end}")
        chunk_df = statcast(start_dt=chunk_start, end_dt=chunk_end)
        if chunk_df is not None and not chunk_df.empty:
            all_frames.append(chunk_df)

    if not all_frames:
        return pd.DataFrame()

    df = pd.concat(all_frames, ignore_index=True)

    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    if {"game_pk", "at_bat_number", "pitch_number"}.issubset(df.columns):
        df = df.sort_values(["game_date", "game_pk", "at_bat_number", "pitch_number"]).reset_index(drop=True)
    elif {"game_pk", "pitch_number"}.issubset(df.columns):
        df = df.sort_values(["game_date", "game_pk", "pitch_number"]).reset_index(drop=True)

    return df


def main():
    start_date = "2025-03-27"
    end_date = "2025-03-31"

    df = extract_statcast_range(start_date, end_date, chunk_days=7)

    if df.empty:
        print("\nNo Statcast data returned.")
        return

    print_basic_summary(df)
    print_priority_field_audit(df)
    print_batted_ball_summary(df)
    print_pitch_type_summary(df)
    print_missingness_summary(df, top_n=30)
    print_column_list(df)

    csv_file = OUT_DIR / f"statcast_pitches_{start_date}_{end_date}.csv"
    parquet_file = OUT_DIR / f"statcast_pitches_{start_date}_{end_date}.parquet"

    df.to_csv(csv_file, index=False)
    df.to_parquet(parquet_file, index=False)

    print("\nFiles saved:")
    print(csv_file)
    print(parquet_file)


if __name__ == "__main__":
    main()