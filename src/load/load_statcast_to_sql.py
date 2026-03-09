from pathlib import Path
import urllib

import pandas as pd
from pybaseball import playerid_reverse_lookup
from sqlalchemy import create_engine


BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "raw"

SERVER = r"RAMSEY_BOLTON\SQLEXPRESS"
DATABASE = "fantasy_baseball"
DRIVER = "ODBC Driver 17 for SQL Server"

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def build_flags(df: pd.DataFrame) -> pd.DataFrame:
    desc = df["description"].fillna("").astype(str).str.lower()

    swing_descriptions = [
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

    in_play_descriptions = [
        "hit_into_play",
        "hit_into_play_no_out",
        "hit_into_play_score",
    ]

    ball_descriptions = [
        "ball",
        "blocked_ball",
        "pitchout",
        "wild_pitch",
        "automatic_ball",
    ]

    flag_df = pd.DataFrame(
        {
            "is_swing": desc.isin(swing_descriptions),
            "is_whiff": desc.str.contains("swinging_strike", na=False),
            "is_called_strike": desc.eq("called_strike") | desc.eq("automatic_strike"),
            "is_ball": desc.isin(ball_descriptions),
            "is_in_play": desc.isin(in_play_descriptions),
            "is_foul": desc.str.contains("foul", na=False),
        },
        index=df.index,
    )

    return pd.concat([df, flag_df], axis=1)


def build_pitch_event_id(df: pd.DataFrame) -> pd.Series:
    key_parts = pd.DataFrame(
        {
            "game_pk": df["game_pk"].fillna(-1).astype("Int64").astype(str),
            "inning": df["inning"].fillna(-1).astype("Int64").astype(str),
            "inning_topbot": df["inning_topbot"].fillna("UNK").astype(str),
            "pitcher": df["pitcher"].fillna(-1).astype("Int64").astype(str),
            "batter": df["batter"].fillna(-1).astype("Int64").astype(str),
            "at_bat_number": df["at_bat_number"].fillna(-1).astype("Int64").astype(str),
            "pitch_number": df["pitch_number"].fillna(-1).astype("Int64").astype(str),
            "pitch_type": df["pitch_type"].fillna("UNK").astype(str),
            "plate_x": pd.to_numeric(df["plate_x"], errors="coerce").round(4).fillna(-9999).astype(str),
            "plate_z": pd.to_numeric(df["plate_z"], errors="coerce").round(4).fillna(-9999).astype(str),
            "release_speed": pd.to_numeric(df["release_speed"], errors="coerce").round(3).fillna(-9999).astype(str),
        },
        index=df.index,
    )

    return key_parts.agg("|".join, axis=1)


def normalize_pitch_group(pitch_type_series: pd.Series) -> pd.Series:
    mapping = {
        "FF": "Fastball",
        "FA": "Fastball",
        "SI": "Fastball",
        "FC": "Fastball",
        "FS": "Offspeed",
        "FO": "Offspeed",
        "CH": "Offspeed",
        "SC": "Offspeed",
        "SL": "Breaking",
        "ST": "Breaking",
        "SV": "Breaking",
        "CU": "Breaking",
        "KC": "Breaking",
        "CS": "Breaking",
        "EP": "Other",
        "KN": "Other",
    }
    return pitch_type_series.fillna("UNK").map(mapping).fillna("Other")


def read_latest_file() -> Path:
    files = sorted(DATA_DIR.glob("statcast_pitches*.csv"))
    if not files:
        raise FileNotFoundError("No statcast_pitches*.csv file found in data/raw.")
    return max(files, key=lambda p: p.stat().st_mtime)


def enrich_batter_names(df: pd.DataFrame) -> pd.DataFrame:
    batter_ids = (
        pd.Series(df["batter"].dropna().unique())
        .astype("Int64")
        .dropna()
        .astype(int)
        .tolist()
    )

    if not batter_ids:
        df["batter_name"] = pd.NA
        return df

    lookup_frames = []
    batch_size = 200

    for i in range(0, len(batter_ids), batch_size):
        batch_ids = batter_ids[i:i + batch_size]
        lookup = playerid_reverse_lookup(batch_ids, key_type="mlbam")
        if lookup is not None and not lookup.empty:
            lookup_frames.append(lookup)

    if not lookup_frames:
        df["batter_name"] = pd.NA
        return df

    lookup_df = pd.concat(lookup_frames, ignore_index=True).drop_duplicates()

    lookup_df["batter"] = pd.to_numeric(lookup_df["key_mlbam"], errors="coerce")
    lookup_df["batter_name"] = (
        lookup_df["name_last"].fillna("").astype(str).str.strip()
        + ", "
        + lookup_df["name_first"].fillna("").astype(str).str.strip()
    ).str.strip(", ")

    lookup_df = lookup_df[["batter", "batter_name"]].drop_duplicates(subset=["batter"])

    df = df.merge(lookup_df, on="batter", how="left")
    return df


def main() -> None:
    file = read_latest_file()
    print("Loading file:", file)

    df = pd.read_csv(file, low_memory=False)

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["season"] = df["game_date"].dt.year
    df["game_date"] = df["game_date"].dt.date

    df = enrich_batter_names(df)
    df = build_flags(df)

    pitch_event_id = build_pitch_event_id(df)
    df = df.copy()
    df["pitch_event_id"] = pitch_event_id
    df["pitch_group"] = normalize_pitch_group(df["pitch_type"])
    df["source_system"] = "pybaseball_statcast"
    df["source_file"] = file.name

    keep_cols = [
        "pitch_event_id",
        "game_pk",
        "game_date",
        "season",
        "game_type",
        "at_bat_number",
        "pitch_number",
        "inning",
        "inning_topbot",
        "pitcher",
        "player_name",
        "batter",
        "batter_name",
        "stand",
        "p_throws",
        "balls",
        "strikes",
        "outs_when_up",
        "pitch_type",
        "pitch_name",
        "pitch_group",
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
        "vx0",
        "vy0",
        "vz0",
        "ax",
        "ay",
        "az",
        "sz_top",
        "sz_bot",
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
        "on_1b",
        "on_2b",
        "on_3b",
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
        "is_swing",
        "is_whiff",
        "is_called_strike",
        "is_ball",
        "is_in_play",
        "is_foul",
        "source_system",
        "source_file",
    ]

    missing_cols = [c for c in keep_cols if c not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing expected columns: {missing_cols}")

    df = df[keep_cols].copy()

    df.rename(
        columns={
            "inning_topbot": "inning_half",
            "pitcher": "pitcher_id",
            "player_name": "pitcher_name",
            "batter": "batter_id",
            "stand": "batter_stand",
            "p_throws": "pitcher_hand",
        },
        inplace=True,
    )

    final_col_order = [
        "pitch_event_id",
        "game_pk",
        "game_date",
        "season",
        "game_type",
        "at_bat_number",
        "pitch_number",
        "inning",
        "inning_half",
        "pitcher_id",
        "pitcher_name",
        "batter_id",
        "batter_name",
        "pitcher_hand",
        "batter_stand",
        "balls",
        "strikes",
        "outs_when_up",
        "pitch_type",
        "pitch_name",
        "pitch_group",
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
        "vx0",
        "vy0",
        "vz0",
        "ax",
        "ay",
        "az",
        "sz_top",
        "sz_bot",
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
        "on_1b",
        "on_2b",
        "on_3b",
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
        "is_swing",
        "is_whiff",
        "is_called_strike",
        "is_ball",
        "is_in_play",
        "is_foul",
        "source_system",
        "source_file",
    ]

    df = df[final_col_order].copy()
    df.drop_duplicates(subset=["pitch_event_id"], inplace=True)

    df.to_sql(
        "pitches",
        engine,
        schema="clean",
        if_exists="append",
        index=False,
        chunksize=1000,
    )

    print("Rows loaded:", len(df))
    print("Distinct pitch_event_id:", df["pitch_event_id"].nunique())
    print("Balls in play loaded:", int(df["is_in_play"].sum()))
    print("Rows with hc_x:", int(df["hc_x"].notna().sum()))
    print(
        "Rows with estimated_woba_using_speedangle:",
        int(df["estimated_woba_using_speedangle"].notna().sum()),
    )
    print("Rows with batter_name:", int(df["batter_name"].notna().sum()))


if __name__ == "__main__":
    main()