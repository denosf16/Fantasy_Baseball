from pathlib import Path
import urllib

import pandas as pd
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
    desc = df["description"].fillna("").astype(str)

    flag_df = pd.DataFrame(
        {
            "is_swing": desc.str.contains("swing", case=False, na=False),
            "is_whiff": desc.str.contains("swinging_strike", case=False, na=False),
            "is_called_strike": desc.str.contains("called_strike", case=False, na=False),
            "is_ball": desc.str.contains("ball", case=False, na=False),
            "is_in_play": desc.str.contains("in_play", case=False, na=False),
            "is_foul": desc.str.contains("foul", case=False, na=False),
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
            "pitch_number": df["pitch_number"].fillna(-1).astype("Int64").astype(str)
            if "pitch_number" in df.columns
            else pd.Series(["-1"] * len(df), index=df.index),
            "pitch_type": df["pitch_type"].fillna("UNK").astype(str),
            "plate_x": df["plate_x"].round(4).fillna(-9999).astype(str),
            "plate_z": df["plate_z"].round(4).fillna(-9999).astype(str),
            "release_speed": df["release_speed"].round(3).fillna(-9999).astype(str),
        },
        index=df.index,
    )

    return key_parts.agg("|".join, axis=1)


def main() -> None:
    files = sorted(DATA_DIR.glob("statcast_pitches*.csv"))
    if not files:
        raise FileNotFoundError("No statcast_pitches*.csv file found in data/raw.")

    file = files[0]
    df = pd.read_csv(file)

    df["game_date"] = pd.to_datetime(df["game_date"])
    df["season"] = df["game_date"].dt.year
    df["game_date"] = df["game_date"].dt.date

    df = build_flags(df)
    df["pitch_event_id"] = build_pitch_event_id(df)

    keep_cols = [
        "pitch_event_id",
        "game_pk",
        "game_date",
        "season",
        "inning",
        "inning_topbot",
        "pitcher",
        "player_name",
        "batter",
        "stand",
        "pitch_type",
        "pitch_name",
        "release_speed",
        "release_spin_rate",
        "plate_x",
        "plate_z",
        "pfx_x",
        "pfx_z",
        "zone",
        "description",
        "events",
        "launch_speed",
        "launch_angle",
        "hit_distance_sc",
        "is_swing",
        "is_whiff",
        "is_called_strike",
        "is_ball",
        "is_in_play",
        "is_foul",
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
        },
        inplace=True,
    )

    df.drop_duplicates(subset=["pitch_event_id"], inplace=True)

    df.to_sql(
        "pitches",
        engine,
        schema="clean",
        if_exists="append",
        index=False,
        chunksize=500,
    )

    print("Rows loaded:", len(df))
    print("Distinct pitch_event_id:", df["pitch_event_id"].nunique())


if __name__ == "__main__":
    main()