from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import urllib
import uuid

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "raw"

SERVER = r"RAMSEY_BOLTON\SQLEXPRESS"
DATABASE = "fantasy_baseball"
DRIVER = "ODBC Driver 17 for SQL Server"

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def build_flags(df):

    df["is_swing"] = df["description"].str.contains("swing", case=False, na=False)
    df["is_whiff"] = df["description"].str.contains("swinging_strike", case=False, na=False)
    df["is_called_strike"] = df["description"].str.contains("called_strike", case=False, na=False)
    df["is_ball"] = df["description"].str.contains("ball", case=False, na=False)
    df["is_in_play"] = df["description"].str.contains("in_play", case=False, na=False)
    df["is_foul"] = df["description"].str.contains("foul", case=False, na=False)

    return df


def main():

    file = list(DATA_DIR.glob("statcast_pitches*.csv"))[0]

    df = pd.read_csv(file)

    df = build_flags(df)

    df["pitch_event_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    keep_cols = [
        "pitch_event_id",
        "game_pk",
        "game_date",
        "inning",
        "inning_topbot",
        "pitcher",
        "player_name",
        "batter",
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

    df = df[keep_cols]

    df.rename(
        columns={
            "inning_topbot": "inning_half",
            "pitcher": "pitcher_id",
            "player_name": "pitcher_name",
            "batter": "batter_id",
        },
        inplace=True,
    )

    df.to_sql(
        "pitches",
        engine,
        schema="clean",
        if_exists="append",
        index=False,
        chunksize=500,
    )

    print("Rows loaded:", len(df))


if __name__ == "__main__":
    main()