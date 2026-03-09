from datetime import datetime
import math
import urllib

import pandas as pd
from sqlalchemy import create_engine, text


SERVER = r"RAMSEY_BOLTON\SQLEXPRESS"
DATABASE = "fantasy_baseball"
DRIVER = "ODBC Driver 17 for SQL Server"

TARGET_SCHEMA = "clean"
TARGET_TABLE = "batted_balls"
CHUNK_SIZE = 1000

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)


EXPECTED_COLUMNS = [
    "pitch_event_id",
    "game_pk",
    "game_date",
    "season",
    "game_type",
    "at_bat_number",
    "pitch_number",
    "inning",
    "inning_half",
    "outs_when_up",
    "batter_id",
    "pitcher_id",
    "batter_name",
    "pitcher_name",
    "batter_stand",
    "pitcher_hand",
    "balls",
    "strikes",
    "pitch_type",
    "pitch_name",
    "pitch_group",
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
    "woba_value",
    "woba_denom",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "bat_score",
    "fld_score",
    "delta_run_exp",
    "delta_home_win_exp",
    "is_hit",
    "is_single",
    "is_double",
    "is_triple",
    "is_home_run",
    "is_ground_ball",
    "is_line_drive",
    "is_fly_ball",
    "is_popup",
    "is_hard_hit",
    "is_barrel",
    "source_system",
    "source_file",
    "source_load_datetime",
]

REQUIRED_COLUMNS = [
    "pitch_event_id",
    "game_pk",
    "batter_id",
    "pitcher_id",
]

INT_COLUMNS = [
    "game_pk",
    "season",
    "at_bat_number",
    "pitch_number",
    "inning",
    "outs_when_up",
    "balls",
    "strikes",
    "home_score",
    "away_score",
    "bat_score",
    "fld_score",
]

FLOAT_COLUMNS = [
    "launch_speed",
    "launch_angle",
    "launch_speed_angle",
    "hit_distance_sc",
    "hc_x",
    "hc_y",
    "estimated_ba_using_speedangle",
    "estimated_slg_using_speedangle",
    "estimated_woba_using_speedangle",
    "woba_value",
    "woba_denom",
    "delta_run_exp",
    "delta_home_win_exp",
]

BOOL_COLUMNS = [
    "is_hit",
    "is_single",
    "is_double",
    "is_triple",
    "is_home_run",
    "is_ground_ball",
    "is_line_drive",
    "is_fly_ball",
    "is_popup",
    "is_hard_hit",
    "is_barrel",
]

DATE_COLUMNS = [
    "game_date",
]

DATETIME_COLUMNS = [
    "source_load_datetime",
]

TEXT_COLUMNS = [
    "pitch_event_id",
    "game_type",
    "inning_half",
    "batter_name",
    "pitcher_name",
    "batter_stand",
    "pitcher_hand",
    "pitch_type",
    "pitch_name",
    "pitch_group",
    "description",
    "events",
    "bb_type",
    "home_team",
    "away_team",
    "source_system",
    "source_file",
]


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{now_str()}] {msg}")


def extract_source_data():
    sql = """
    SELECT
        p.pitch_event_id,
        p.game_pk,
        p.game_date,
        p.season,
        p.game_type,

        p.at_bat_number,
        p.pitch_number,
        p.inning,
        p.inning_half,
        p.outs_when_up,

        p.batter_id,
        p.pitcher_id,
        p.batter_name,
        p.pitcher_name,

        p.batter_stand,
        p.pitcher_hand,

        p.balls,
        p.strikes,

        p.pitch_type,
        p.pitch_name,
        p.pitch_group,

        p.description,
        p.events,
        p.bb_type,

        p.launch_speed,
        p.launch_angle,
        p.launch_speed_angle,
        p.hit_distance_sc,
        p.hc_x,
        p.hc_y,

        p.estimated_ba_using_speedangle,
        p.estimated_slg_using_speedangle,
        p.estimated_woba_using_speedangle,
        p.woba_value,
        p.woba_denom,

        p.home_team,
        p.away_team,
        p.home_score,
        p.away_score,
        p.bat_score,
        p.fld_score,
        p.delta_run_exp,
        p.delta_home_win_exp,

        p.source_system,
        p.source_file,
        p.source_load_datetime
    FROM clean.pitches p
    WHERE
        p.is_in_play = 1
        AND (
            p.launch_speed IS NOT NULL
            OR p.launch_angle IS NOT NULL
            OR p.bb_type IS NOT NULL
            OR p.hc_x IS NOT NULL
            OR p.hc_y IS NOT NULL
        );
    """

    log("Reading source data from clean.pitches")
    df = pd.read_sql(sql, engine)
    log(f"Source rows extracted: {len(df)}")
    return df


def normalize_blank_strings(df):
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
            df[col] = df[col].replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
    return df


def add_derived_flags(df):
    hit_events = {"single", "double", "triple", "home_run"}

    df["events_norm"] = df["events"].astype("string").str.lower()
    df["bb_type_norm"] = df["bb_type"].astype("string").str.lower()

    df["is_hit"] = df["events_norm"].isin(hit_events)
    df["is_single"] = df["events_norm"].eq("single")
    df["is_double"] = df["events_norm"].eq("double")
    df["is_triple"] = df["events_norm"].eq("triple")
    df["is_home_run"] = df["events_norm"].eq("home_run")

    df["is_ground_ball"] = df["bb_type_norm"].eq("ground_ball")
    df["is_line_drive"] = df["bb_type_norm"].eq("line_drive")
    df["is_fly_ball"] = df["bb_type_norm"].eq("fly_ball")
    df["is_popup"] = df["bb_type_norm"].eq("popup")

    df["is_hard_hit"] = df["launch_speed"].ge(95).fillna(False)

    df["is_barrel"] = (
        df["launch_speed"].ge(98)
        & df["launch_angle"].between(26, 30, inclusive="both")
    ).fillna(False)

    return df


def coerce_types(df):
    for col in INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in BOOL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def finalize_dataframe(df):
    df = normalize_blank_strings(df)
    df = add_derived_flags(df)
    df = coerce_types(df)

    df = df.drop(columns=["events_norm", "bb_type_norm"])

    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    return df[EXPECTED_COLUMNS]


def print_observability(df):
    log("Batted ball table observability summary")
    log(f"Row count: {len(df)}")
    log(f"Distinct pitch_event_id: {df['pitch_event_id'].nunique(dropna=True)}")

    dupes = int(df.duplicated(subset=["pitch_event_id"]).sum())
    log(f"Duplicate pitch_event_id rows: {dupes}")

    log("Column completeness snapshot:")
    for col in [
        "pitch_event_id",
        "game_pk",
        "game_date",
        "batter_id",
        "pitcher_id",
        "events",
        "bb_type",
        "launch_speed",
        "launch_angle",
        "hc_x",
        "hc_y",
        "estimated_woba_using_speedangle",
        "home_team",
        "away_team",
    ]:
        if col in df.columns:
            non_null = int(df[col].notna().sum())
            pct = round((non_null / len(df)) * 100, 2) if len(df) else 0.0
            log(f"  {col}: {non_null}/{len(df)} ({pct}%)")

    log(f"Hits: {int(df['is_hit'].fillna(False).sum())}")
    log(f"Hard-hit balls: {int(df['is_hard_hit'].fillna(False).sum())}")
    log(f"Barrels: {int(df['is_barrel'].fillna(False).sum())}")

    bb_mix = (
        df["bb_type"]
        .fillna("UNKNOWN")
        .value_counts(dropna=False)
        .head(10)
        .to_dict()
    )
    log(f"Top batted-ball types: {bb_mix}")


def validate_dataframe(df):
    missing_required = {
        col: int(df[col].isna().sum())
        for col in REQUIRED_COLUMNS
        if col in df.columns
    }
    log(f"Required-column null counts: {missing_required}")

    if df["pitch_event_id"].isna().any():
        raise ValueError("pitch_event_id contains NULL values")

    if df.duplicated(subset=["pitch_event_id"]).any():
        dupes = int(df.duplicated(subset=["pitch_event_id"]).sum())
        raise ValueError(f"Duplicate pitch_event_id detected before load: {dupes}")


def truncate_target():
    log("Truncating clean.batted_balls")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE clean.batted_balls;"))


def load_target(df):
    total_rows = len(df)
    chunks = math.ceil(total_rows / CHUNK_SIZE) if total_rows else 0

    log(f"Loading {total_rows} rows into clean.batted_balls in {chunks} chunks")

    for i in range(chunks):
        start = i * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, total_rows)
        chunk = df.iloc[start:end].copy()

        chunk.to_sql(
            TARGET_TABLE,
            engine,
            schema=TARGET_SCHEMA,
            if_exists="append",
            index=False,
            chunksize=CHUNK_SIZE,
            method=None,
        )

        log(f"Loaded rows {start + 1}-{end}")


def validate_target_count(expected_rows):
    with engine.begin() as conn:
        actual_rows = conn.execute(
            text("SELECT COUNT(*) FROM clean.batted_balls;")
        ).scalar()

    log(f"Expected rows: {expected_rows}")
    log(f"Actual rows:   {actual_rows}")

    if actual_rows != expected_rows:
        raise ValueError(
            f"Row count mismatch. Expected {expected_rows}, got {actual_rows}"
        )


def main():
    started = datetime.now()
    log("Starting build_batted_balls")

    df = extract_source_data()
    df = finalize_dataframe(df)
    print_observability(df)
    validate_dataframe(df)

    truncate_target()
    load_target(df)
    validate_target_count(len(df))

    duration = round((datetime.now() - started).total_seconds(), 2)
    log(f"build_batted_balls complete in {duration} seconds")


if __name__ == "__main__":
    main()