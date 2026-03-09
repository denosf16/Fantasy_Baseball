from datetime import datetime
import math
import urllib

import pandas as pd
from sqlalchemy import create_engine, text


SERVER = r"RAMSEY_BOLTON\SQLEXPRESS"
DATABASE = "fantasy_baseball"
DRIVER = "ODBC Driver 17 for SQL Server"

TARGET_SCHEMA = "clean"
TARGET_TABLE = "at_bats"
CHUNK_SIZE = 1000

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)


EXPECTED_COLUMNS = [
    "at_bat_key",
    "game_pk",
    "game_date",
    "season",
    "game_type",
    "at_bat_number",
    "inning",
    "inning_half",
    "outs_when_up",
    "batter_id",
    "pitcher_id",
    "batter_name",
    "pitcher_name",
    "batter_stand",
    "pitcher_hand",
    "on_1b_start",
    "on_2b_start",
    "on_3b_start",
    "home_team",
    "away_team",
    "home_score_start",
    "away_score_start",
    "bat_score_start",
    "fld_score_start",
    "first_pitch_type",
    "first_pitch_name",
    "first_pitch_group",
    "last_pitch_type",
    "last_pitch_name",
    "last_pitch_group",
    "pitch_count",
    "swing_count",
    "whiff_count",
    "called_strike_count",
    "foul_count",
    "ball_count",
    "in_play_pitch_count",
    "distinct_pitch_types_seen",
    "balls_before_final_pitch",
    "strikes_before_final_pitch",
    "terminal_description",
    "terminal_event",
    "terminal_bb_type",
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
    "is_pa",
    "is_ab",
    "is_hit",
    "is_single",
    "is_double",
    "is_triple",
    "is_home_run",
    "is_walk",
    "is_strikeout",
    "is_hbp",
    "is_sac_fly",
    "is_sac_bunt",
    "is_reached_on_error",
    "is_in_play_pa",
    "source_system",
    "source_file",
    "source_load_datetime",
]


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{now_str()}] {msg}")


def extract_source_data():
    sql = """
    WITH base AS (
        SELECT
            p.*,
            ROW_NUMBER() OVER (
                PARTITION BY p.game_pk, p.at_bat_number
                ORDER BY p.pitch_number ASC
            ) AS rn_first,
            ROW_NUMBER() OVER (
                PARTITION BY p.game_pk, p.at_bat_number
                ORDER BY p.pitch_number DESC
            ) AS rn_last
        FROM clean.pitches p
        WHERE p.at_bat_number IS NOT NULL
    ),
    first_pitch AS (
        SELECT *
        FROM base
        WHERE rn_first = 1
    ),
    last_pitch AS (
        SELECT *
        FROM base
        WHERE rn_last = 1
    ),
    pa_agg AS (
        SELECT
            game_pk,
            at_bat_number,
            COUNT(*) AS pitch_count,
            SUM(CASE WHEN is_swing = 1 THEN 1 ELSE 0 END) AS swing_count,
            SUM(CASE WHEN is_whiff = 1 THEN 1 ELSE 0 END) AS whiff_count,
            SUM(CASE WHEN is_called_strike = 1 THEN 1 ELSE 0 END) AS called_strike_count,
            SUM(CASE WHEN is_foul = 1 THEN 1 ELSE 0 END) AS foul_count,
            SUM(CASE WHEN is_ball = 1 THEN 1 ELSE 0 END) AS ball_count,
            SUM(CASE WHEN is_in_play = 1 THEN 1 ELSE 0 END) AS in_play_pitch_count,
            COUNT(DISTINCT pitch_type) AS distinct_pitch_types_seen
        FROM clean.pitches
        WHERE at_bat_number IS NOT NULL
        GROUP BY game_pk, at_bat_number
    )
    SELECT
        CONCAT(fp.game_pk, '|', fp.at_bat_number) AS at_bat_key,

        fp.game_pk,
        fp.game_date,
        fp.season,
        fp.game_type,

        fp.at_bat_number,
        fp.inning,
        fp.inning_half,
        fp.outs_when_up,

        fp.batter_id,
        fp.pitcher_id,
        fp.batter_name,
        fp.pitcher_name,
        fp.batter_stand,
        fp.pitcher_hand,

        fp.on_1b AS on_1b_start,
        fp.on_2b AS on_2b_start,
        fp.on_3b AS on_3b_start,

        fp.home_team,
        fp.away_team,
        fp.home_score AS home_score_start,
        fp.away_score AS away_score_start,
        fp.bat_score AS bat_score_start,
        fp.fld_score AS fld_score_start,

        fp.pitch_type AS first_pitch_type,
        fp.pitch_name AS first_pitch_name,
        fp.pitch_group AS first_pitch_group,

        lp.pitch_type AS last_pitch_type,
        lp.pitch_name AS last_pitch_name,
        lp.pitch_group AS last_pitch_group,

        agg.pitch_count,
        agg.swing_count,
        agg.whiff_count,
        agg.called_strike_count,
        agg.foul_count,
        agg.ball_count,
        agg.in_play_pitch_count,
        agg.distinct_pitch_types_seen,

        lp.balls AS balls_before_final_pitch,
        lp.strikes AS strikes_before_final_pitch,

        lp.description AS terminal_description,
        lp.events AS terminal_event,
        lp.bb_type AS terminal_bb_type,

        lp.launch_speed,
        lp.launch_angle,
        lp.launch_speed_angle,
        lp.hit_distance_sc,
        lp.hc_x,
        lp.hc_y,

        lp.estimated_ba_using_speedangle,
        lp.estimated_slg_using_speedangle,
        lp.estimated_woba_using_speedangle,
        lp.woba_value,
        lp.woba_denom,

        lp.delta_run_exp,
        lp.delta_home_win_exp,

        CAST(1 AS bit) AS is_pa,

        fp.source_system,
        fp.source_file,
        fp.source_load_datetime
    FROM first_pitch fp
    INNER JOIN last_pitch lp
        ON fp.game_pk = lp.game_pk
       AND fp.at_bat_number = lp.at_bat_number
    INNER JOIN pa_agg agg
        ON fp.game_pk = agg.game_pk
       AND fp.at_bat_number = agg.at_bat_number;
    """

    log("Reading source data from clean.pitches")
    df = pd.read_sql(sql, engine)
    log(f"At-bat source rows extracted: {len(df)}")
    return df


def normalize_blank_strings(df):
    text_cols = [
        "at_bat_key",
        "game_type",
        "inning_half",
        "batter_name",
        "pitcher_name",
        "batter_stand",
        "pitcher_hand",
        "home_team",
        "away_team",
        "first_pitch_type",
        "first_pitch_name",
        "first_pitch_group",
        "last_pitch_type",
        "last_pitch_name",
        "last_pitch_group",
        "terminal_description",
        "terminal_event",
        "terminal_bb_type",
        "source_system",
        "source_file",
    ]

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
            df[col] = df[col].replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})

    return df


def add_derived_flags(df):
    event_norm = df["terminal_event"].astype("string").str.lower()

    hit_events = {"single", "double", "triple", "home_run"}
    walk_events = {"walk", "intent_walk"}
    strikeout_events = {"strikeout", "strikeout_double_play"}

    df["is_hit"] = event_norm.isin(hit_events)
    df["is_single"] = event_norm.eq("single")
    df["is_double"] = event_norm.eq("double")
    df["is_triple"] = event_norm.eq("triple")
    df["is_home_run"] = event_norm.eq("home_run")
    df["is_walk"] = event_norm.isin(walk_events)
    df["is_strikeout"] = event_norm.isin(strikeout_events)
    df["is_hbp"] = event_norm.eq("hit_by_pitch")
    df["is_sac_fly"] = event_norm.eq("sac_fly")
    df["is_sac_bunt"] = event_norm.eq("sac_bunt")
    df["is_reached_on_error"] = event_norm.eq("field_error")
    df["is_in_play_pa"] = df["in_play_pitch_count"].fillna(0).gt(0)

    # AB approximation
    df["is_ab"] = (
        ~df["is_walk"].fillna(False)
        & ~df["is_hbp"].fillna(False)
        & ~df["is_sac_fly"].fillna(False)
        & ~df["is_sac_bunt"].fillna(False)
    )

    return df


def coerce_types(df):
    int_cols = [
        "game_pk", "season", "at_bat_number", "inning", "outs_when_up",
        "balls_before_final_pitch", "strikes_before_final_pitch",
        "pitch_count", "swing_count", "whiff_count", "called_strike_count",
        "foul_count", "ball_count", "in_play_pitch_count", "distinct_pitch_types_seen",
        "home_score_start", "away_score_start", "bat_score_start", "fld_score_start"
    ]

    bigint_cols = ["batter_id", "pitcher_id", "on_1b_start", "on_2b_start", "on_3b_start"]

    float_cols = [
        "launch_speed", "launch_angle", "launch_speed_angle", "hit_distance_sc",
        "hc_x", "hc_y", "estimated_ba_using_speedangle", "estimated_slg_using_speedangle",
        "estimated_woba_using_speedangle", "woba_value", "woba_denom",
        "delta_run_exp", "delta_home_win_exp"
    ]

    bool_cols = [
        "is_pa", "is_ab", "is_hit", "is_single", "is_double", "is_triple",
        "is_home_run", "is_walk", "is_strikeout", "is_hbp", "is_sac_fly",
        "is_sac_bunt", "is_reached_on_error", "is_in_play_pa"
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in bigint_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

    if "source_load_datetime" in df.columns:
        df["source_load_datetime"] = pd.to_datetime(df["source_load_datetime"], errors="coerce")

    return df


def finalize_dataframe(df):
    df = normalize_blank_strings(df)
    df = add_derived_flags(df)
    df = coerce_types(df)

    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    return df[EXPECTED_COLUMNS]


def print_observability(df):
    log("At-bat observability summary")
    log(f"Row count: {len(df)}")
    log(f"Distinct at_bat_key: {df['at_bat_key'].nunique(dropna=True)}")

    dupes = int(df.duplicated(subset=["at_bat_key"]).sum())
    log(f"Duplicate at_bat_key rows: {dupes}")

    for col in [
        "at_bat_key",
        "game_pk",
        "at_bat_number",
        "batter_id",
        "pitcher_id",
        "terminal_event",
        "pitch_count",
        "launch_speed",
        "estimated_woba_using_speedangle",
    ]:
        non_null = int(df[col].notna().sum())
        pct = round((non_null / len(df)) * 100, 2) if len(df) else 0.0
        log(f"{col}: {non_null}/{len(df)} ({pct}%)")

    log(f"Hits: {int(df['is_hit'].fillna(False).sum())}")
    log(f"Walks: {int(df['is_walk'].fillna(False).sum())}")
    log(f"Strikeouts: {int(df['is_strikeout'].fillna(False).sum())}")
    log(f"Home runs: {int(df['is_home_run'].fillna(False).sum())}")
    log(f"In-play PAs: {int(df['is_in_play_pa'].fillna(False).sum())}")


def validate_dataframe(df):
    if df["at_bat_key"].isna().any():
        raise ValueError("at_bat_key contains NULL values")

    if df.duplicated(subset=["at_bat_key"]).any():
        dupes = int(df.duplicated(subset=["at_bat_key"]).sum())
        raise ValueError(f"Duplicate at_bat_key detected before load: {dupes}")

    required_nulls = {
        col: int(df[col].isna().sum())
        for col in ["at_bat_key", "game_pk", "at_bat_number", "batter_id", "pitcher_id"]
    }
    log(f"Required-column null counts: {required_nulls}")


def truncate_target():
    log("Truncating clean.at_bats")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE clean.at_bats;"))


def load_target(df):
    total_rows = len(df)
    chunks = math.ceil(total_rows / CHUNK_SIZE) if total_rows else 0

    log(f"Loading {total_rows} rows into clean.at_bats in {chunks} chunks")

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
            text("SELECT COUNT(*) FROM clean.at_bats;")
        ).scalar()

    log(f"Expected rows: {expected_rows}")
    log(f"Actual rows:   {actual_rows}")

    if actual_rows != expected_rows:
        raise ValueError(
            f"Row count mismatch. Expected {expected_rows}, got {actual_rows}"
        )


def main():
    started = datetime.now()
    log("Starting build_at_bats")

    df = extract_source_data()
    df = finalize_dataframe(df)
    print_observability(df)
    validate_dataframe(df)

    truncate_target()
    load_target(df)
    validate_target_count(len(df))

    duration = round((datetime.now() - started).total_seconds(), 2)
    log(f"build_at_bats complete in {duration} seconds")


if __name__ == "__main__":
    main()