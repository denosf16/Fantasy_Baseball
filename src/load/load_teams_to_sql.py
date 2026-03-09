from pathlib import Path
from datetime import datetime
import math
import urllib

import pandas as pd
from sqlalchemy import create_engine, text


BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "raw"

SERVER = r"RAMSEY_BOLTON\SQLEXPRESS"
DATABASE = "fantasy_baseball"
DRIVER = "ODBC Driver 17 for SQL Server"

TARGET_SCHEMA = "clean"
TARGET_TABLE = "teams"
CHUNK_SIZE = 100

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)


EXPECTED_COLUMNS = [
    "team_id",
    "team_name",
    "team_code",
    "abbreviation",
    "team_name_short",
    "location_name",
    "franchise_name",
    "club_name",
    "league_id",
    "league_name",
    "division_id",
    "division_name",
    "venue_id",
    "venue_name",
    "first_year_of_play",
    "active_flag",
    "source_system",
    "source_load_datetime"
]

REQUIRED_COLUMNS = [
    "team_id",
    "team_name",
    "league_name",
    "division_name",
    "active_flag",
    "source_system",
    "source_load_datetime"
]


INT_COLUMNS = [
    "team_id",
    "league_id",
    "division_id",
    "venue_id"
]

BOOL_COLUMNS = [
    "active_flag"
]

DATETIME_COLUMNS = [
    "source_load_datetime"
]


TEXT_COLUMNS = [
    "team_name",
    "team_code",
    "abbreviation",
    "team_name_short",
    "location_name",
    "franchise_name",
    "club_name",
    "league_name",
    "division_name",
    "venue_name",
    "first_year_of_play",
    "source_system"
]


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{now_str()}] {msg}")


def load_csv(path: Path):

    if not path.exists():
        raise FileNotFoundError(path)

    log(f"Reading CSV: {path}")

    df = pd.read_csv(path)

    log(f"Rows read: {len(df)}")
    log(f"Columns read: {len(df.columns)}")

    return df


def validate_columns(df):

    missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]

    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    missing_expected = [c for c in EXPECTED_COLUMNS if c not in df.columns]

    if missing_expected:
        log(f"Warning: expected columns missing: {missing_expected}")


def normalize_blank_strings(df):

    for col in TEXT_COLUMNS:

        if col in df.columns:

            df[col] = df[col].astype("string").str.strip()

            df[col] = df[col].replace(
                {"": pd.NA, "None": pd.NA, "nan": pd.NA}
            )

    return df


def coerce_types(df):

    for col in INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in BOOL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def reorder_columns(df):

    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    return df[EXPECTED_COLUMNS]


def print_duplicate_summary(df):

    dupes = df.duplicated(subset=["team_id"]).sum()

    log(f"Duplicate team_id rows: {dupes}")

    if dupes > 0:

        dup_df = (
            df.groupby("team_id")
            .size()
            .reset_index(name="count")
            .query("count > 1")
        )

        print(dup_df)

        raise ValueError("Duplicate team_id detected")


def print_completeness(df):

    log("Column completeness snapshot:")

    total = len(df)

    cols = [
        "team_id",
        "team_name",
        "team_code",
        "abbreviation",
        "league_name",
        "division_name",
        "venue_id",
        "venue_name",
        "first_year_of_play",
        "active_flag"
    ]

    for col in cols:

        if col in df.columns:

            non_null = df[col].notna().sum()

            pct = round((non_null / total) * 100, 2)

            log(f"{col}: {non_null}/{total} ({pct}%)")


def truncate_table():

    log("Truncating clean.teams")

    with engine.begin() as conn:

        conn.execute(text(f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE}"))


def sql_row_count():

    with engine.begin() as conn:

        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
        ).scalar()

    return result


def load_chunks(df):

    total_rows = len(df)

    chunks = math.ceil(total_rows / CHUNK_SIZE)

    log(f"Loading {total_rows} rows in {chunks} chunks")

    for i in range(chunks):

        start = i * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, total_rows)

        chunk = df.iloc[start:end]

        chunk.to_sql(
            TARGET_TABLE,
            engine,
            schema=TARGET_SCHEMA,
            if_exists="append",
            index=False,
            method=None
        )

        log(f"Loaded rows {start+1} - {end}")


def main():

    start_time = datetime.now()

    log("Starting team load")

    file = DATA_DIR / "mlb_teams.csv"

    df = load_csv(file)

    validate_columns(df)

    df = normalize_blank_strings(df)

    df = coerce_types(df)

    df = reorder_columns(df)

    print_duplicate_summary(df)

    print_completeness(df)

    truncate_table()

    load_chunks(df)

    sql_count = sql_row_count()

    log(f"CSV rows: {len(df)}")
    log(f"SQL rows: {sql_count}")

    if sql_count != len(df):
        raise ValueError("Row count mismatch")

    duration = round((datetime.now() - start_time).total_seconds(), 2)

    log(f"Load complete in {duration} seconds")


if __name__ == "__main__":
    main()