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
TARGET_TABLE = "games"
CHUNK_SIZE = 500

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)


EXPECTED_COLUMNS = [
    "game_pk",
    "game_guid",
    "game_date",
    "official_date",
    "season",
    "game_type",
    "abstract_game_state",
    "coded_game_state",
    "detailed_state",
    "status_code",
    "start_time_tbd",
    "abstract_game_code",
    "game_datetime_utc",
    "double_header",
    "day_night",
    "description",
    "scheduled_innings",
    "games_in_series",
    "series_game_number",
    "series_description",
    "home_team_id",
    "home_team_name",
    "home_league_id",
    "home_sport_wins",
    "home_sport_losses",
    "home_sport_pct",
    "home_score",
    "home_is_winner",
    "home_split_squad",
    "home_series_number",
    "away_team_id",
    "away_team_name",
    "away_league_id",
    "away_sport_wins",
    "away_sport_losses",
    "away_sport_pct",
    "away_score",
    "away_is_winner",
    "away_split_squad",
    "away_series_number",
    "venue_id",
    "venue_name",
    "resume_date",
    "resume_game_date",
    "resume_game_code",
    "if_necessary",
    "if_necessary_description",
    "calendar_event_id",
    "source_system",
    "source_load_datetime",
]

REQUIRED_COLUMNS = [
    "game_pk",
    "game_date",
    "season",
    "game_type",
    "home_team_id",
    "away_team_id",
    "source_system",
    "source_load_datetime",
]

TEXT_COLUMNS = [
    "game_guid",
    "game_type",
    "abstract_game_state",
    "coded_game_state",
    "detailed_state",
    "status_code",
    "abstract_game_code",
    "double_header",
    "day_night",
    "description",
    "series_description",
    "home_team_name",
    "away_team_name",
    "resume_game_code",
    "if_necessary",
    "if_necessary_description",
    "calendar_event_id",
    "venue_name",
    "source_system",
]

INT_COLUMNS = [
    "game_pk",
    "season",
    "scheduled_innings",
    "games_in_series",
    "series_game_number",
    "home_team_id",
    "home_league_id",
    "home_sport_wins",
    "home_sport_losses",
    "home_score",
    "home_series_number",
    "away_team_id",
    "away_league_id",
    "away_sport_wins",
    "away_sport_losses",
    "away_score",
    "away_series_number",
    "venue_id",
]

FLOAT_COLUMNS = [
    "home_sport_pct",
    "away_sport_pct",
]

BOOL_COLUMNS = [
    "start_time_tbd",
    "home_is_winner",
    "home_split_squad",
    "away_is_winner",
    "away_split_squad",
]

DATE_COLUMNS = [
    "game_date",
    "official_date",
    "resume_date",
    "resume_game_date",
]

DATETIME_COLUMNS = [
    "game_datetime_utc",
    "source_load_datetime",
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now_str()}] {msg}")


def get_latest_schedule_file() -> Path:
    files = sorted(DATA_DIR.glob("mlb_schedule_*.csv"))
    files = [f for f in files if "_failures" not in f.name]

    if not files:
        raise FileNotFoundError(f"No schedule CSV found in {DATA_DIR}")

    latest = max(files, key=lambda p: p.stat().st_mtime)
    return latest


def load_csv(path: Path) -> pd.DataFrame:
    log(f"Reading CSV: {path}")
    df = pd.read_csv(path)
    log(f"Rows read: {len(df)}")
    log(f"Columns read: {len(df.columns)}")
    return df


def validate_columns(df: pd.DataFrame) -> None:
    missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    missing_expected = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    extra_columns = [c for c in df.columns if c not in EXPECTED_COLUMNS]

    if missing_expected:
        log(f"Warning: expected columns missing from CSV: {missing_expected}")

    if extra_columns:
        log(f"Warning: extra columns found in CSV: {extra_columns}")


def normalize_blank_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
            df[col] = df[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return df


def normalize_boolean_strings(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "TRUE": True,
        "FALSE": False,
        "True": True,
        "False": False,
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "Y": True,
        "N": False,
        "y": True,
        "n": False,
    }

    for col in BOOL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].map(lambda x: mapping.get(str(x), x) if pd.notna(x) else pd.NA)
    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
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
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[col] = df[col].dt.tz_localize(None)

    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[EXPECTED_COLUMNS]


def print_duplicate_summary(df: pd.DataFrame) -> None:
    dupes = int(df.duplicated(subset=["game_pk"]).sum())
    distinct_games = int(df["game_pk"].nunique(dropna=True))

    log(f"Distinct game_pk count: {distinct_games}")
    log(f"Duplicate game_pk rows: {dupes}")

    if dupes > 0:
        dup_df = (
            df.groupby("game_pk", dropna=False)
            .size()
            .reset_index(name="row_count")
            .query("row_count > 1")
            .sort_values(["row_count", "game_pk"], ascending=[False, True])
        )
        log("Top duplicate game_pk values:")
        print(dup_df.head(10).to_string(index=False))
        raise ValueError("Duplicate game_pk values detected in CSV. Resolve before load.")


def print_completeness(df: pd.DataFrame) -> None:
    total = len(df)
    log("Column completeness snapshot:")

    for col in [
        "game_pk",
        "game_date",
        "official_date",
        "season",
        "game_type",
        "detailed_state",
        "game_datetime_utc",
        "home_team_id",
        "home_team_name",
        "away_team_id",
        "away_team_name",
        "home_score",
        "away_score",
        "venue_id",
        "venue_name",
        "double_header",
        "day_night",
        "series_description",
        "scheduled_innings",
        "source_system",
        "source_load_datetime",
    ]:
        if col in df.columns:
            non_null = int(df[col].notna().sum())
            pct = round((non_null / total) * 100, 2) if total else 0.0
            log(f"  {col}: {non_null}/{total} non-null ({pct}%)")


def truncate_target_table() -> None:
    sql = f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE};"
    log(f"Truncating target table: {TARGET_SCHEMA}.{TARGET_TABLE}")

    with engine.begin() as conn:
        conn.execute(text(sql))


def get_sql_row_count() -> int:
    sql = f"SELECT COUNT(*) AS row_count FROM {TARGET_SCHEMA}.{TARGET_TABLE};"
    with engine.begin() as conn:
        result = conn.execute(text(sql)).scalar_one()
    return int(result)


def load_in_chunks(df: pd.DataFrame) -> None:
    total_rows = len(df)
    total_chunks = math.ceil(total_rows / CHUNK_SIZE) if total_rows else 0

    log(f"Beginning SQL load | rows={total_rows} | chunk_size={CHUNK_SIZE} | chunks={total_chunks}")

    for i in range(total_chunks):
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

        log(f"Loaded chunk {i + 1}/{total_chunks} | rows {start + 1}-{end}")


def main() -> None:
    started_at = datetime.now()
    log("Starting schedule load to SQL")

    file = get_latest_schedule_file()
    df = load_csv(file)

    validate_columns(df)
    df = normalize_blank_strings(df)
    df = normalize_boolean_strings(df)
    df = coerce_types(df)
    df = reorder_columns(df)

    log("Preview of dtypes after coercion:")
    print(df.dtypes.to_string())

    print_duplicate_summary(df)
    print_completeness(df)

    required_nulls = {
        col: int(df[col].isna().sum())
        for col in REQUIRED_COLUMNS
        if col in df.columns
    }
    log(f"Required-column null counts: {required_nulls}")

    truncate_target_table()
    load_in_chunks(df)

    sql_row_count = get_sql_row_count()
    csv_row_count = len(df)

    log(f"CSV rows prepared: {csv_row_count}")
    log(f"SQL rows loaded:   {sql_row_count}")

    if sql_row_count != csv_row_count:
        raise ValueError(
            f"Row count mismatch after load. CSV={csv_row_count}, SQL={sql_row_count}"
        )

    ended_at = datetime.now()
    duration_seconds = round((ended_at - started_at).total_seconds(), 2)

    log("Schedule load complete")
    log(f"Duration (seconds): {duration_seconds}")


if __name__ == "__main__":
    main()