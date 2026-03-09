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

CSV_FILE = DATA_DIR / "mlb_players.csv"
TARGET_SCHEMA = "clean"
TARGET_TABLE = "players"
CHUNK_SIZE = 200

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)


EXPECTED_COLUMNS = [
    "player_id",
    "full_name",
    "first_name",
    "last_name",
    "primary_number",
    "birth_date",
    "current_age",
    "birth_city",
    "birth_state_province",
    "birth_country",
    "height",
    "weight",
    "active_flag",
    "primary_position_code",
    "primary_position_name",
    "primary_position_type",
    "primary_position",
    "bat_side",
    "bat_side_description",
    "pitch_hand",
    "pitch_hand_description",
    "use_name",
    "use_last_name",
    "middle_name",
    "boxscore_name",
    "nick_name",
    "gender",
    "is_player",
    "is_verified",
    "draft_year",
    "mlb_debut_date",
    "name_first_last",
    "name_slug",
    "first_last_name",
    "last_first_name",
    "last_init_name",
    "init_last_name",
    "full_fml_name",
    "full_lfm_name",
    "strike_zone_top",
    "strike_zone_bottom",
    "current_team_id",
    "current_team_name",
    "roster_status_code",
    "roster_status_description",
    "source_system",
    "source_load_datetime",
]


REQUIRED_COLUMNS = [
    "player_id",
    "full_name",
    "primary_position",
    "current_team_id",
    "active_flag",
    "source_system",
    "source_load_datetime",
]


TEXT_COLUMNS = [
    "full_name",
    "first_name",
    "last_name",
    "primary_number",
    "birth_city",
    "birth_state_province",
    "birth_country",
    "height",
    "primary_position_code",
    "primary_position_name",
    "primary_position_type",
    "primary_position",
    "bat_side",
    "bat_side_description",
    "pitch_hand",
    "pitch_hand_description",
    "use_name",
    "use_last_name",
    "middle_name",
    "boxscore_name",
    "nick_name",
    "gender",
    "name_first_last",
    "name_slug",
    "first_last_name",
    "last_first_name",
    "last_init_name",
    "init_last_name",
    "full_fml_name",
    "full_lfm_name",
    "current_team_name",
    "roster_status_code",
    "roster_status_description",
    "source_system",
]

INT_COLUMNS = [
    "player_id",
    "current_age",
    "weight",
    "draft_year",
    "current_team_id",
]

FLOAT_COLUMNS = [
    "strike_zone_top",
    "strike_zone_bottom",
]

BOOL_COLUMNS = [
    "active_flag",
    "is_player",
    "is_verified",
]

DATE_COLUMNS = [
    "birth_date",
    "mlb_debut_date",
]

DATETIME_COLUMNS = [
    "source_load_datetime",
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now_str()}] {msg}")


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

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
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    present_expected = [c for c in EXPECTED_COLUMNS if c in df.columns]
    missing_expected = [c for c in EXPECTED_COLUMNS if c not in df.columns]

    if missing_expected:
        for col in missing_expected:
            df[col] = pd.NA

    return df[EXPECTED_COLUMNS]


def print_completeness(df: pd.DataFrame) -> None:
    log("Column completeness snapshot:")
    total = len(df)

    for col in EXPECTED_COLUMNS:
        if col in df.columns:
            non_null = int(df[col].notna().sum())
            pct = round((non_null / total) * 100, 2) if total else 0.0
            log(f"  {col}: {non_null}/{total} non-null ({pct}%)")


def print_duplicate_summary(df: pd.DataFrame) -> None:
    dupes = int(df.duplicated(subset=["player_id"]).sum())
    distinct_players = int(df["player_id"].nunique(dropna=True))

    log(f"Distinct player_id count: {distinct_players}")
    log(f"Duplicate player_id rows: {dupes}")

    if dupes > 0:
        dup_df = (
            df.groupby("player_id", dropna=False)
            .size()
            .reset_index(name="row_count")
            .query("row_count > 1")
            .sort_values(["row_count", "player_id"], ascending=[False, True])
        )
        log("Top duplicate player_ids:")
        print(dup_df.head(10).to_string(index=False))
        raise ValueError("Duplicate player_id values detected in CSV. Resolve before load.")


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
    log("Starting player load to SQL")

    df = load_csv(CSV_FILE)
    validate_columns(df)
    df = normalize_blank_strings(df)
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

    log("Player load complete")
    log(f"Duration (seconds): {duration_seconds}")


if __name__ == "__main__":
    main()