import sys

import pandas as pd

from src.db_connect import engine
from src.visuals.plate_appearance_profile_report import main as plate_appearance_profile_report_main


def get_entities(entity_type: str, time_grain: str, value: int) -> pd.DataFrame:
    if entity_type == "hitter":
        id_col = "batter_id"
        name_col = "batter_name"
    else:
        id_col = "pitcher_id"
        name_col = "pitcher_name"

    if time_grain == "game":
        query = f"""
            SELECT DISTINCT
                {id_col} AS entity_id,
                {name_col} AS entity_name
            FROM clean.at_bats
            WHERE game_pk = {value}
              AND {id_col} IS NOT NULL
            ORDER BY {name_col}, {id_col}
        """
    else:
        query = f"""
            SELECT DISTINCT
                {id_col} AS entity_id,
                {name_col} AS entity_name
            FROM clean.at_bats
            WHERE season = {value}
              AND {id_col} IS NOT NULL
            ORDER BY {name_col}, {id_col}
        """

    return pd.read_sql(query, engine)


def generate_reports(entity_type: str, time_grain: str, value: int) -> None:
    entities = get_entities(entity_type, time_grain, value)

    if entities.empty:
        print(f"No {entity_type}s found for {time_grain}={value}")
        return

    entities = entities.copy().sort_values(["entity_name", "entity_id"], na_position="last")
    total_entities = len(entities)

    print(f"\nFound {total_entities} {entity_type}s for {time_grain}={value}\n")

    original_argv = sys.argv.copy()

    success_count = 0
    fail_count = 0
    failed_entities: list[str] = []

    try:
        for idx, (_, row) in enumerate(entities.iterrows(), start=1):
            entity_id = int(row["entity_id"])
            entity_name = (
                str(row["entity_name"]).strip()
                if pd.notna(row["entity_name"])
                else f"{entity_type.title()} {entity_id}"
            )

            print(
                f"[{idx}/{total_entities}] Generating plate appearance profile: "
                f"{entity_name} ({entity_id})"
            )

            try:
                sys.argv = [
                    "plate_appearance_profile_report.py",
                    entity_type,
                    time_grain,
                    str(entity_id),
                    str(value),
                ]
                plate_appearance_profile_report_main()
                success_count += 1

            except Exception as e:
                fail_count += 1
                failed_entities.append(f"{entity_name} ({entity_id})")
                print(f"Failed for {entity_type}: {entity_name} ({entity_id})")
                print(f"Error: {e}\n")

    finally:
        sys.argv = original_argv

    print("\nBatch generation complete.")
    print(f"Entity Type: {entity_type}")
    print(f"Time Grain:  {time_grain}")
    print(f"Value:       {value}")
    print(f"Total:       {total_entities}")
    print(f"Successful:  {success_count}")
    print(f"Failed:      {fail_count}")

    if failed_entities:
        print("\nFailed entities:")
        for entity in failed_entities:
            print(f" - {entity}")

    print("")


def main() -> None:
    if len(sys.argv) >= 4:
        entity_type = str(sys.argv[1]).strip().lower()
        time_grain = str(sys.argv[2]).strip().lower()
        value = int(sys.argv[3])
    else:
        print("\nUsage:")
        print(
            "python -m src.generators.generate_plate_appearance_profile_reports "
            "<entity_type> <time_grain> <value>\n"
        )
        print("Examples:")
        print("python -m src.generators.generate_plate_appearance_profile_reports hitter game 778496")
        print("python -m src.generators.generate_plate_appearance_profile_reports pitcher season 2025\n")
        return

    if entity_type not in {"hitter", "pitcher"}:
        raise ValueError("entity_type must be 'hitter' or 'pitcher'")

    if time_grain not in {"game", "season"}:
        raise ValueError("time_grain must be 'game' or 'season'")

    print(
        f"\nGenerating plate appearance profile reports for "
        f"{entity_type} | {time_grain}={value}..."
    )
    generate_reports(entity_type, time_grain, value)


if __name__ == "__main__":
    main()