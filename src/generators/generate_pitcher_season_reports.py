import sys

import pandas as pd

from src.db_connect import engine
from src.visuals.pitcher_season_report import main as pitcher_season_report_main


def get_pitchers_in_season(season: int) -> pd.DataFrame:
    query = f"""
        SELECT DISTINCT
            pitcher_id,
            pitcher_name
        FROM clean.pitches
        WHERE season = {season}
          AND pitcher_id IS NOT NULL
        ORDER BY pitcher_name, pitcher_id
    """
    return pd.read_sql(query, engine)


def generate_reports(season: int) -> None:
    """
    Generate pitcher season reports for every pitcher in a given season.

    This script is an orchestrator only.
    The individual pitcher_season_report script is responsible for:
        - querying data
        - building the chart
        - routing the output path
        - saving the PNG
    """

    pitchers = get_pitchers_in_season(season)

    if pitchers.empty:
        print(f"No pitchers found for season {season}")
        return

    pitchers = pitchers.copy()

    if "pitcher_name" in pitchers.columns:
        pitchers = pitchers.sort_values(["pitcher_name", "pitcher_id"], na_position="last")
    else:
        pitchers = pitchers.sort_values(["pitcher_id"])

    total_pitchers = len(pitchers)

    print(f"\nFound {total_pitchers} pitchers for season {season}\n")

    original_argv = sys.argv.copy()

    success_count = 0
    fail_count = 0
    failed_pitchers: list[str] = []

    try:
        for idx, (_, row) in enumerate(pitchers.iterrows(), start=1):
            pitcher_id = int(row["pitcher_id"])
            pitcher_name = (
                str(row["pitcher_name"]).strip()
                if "pitcher_name" in row and pd.notna(row["pitcher_name"])
                else f"Pitcher {pitcher_id}"
            )

            print(f"[{idx}/{total_pitchers}] Generating pitcher season report: {pitcher_name} ({pitcher_id})")

            try:
                sys.argv = [
                    "pitcher_season_report.py",
                    str(pitcher_id),
                    str(season),
                ]

                pitcher_season_report_main()
                success_count += 1

            except Exception as e:
                fail_count += 1
                failed_pitchers.append(f"{pitcher_name} ({pitcher_id})")
                print(f"Failed for pitcher: {pitcher_name} ({pitcher_id})")
                print(f"Error: {e}\n")

    finally:
        sys.argv = original_argv

    print("\nBatch generation complete.")
    print(f"Season:     {season}")
    print(f"Total:      {total_pitchers}")
    print(f"Successful: {success_count}")
    print(f"Failed:     {fail_count}")

    if failed_pitchers:
        print("\nFailed pitchers:")
        for pitcher in failed_pitchers:
            print(f" - {pitcher}")

    print("")


def main() -> None:
    if len(sys.argv) >= 2:
        season = int(sys.argv[1])
    else:
        print("\nUsage:")
        print("python -m src.generators.generate_pitcher_season_reports <season>\n")
        return

    print(f"\nGenerating pitcher season reports for season {season}...")
    generate_reports(season)


if __name__ == "__main__":
    main()