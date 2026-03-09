import sys

import pandas as pd

from src.db_connect import engine
from src.visuals.hitter_season_report import main as hitter_season_report_main


def get_hitters_in_season(season: int) -> pd.DataFrame:
    query = f"""
        SELECT DISTINCT
            batter_id,
            batter_name
        FROM clean.pitches
        WHERE season = {season}
          AND batter_id IS NOT NULL
        ORDER BY batter_name, batter_id
    """
    return pd.read_sql(query, engine)


def generate_reports(season: int) -> None:
    """
    Generate hitter season reports for every hitter in a given season.

    This script is an orchestrator only.
    The individual hitter_season_report script is responsible for:
        - querying data
        - building the chart
        - routing the output path
        - saving the PNG
    """

    hitters = get_hitters_in_season(season)

    if hitters.empty:
        print(f"No hitters found for season {season}")
        return

    hitters = hitters.copy()

    if "batter_name" in hitters.columns:
        hitters = hitters.sort_values(["batter_name", "batter_id"], na_position="last")
    else:
        hitters = hitters.sort_values(["batter_id"])

    total_hitters = len(hitters)

    print(f"\nFound {total_hitters} hitters for season {season}\n")

    original_argv = sys.argv.copy()

    success_count = 0
    fail_count = 0
    failed_hitters: list[str] = []

    try:
        for idx, (_, row) in enumerate(hitters.iterrows(), start=1):
            batter_id = int(row["batter_id"])
            batter_name = (
                str(row["batter_name"]).strip()
                if "batter_name" in row and pd.notna(row["batter_name"])
                else f"Hitter {batter_id}"
            )

            print(f"[{idx}/{total_hitters}] Generating hitter season report: {batter_name} ({batter_id})")

            try:
                sys.argv = [
                    "hitter_season_report.py",
                    str(batter_id),
                    str(season),
                ]

                hitter_season_report_main()
                success_count += 1

            except Exception as e:
                fail_count += 1
                failed_hitters.append(f"{batter_name} ({batter_id})")
                print(f"Failed for hitter: {batter_name} ({batter_id})")
                print(f"Error: {e}\n")

    finally:
        sys.argv = original_argv

    print("\nBatch generation complete.")
    print(f"Season:     {season}")
    print(f"Total:      {total_hitters}")
    print(f"Successful: {success_count}")
    print(f"Failed:     {fail_count}")

    if failed_hitters:
        print("\nFailed hitters:")
        for hitter in failed_hitters:
            print(f" - {hitter}")

    print("")


def main() -> None:
    if len(sys.argv) >= 2:
        season = int(sys.argv[1])
    else:
        print("\nUsage:")
        print("python -m src.generators.generate_hitter_season_reports <season>\n")
        return

    print(f"\nGenerating hitter season reports for season {season}...")
    generate_reports(season)


if __name__ == "__main__":
    main()