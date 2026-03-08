from pathlib import Path
import sys

import pandas as pd

from src.db_connect import engine
from src.queries.statcast_queries import get_hitter_games_in_season
from src.visuals.hitter_season_report import main as hitter_season_report_main


BASE_DIR = Path(__file__).resolve().parents[2]


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
    """

    hitters = get_hitters_in_season(season)

    if hitters.empty:
        print(f"No hitters found for season {season}")
        return

    print(f"\nFound {len(hitters)} hitters for season {season}\n")

    original_argv = sys.argv.copy()

    success_count = 0
    fail_count = 0
    skipped_count = 0

    try:
        for _, row in hitters.iterrows():
            batter_id = int(row["batter_id"])
            batter_name = (
                row["batter_name"]
                if pd.notna(row["batter_name"])
                else f"Hitter {batter_id}"
            )

            games = get_hitter_games_in_season(engine, batter_id, season)

            if games.empty:
                skipped_count += 1
                print(f"Skipping hitter with no games: {batter_name} ({batter_id})")
                continue

            print(f"Generating season report: {batter_name} ({batter_id})")

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
                print(f"Failed for hitter: {batter_name} ({batter_id})")
                print(f"Error: {e}\n")

    finally:
        sys.argv = original_argv

    print("\nBatch generation complete.")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Skipped: {skipped_count}\n")


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