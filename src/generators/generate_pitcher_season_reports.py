from pathlib import Path
import sys

import pandas as pd

from src.db_connect import engine
from src.queries.statcast_queries import get_pitcher_games_in_season
from src.visuals.pitcher_season_report import main as pitcher_season_report_main


BASE_DIR = Path(__file__).resolve().parents[2]


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
    """

    pitchers = get_pitchers_in_season(season)

    if pitchers.empty:
        print(f"No pitchers found for season {season}")
        return

    print(f"\nFound {len(pitchers)} pitchers for season {season}\n")

    original_argv = sys.argv.copy()

    success_count = 0
    fail_count = 0
    skipped_count = 0

    try:
        for _, row in pitchers.iterrows():
            pitcher_id = int(row["pitcher_id"])
            pitcher_name = (
                row["pitcher_name"]
                if pd.notna(row["pitcher_name"])
                else f"Pitcher {pitcher_id}"
            )

            games = get_pitcher_games_in_season(engine, pitcher_id, season)

            if games.empty:
                skipped_count += 1
                print(f"Skipping pitcher with no games: {pitcher_name} ({pitcher_id})")
                continue

            print(f"Generating season report: {pitcher_name} ({pitcher_id})")

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
                print(f"Failed for pitcher: {pitcher_name} ({pitcher_id})")
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
        print("python -m src.generators.generate_pitcher_season_reports <season>\n")
        return

    print(f"\nGenerating pitcher season reports for season {season}...")
    generate_reports(season)


if __name__ == "__main__":
    main()