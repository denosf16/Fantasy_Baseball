from pathlib import Path
import sys

import pandas as pd

from src.db_connect import engine
from src.queries.statcast_queries import get_game_pitchers
from src.visuals.pitcher_game_report import main as pitcher_game_report_main


BASE_DIR = Path(__file__).resolve().parents[2]


def generate_reports(game_pk: int) -> None:
    """
    Generate pitcher game reports for every pitcher in a given game.
    """

    pitchers = get_game_pitchers(engine, game_pk)

    if pitchers.empty:
        print(f"No pitchers found for game {game_pk}")
        return

    print(f"\nFound {len(pitchers)} pitchers for game {game_pk}\n")

    original_argv = sys.argv.copy()

    success_count = 0
    fail_count = 0

    try:
        for _, row in pitchers.iterrows():
            pitcher_id = int(row["pitcher_id"])
            pitcher_name = (
                row["pitcher_name"]
                if pd.notna(row["pitcher_name"])
                else f"Pitcher {pitcher_id}"
            )

            print(f"Generating report: {pitcher_name} ({pitcher_id})")

            try:
                sys.argv = [
                    "pitcher_game_report.py",
                    str(pitcher_id),
                    str(game_pk),
                ]

                pitcher_game_report_main()
                success_count += 1

            except Exception as e:
                fail_count += 1
                print(f"Failed for pitcher: {pitcher_name} ({pitcher_id})")
                print(f"Error: {e}\n")

    finally:
        sys.argv = original_argv

    print("\nBatch generation complete.")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}\n")


def main() -> None:
    if len(sys.argv) >= 2:
        game_pk = int(sys.argv[1])
    else:
        print("\nUsage:")
        print("python -m src.generators.generate_pitcher_game_reports <game_pk>\n")
        return

    print(f"\nGenerating pitcher game reports for game {game_pk}...")
    generate_reports(game_pk)


if __name__ == "__main__":
    main()