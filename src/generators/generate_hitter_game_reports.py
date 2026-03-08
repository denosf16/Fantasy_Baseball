from pathlib import Path
import sys

import pandas as pd

from src.db_connect import engine
from src.queries.statcast_queries import get_game_hitters
from src.visuals.hitter_game_report import main as hitter_game_report_main


BASE_DIR = Path(__file__).resolve().parents[2]


def generate_reports(game_pk: int) -> None:
    """
    Generate hitter game reports for every hitter in a given game.
    """

    hitters = get_game_hitters(engine, game_pk)

    if hitters.empty:
        print(f"No hitters found for game {game_pk}")
        return

    print(f"\nFound {len(hitters)} hitters for game {game_pk}\n")

    original_argv = sys.argv.copy()

    success_count = 0
    fail_count = 0

    try:
        for _, row in hitters.iterrows():
            batter_id = int(row["batter_id"])
            batter_name = (
                row["batter_name"]
                if pd.notna(row["batter_name"])
                else f"Hitter {batter_id}"
            )

            print(f"Generating report: {batter_name} ({batter_id})")

            try:
                sys.argv = [
                    "hitter_game_report.py",
                    str(batter_id),
                    str(game_pk),
                ]

                hitter_game_report_main()
                success_count += 1

            except Exception as e:
                fail_count += 1
                print(f"Failed for hitter: {batter_name} ({batter_id})")
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
        print("python -m src.generators.generate_hitter_game_reports <game_pk>\n")
        return

    print(f"\nGenerating hitter game reports for game {game_pk}...")
    generate_reports(game_pk)


if __name__ == "__main__":
    main()