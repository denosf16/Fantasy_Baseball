import sys

import pandas as pd

from src.db_connect import engine
from src.queries.statcast_queries import get_game_hitters
from src.visuals.hitter_game_report import main as hitter_game_report_main


def generate_reports(game_pk: int) -> None:
    """
    Generate hitter game reports for every hitter in a given game.

    This script is an orchestrator only.
    The individual hitter_game_report script is responsible for:
        - querying data
        - building the chart
        - routing the output path
        - saving the PNG
    """

    hitters = get_game_hitters(engine, game_pk)

    if hitters.empty:
        print(f"No hitters found for game {game_pk}")
        return

    hitters = hitters.copy()

    if "batter_name" in hitters.columns:
        hitters = hitters.sort_values(["batter_name", "batter_id"], na_position="last")
    else:
        hitters = hitters.sort_values(["batter_id"])

    total_hitters = len(hitters)

    print(f"\nFound {total_hitters} hitters for game {game_pk}\n")

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

            print(f"[{idx}/{total_hitters}] Generating hitter game report: {batter_name} ({batter_id})")

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
                failed_hitters.append(f"{batter_name} ({batter_id})")
                print(f"Failed for hitter: {batter_name} ({batter_id})")
                print(f"Error: {e}\n")

    finally:
        sys.argv = original_argv

    print("\nBatch generation complete.")
    print(f"Game PK:    {game_pk}")
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
        game_pk = int(sys.argv[1])
    else:
        print("\nUsage:")
        print("python -m src.generators.generate_hitter_game_reports <game_pk>\n")
        return

    print(f"\nGenerating hitter game reports for game {game_pk}...")
    generate_reports(game_pk)


if __name__ == "__main__":
    main()