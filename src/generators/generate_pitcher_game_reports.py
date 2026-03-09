import sys

import pandas as pd

from src.db_connect import engine
from src.queries.statcast_queries import get_game_pitchers
from src.visuals.pitcher_game_report import main as pitcher_game_report_main


def generate_reports(game_pk: int) -> None:
    """
    Generate pitcher game reports for every pitcher in a given game.

    This script is an orchestrator only.
    The individual pitcher_game_report script is responsible for:
        - querying data
        - building the chart
        - routing the output path
        - saving the PNG
    """

    pitchers = get_game_pitchers(engine, game_pk)

    if pitchers.empty:
        print(f"No pitchers found for game {game_pk}")
        return

    pitchers = pitchers.copy()

    if "pitcher_name" in pitchers.columns:
        pitchers = pitchers.sort_values(["pitcher_name", "pitcher_id"], na_position="last")
    else:
        pitchers = pitchers.sort_values(["pitcher_id"])

    total_pitchers = len(pitchers)

    print(f"\nFound {total_pitchers} pitchers for game {game_pk}\n")

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

            print(f"[{idx}/{total_pitchers}] Generating pitcher game report: {pitcher_name} ({pitcher_id})")

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
                failed_pitchers.append(f"{pitcher_name} ({pitcher_id})")
                print(f"Failed for pitcher: {pitcher_name} ({pitcher_id})")
                print(f"Error: {e}\n")

    finally:
        sys.argv = original_argv

    print("\nBatch generation complete.")
    print(f"Game PK:    {game_pk}")
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
        game_pk = int(sys.argv[1])
    else:
        print("\nUsage:")
        print("python -m src.generators.generate_pitcher_game_reports <game_pk>\n")
        return

    print(f"\nGenerating pitcher game reports for game {game_pk}...")
    generate_reports(game_pk)


if __name__ == "__main__":
    main()