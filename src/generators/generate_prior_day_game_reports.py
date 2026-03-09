import sys
from datetime import datetime, timedelta

import pandas as pd

from src.db_connect import engine
from src.generators.generate_pitcher_game_reports import generate_reports as generate_pitcher_reports
from src.generators.generate_hitter_game_reports import generate_reports as generate_hitter_reports


def get_games_for_date(game_date: str) -> pd.DataFrame:
    query = f"""
        SELECT DISTINCT
            game_pk,
            game_date
        FROM clean.pitches
        WHERE CAST(game_date AS date) = '{game_date}'
          AND game_pk IS NOT NULL
        ORDER BY game_pk
    """
    return pd.read_sql(query, engine)


def generate_prior_day_reports(target_date: str) -> None:
    """
    Generate all hitter and pitcher game reports for every game on a target date.

    This script is a top-level orchestrator only.
    The downstream generator scripts are responsible for:
        - discovering hitters / pitchers in each game
        - calling the individual report scripts
        - reporting per-game batch success / failure
    """

    games = get_games_for_date(target_date)

    if games.empty:
        print(f"No games found for {target_date}")
        return

    games = games.copy().sort_values(["game_date", "game_pk"])

    total_games = len(games)

    print(f"\nFound {total_games} games for {target_date}\n")

    pitcher_batch_success = 0
    hitter_batch_success = 0
    fail_count = 0
    failed_batches: list[str] = []

    for idx, (_, row) in enumerate(games.iterrows(), start=1):
        game_pk = int(row["game_pk"])

        print(f"\n[{idx}/{total_games}] Processing game {game_pk} ({target_date})")

        try:
            print("Running pitcher game report batch...")
            generate_pitcher_reports(game_pk)
            pitcher_batch_success += 1
        except Exception as e:
            fail_count += 1
            failed_batches.append(f"Pitcher batch failed for game {game_pk}")
            print(f"Pitcher generator failed for game {game_pk}")
            print(f"Error: {e}\n")

        try:
            print("Running hitter game report batch...")
            generate_hitter_reports(game_pk)
            hitter_batch_success += 1
        except Exception as e:
            fail_count += 1
            failed_batches.append(f"Hitter batch failed for game {game_pk}")
            print(f"Hitter generator failed for game {game_pk}")
            print(f"Error: {e}\n")

    print("\nPrior-day batch generation complete.")
    print(f"Date:                       {target_date}")
    print(f"Games processed:            {total_games}")
    print(f"Pitcher batches completed:  {pitcher_batch_success}")
    print(f"Hitter batches completed:   {hitter_batch_success}")
    print(f"Failures:                   {fail_count}")

    if failed_batches:
        print("\nFailed batches:")
        for item in failed_batches:
            print(f" - {item}")

    print("")


def main() -> None:
    if len(sys.argv) >= 2:
        target_date = sys.argv[1]
    else:
        target_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\nGenerating all hitter and pitcher game reports for {target_date}...")
    generate_prior_day_reports(target_date)


if __name__ == "__main__":
    main()