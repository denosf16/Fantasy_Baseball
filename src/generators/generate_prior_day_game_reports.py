from pathlib import Path
import sys
from datetime import datetime, timedelta

import pandas as pd

from src.db_connect import engine
from src.generators.generate_pitcher_game_reports import generate_reports as generate_pitcher_reports
from src.generators.generate_hitter_game_reports import generate_reports as generate_hitter_reports


BASE_DIR = Path(__file__).resolve().parents[2]


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
    games = get_games_for_date(target_date)

    if games.empty:
        print(f"No games found for {target_date}")
        return

    print(f"\nFound {len(games)} games for {target_date}\n")

    pitcher_success = 0
    hitter_success = 0
    fail_count = 0

    for _, row in games.iterrows():
        game_pk = int(row["game_pk"])
        print(f"\nProcessing game {game_pk} ({target_date})")

        try:
            print("Running pitcher game reports...")
            generate_pitcher_reports(game_pk)
            pitcher_success += 1
        except Exception as e:
            fail_count += 1
            print(f"Pitcher generator failed for game {game_pk}")
            print(f"Error: {e}\n")

        try:
            print("Running hitter game reports...")
            generate_hitter_reports(game_pk)
            hitter_success += 1
        except Exception as e:
            fail_count += 1
            print(f"Hitter generator failed for game {game_pk}")
            print(f"Error: {e}\n")

    print("\nPrior-day batch generation complete.")
    print(f"Pitcher game batches completed: {pitcher_success}")
    print(f"Hitter game batches completed: {hitter_success}")
    print(f"Failures: {fail_count}\n")


def main() -> None:
    if len(sys.argv) >= 2:
        target_date = sys.argv[1]
    else:
        target_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\nGenerating all hitter and pitcher game reports for {target_date}...")
    generate_prior_day_reports(target_date)


if __name__ == "__main__":
    main()