import sys

from src.generators.generate_hitter_game_reports import generate_reports as generate_hitter_game_reports
from src.generators.generate_pitcher_game_reports import generate_reports as generate_pitcher_game_reports
from src.generators.generate_hitter_season_reports import generate_reports as generate_hitter_season_reports
from src.generators.generate_pitcher_season_reports import generate_reports as generate_pitcher_season_reports
from src.generators.generate_batted_ball_profile_reports import generate_reports as generate_batted_ball_profile_reports
from src.generators.generate_plate_appearance_profile_reports import generate_reports as generate_plate_appearance_profile_reports


def run_full_report_test(game_pk: int, season: int) -> None:
    print("\n=== FULL REPORT TEST START ===\n")
    print(f"Game PK: {game_pk}")
    print(f"Season:  {season}\n")

    # Current player overview reports
    print("\n--- PLAYER OVERVIEW | GAME | HITTING ---")
    generate_hitter_game_reports(game_pk)

    print("\n--- PLAYER OVERVIEW | GAME | PITCHING ---")
    generate_pitcher_game_reports(game_pk)

    print("\n--- PLAYER OVERVIEW | SEASON | HITTING ---")
    generate_hitter_season_reports(season)

    print("\n--- PLAYER OVERVIEW | SEASON | PITCHING ---")
    generate_pitcher_season_reports(season)

    # New batted ball profile reports
    print("\n--- BATTED BALL PROFILE | GAME | HITTING ---")
    generate_batted_ball_profile_reports("hitter", "game", game_pk)

    print("\n--- BATTED BALL PROFILE | GAME | PITCHING ---")
    generate_batted_ball_profile_reports("pitcher", "game", game_pk)

    print("\n--- BATTED BALL PROFILE | SEASON | HITTING ---")
    generate_batted_ball_profile_reports("hitter", "season", season)

    print("\n--- BATTED BALL PROFILE | SEASON | PITCHING ---")
    generate_batted_ball_profile_reports("pitcher", "season", season)

    # New plate appearance profile reports
    print("\n--- PLATE APPEARANCE PROFILE | GAME | HITTING ---")
    generate_plate_appearance_profile_reports("hitter", "game", game_pk)

    print("\n--- PLATE APPEARANCE PROFILE | GAME | PITCHING ---")
    generate_plate_appearance_profile_reports("pitcher", "game", game_pk)

    print("\n--- PLATE APPEARANCE PROFILE | SEASON | HITTING ---")
    generate_plate_appearance_profile_reports("hitter", "season", season)

    print("\n--- PLATE APPEARANCE PROFILE | SEASON | PITCHING ---")
    generate_plate_appearance_profile_reports("pitcher", "season", season)

    print("\n=== FULL REPORT TEST COMPLETE ===\n")


def main() -> None:
    if len(sys.argv) >= 3:
        game_pk = int(sys.argv[1])
        season = int(sys.argv[2])
    else:
        print("\nUsage:")
        print("python -m src.generators.run_full_report_test <game_pk> <season>\n")
        print("Example:")
        print("python -m src.generators.run_full_report_test 778496 2025\n")
        return

    run_full_report_test(game_pk, season)


if __name__ == "__main__":
    main()