from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR / "outputs"


def create_output_structure() -> None:
    """
    Create standardized output folder hierarchy for report generation.

    Structure:

        outputs/
            png/
            pdf/
            manifests/

            {report_family}/
                game/team/
                season/team/

    Report families currently supported:
        player_overview
        matchup_preview
        batted_ball_profile
        plate_appearance_profile
        arsenal
        rolling_form
    """

    report_families = [
        "player_overview",
        "matchup_preview",
        "batted_ball_profile",
        "plate_appearance_profile",
        "arsenal",
        "rolling_form",
    ]

    folders = []

    # Root directories
    folders.extend([
        "outputs/png",
        "outputs/pdf",
        "outputs/manifests",
    ])

    # Report family structures
    for family in report_families:
        folders.extend([
            f"outputs/png/{family}/game/team",
            f"outputs/png/{family}/season/team",
            f"outputs/pdf/{family}/game/team",
            f"outputs/pdf/{family}/season/team",
        ])

    created_count = 0
    skipped_count = 0

    for folder in folders:
        path = BASE_DIR / folder

        if path.exists():
            skipped_count += 1
        else:
            path.mkdir(parents=True, exist_ok=True)
            created_count += 1
            print(f"Created: {path}")

    print("\nSummary")
    print(f"Folders created: {created_count}")
    print(f"Folders already existed: {skipped_count}")


def main() -> None:
    print("\nCreating standardized output folder structure...\n")
    create_output_structure()
    print("\nOutput folders ready.\n")


if __name__ == "__main__":
    main()