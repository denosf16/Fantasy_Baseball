from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def create_output_structure() -> None:
    """
    Create standardized output folder hierarchy for reports.
    """

    folders = [
        "outputs/png/pitchers/game",
        "outputs/png/pitchers/season",
        "outputs/png/hitters/game",
        "outputs/png/hitters/season",
    ]

    for folder in folders:
        path = BASE_DIR / folder
        path.mkdir(parents=True, exist_ok=True)
        print(f"Created: {path}")


def main() -> None:
    print("\nCreating report output folder structure...\n")
    create_output_structure()
    print("\nOutput folders ready.\n")


if __name__ == "__main__":
    main()