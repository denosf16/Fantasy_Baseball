from pathlib import Path
from typing import Optional


BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUTS_DIR = BASE_DIR / "outputs"


VALID_FORMATS = {"png", "pdf"}
VALID_TIME_GRAINS = {"game", "season"}
VALID_ENTITY_SCOPES = {"team"}
VALID_SIDES = {"hitting", "pitching"}


def slugify(value: str) -> str:
    """
    Convert a string to a filesystem-friendly slug.
    """
    value = (value or "").strip().lower()

    replacements = {
        " ": "_",
        "-": "_",
        "/": "_",
        "\\": "_",
        ":": "",
        ",": "",
        ".": "",
        "'": "",
        '"': "",
        "(": "",
        ")": "",
        "[": "",
        "]": "",
        "{": "",
        "}": "",
        "&": "and",
        "__": "_",
    }

    for old, new in replacements.items():
        value = value.replace(old, new)

    while "__" in value:
        value = value.replace("__", "_")

    return value.strip("_")


def validate_output_inputs(
    output_format: str,
    report_family: str,
    time_grain: str,
    entity_scope: str,
    side: str,
    team_code: str,
    game_date: Optional[str],
    season: Optional[int],
) -> None:
    """
    Validate routing inputs before building paths.
    """
    if output_format not in VALID_FORMATS:
        raise ValueError(f"Invalid output_format: {output_format}")

    if time_grain not in VALID_TIME_GRAINS:
        raise ValueError(f"Invalid time_grain: {time_grain}")

    if entity_scope not in VALID_ENTITY_SCOPES:
        raise ValueError(f"Invalid entity_scope: {entity_scope}")

    if side not in VALID_SIDES:
        raise ValueError(f"Invalid side: {side}")

    if not report_family or not str(report_family).strip():
        raise ValueError("report_family is required")

    if not team_code or not str(team_code).strip():
        raise ValueError("team_code is required")

    if time_grain == "game" and not game_date:
        raise ValueError("game_date is required for game-level reports")

    if time_grain == "season" and season is None:
        raise ValueError("season is required for season-level reports")


def get_output_dir(
    report_family: str,
    time_grain: str,
    side: str,
    team_code: str,
    output_format: str = "png",
    entity_scope: str = "team",
    game_date: Optional[str] = None,
    season: Optional[int] = None,
    create: bool = True,
) -> Path:
    """
    Build the output directory for a report.

    Examples:
        outputs/png/player_overview/game/team/SEA/2025-03-31/hitting/
        outputs/png/player_overview/season/team/SEA/2025/hitting/
    """
    validate_output_inputs(
        output_format=output_format,
        report_family=report_family,
        time_grain=time_grain,
        entity_scope=entity_scope,
        side=side,
        team_code=team_code,
        game_date=game_date,
        season=season,
    )

    report_family_slug = slugify(report_family)
    team_code_slug = slugify(team_code).upper()

    parts = [
        OUTPUTS_DIR,
        output_format,
        report_family_slug,
        time_grain,
        entity_scope,
        team_code_slug,
    ]

    if time_grain == "game":
        parts.append(str(game_date))
    elif time_grain == "season":
        parts.append(str(season))

    parts.append(side)

    output_dir = Path(*parts)

    if create:
        output_dir.mkdir(parents=True, exist_ok=True)

    return output_dir


def get_output_filename(
    player_name: str,
    player_id: int | str,
    extension: str = "png",
) -> str:
    """
    Build a standard player report filename.

    Example:
        julio_rodriguez_677594.png
    """
    player_slug = slugify(player_name)
    return f"{player_slug}_{player_id}.{extension}"


def get_output_path(
    report_family: str,
    time_grain: str,
    side: str,
    team_code: str,
    player_name: str,
    player_id: int | str,
    output_format: str = "png",
    entity_scope: str = "team",
    game_date: Optional[str] = None,
    season: Optional[int] = None,
    create: bool = True,
) -> Path:
    """
    Build the full output path for a player report.

    Example:
        outputs/png/player_overview/game/team/SEA/2025-03-31/hitting/julio_rodriguez_677594.png
    """
    output_dir = get_output_dir(
        report_family=report_family,
        time_grain=time_grain,
        side=side,
        team_code=team_code,
        output_format=output_format,
        entity_scope=entity_scope,
        game_date=game_date,
        season=season,
        create=create,
    )

    filename = get_output_filename(
        player_name=player_name,
        player_id=player_id,
        extension=output_format,
    )

    return output_dir / filename


def get_bundle_output_path(
    report_family: str,
    time_grain: str,
    side: str,
    team_code: str,
    output_format: str = "pdf",
    entity_scope: str = "team",
    game_date: Optional[str] = None,
    season: Optional[int] = None,
    create: bool = True,
) -> Path:
    """
    Build a standard team bundle filename.

    Examples:
        SEA_2025-03-31_hitting_report_bundle.pdf
        SEA_2025_pitching_report_bundle.pdf
    """
    output_dir = get_output_dir(
        report_family=report_family,
        time_grain=time_grain,
        side=side,
        team_code=team_code,
        output_format=output_format,
        entity_scope=entity_scope,
        game_date=game_date,
        season=season,
        create=create,
    )

    team_code_slug = slugify(team_code).upper()

    if time_grain == "game":
        stem = f"{team_code_slug}_{game_date}_{side}_report_bundle"
    else:
        stem = f"{team_code_slug}_{season}_{side}_report_bundle"

    return output_dir / f"{stem}.{output_format}"


if __name__ == "__main__":
    # Example usage
    example_game_path = get_output_path(
        report_family="player_overview",
        time_grain="game",
        side="hitting",
        team_code="SEA",
        game_date="2025-03-31",
        player_name="Julio Rodríguez",
        player_id=677594,
        output_format="png",
    )

    example_season_path = get_output_path(
        report_family="player_overview",
        time_grain="season",
        side="pitching",
        team_code="SEA",
        season=2025,
        player_name="Luis Castillo",
        player_id=622491,
        output_format="png",
    )

    example_bundle_path = get_bundle_output_path(
        report_family="player_overview",
        time_grain="game",
        side="hitting",
        team_code="SEA",
        game_date="2025-03-31",
        output_format="pdf",
    )

    print("Example game path:   ", example_game_path)
    print("Example season path: ", example_season_path)
    print("Example bundle path: ", example_bundle_path)