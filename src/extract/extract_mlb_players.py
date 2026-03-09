from pathlib import Path
from datetime import datetime
import time
import json
import requests
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

PLAYERS_OUT_FILE = OUT_DIR / "mlb_players.csv"
FAILURES_OUT_FILE = OUT_DIR / "mlb_players_failures.csv"
SUMMARY_OUT_FILE = LOG_DIR / "extract_mlb_players_summary.json"

BASE_URL = "https://statsapi.mlb.com/api/v1"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 1.5
PLAYER_SLEEP_SECONDS = 0.05


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now_str()}] {msg}")


def get_json(url: str, timeout: int = REQUEST_TIMEOUT, max_retries: int = MAX_RETRIES) -> dict:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            log(f"Request failed (attempt {attempt}/{max_retries}) for URL: {url}")
            log(f"Error: {exc}")

            if attempt < max_retries:
                time.sleep(RETRY_SLEEP_SECONDS)

    raise last_error


def get_team_ids() -> list[int]:
    url = f"{BASE_URL}/teams?sportId=1"
    data = get_json(url)
    teams = data.get("teams", [])

    team_ids = sorted({team["id"] for team in teams if "id" in team})
    log(f"Discovered {len(team_ids)} MLB team ids")
    return team_ids


def get_team_roster(team_id: int) -> list[dict]:
    url = f"{BASE_URL}/teams/{team_id}/roster"
    data = get_json(url)
    return data.get("roster", [])


def get_player_details(player_id: int) -> dict:
    url = f"{BASE_URL}/people/{player_id}"
    data = get_json(url)
    people = data.get("people", [])

    if not people:
        raise ValueError(f"No player details returned for player_id={player_id}")

    return people[0]


def safe_get(d: dict, *keys, default=None):
    cur = d
    for key in keys:
        if cur is None:
            return default
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


def build_player_row(roster_player: dict, player_details: dict, team_id: int) -> dict:
    person = roster_player.get("person", {})
    roster_position = roster_player.get("position", {})

    return {
        "player_id": person.get("id"),
        "full_name": player_details.get("fullName") or person.get("fullName"),
        "first_name": player_details.get("firstName"),
        "last_name": player_details.get("lastName"),
        "primary_number": player_details.get("primaryNumber"),
        "birth_date": player_details.get("birthDate"),
        "current_age": player_details.get("currentAge"),
        "birth_city": player_details.get("birthCity"),
        "birth_state_province": player_details.get("birthStateProvince"),
        "birth_country": player_details.get("birthCountry"),
        "height": player_details.get("height"),
        "weight": player_details.get("weight"),
        "active_flag": player_details.get("active"),
        "primary_position_code": safe_get(player_details, "primaryPosition", "code"),
        "primary_position_name": safe_get(player_details, "primaryPosition", "name"),
        "primary_position_type": safe_get(player_details, "primaryPosition", "type"),
        "primary_position": (
            safe_get(player_details, "primaryPosition", "abbreviation")
            or roster_position.get("abbreviation")
        ),
        "bat_side": safe_get(player_details, "batSide", "code"),
        "bat_side_description": safe_get(player_details, "batSide", "description"),
        "pitch_hand": safe_get(player_details, "pitchHand", "code"),
        "pitch_hand_description": safe_get(player_details, "pitchHand", "description"),
        "use_name": player_details.get("useName"),
        "use_last_name": player_details.get("useLastName"),
        "middle_name": player_details.get("middleName"),
        "boxscore_name": player_details.get("boxscoreName"),
        "nick_name": player_details.get("nickName"),
        "gender": player_details.get("gender"),
        "is_player": player_details.get("isPlayer"),
        "is_verified": player_details.get("isVerified"),
        "draft_year": player_details.get("draftYear"),
        "mlb_debut_date": player_details.get("mlbDebutDate"),
        "name_first_last": player_details.get("nameFirstLast"),
        "name_slug": player_details.get("nameSlug"),
        "first_last_name": player_details.get("firstLastName"),
        "last_first_name": player_details.get("lastFirstName"),
        "last_init_name": player_details.get("lastInitName"),
        "init_last_name": player_details.get("initLastName"),
        "full_fml_name": player_details.get("fullFMLName"),
        "full_lfm_name": player_details.get("fullLFMName"),
        "strike_zone_top": player_details.get("strikeZoneTop"),
        "strike_zone_bottom": player_details.get("strikeZoneBottom"),
        "current_team_id": safe_get(player_details, "currentTeam", "id", default=team_id) or team_id,
        "current_team_name": safe_get(player_details, "currentTeam", "name"),
        "roster_status_code": roster_player.get("status", {}).get("code"),
        "roster_status_description": roster_player.get("status", {}).get("description"),
        "source_system": "mlb_stats_api",
        "source_load_datetime": datetime.now().isoformat(timespec="seconds")
    }


def extract_players() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    team_ids = get_team_ids()

    rows = []
    failures = []
    seen_player_ids = set()

    team_count = len(team_ids)
    roster_rows_seen = 0
    players_attempted = 0
    players_extracted = 0

    started_at = datetime.now()

    for team_index, team_id in enumerate(team_ids, start=1):
        log(f"Team {team_index}/{team_count} | fetching roster for team_id={team_id}")

        try:
            roster = get_team_roster(team_id)
        except Exception as exc:
            log(f"FAILED roster extraction for team_id={team_id}: {exc}")
            failures.append({
                "team_id": team_id,
                "player_id": None,
                "stage": "roster",
                "error": str(exc)
            })
            continue

        log(f"Team {team_index}/{team_count} | roster size={len(roster)}")

        for roster_player in roster:
            roster_rows_seen += 1

            person = roster_player.get("person", {})
            player_id = person.get("id")

            if player_id is None:
                failures.append({
                    "team_id": team_id,
                    "player_id": None,
                    "stage": "roster_parse",
                    "error": "Missing player_id in roster payload"
                })
                continue

            if player_id in seen_player_ids:
                continue

            seen_player_ids.add(player_id)
            players_attempted += 1

            if players_attempted % 25 == 0:
                log(
                    f"Progress | attempted={players_attempted} "
                    f"success={players_extracted} "
                    f"failures={len(failures)}"
                )

            try:
                details = get_player_details(player_id)
                row = build_player_row(roster_player, details, team_id)
                rows.append(row)
                players_extracted += 1
            except Exception as exc:
                log(f"FAILED player detail extraction for player_id={player_id}: {exc}")
                failures.append({
                    "team_id": team_id,
                    "player_id": player_id,
                    "stage": "player_details",
                    "error": str(exc)
                })

            time.sleep(PLAYER_SLEEP_SECONDS)

    ended_at = datetime.now()
    duration_seconds = round((ended_at - started_at).total_seconds(), 2)

    df_players = pd.DataFrame(rows).drop_duplicates(subset=["player_id"]).sort_values("player_id")
    df_failures = pd.DataFrame(failures)

    summary = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "ended_at": ended_at.isoformat(timespec="seconds"),
        "duration_seconds": duration_seconds,
        "team_count": team_count,
        "roster_rows_seen": roster_rows_seen,
        "unique_players_attempted": players_attempted,
        "players_extracted": int(len(df_players)),
        "failure_count": int(len(df_failures)),
        "duplicate_players_removed": int(players_extracted - len(df_players)),
        "output_file": str(PLAYERS_OUT_FILE),
        "failure_file": str(FAILURES_OUT_FILE)
    }

    return df_players, df_failures, summary


def print_observability_summary(df_players: pd.DataFrame, df_failures: pd.DataFrame, summary: dict) -> None:
    log("Extraction complete")
    log(json.dumps(summary, indent=2))

    if df_players.empty:
        log("No players extracted")
        return

    log("Column completeness snapshot:")
    for col in [
        "player_id",
        "full_name",
        "first_name",
        "last_name",
        "primary_position",
        "bat_side",
        "pitch_hand",
        "current_team_id",
        "current_team_name",
        "birth_date",
        "height",
        "weight",
        "mlb_debut_date",
        "strike_zone_top",
        "strike_zone_bottom"
    ]:
        if col in df_players.columns:
            non_null = int(df_players[col].notna().sum())
            pct = round(100 * non_null / len(df_players), 2)
            log(f"  {col}: non_null={non_null}/{len(df_players)} ({pct}%)")

    if not df_failures.empty:
        log("Failure breakdown by stage:")
        stage_counts = df_failures["stage"].value_counts().to_dict()
        for stage, count in stage_counts.items():
            log(f"  {stage}: {count}")


def main():
    log("Starting MLB player extraction")

    df_players, df_failures, summary = extract_players()

    df_players.to_csv(PLAYERS_OUT_FILE, index=False)

    if not df_failures.empty:
        df_failures.to_csv(FAILURES_OUT_FILE, index=False)
    elif FAILURES_OUT_FILE.exists():
        FAILURES_OUT_FILE.unlink()

    with open(SUMMARY_OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print_observability_summary(df_players, df_failures, summary)

    log(f"Players written to: {PLAYERS_OUT_FILE}")
    if not df_failures.empty:
        log(f"Failures written to: {FAILURES_OUT_FILE}")
    log(f"Summary written to: {SUMMARY_OUT_FILE}")


if __name__ == "__main__":
    main()