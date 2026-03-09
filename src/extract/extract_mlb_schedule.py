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

BASE_URL = "https://statsapi.mlb.com/api/v1/schedule"
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 2.0

START_DATE = "2025-03-27"
END_DATE = "2025-03-31"

OUT_FILE = OUT_DIR / f"mlb_schedule_{START_DATE}_to_{END_DATE}.csv"
FAILURES_OUT_FILE = OUT_DIR / f"mlb_schedule_{START_DATE}_to_{END_DATE}_failures.csv"
SUMMARY_OUT_FILE = LOG_DIR / f"extract_mlb_schedule_{START_DATE}_to_{END_DATE}_summary.json"


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now_str()}] {msg}")


def get_json(url: str, params: dict, timeout: int = REQUEST_TIMEOUT, max_retries: int = MAX_RETRIES) -> dict:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            log(f"Request failed (attempt {attempt}/{max_retries})")
            log(f"URL: {url}")
            log(f"Params: {params}")
            log(f"Error: {exc}")

            if attempt < max_retries:
                time.sleep(RETRY_SLEEP_SECONDS)

    raise last_error


def safe_get(d: dict, *keys, default=None):
    cur = d
    for key in keys:
        if cur is None:
            return default
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


def build_game_row(game: dict, game_date: str) -> dict:
    teams = game.get("teams", {})
    home = teams.get("home", {})
    away = teams.get("away", {})
    venue = game.get("venue", {})
    status = game.get("status", {})
    content = game.get("content", {})

    return {
        "game_pk": game.get("gamePk"),
        "game_guid": game.get("gameGuid"),
        "game_date": game_date,
        "official_date": game.get("officialDate"),
        "season": game.get("season"),
        "game_type": game.get("gameType"),

        "abstract_game_state": status.get("abstractGameState"),
        "coded_game_state": status.get("codedGameState"),
        "detailed_state": status.get("detailedState"),
        "status_code": status.get("statusCode"),
        "start_time_tbd": status.get("startTimeTBD"),
        "abstract_game_code": status.get("abstractGameCode"),

        "game_datetime_utc": game.get("gameDate"),
        "double_header": game.get("doubleHeader"),
        "day_night": game.get("dayNight"),
        "description": game.get("description"),
        "scheduled_innings": game.get("scheduledInnings"),
        "games_in_series": game.get("gamesInSeries"),
        "series_game_number": game.get("seriesGameNumber"),
        "series_description": game.get("seriesDescription"),

        "home_team_id": safe_get(home, "team", "id"),
        "home_team_name": safe_get(home, "team", "name"),
        "home_league_id": safe_get(home, "leagueRecord", "league", "id"),
        "home_sport_wins": safe_get(home, "leagueRecord", "wins"),
        "home_sport_losses": safe_get(home, "leagueRecord", "losses"),
        "home_sport_pct": safe_get(home, "leagueRecord", "pct"),
        "home_score": home.get("score"),
        "home_is_winner": home.get("isWinner"),
        "home_split_squad": safe_get(home, "splitSquad"),
        "home_series_number": safe_get(home, "seriesNumber"),

        "away_team_id": safe_get(away, "team", "id"),
        "away_team_name": safe_get(away, "team", "name"),
        "away_league_id": safe_get(away, "leagueRecord", "league", "id"),
        "away_sport_wins": safe_get(away, "leagueRecord", "wins"),
        "away_sport_losses": safe_get(away, "leagueRecord", "losses"),
        "away_sport_pct": safe_get(away, "leagueRecord", "pct"),
        "away_score": away.get("score"),
        "away_is_winner": away.get("isWinner"),
        "away_split_squad": safe_get(away, "splitSquad"),
        "away_series_number": safe_get(away, "seriesNumber"),

        "venue_id": venue.get("id"),
        "venue_name": venue.get("name"),

        "resume_date": game.get("resumeDate"),
        "resume_game_date": game.get("resumeGameDate"),
        "resume_game_code": game.get("resumeGameCode"),
        "if_necessary": game.get("ifNecessary"),
        "if_necessary_description": game.get("ifNecessaryDescription"),

        "calendar_event_id": content.get("calendarEventID"),

        "source_system": "mlb_stats_api",
        "source_load_datetime": datetime.now().isoformat(timespec="seconds"),
    }


def extract_schedule(start_date: str, end_date: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    params = {
        "sportId": 1,
        "startDate": start_date,
        "endDate": end_date,
        "hydrate": (
            "team,"
            "linescore,"
            "venue,"
            "flags,"
            "seriesStatus,"
            "tickets,"
            "broadcasts(all),"
            "decisions,"
            "person,"
            "probablePitcher,"
            "review"
        ),
    }

    started_at = datetime.now()
    failures = []
    rows = []

    log(f"Starting schedule extraction for {start_date} to {end_date}")

    try:
        data = get_json(BASE_URL, params=params)
    except Exception as exc:
        failures.append({
            "stage": "schedule_request",
            "game_pk": None,
            "error": str(exc)
        })
        df_failures = pd.DataFrame(failures)
        summary = {
            "started_at": started_at.isoformat(timespec="seconds"),
            "ended_at": datetime.now().isoformat(timespec="seconds"),
            "duration_seconds": round((datetime.now() - started_at).total_seconds(), 2),
            "start_date": start_date,
            "end_date": end_date,
            "date_blocks": 0,
            "games_extracted": 0,
            "failure_count": len(df_failures),
            "output_file": str(OUT_FILE),
            "failure_file": str(FAILURES_OUT_FILE)
        }
        return pd.DataFrame(), df_failures, summary

    dates = data.get("dates", [])
    total_date_blocks = len(dates)
    total_games_seen = 0

    log(f"Date blocks returned: {total_date_blocks}")

    for idx, date_block in enumerate(dates, start=1):
        game_date = date_block.get("date")
        games = date_block.get("games", [])

        log(f"Processing date block {idx}/{total_date_blocks} | date={game_date} | games={len(games)}")

        for game in games:
            total_games_seen += 1
            game_pk = game.get("gamePk")

            try:
                rows.append(build_game_row(game, game_date))
            except Exception as exc:
                log(f"Failed to parse game_pk={game_pk}: {exc}")
                failures.append({
                    "stage": "game_parse",
                    "game_pk": game_pk,
                    "error": str(exc)
                })

    ended_at = datetime.now()
    duration_seconds = round((ended_at - started_at).total_seconds(), 2)

    df_games = pd.DataFrame(rows)
    if not df_games.empty:
        df_games = df_games.drop_duplicates(subset=["game_pk"]).sort_values(["game_date", "game_pk"])

    df_failures = pd.DataFrame(failures)

    summary = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "ended_at": ended_at.isoformat(timespec="seconds"),
        "duration_seconds": duration_seconds,
        "start_date": start_date,
        "end_date": end_date,
        "date_blocks": total_date_blocks,
        "games_seen": total_games_seen,
        "games_extracted": int(len(df_games)),
        "failure_count": int(len(df_failures)),
        "output_file": str(OUT_FILE),
        "failure_file": str(FAILURES_OUT_FILE)
    }

    return df_games, df_failures, summary


def print_observability_summary(df_games: pd.DataFrame, df_failures: pd.DataFrame, summary: dict) -> None:
    log("Schedule extraction complete")
    log(json.dumps(summary, indent=2))

    if df_games.empty:
        log("No games extracted")
        return

    log("Column completeness snapshot:")
    key_cols = [
        "game_pk",
        "official_date",
        "season",
        "game_type",
        "detailed_state",
        "game_datetime_utc",
        "home_team_id",
        "home_team_name",
        "away_team_id",
        "away_team_name",
        "home_score",
        "away_score",
        "venue_id",
        "venue_name",
        "double_header",
        "day_night",
        "series_description",
        "scheduled_innings",
        "source_system",
        "source_load_datetime",
    ]

    for col in key_cols:
        if col in df_games.columns:
            non_null = int(df_games[col].notna().sum())
            pct = round((non_null / len(df_games)) * 100, 2)
            log(f"  {col}: non_null={non_null}/{len(df_games)} ({pct}%)")

    dupes = int(df_games.duplicated(subset=["game_pk"]).sum())
    log(f"Duplicate game_pk rows after de-dupe: {dupes}")

    if not df_failures.empty:
        log("Failure breakdown by stage:")
        stage_counts = df_failures["stage"].value_counts().to_dict()
        for stage, count in stage_counts.items():
            log(f"  {stage}: {count}")


def main() -> None:
    df_games, df_failures, summary = extract_schedule(START_DATE, END_DATE)

    df_games.to_csv(OUT_FILE, index=False)

    if not df_failures.empty:
        df_failures.to_csv(FAILURES_OUT_FILE, index=False)
    elif FAILURES_OUT_FILE.exists():
        FAILURES_OUT_FILE.unlink()

    with open(SUMMARY_OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print_observability_summary(df_games, df_failures, summary)

    log(f"Games written to: {OUT_FILE}")
    if not df_failures.empty:
        log(f"Failures written to: {FAILURES_OUT_FILE}")
    log(f"Summary written to: {SUMMARY_OUT_FILE}")


if __name__ == "__main__":
    main()