from pathlib import Path
from datetime import datetime
import json
import requests
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"

OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://statsapi.mlb.com/api/v1/teams"
REQUEST_TIMEOUT = 30

OUT_FILE = OUT_DIR / "mlb_teams.csv"
FAIL_FILE = OUT_DIR / "mlb_teams_failures.csv"
SUMMARY_FILE = LOG_DIR / "extract_mlb_teams_summary.json"


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{now_str()}] {msg}")


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default


def extract_teams():

    params = {
        "sportId": 1,
        "hydrate": "league,division,venue"
    }

    started_at = datetime.now()

    log("Starting MLB teams extraction")

    response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json()

    teams = data.get("teams", [])

    rows = []
    failures = []

    for team in teams:

        try:

            rows.append({

                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "team_code": team.get("teamCode"),
                "abbreviation": team.get("abbreviation"),
                "team_name_short": team.get("teamName"),
                "location_name": team.get("locationName"),

                "franchise_name": team.get("franchiseName"),
                "club_name": team.get("clubName"),

                "league_id": safe_get(team, "league", "id"),
                "league_name": safe_get(team, "league", "name"),

                "division_id": safe_get(team, "division", "id"),
                "division_name": safe_get(team, "division", "name"),

                "venue_id": safe_get(team, "venue", "id"),
                "venue_name": safe_get(team, "venue", "name"),

                "first_year_of_play": team.get("firstYearOfPlay"),

                "active_flag": team.get("active"),

                "source_system": "mlb_stats_api",
                "source_load_datetime": datetime.now().isoformat(timespec="seconds")

            })

        except Exception as exc:

            failures.append({
                "team_id": team.get("id"),
                "error": str(exc)
            })

    df = pd.DataFrame(rows)

    ended_at = datetime.now()

    summary = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "ended_at": ended_at.isoformat(timespec="seconds"),
        "duration_seconds": round((ended_at - started_at).total_seconds(), 2),
        "teams_seen": len(teams),
        "teams_extracted": len(df),
        "failure_count": len(failures),
        "output_file": str(OUT_FILE),
        "failure_file": str(FAIL_FILE)
    }

    return df, pd.DataFrame(failures), summary


def print_observability(df, failures, summary):

    log("Extraction complete")
    log(json.dumps(summary, indent=2))

    if df.empty:
        log("No teams extracted")
        return

    log("Column completeness snapshot:")

    key_cols = [
        "team_id",
        "team_name",
        "team_code",
        "abbreviation",
        "league_name",
        "division_name",
        "venue_id",
        "venue_name",
        "first_year_of_play",
        "active_flag"
    ]

    for col in key_cols:

        if col in df.columns:
            non_null = df[col].notna().sum()
            pct = round((non_null / len(df)) * 100, 2)

            log(f"  {col}: {non_null}/{len(df)} ({pct}%)")

    dupes = df.duplicated(subset=["team_id"]).sum()
    log(f"Duplicate team_id rows: {dupes}")

    if not failures.empty:

        log("Failures detected:")
        print(failures.head())


def main():

    df, failures, summary = extract_teams()

    df.to_csv(OUT_FILE, index=False)

    if not failures.empty:
        failures.to_csv(FAIL_FILE, index=False)

    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)

    print_observability(df, failures, summary)

    log(f"Teams written to: {OUT_FILE}")
    log(f"Summary written to: {SUMMARY_FILE}")


if __name__ == "__main__":
    main()