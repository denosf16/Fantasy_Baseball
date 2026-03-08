from pathlib import Path
from datetime import datetime
import json

import requests
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "raw"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def extract_schedule(start_date: str, end_date: str) -> pd.DataFrame:
    url = "https://statsapi.mlb.com/api/v1/schedule"
    params = {
        "sportId": 1,
        "startDate": start_date,
        "endDate": end_date,
        "hydrate": "team,linescore,venue",
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    rows = []

    for date_block in data.get("dates", []):
        game_date = date_block.get("date")

        for game in date_block.get("games", []):
            teams = game.get("teams", {})
            home = teams.get("home", {})
            away = teams.get("away", {})
            venue = game.get("venue", {})

            rows.append(
                {
                    "game_pk": game.get("gamePk"),
                    "game_date": game_date,
                    "season": game.get("season"),
                    "game_type": game.get("gameType"),
                    "status_detailed": game.get("status", {}).get("detailedState"),
                    "home_team_id": home.get("team", {}).get("id"),
                    "away_team_id": away.get("team", {}).get("id"),
                    "home_score": home.get("score"),
                    "away_score": away.get("score"),
                    "venue_name": venue.get("name"),
                    "source_system": "mlb_stats_api",
                    "source_load_datetime": datetime.utcnow().isoformat(),
                }
            )

    return pd.DataFrame(rows)


def main() -> None:
    start_date = "2025-03-27"
    end_date = "2025-03-31"

    df = extract_schedule(start_date, end_date)

    out_file = OUT_DIR / f"mlb_schedule_{start_date}_to_{end_date}.csv"
    df.to_csv(out_file, index=False)

    print(f"Rows extracted: {len(df)}")
    print(f"Output file: {out_file}")


if __name__ == "__main__":
    main()