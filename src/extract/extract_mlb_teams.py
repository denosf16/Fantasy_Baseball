from pathlib import Path
import requests
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "raw"


def main():

    url = "https://statsapi.mlb.com/api/v1/teams?sportId=1"

    data = requests.get(url).json()

    rows = []

    for team in data["teams"]:
        rows.append({
            "team_id": team["id"],
            "team_name": team["name"],
            "team_code": team["teamCode"],
            "abbreviation": team["abbreviation"],
            "league_name": team["league"]["name"],
            "division_name": team["division"]["name"],
            "active_flag": team["active"],
            "source_system": "mlb_stats_api"
        })

    df = pd.DataFrame(rows)

    out_file = OUT_DIR / "mlb_teams.csv"
    df.to_csv(out_file, index=False)

    print("Rows extracted:", len(df))


if __name__ == "__main__":
    main()