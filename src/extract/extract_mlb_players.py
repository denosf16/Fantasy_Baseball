from pathlib import Path
import requests
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "raw"


def get_team_ids():
    url = "https://statsapi.mlb.com/api/v1/teams?sportId=1"
    data = requests.get(url).json()
    return [t["id"] for t in data["teams"]]


def extract_players():

    team_ids = get_team_ids()

    rows = []

    for team_id in team_ids:

        url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
        data = requests.get(url).json()

        for player in data["roster"]:

            person = player["person"]

            rows.append({
                "player_id": person["id"],
                "full_name": person["fullName"],
                "primary_position": player["position"]["abbreviation"],
                "current_team_id": team_id,
                "active_flag": True,
                "source_system": "mlb_stats_api"
            })

    df = pd.DataFrame(rows).drop_duplicates(subset=["player_id"])

    return df


def main():

    df = extract_players()

    out_file = OUT_DIR / "mlb_players.csv"
    df.to_csv(out_file, index=False)

    print("Players extracted:", len(df))


if __name__ == "__main__":
    main()