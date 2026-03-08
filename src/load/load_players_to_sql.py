from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import urllib

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "raw"

SERVER = r"RAMSEY_BOLTON\SQLEXPRESS"
DATABASE = "fantasy_baseball"
DRIVER = "ODBC Driver 17 for SQL Server"

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def main():
    file = DATA_DIR / "mlb_players.csv"
    df = pd.read_csv(file)

    df.to_sql(
        "players",
        engine,
        schema="clean",
        if_exists="append",
        index=False,
        chunksize=200
    )

    print("Rows loaded:", len(df))


if __name__ == "__main__":
    main()