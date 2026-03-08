from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import urllib

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "outputs" / "png"

SERVER = r"RAMSEY_BOLTON\SQLEXPRESS"
DATABASE = "fantasy_baseball"
DRIVER = "ODBC Driver 17 for SQL Server"

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def main():

    pitcher_id = 543135
    game_pk = 778553

    query = f"""
        SELECT *
        FROM mart.pitcher_game_chart_input
        WHERE pitcher_id = {pitcher_id}
        AND game_pk = {game_pk}
    """

    df = pd.read_sql(query, engine)

    fig, ax = plt.subplots(figsize=(6, 6))

    scatter = ax.scatter(
        df["plate_x"],
        df["plate_z"],
        c=df["release_speed"],
        cmap="coolwarm",
        alpha=0.7
    )

    ax.set_title(f"Pitch Location - Pitcher {pitcher_id}")
    ax.set_xlabel("Plate X")
    ax.set_ylabel("Plate Z")

    plt.colorbar(scatter, label="Velocity")

    output_file = OUT_DIR / f"pitch_chart_{pitcher_id}_{game_pk}.png"

    plt.savefig(output_file, dpi=300)
    plt.close()

    print("Chart saved:", output_file)


if __name__ == "__main__":
    main()