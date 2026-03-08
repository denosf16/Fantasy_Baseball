from pathlib import Path
import pandas as pd
from pybaseball import statcast

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "data" / "raw"


def main():

    start_date = "2025-03-27"
    end_date = "2025-03-31"

    df = statcast(start_dt=start_date, end_dt=end_date)

    out_file = OUT_DIR / f"statcast_pitches_{start_date}_{end_date}.csv"

    df.to_csv(out_file, index=False)

    print("Rows extracted:", len(df))
    print("File:", out_file)


if __name__ == "__main__":
    main()