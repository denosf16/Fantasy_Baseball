import os
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / "config" / "connections.env"

load_dotenv(ENV_PATH)


def get_connection() -> pyodbc.Connection:
    server = os.getenv("SQL_SERVER", r"RAMSEY_BOLTON\SQLEXPRESS")
    database = os.getenv("SQL_DATABASE", "fantasy_baseball")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
    trusted_connection = os.getenv("SQL_TRUSTED_CONNECTION", "yes")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection={trusted_connection};"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str)


if __name__ == "__main__":
    conn = get_connection()
    print("Connected successfully.")
    conn.close()