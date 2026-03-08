import os
import urllib.parse
from pathlib import Path

import pyodbc
from sqlalchemy import create_engine
from dotenv import load_dotenv


# ------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / "config" / "connections.env"

load_dotenv(ENV_PATH)


# ------------------------------------------------------------
# Read configuration
# ------------------------------------------------------------
SQL_SERVER = os.getenv("SQL_SERVER", r"RAMSEY_BOLTON\SQLEXPRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE", "fantasy_baseball")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
SQL_TRUSTED_CONNECTION = os.getenv("SQL_TRUSTED_CONNECTION", "yes")


# ------------------------------------------------------------
# Build connection string
# ------------------------------------------------------------
def _build_conn_str() -> str:
    return (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"Trusted_Connection={SQL_TRUSTED_CONNECTION};"
        "TrustServerCertificate=yes;"
    )


# ------------------------------------------------------------
# SQLAlchemy Engine (used by pandas / queries)
# ------------------------------------------------------------
params = urllib.parse.quote_plus(_build_conn_str())

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


# ------------------------------------------------------------
# Optional direct pyodbc connection (rarely needed)
# ------------------------------------------------------------
def get_connection() -> pyodbc.Connection:
    return pyodbc.connect(_build_conn_str())


# ------------------------------------------------------------
# Connection test
# ------------------------------------------------------------
if __name__ == "__main__":
    print("Testing database connection...")

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DB_NAME()")

    db_name = cursor.fetchone()[0]
    print(f"Connected successfully to database: {db_name}")

    conn.close()