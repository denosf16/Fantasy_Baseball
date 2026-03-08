from pathlib import Path

# ============================================================
# CONFIG
# ============================================================
BASE_DIR = Path(r"C:\Repos\Public_Projects_Clean\Fantasy_Baseball")

# ============================================================
# DIRECTORY STRUCTURE
# ============================================================
DIRS = [
    "config",
    "sql",
    "sql/ddl",
    "sql/checks",
    "src",
    "src/extract",
    "src/transform",
    "src/load",
    "src/checks",
    "src/marts",
    "src/visuals",
    "notebooks",
    "outputs",
    "outputs/pdf",
    "outputs/png",
    "data",
    "data/raw",
    "data/processed",
    "data/cache",
    "logs",
]

# ============================================================
# FILE TEMPLATES
# ============================================================
FILES = {
    ".gitignore": """# Python
__pycache__/
*.py[cod]
*.pyo
*.pyd
*.so
*.egg-info/
.venv/
venv/
env/

# Jupyter
.ipynb_checkpoints/

# Environment / secrets
config/connections.env
.env

# OS / editor
.DS_Store
Thumbs.db
.vscode/
.idea/

# Logs
logs/
*.log

# Data outputs
outputs/pdf/*.pdf
outputs/png/*.png

# Local data
data/raw/*
data/processed/*
data/cache/*

# Keep folder structure
!data/raw/.gitkeep
!data/processed/.gitkeep
!data/cache/.gitkeep
!logs/.gitkeep
!outputs/pdf/.gitkeep
!outputs/png/.gitkeep
""",

    "README.md": """# Fantasy Baseball

A portfolio-first fantasy baseball project focused on:

- MLB data ingestion
- Statcast enrichment
- SQL-based modeling infrastructure
- Pitcher and hitter game visualizations
- Future dynasty rankings, valuations, and projections

## Initial Build Order

1. Repository bootstrap
2. SQL schema creation
3. MLB backbone ingestion
4. Statcast ingestion
5. Cleaning and validation
6. Chart-ready marts
7. PDF/PNG visual prototypes
8. App development

## Working Directory

`C:\\Repos\\Public_Projects_Clean\\Fantasy_Baseball`

## Suggested Commit Flow

- bootstrap repo structure
- add sql ddl
- add mlb extract scripts
- add raw load scripts
- add clean transforms
- add validation checks
- add chart marts
- add first visuals
""",

    "requirements.txt": """pandas
numpy
requests
pyodbc
sqlalchemy
python-dotenv
pyyaml
pybaseball
jupyter
matplotlib
plotly
""",

    "config/settings.yaml": """project:
  name: fantasy_baseball
  environment: dev

paths:
  base_dir: C:/Repos/Public_Projects_Clean/Fantasy_Baseball
  raw_data: C:/Repos/Public_Projects_Clean/Fantasy_Baseball/data/raw
  processed_data: C:/Repos/Public_Projects_Clean/Fantasy_Baseball/data/processed
  cache_data: C:/Repos/Public_Projects_Clean/Fantasy_Baseball/data/cache
  pdf_output: C:/Repos/Public_Projects_Clean/Fantasy_Baseball/outputs/pdf
  png_output: C:/Repos/Public_Projects_Clean/Fantasy_Baseball/outputs/png
  logs: C:/Repos/Public_Projects_Clean/Fantasy_Baseball/logs

sql:
  schemas:
    raw: raw
    clean: clean
    mart: mart

mlb:
  start_season: 2025
  statcast_start_date: 2025-03-01
  statcast_end_date: 2025-03-31
""",

    "config/connections.env.example": """# Copy this file to connections.env and fill in your real values

SQL_SERVER=YOUR_SERVER_NAME
SQL_DATABASE=FantasyBaseball
SQL_DRIVER=ODBC Driver 17 for SQL Server
SQL_TRUSTED_CONNECTION=yes

# If using SQL auth instead
# SQL_UID=your_username
# SQL_PWD=your_password
""",

    "sql/ddl/create_schemas.sql": """IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw')
    EXEC('CREATE SCHEMA raw');

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'clean')
    EXEC('CREATE SCHEMA clean');

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart')
    EXEC('CREATE SCHEMA mart');
""",

    "sql/ddl/raw_tables.sql": """-- Raw ingestion tables
-- Initial placeholders. We will define these in detail next.

-- Example:
-- CREATE TABLE raw.mlb_schedule (...);
-- CREATE TABLE raw.mlb_players (...);
-- CREATE TABLE raw.statcast_pitches (...);
""",

    "sql/ddl/clean_tables.sql": """-- Cleaned analysis tables
-- Initial placeholders. We will define these in detail next.

-- Example:
-- CREATE TABLE clean.games (...);
-- CREATE TABLE clean.players (...);
-- CREATE TABLE clean.pitches (...);
""",

    "sql/ddl/mart_views.sql": """-- Chart-ready marts / views
-- Initial placeholders. We will define these in detail after clean tables are ready.

-- Example:
-- CREATE VIEW mart.pitcher_game_chart_input AS ...
-- CREATE VIEW mart.hitter_game_chart_input AS ...
""",

    "sql/checks/row_count_checks.sql": """-- Row count checks
-- Add source vs target reconciliation queries here.
""",

    "sql/checks/duplicate_checks.sql": """-- Duplicate checks
-- Add unique key validation queries here.
""",

    "sql/checks/null_checks.sql": """-- Null checks
-- Add required field null validation queries here.
""",

    "src/__init__.py": "",
    "src/extract/__init__.py": "",
    "src/transform/__init__.py": "",
    "src/load/__init__.py": "",
    "src/checks/__init__.py": "",
    "src/marts/__init__.py": "",
    "src/visuals/__init__.py": "",

    "src/extract/extract_mlb_schedule.py": '''"""
Extract MLB schedule data.
"""

from pathlib import Path


def main() -> None:
    print("extract_mlb_schedule.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/extract/extract_mlb_players.py": '''"""
Extract MLB player data.
"""

from pathlib import Path


def main() -> None:
    print("extract_mlb_players.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/extract/extract_statcast_pitches.py": '''"""
Extract Statcast pitch-level data.
"""

from pathlib import Path


def main() -> None:
    print("extract_statcast_pitches.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/transform/clean_games.py": '''"""
Transform raw game data into clean.games.
"""


def main() -> None:
    print("clean_games.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/transform/clean_players.py": '''"""
Transform raw player data into clean.players.
"""


def main() -> None:
    print("clean_players.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/transform/clean_pitches.py": '''"""
Transform raw Statcast pitch data into clean.pitches.
"""


def main() -> None:
    print("clean_pitches.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/load/load_raw_tables.py": '''"""
Load raw extracted data into SQL raw schema.
"""


def main() -> None:
    print("load_raw_tables.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/load/load_clean_tables.py": '''"""
Load cleaned data into SQL clean schema.
"""


def main() -> None:
    print("load_clean_tables.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/checks/validate_raw.py": '''"""
Validate raw schema loads.
"""


def main() -> None:
    print("validate_raw.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/checks/validate_clean.py": '''"""
Validate clean schema loads.
"""


def main() -> None:
    print("validate_clean.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/marts/build_pitcher_game_chart_input.py": '''"""
Build pitcher game chart mart/view inputs.
"""


def main() -> None:
    print("build_pitcher_game_chart_input.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/marts/build_hitter_game_chart_input.py": '''"""
Build hitter game chart mart/view inputs.
"""


def main() -> None:
    print("build_hitter_game_chart_input.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/visuals/pitcher_game_report.py": '''"""
Generate pitcher game PDF/PNG report.
"""


def main() -> None:
    print("pitcher_game_report.py placeholder")


if __name__ == "__main__":
    main()
''',

    "src/visuals/hitter_game_report.py": '''"""
Generate hitter game PDF/PNG report.
"""


def main() -> None:
    print("hitter_game_report.py placeholder")


if __name__ == "__main__":
    main()
''',

    "notebooks/.gitkeep": "",
    "outputs/pdf/.gitkeep": "",
    "outputs/png/.gitkeep": "",
    "data/raw/.gitkeep": "",
    "data/processed/.gitkeep": "",
    "data/cache/.gitkeep": "",
    "logs/.gitkeep": "",
}

# ============================================================
# HELPERS
# ============================================================
def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main() -> None:
    print(f"Creating repo structure under: {BASE_DIR}")

    for rel_dir in DIRS:
        dir_path = BASE_DIR / rel_dir
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"[DIR ] {dir_path}")

    for rel_file, content in FILES.items():
        file_path = BASE_DIR / rel_file
        write_file(file_path, content)
        print(f"[FILE] {file_path}")

    print("\\nRepo bootstrap complete.")
    print(f"Base path: {BASE_DIR}")
    print("\\nNext recommended steps:")
    print("1. cd into repo")
    print("2. create venv")
    print("3. git init")
    print("4. first commit")
    print("5. add remote")
    print("6. push to GitHub")


if __name__ == "__main__":
    main()