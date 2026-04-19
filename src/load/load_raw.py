"""
load_raw.py — dumb loader from files → DuckDB raw schema.

This replaces the old load_to_warehouse.py. It has no business logic — it just
lands the files that the extract/transform layer produced into `raw.*` tables
in DuckDB. dbt takes over from there.

Why "dumb": business logic (renaming, casting, dim/fact building) lives in dbt
SQL models where it's reviewable, testable, and auto-documented. Python just
handles "file in the right place → table in the warehouse".

Tables produced (all in schema `raw`):
    raw.participants       ← data/stg/stg_participants.csv       (all VARCHAR)
    raw.champions          ← data/raw/champions.json              (auto-typed)
    raw.queues             ← data/raw/queues.json                 (auto-typed)
    raw.champion_mastery   ← data/raw/champion_mastery.json       (auto-typed)

The participants CSV is loaded as all-VARCHAR so that the staging layer does
the casting (single source of truth for types). The JSON files use DuckDB's
type auto-inference — fine because the JSON keys/types come from Riot's API
directly and are already consistent.
"""

from pathlib import Path

import duckdb


# -----------------------------------------------------------------------------
# Paths — everything is relative to the project root so the script works no
# matter where it's invoked from.
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR     = PROJECT_ROOT / "data"
RAW_DIR      = DATA_DIR / "raw"
STG_DIR      = DATA_DIR / "stg"
DUCKDB_PATH  = DATA_DIR / "warehouse" / "lol_dw.duckdb"


def _load_csv_all_varchar(con, table, csv_path):
    """Create raw.<table> from a CSV, keeping every column as VARCHAR."""
    if not csv_path.exists():
        print(f"  [skip] {csv_path.name} not found — {table} not loaded")
        return

    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw.{table} AS
        SELECT *
        FROM read_csv_auto('{csv_path.as_posix()}', header=true, all_varchar=true)
        """
    )
    n = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
    print(f"  [ok]   raw.{table:<20} ← {csv_path.name} ({n} rows)")


def _load_json(con, table, json_path):
    """Create raw.<table> from a JSON file (array of objects). Types auto-inferred."""
    if not json_path.exists():
        print(f"  [skip] {json_path.name} not found — {table} not loaded")
        return

    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw.{table} AS
        SELECT *
        FROM read_json_auto('{json_path.as_posix()}')
        """
    )
    n = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
    print(f"  [ok]   raw.{table:<20} ← {json_path.name} ({n} rows)")


def load_raw():
    """Entry point — load everything."""
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        _load_csv_all_varchar(con, "participants",     STG_DIR / "stg_participants.csv")
        _load_json(           con, "champions",        RAW_DIR / "champions.json")
        _load_json(           con, "queues",           RAW_DIR / "queues.json")
        _load_json(           con, "champion_mastery", RAW_DIR / "champion_mastery.json")

        # Flush WAL into the main file so the next process (dbt) sees a
        # consistent DB. Without this, dbt may need to replay an outstanding
        # WAL on open — which can fail on filesystems that restrict deletion
        # of the .wal file.
        con.execute("CHECKPOINT")
    finally:
        con.close()

    print(f"  Raw load complete. DB: {DUCKDB_PATH}")


if __name__ == "__main__":
    load_raw()
