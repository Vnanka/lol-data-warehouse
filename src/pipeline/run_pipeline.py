"""
run_pipeline.py — the single entry point for the LoL data warehouse pipeline.

Usage:
    # Run the full pipeline (extract + transform + load):
    python src/pipeline/run_pipeline.py

    # Skip the API calls (use existing raw data — faster for development):
    python src/pipeline/run_pipeline.py --skip-extract

How it works:
    1. Loads .env so all scripts can read API keys / PUUID
    2. Imports functions from each stage
    3. Calls them in order: extract → transform → load
"""

import sys
import os
import argparse
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Make Python aware of the src/ folder so our imports work.
#
# Without this, Python wouldn't know where to find "extract", "transform", etc.
# sys.path is the list of directories Python searches when you do `import`.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Now we can import our functions using their module paths
from extract.fetch_all_match_ids import fetch_match_ids
from extract.fetch_match_details import fetch_match_details
from transform.build_stg_participants_csv import build_stg_participants
from transform.build_stg_my_games_csv import build_stg_my_games
from load.load_to_warehouse import main as load_warehouse


def run_pipeline(skip_extract: bool = False) -> None:
    """
    Runs the full ETL pipeline in order.

    Parameters:
        skip_extract - if True, skips the Riot API calls and uses
                       whatever raw data is already on disk.
                       Useful during development so you don't burn API rate limits.
    """
    # Load .env once here — all functions will be able to read env vars after this
    load_dotenv()

    puuid = os.getenv("PUUID")
    if not puuid:
        raise RuntimeError("PUUID not set in .env — run fetch_puuid.py first to find it")

    # -----------------------------------------------------------------------
    # EXTRACT — pull data from Riot API
    # -----------------------------------------------------------------------
    if not skip_extract:
        print("\n=== EXTRACT: Fetching match IDs ===")
        fetch_match_ids(puuid)

        print("\n=== EXTRACT: Fetching match details ===")
        fetch_match_details()
    else:
        print("\n=== EXTRACT: Skipped (--skip-extract flag set) ===")

    # -----------------------------------------------------------------------
    # TRANSFORM — parse raw JSON into clean staging CSVs
    # -----------------------------------------------------------------------
    print("\n=== TRANSFORM: Building stg_participants ===")
    build_stg_participants()

    print("\n=== TRANSFORM: Building stg_my_games ===")
    build_stg_my_games(puuid)

    # -----------------------------------------------------------------------
    # LOAD — push staging data into the SQLite warehouse
    # -----------------------------------------------------------------------
    print("\n=== LOAD: Loading into warehouse ===")
    load_warehouse()

    print("\n=== Pipeline complete! ===")


# ---------------------------------------------------------------------------
# Argument parsing — this lets you pass flags from the command line.
# `argparse` is Python's built-in library for this.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LoL Data Warehouse Pipeline")
    parser.add_argument(
        "--skip-extract",
        action="store_true",  # means: if the flag is present, set to True
        help="Skip Riot API calls and use existing raw data",
    )
    args = parser.parse_args()

    run_pipeline(skip_extract=args.skip_extract)
