"""
run_pipeline.py — single entry point for the LoL data warehouse pipeline.

Architecture (post-dbt refactor):

    ┌──────────┐    ┌───────────┐    ┌──────────┐    ┌──────────┐
    │ EXTRACT  │ →  │ TRANSFORM │ →  │ LOAD RAW │ →  │   DBT    │
    │ (Python) │    │ (Python)  │    │ (Python) │    │  build   │
    └──────────┘    └───────────┘    └──────────┘    └──────────┘
      Riot API         JSON → CSV       files →          raw →
      → files          flatten          DuckDB raw      staging → marts

Usage:
    # Full pipeline (Riot API → warehouse):
    python src/pipeline/run_pipeline.py

    # Re-run without re-hitting the API (uses data already on disk):
    python src/pipeline/run_pipeline.py --skip-extract

    # Skip dbt (Python load only) — useful when iterating on extract/transform:
    python src/pipeline/run_pipeline.py --skip-dbt

    # Only run dbt (skip everything Python):
    python src/pipeline/run_pipeline.py --skip-extract --skip-transform --skip-load
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Make Python aware of src/ so our imports resolve.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from extract.fetch_champion_data import fetch_champion_data
from extract.fetch_queue_data import fetch_queue_data
from extract.fetch_champion_mastery import fetch_champion_mastery
from extract.fetch_all_match_ids import fetch_match_ids
from extract.fetch_match_details import fetch_match_details
from transform.build_stg_participants_csv import build_stg_participants
from load.load_raw import load_raw


DBT_DIR = PROJECT_ROOT / "dbt"


def _find_dbt_executable() -> str:
    """
    Locate the dbt executable that belongs to the *current* Python interpreter.

    Using a bare "dbt" on PATH is fragile on Windows — the script may be
    launched from a context where the venv isn't activated (scheduled task,
    IDE run button). Resolving dbt via sys.executable's Scripts/ folder
    guarantees we use the dbt installed in the same venv as this script.
    """
    python_dir = Path(sys.executable).parent
    candidate = python_dir / ("dbt.exe" if os.name == "nt" else "dbt")
    if candidate.exists():
        return str(candidate)
    # Fallback: trust PATH (Linux/macOS with a system-wide install).
    return "dbt"


def run_dbt_build() -> None:
    """Invoke `dbt build` — runs all models AND tests in dependency order."""
    dbt_exe = _find_dbt_executable()
    cmd = [
        dbt_exe, "build",
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
    ]
    print(f"  Running: {' '.join(cmd)}")
    try:
        # cwd=DBT_DIR so the relative `path:` in profiles.yml
        # (../data/warehouse/lol_dw.duckdb) resolves correctly regardless
        # of where the pipeline is invoked from.
        subprocess.run(cmd, cwd=str(DBT_DIR), check=True)
    except FileNotFoundError:
        raise RuntimeError(
            "`dbt` not found. Activate the venv and install requirements:\n"
            "    pip install -r requirements.txt\n"
            "Then verify with: dbt --version"
        )


def run_pipeline(
    skip_extract: bool = False,
    skip_transform: bool = False,
    skip_load: bool = False,
    skip_dbt: bool = False,
) -> None:
    load_dotenv()

    puuid = os.getenv("PUUID")
    if not puuid:
        raise RuntimeError("PUUID not set in .env — run fetch_puuid.py first to find it")

    # -----------------------------------------------------------------------
    # 1. EXTRACT — pull data from Riot API onto disk
    # -----------------------------------------------------------------------
    if not skip_extract:
        print("\n=== EXTRACT: Champion data (Data Dragon) ===")
        fetch_champion_data()

        print("\n=== EXTRACT: Queue data (Riot static) ===")
        fetch_queue_data()

        print("\n=== EXTRACT: Champion mastery ===")
        fetch_champion_mastery(puuid)

        print("\n=== EXTRACT: Match IDs ===")
        fetch_match_ids(puuid)

        print("\n=== EXTRACT: Match details ===")
        fetch_match_details()
    else:
        print("\n=== EXTRACT: skipped ===")

    # -----------------------------------------------------------------------
    # 2. TRANSFORM — flatten raw match JSONs into a wide CSV
    # -----------------------------------------------------------------------
    if not skip_transform:
        print("\n=== TRANSFORM: stg_participants.csv ===")
        build_stg_participants()
    else:
        print("\n=== TRANSFORM: skipped ===")

    # -----------------------------------------------------------------------
    # 3. LOAD RAW — land files into DuckDB raw.* tables (no business logic)
    # -----------------------------------------------------------------------
    if not skip_load:
        print("\n=== LOAD RAW: files → DuckDB raw.* ===")
        load_raw()
    else:
        print("\n=== LOAD RAW: skipped ===")

    # -----------------------------------------------------------------------
    # 4. DBT — build staging + marts and run tests in one command
    # -----------------------------------------------------------------------
    if not skip_dbt:
        print("\n=== DBT: build + test ===")
        run_dbt_build()
    else:
        print("\n=== DBT: skipped ===")

    print("\n=== Pipeline complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LoL Data Warehouse pipeline")
    parser.add_argument("--skip-extract",   action="store_true", help="Skip Riot API calls")
    parser.add_argument("--skip-transform", action="store_true", help="Skip JSON→CSV flatten")
    parser.add_argument("--skip-load",      action="store_true", help="Skip DuckDB raw load")
    parser.add_argument("--skip-dbt",       action="store_true", help="Skip `dbt build`")
    args = parser.parse_args()

    run_pipeline(
        skip_extract=args.skip_extract,
        skip_transform=args.skip_transform,
        skip_load=args.skip_load,
        skip_dbt=args.skip_dbt,
    )
