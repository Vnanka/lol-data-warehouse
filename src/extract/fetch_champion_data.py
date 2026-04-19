"""
fetch_champion_data.py — pull champion master data from Riot Data Dragon.

Writes a JSON array to data/raw/champions.json. Each element preserves Data
Dragon's field naming (`key`, `id`, `name`, `title`, `tags`) so that raw data
stays close to its source. The dbt staging model (stg_champions.sql) is where
we rename to our warehouse conventions.
"""

import json
from pathlib import Path

import requests

PROJECT_ROOT   = Path(__file__).resolve().parents[2]
OUT_FILE       = PROJECT_ROOT / "data" / "raw" / "champions.json"

VERSIONS_URL   = "https://ddragon.leagueoflegends.com/api/versions.json"
CHAMPION_URL   = "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"


def fetch_champion_data(out_file: Path = OUT_FILE) -> None:
    version = requests.get(VERSIONS_URL, timeout=10).json()[0]
    print(f"  Data Dragon version: {version}")

    champions = requests.get(CHAMPION_URL.format(version=version), timeout=10).json()["data"]
    print(f"  Champions fetched: {len(champions)}")

    # Keep only the fields we use — avoids dumping kilobytes of stats/art/etc per champion.
    rows = [
        {
            "key":   c["key"],
            "id":    c["id"],
            "name":  c["name"],
            "title": c["title"],
            "tags":  c.get("tags", []),
        }
        for c in champions.values()
    ]

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"  Wrote {len(rows)} rows → {out_file}")


if __name__ == "__main__":
    fetch_champion_data()
