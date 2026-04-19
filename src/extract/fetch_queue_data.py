"""
fetch_queue_data.py — pull queue (game mode) master data from Riot's static
queues endpoint.

Writes the raw API response as-is to data/raw/queues.json. Columns used by the
dbt staging layer: queueId, description, map, notes.
"""

import json
from pathlib import Path
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUT_FILE     = PROJECT_ROOT / "data" / "raw" / "queues.json"
QUEUES_URL   = "https://static.developer.riotgames.com/docs/lol/queues.json"


def fetch_queue_data(out_file: Path = OUT_FILE) -> None:
    queues = requests.get(QUEUES_URL, timeout=10).json()
    print(f"  Queues fetched: {len(queues)}")

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(queues, f, ensure_ascii=False, indent=2)

    print(f"  Wrote {len(queues)} rows → {out_file}")


if __name__ == "__main__":
    fetch_queue_data()
