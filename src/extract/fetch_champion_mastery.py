"""
fetch_champion_mastery.py — pull per-(puuid, championId) mastery from Riot.

Writes data/raw/champion_mastery.json — list of mastery entries, each with a
`fetched_at` ISO timestamp so we know when the snapshot was taken. The API
shape (camelCase keys, lastPlayTime as epoch-ms) is preserved; conversion to
snake_case and TIMESTAMP happens in the dbt staging layer.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT   = Path(__file__).resolve().parents[2]
OUT_FILE       = PROJECT_ROOT / "data" / "raw" / "champion_mastery.json"

PLATFORM_HOST  = "https://euw1.api.riotgames.com"
MASTERY_URL    = PLATFORM_HOST + "/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}"


def fetch_champion_mastery(puuid: str, out_file: Path = OUT_FILE) -> None:
    api_key = os.getenv("RIOT_API_KEY")
    if not api_key:
        raise RuntimeError("RIOT_API_KEY not set in .env")

    resp = requests.get(
        MASTERY_URL.format(puuid=puuid),
        headers={"X-Riot-Token": api_key},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Mastery API error {resp.status_code}: {resp.text}")

    entries = resp.json()
    print(f"  Champion mastery entries fetched: {len(entries)}")

    # Stamp every row with when we pulled it — lets dbt reason about snapshot freshness.
    fetched_at = datetime.now(timezone.utc).isoformat()
    for e in entries:
        e["fetched_at"] = fetched_at

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)

    print(f"  Wrote {len(entries)} rows → {out_file}")


if __name__ == "__main__":
    load_dotenv()
    puuid = os.getenv("PUUID")
    if not puuid:
        raise RuntimeError("PUUID not set in .env")
    fetch_champion_mastery(puuid)
