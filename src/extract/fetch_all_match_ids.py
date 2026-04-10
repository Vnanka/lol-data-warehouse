import json
import os
import time
import requests
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT_FILE = PROJECT_ROOT / "data" / "raw" / "match_ids.json"


def fetch_match_ids(puuid: str, out_file: Path = DEFAULT_OUT_FILE) -> list[str]:
    """
    Fetches all match IDs for a given PUUID from the Riot API.
    Paginates until no more results. Saves to out_file. Returns list of IDs.
    """
    api_key = os.getenv("RIOT_API_KEY")
    if not api_key:
        raise RuntimeError("RIOT_API_KEY not set in .env")

    headers = {"X-Riot-Token": api_key}
    base_url = "https://europe.api.riotgames.com"
    all_ids: list[str] = []
    start = 0
    count = 100

    while True:
        url = (
            base_url
            + "/lol/match/v5/matches/by-puuid/"
            + puuid
            + "/ids?start=" + str(start)
            + "&count=" + str(count)
        )
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            print(f"Error {resp.status_code}: {resp.text}")
            break
        batch = resp.json()
        print(f"Fetched {len(batch)} IDs at start={start}")
        if not batch:
            break
        all_ids.extend(batch)
        start += count
        time.sleep(1)

    print(f"Total match IDs collected: {len(all_ids)}")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_ids, f, indent=2)
    print(f"Saved to {out_file}")
    return all_ids


if __name__ == "__main__":
    load_dotenv()
    puuid = os.getenv("PUUID")
    if not puuid:
        raise RuntimeError("PUUID not set in .env")
    fetch_match_ids(puuid)
