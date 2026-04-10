import json
import os
import time
import requests
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_IDS_FILE = PROJECT_ROOT / "data" / "raw" / "match_ids.json"
DEFAULT_MATCHES_DIR = PROJECT_ROOT / "data" / "raw" / "matches"


def _fetch_with_backoff(url: str, headers: dict, timeout: int = 30, max_retries: int = 5):
    """
    Helper function (private, hence the _ prefix) that retries on 429 rate limits.
    The _ prefix is a Python convention meaning "this is internal, not for outside use".
    """
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, timeout=timeout)

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "2"))
            print(f"429 rate limited. Sleeping {retry_after}s...")
            time.sleep(retry_after)
            continue

        return r

    return r  # return whatever the last response was after all retries


def fetch_match_details(
    match_ids_file: Path = DEFAULT_IDS_FILE,
    output_dir: Path = DEFAULT_MATCHES_DIR,
) -> None:
    """
    Downloads full match JSON for each match ID in match_ids_file.
    Skips matches already downloaded (incremental).
    Saves each match as data/raw/matches/{match_id}.json.

    Parameters:
        match_ids_file - path to the JSON file containing match ID list
        output_dir     - directory to save individual match JSON files
    """
    api_key = os.getenv("RIOT_API_KEY")
    if not api_key:
        raise RuntimeError("RIOT_API_KEY not found in .env")

    headers = {"X-Riot-Token": api_key}

    with open(match_ids_file, "r", encoding="utf-8") as f:
        match_ids = json.load(f)

    output_dir.mkdir(parents=True, exist_ok=True)

    saved = 0
    skipped = 0

    for i, match_id in enumerate(match_ids, start=1):
        out_path = output_dir / f"{match_id}.json"

        # Skip if already downloaded — this is the incremental behaviour
        if out_path.exists():
            skipped += 1
            continue

        url = f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}"
        r = _fetch_with_backoff(url, headers)

        if r.status_code != 200:
            print(f"[{i}/{len(match_ids)}] Failed {match_id}: {r.status_code} {r.text[:200]}")
            continue

        out_path.write_text(r.text, encoding="utf-8")
        saved += 1
        print(f"[{i}/{len(match_ids)}] Saved {match_id}")

        time.sleep(0.25)  # polite delay

    print(f"Done. Saved={saved}, Skipped={skipped}")


if __name__ == "__main__":
    load_dotenv()
    fetch_match_details()
