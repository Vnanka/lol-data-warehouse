import json
import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("RIOT_API_KEY")
if not api_key:
    raise RuntimeError("RIOT_API_KEY not found")

headers = {"X-Riot-Token": api_key}

# Read match IDs from file
with open("data/raw/match_ids_latest.json", "r", encoding="utf-8") as f:
    match_ids = json.load(f)

os.makedirs("data/raw/matches", exist_ok=True)

def fetch_with_backoff(url: str, headers: dict, timeout: int = 30, max_retries: int = 5):
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, timeout=timeout)

        # Rate limit handling
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "2"))
            print(f"429 rate limited. Sleeping {retry_after}s...")
            time.sleep(retry_after)
            continue

        return r

    return r  # last response

saved = 0
skipped = 0

for i, match_id in enumerate(match_ids, start=1):
    out_path = f"data/raw/matches/{match_id}.json"

    # Skip if already downloaded
    if os.path.exists(out_path):
        skipped += 1
        continue

    url = f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}"
    r = fetch_with_backoff(url, headers)

    if r.status_code != 200:
        print(f"[{i}/{len(match_ids)}] Failed {match_id}: {r.status_code} {r.text[:200]}")
        continue

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(r.text)

    saved += 1
    print(f"[{i}/{len(match_ids)}] Saved {match_id}")

    # Small polite delay to reduce chance of 429
    time.sleep(0.25)

print(f"Done. Saved={saved}, Skipped={skipped}")
