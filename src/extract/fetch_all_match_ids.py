import json
import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("RIOT_API_KEY")
if not API_KEY:
    raise RuntimeError("RIOT_API_KEY not set")

PUUID_FILE = "data/raw/puuid.json"
OUT_FILE = "data/raw/match_ids.json"

with open(PUUID_FILE, "r", encoding="utf-8") as f:
    puuid = json.load(f)["puuid"]

REGION_ROUTING = "europe"  # because your account is EUW
BASE_URL = f"https://{REGION_ROUTING}.api.riotgames.com"

headers = {"X-Riot-Token": API_KEY}

all_ids: list[str] = []

start = 0
count = 100  # max per request

while True:
    url = (
        f"{BASE_URL}/lol/match/v5/matches/by-puuid/"
        f"{puuid}/ids?start={start}&count={count}"
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

    # next page
    start += count

    # be kind to rate limits
    time.sleep(1)

print(f"Total IDs collected: {len(all_ids)}")

os.makedirs("data/raw", exist_ok=True)
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(all_ids, f, indent=2)

print(f"Saved to {OUT_FILE}")
