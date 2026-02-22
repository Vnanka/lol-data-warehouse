import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("RIOT_API_KEY")
if not api_key:
    raise RuntimeError("RIOT_API_KEY not found")

headers = {"X-Riot-Token": api_key}

# Read PUUID from local file (so we don't keep calling account endpoint)
with open("data/raw/puuid.json", "r", encoding="utf-8") as f:
    puuid_data = json.load(f)

puuid = puuid_data["puuid"]

# Match-V5 uses REGIONAL routing too (Europe for EUW/EUNE)
# We'll fetch the latest N games; start small.
count = 20
url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count={count}"

r = requests.get(url, headers=headers, timeout=30)
print("Status code:", r.status_code)

if r.status_code != 200:
    print("Response:", r.text)
    raise SystemExit(1)

match_ids = r.json()
print(f"Fetched {len(match_ids)} match IDs.")
print("First 5:", match_ids[:5])

# Save to raw folder
out_path = "data/raw/match_ids_latest.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(match_ids, f, indent=2)

print("Saved:", out_path)
