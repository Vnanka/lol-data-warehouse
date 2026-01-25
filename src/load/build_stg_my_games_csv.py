import json
import csv

PUUID_FILE = "data/raw/puuid.json"
PARTICIPANTS_FILE = "data/stg/stg_participants.csv"
OUT_FILE = "data/stg/stg_my_games.csv"

# Load your puuid
with open(PUUID_FILE, "r", encoding="utf-8") as f:
    puuid_data = json.load(f)
my_puuid = puuid_data["puuid"]

rows = []
fieldnames = None

with open(PARTICIPANTS_FILE, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames  # 👈 capture once, safely

    for row in reader:
        if row["participant_puuid"] == my_puuid:
            rows.append(row)

# Write filtered CSV
with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames) # type: ignore
    writer.writeheader()
    writer.writerows(rows)

print(f"Filtered {len(rows)} rows into {OUT_FILE}")
