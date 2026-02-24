import json
import os
import csv

MATCH_DIR = "data/raw/matches"
OUT_FILE = "data/stg/stg_participants.csv"

os.makedirs("data/stg", exist_ok=True)

# Columns we want in the staging table
FIELDS = [
    "match_id",
    "game_creation",
    "game_duration",
    "queue_id",
    "game_version",
    "platform_id",
    "participant_puuid",
    "summoner_name",
    "champion_id",
    "champion_name",
    "team_id",
    "win",
    "kills",
    "deaths",
    "assists",
    "total_damage_to_champions",
    "gold_earned",
    "total_minions_killed",
    "vision_score",
    "role",
    "teamPosition",
    "individualPosition",
    "lane"
]

rows = []

for filename in os.listdir(MATCH_DIR):
    if not filename.endswith(".json"):
        continue

    match_id = filename.replace(".json", "")
    path = os.path.join(MATCH_DIR, filename)

    with open(path, "r", encoding="utf-8") as f:
        match = json.load(f)

    info = match.get("info", {})
    # metadata = match.get("metadata", {})  # for later if we need

    for p in info.get("participants", []):
        row = {
            "match_id": match_id,
            "game_creation": info.get("gameCreation"),
            "game_duration": info.get("gameDuration"),
            "queue_id": info.get("queueId"),
            "game_version": info.get("gameVersion"),
            "platform_id": info.get("platformId"),
            "participant_puuid": p.get("puuid"),
            "summoner_name": p.get("summonerName"),
            "champion_id": p.get("championId"),
            "champion_name": p.get("championName"),
            "team_id": p.get("teamId"),
            "win": p.get("win"),
            "kills": p.get("kills"),
            "deaths": p.get("deaths"),
            "assists": p.get("assists"),
            "total_damage_to_champions": p.get("totalDamageDealtToChampions"),
            "gold_earned": p.get("goldEarned"),
            "total_minions_killed": p.get("totalMinionsKilled"),
            "vision_score": p.get("visionScore"),
            "role": p.get("role"),
            "teamPosition": p.get("teamPosition"),
            "individualPosition": p.get("teamPosition"),
            "lane": p.get("lane")
        }
        rows.append(row)

# Write CSV
with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to {OUT_FILE}")
