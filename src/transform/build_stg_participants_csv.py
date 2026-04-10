import json
import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MATCH_DIR = PROJECT_ROOT / "data" / "raw" / "matches"
DEFAULT_OUT_FILE = PROJECT_ROOT / "data" / "stg" / "stg_participants.csv"

# Columns to extract into the staging table.
# This list also controls the column order in the CSV.
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
    "team_position",
    "individual_position",
    "lane",
]


def build_stg_participants(
    match_dir: Path = DEFAULT_MATCH_DIR,
    out_file: Path = DEFAULT_OUT_FILE,
) -> None:
    """
    Reads all raw match JSON files and extracts one row per participant.
    Writes the result to a staging CSV file.

    Parameters:
        match_dir - directory containing raw match JSON files
        out_file  - path to write the output CSV
    """
    out_file.parent.mkdir(parents=True, exist_ok=True)

    rows = []

    for path in match_dir.glob("*.json"):
        match_id = path.stem  # filename without the .json extension

        with open(path, "r", encoding="utf-8") as f:
            match = json.load(f)

        info = match.get("info", {})

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
                "team_position": p.get("teamPosition"),
                "individual_position": p.get("individualPosition"),
                "lane": p.get("lane"),
            }
            rows.append(row)

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_file}")


if __name__ == "__main__":
    build_stg_participants()
