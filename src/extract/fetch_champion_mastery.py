import sqlite3
import os
import requests
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "lol_dw.sqlite"
PLATFORM_HOST = "https://euw1.api.riotgames.com"
MASTERY_URL = PLATFORM_HOST + "/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}"


def fetch_champion_mastery(puuid: str, db_path: Path = DB_PATH) -> None:
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

    fetched_at = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Ensure the summoner exists in dim_summoner so the FK is satisfied
    cur.execute(
        "INSERT INTO dim_summoner (puuid) VALUES (?) ON CONFLICT(puuid) DO NOTHING",
        (puuid,),
    )

    cur.executemany(
        """
        INSERT INTO fact_champion_mastery (
            puuid, champion_id, champion_level, champion_points,
            last_play_time, points_since_last_level, points_until_next_level,
            chest_granted, tokens_earned, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(puuid, champion_id) DO UPDATE SET
            champion_level              = excluded.champion_level,
            champion_points             = excluded.champion_points,
            last_play_time              = excluded.last_play_time,
            points_since_last_level     = excluded.points_since_last_level,
            points_until_next_level     = excluded.points_until_next_level,
            chest_granted               = excluded.chest_granted,
            tokens_earned               = excluded.tokens_earned,
            fetched_at                  = excluded.fetched_at
        """,
        [
            (
                e["puuid"],
                e["championId"],
                e["championLevel"],
                e["championPoints"],
                datetime.fromtimestamp(e["lastPlayTime"] / 1000, tz=timezone.utc).isoformat(),
                e.get("championPointsSinceLastLevel"),
                e.get("championPointsUntilNextLevel"),
                int(e.get("chestGranted", False)),
                e.get("tokensEarned"),
                fetched_at,
            )
            for e in entries
        ],
    )

    conn.commit()
    conn.close()
    print(f"  fact_champion_mastery upserted: {len(entries)} rows")


if __name__ == "__main__":
    load_dotenv()
    puuid = os.getenv("PUUID")
    if not puuid:
        raise RuntimeError("PUUID not set in .env")
    fetch_champion_mastery(puuid)
