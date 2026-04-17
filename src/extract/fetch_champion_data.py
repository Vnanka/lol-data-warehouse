import sqlite3
import requests
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "lol_dw.sqlite"
VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json"
CHAMPION_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"


def fetch_champion_data(db_path: Path = DB_PATH) -> None:
    version = requests.get(VERSIONS_URL, timeout=10).json()[0]
    print(f"  Data Dragon version: {version}")

    champions = requests.get(CHAMPION_URL.format(version=version), timeout=10).json()["data"]
    print(f"  Champions fetched: {len(champions)}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executemany(
        """
        INSERT INTO dim_champion (champion_id, champion_name, champion_key, title, tags)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(champion_id) DO UPDATE SET
            champion_name = excluded.champion_name,
            champion_key  = excluded.champion_key,
            title         = excluded.title,
            tags          = excluded.tags
        """,
        [
            (
                int(c["key"]),
                c["name"],
                c["id"],
                c["title"],
                ", ".join(c.get("tags", [])),
            )
            for c in champions.values()
        ],
    )

    conn.commit()
    conn.close()
    print(f"  dim_champion upserted: {len(champions)} rows")


if __name__ == "__main__":
    fetch_champion_data()
