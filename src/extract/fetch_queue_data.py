import sqlite3
import requests
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "lol_dw.sqlite"
QUEUES_URL = "https://static.developer.riotgames.com/docs/lol/queues.json"


def fetch_queue_data(db_path: Path = DB_PATH) -> None:
    queues = requests.get(QUEUES_URL, timeout=10).json()
    print(f"  Queues fetched: {len(queues)}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executemany(
        """
        INSERT INTO dim_queue (queue_id, queue_description)
        VALUES (?, ?)
        ON CONFLICT(queue_id) DO UPDATE SET
            queue_description = excluded.queue_description
        """,
        [
            (q["queueId"], q.get("description"))
            for q in queues
        ],
    )

    conn.commit()
    conn.close()
    print(f"  dim_queue upserted: {len(queues)} rows")


if __name__ == "__main__":
    fetch_queue_data()
