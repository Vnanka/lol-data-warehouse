# src/load/load_to_warehouse.py

import sqlite3
from pathlib import Path

import pandas as pd


# -----------------------------------------
# Paths
# -----------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../lol-data-warehouse
DATA_DIR = PROJECT_ROOT / "data"
STG_DIR = DATA_DIR / "stg"
WAREHOUSE_DB = DATA_DIR / "warehouse" / "lol_dw.sqlite"
DDL_PATH = PROJECT_ROOT / "sql" / "create_warehouse.sql"


# -----------------------------------------
# DB init
# -----------------------------------------

def init_db() -> sqlite3.Connection:
    WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(WAREHOUSE_DB)
    conn.execute("PRAGMA foreign_keys = ON;")

    with open(DDL_PATH, "r", encoding="utf-8") as f:
        ddl = f.read()
    conn.executescript(ddl)
    conn.commit()
    return conn


# -----------------------------------------
# Load staging
# -----------------------------------------

def load_stg_participants() -> pd.DataFrame:
    path = STG_DIR / "stg_participants.csv"
    df = pd.read_csv(path)

    # Ensure the expected columns exist
    expected = {
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
        "lane",
    }
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"stg_participants.csv is missing columns: {missing}")

    # Convert Riot ms timestamp to ISO datetime string
    # game_creation is in ms since epoch
    df["game_creation_iso"] = pd.to_datetime(
        df["game_creation"], unit="ms", utc=True
    ).astype(str)

    # Use game_duration as time_played_sec (good enough for now)
    df["time_played_sec"] = df["game_duration"]

    # CS = total_minions_killed (you can later add jungle minions if you want)
    df["cs"] = df["total_minions_killed"].fillna(0)

    # Normalise win to 0/1
    df["win_int"] = df["win"].astype(str).str.lower().isin(["true", "1", "win"]).astype(int)

    return df


# -----------------------------------------
# Dimension upserts
# -----------------------------------------

def upsert_dim_summoner(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Insert new summoners by puuid; update last_seen_at."""
    cur = conn.cursor()

    # Latest info per puuid
    summ_df = (
        df[["participant_puuid", "summoner_name", "game_creation_iso"]]
        .sort_values("game_creation_iso")
        .drop_duplicates(subset=["participant_puuid"], keep="last")
    )

    for _, row in summ_df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_summoner (puuid, summoner_name, last_seen_at)
            VALUES (?, ?, ?)
            ON CONFLICT(puuid) DO UPDATE SET
                summoner_name = excluded.summoner_name,
                last_seen_at = excluded.last_seen_at
            """,
            (row["participant_puuid"], row["summoner_name"], row["game_creation_iso"]),
        )

    conn.commit()


def upsert_dim_champion(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Insert champions (id + name)."""
    cur = conn.cursor()

    champ_df = (
        df[["champion_id", "champion_name"]]
        .dropna(subset=["champion_id"])
        .drop_duplicates(subset=["champion_id"])
    )

    for _, row in champ_df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_champion (champion_id, champion_name)
            VALUES (?, ?)
            ON CONFLICT(champion_id) DO UPDATE SET
                champion_name = excluded.champion_name
            """,
            (int(row["champion_id"]), row["champion_name"]),
        )

    conn.commit()


def upsert_dim_queue(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Insert queue IDs. Description can be populated later."""
    cur = conn.cursor()

    queue_df = (
        df[["queue_id"]]
        .dropna(subset=["queue_id"])
        .drop_duplicates(subset=["queue_id"])
    )

    for _, row in queue_df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_queue (queue_id)
            VALUES (?)
            ON CONFLICT(queue_id) DO NOTHING
            """,
            (int(row["queue_id"]),),
        )

    conn.commit()


def upsert_dim_match(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Insert matches (one row per match_id)."""
    cur = conn.cursor()

    match_df = (
        df[
            [
                "match_id",
                "game_creation_iso",
                "game_duration",
                "queue_id",
                "game_version",
                "platform_id",
            ]
        ]
        .drop_duplicates(subset=["match_id"])
    )

    for _, row in match_df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_match (
                match_id,
                game_creation,
                game_duration_sec,
                queue_id,
                game_version,
                platform
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id) DO UPDATE SET
                game_creation     = excluded.game_creation,
                game_duration_sec = excluded.game_duration_sec,
                queue_id          = excluded.queue_id,
                game_version      = excluded.game_version,
                platform          = excluded.platform
            """,
            (
                row["match_id"],
                row["game_creation_iso"],
                int(row["game_duration"]) if not pd.isna(row["game_duration"]) else None,
                int(row["queue_id"]) if not pd.isna(row["queue_id"]) else None,
                row["game_version"],
                row["platform_id"],
            ),
        )

    conn.commit()


def get_summoner_key_map(conn: sqlite3.Connection) -> dict[str, int]:
    df = pd.read_sql("SELECT summoner_id, puuid FROM dim_summoner", conn)
    return dict(zip(df["puuid"], df["summoner_id"]))


# -----------------------------------------
# Fact load
# -----------------------------------------

def upsert_fact_participant(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """
    Simple strategy: for all match_ids present in the staging data, delete their
    fact rows then re-insert.
    """
    cur = conn.cursor()

    match_ids = df["match_id"].drop_duplicates().tolist()
    cur.executemany(
        "DELETE FROM fact_participant WHERE match_id = ?",
        [(m,) for m in match_ids],
    )
    conn.commit()

    summoner_key_map = get_summoner_key_map(conn)

    insert_sql = """
        INSERT INTO fact_participant (
            match_id,
            summoner_id,
            champion_id,
            team_id,
            win,
            kills,
            deaths,
            assists,
            gold_earned,
            damage_to_champs,
            cs,
            vision_score,
            role,
            team_position,
            lane,
            time_played_sec
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        puuid = row["participant_puuid"]
        summoner_id = summoner_key_map.get(puuid)
        if summoner_id is None:
            # Shouldn't happen, but skip if it does
            continue

        cur.execute(
            insert_sql,
            (
                row["match_id"],
                int(summoner_id),
                int(row["champion_id"]) if not pd.isna(row["champion_id"]) else None,
                int(row["team_id"]) if not pd.isna(row["team_id"]) else None,
                int(row["win_int"]),
                int(row["kills"]) if not pd.isna(row["kills"]) else None,
                int(row["deaths"]) if not pd.isna(row["deaths"]) else None,
                int(row["assists"]) if not pd.isna(row["assists"]) else None,
                int(row["gold_earned"]) if not pd.isna(row["gold_earned"]) else None,
                int(row["total_damage_to_champions"]) if not pd.isna(row["total_damage_to_champions"]) else None,
                int(row["cs"]) if not pd.isna(row["cs"]) else None,
                int(row["vision_score"]) if not pd.isna(row["vision_score"]) else None,
                row["role"],
                row["teamPosition"],
                row["lane"],
                int(row["time_played_sec"]) if not pd.isna(row["time_played_sec"]) else None,
            ),
        )

    conn.commit()


# -----------------------------------------
# Main
# -----------------------------------------

def main():
    conn = init_db()
    df_part = load_stg_participants()

    upsert_dim_summoner(conn, df_part)
    upsert_dim_champion(conn, df_part)
    upsert_dim_queue(conn, df_part)
    upsert_dim_match(conn, df_part)
    upsert_fact_participant(conn, df_part)

    conn.close()
    print(f"Warehouse load complete. DB: {WAREHOUSE_DB}")


if __name__ == "__main__":
    main()