# src/load/load_to_warehouse.py

import sqlite3
from pathlib import Path

import pandas as pd

# -----------------------------------------
# Paths
# -----------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR     = PROJECT_ROOT / "data"
STG_DIR      = DATA_DIR / "stg"
WAREHOUSE_DB = DATA_DIR / "warehouse" / "lol_dw.sqlite"
DDL_PATH     = PROJECT_ROOT / "sql" / "create_warehouse.sql"


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
# Helpers
# -----------------------------------------

def _int(val):
    """Safely convert a value to int, returning None if missing."""
    try:
        return None if pd.isna(val) else int(val)
    except (TypeError, ValueError):
        return None

def _float(val):
    """Safely convert a value to float, returning None if missing."""
    try:
        return None if pd.isna(val) else float(val)
    except (TypeError, ValueError):
        return None

def _bool_to_int(val):
    """Convert True/False/'True'/'False' strings to 1/0."""
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    if s in ("true", "1"):
        return 1
    if s in ("false", "0"):
        return 0
    return None


# -----------------------------------------
# Load staging
# -----------------------------------------

def load_stg_participants() -> pd.DataFrame:
    path = STG_DIR / "stg_participants.csv"
    df = pd.read_csv(path)

    # Normalise boolean columns that come through as strings
    for col in ["win", "first_blood_kill", "first_blood_assist", "first_tower_kill",
                "surrendered", "early_surrendered"]:
        if col in df.columns:
            df[col] = df[col].apply(_bool_to_int)

    return df


# -----------------------------------------
# Dimension upserts
# -----------------------------------------

def upsert_dim_summoner(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    summ_df = (
        df[["participant_puuid", "riot_id", "summoner_name", "game_creation"]]
        .sort_values("game_creation")
        .drop_duplicates(subset=["participant_puuid"], keep="last")
    )
    for _, row in summ_df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_summoner (puuid, riot_id, summoner_name, last_seen_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(puuid) DO UPDATE SET
                riot_id       = excluded.riot_id,
                summoner_name = excluded.summoner_name,
                last_seen_at  = excluded.last_seen_at
            """,
            (row["participant_puuid"], row.get("riot_id"), row.get("summoner_name"), str(row["game_creation"])),
        )
    conn.commit()


def upsert_dim_champion(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
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
            ON CONFLICT(champion_id) DO UPDATE SET champion_name = excluded.champion_name
            """,
            (_int(row["champion_id"]), row["champion_name"]),
        )
    conn.commit()


def upsert_dim_queue(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    queue_df = (
        df[["queue_id"]]
        .dropna(subset=["queue_id"])
        .drop_duplicates(subset=["queue_id"])
    )
    for _, row in queue_df.iterrows():
        cur.execute(
            "INSERT INTO dim_queue (queue_id) VALUES (?) ON CONFLICT(queue_id) DO NOTHING",
            (_int(row["queue_id"]),),
        )
    conn.commit()


def upsert_dim_match(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    cur = conn.cursor()
    match_df = (
        df[["match_id", "game_creation", "game_duration", "game_mode", "queue_id", "game_version", "platform_id"]]
        .drop_duplicates(subset=["match_id"])
    )
    for _, row in match_df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_match (match_id, game_creation, game_duration_sec, game_mode, queue_id, game_version, platform)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id) DO UPDATE SET
                game_creation     = excluded.game_creation,
                game_duration_sec = excluded.game_duration_sec,
                game_mode         = excluded.game_mode,
                queue_id          = excluded.queue_id,
                game_version      = excluded.game_version,
                platform          = excluded.platform
            """,
            (
                row["match_id"],
                str(row["game_creation"]),
                _int(row["game_duration"]),
                row.get("game_mode"),
                _int(row["queue_id"]),
                row.get("game_version"),
                row.get("platform_id"),
            ),
        )
    conn.commit()


def get_summoner_key_map(conn: sqlite3.Connection) -> dict:
    df = pd.read_sql("SELECT summoner_id, puuid FROM dim_summoner", conn)
    return dict(zip(df["puuid"], df["summoner_id"]))


# -----------------------------------------
# Fact load
# -----------------------------------------

def upsert_fact_participant(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    cur = conn.cursor()

    # Delete existing rows for these matches then re-insert (simple strategy)
    match_ids = df["match_id"].drop_duplicates().tolist()
    cur.executemany(
        "DELETE FROM fact_participant WHERE match_id = ?",
        [(m,) for m in match_ids],
    )
    conn.commit()

    summoner_key_map = get_summoner_key_map(conn)

    insert_sql = """
        INSERT INTO fact_participant (
            match_id, summoner_id, champion_id,
            team_id, team_position, individual_position, role, lane,
            win, kills, deaths, assists, gold_earned, cs, neutral_minions_killed,
            vision_score, champ_level, time_played_sec,
            double_kills, triple_kills, quadra_kills, penta_kills,
            largest_multi_kill, largest_killing_spree, killing_sprees,
            first_blood_kill, first_blood_assist, first_tower_kill,
            wards_placed, wards_killed, detector_wards_placed,
            damage_to_champs, physical_damage_to_champs, magic_damage_to_champs,
            true_damage_to_champs, damage_to_objectives, damage_to_turrets,
            total_damage_taken, total_heal, total_time_spent_dead,
            turret_kills, dragon_kills, baron_kills, objectives_stolen,
            surrendered, early_surrendered,
            kda, kill_participation, damage_per_minute, gold_per_minute,
            solo_kills, vision_score_per_minute, team_damage_pct,
            cs_first_10_min, skillshots_hit, skillshots_dodged
        )
        VALUES (
            ?,?,?,  ?,?,?,?,?,  ?,?,?,?,?,?,?,  ?,?,?,
            ?,?,?,?,  ?,?,?,  ?,?,?,  ?,?,?,
            ?,?,?,  ?,?,?,  ?,?,?,  ?,?,?,?,
            ?,?,  ?,?,?,?,  ?,?,?,  ?,?,?
        )
    """

    for _, row in df.iterrows():
        puuid = row["participant_puuid"]
        summoner_id = summoner_key_map.get(puuid)
        if summoner_id is None:
            continue

        cur.execute(insert_sql, (
            row["match_id"], _int(summoner_id), _int(row.get("champion_id")),
            _int(row.get("team_id")), row.get("team_position"), row.get("individual_position"),
            row.get("role"), row.get("lane"),
            _int(row.get("win")), _int(row.get("kills")), _int(row.get("deaths")),
            _int(row.get("assists")), _int(row.get("gold_earned")), _int(row.get("cs")),
            _int(row.get("neutral_minions_killed")), _int(row.get("vision_score")),
            _int(row.get("champ_level")), _int(row.get("time_played")),
            _int(row.get("double_kills")), _int(row.get("triple_kills")),
            _int(row.get("quadra_kills")), _int(row.get("penta_kills")),
            _int(row.get("largest_multi_kill")), _int(row.get("largest_killing_spree")),
            _int(row.get("killing_sprees")),
            _int(row.get("first_blood_kill")), _int(row.get("first_blood_assist")),
            _int(row.get("first_tower_kill")),
            _int(row.get("wards_placed")), _int(row.get("wards_killed")),
            _int(row.get("detector_wards_placed")),
            _int(row.get("total_damage_to_champions")),
            _int(row.get("physical_damage_to_champions")),
            _int(row.get("magic_damage_to_champions")),
            _int(row.get("true_damage_to_champions")),
            _int(row.get("damage_to_objectives")), _int(row.get("damage_to_turrets")),
            _int(row.get("total_damage_taken")), _int(row.get("total_heal")),
            _int(row.get("total_time_spent_dead")),
            _int(row.get("turret_kills")), _int(row.get("dragon_kills")),
            _int(row.get("baron_kills")), _int(row.get("objectives_stolen")),
            _int(row.get("surrendered")), _int(row.get("early_surrendered")),
            _float(row.get("kda")), _float(row.get("kill_participation")),
            _float(row.get("damage_per_minute")), _float(row.get("gold_per_minute")),
            _int(row.get("solo_kills")), _float(row.get("vision_score_per_minute")),
            _float(row.get("team_damage_pct")), _int(row.get("cs_first_10_min")),
            _int(row.get("skillshots_hit")), _int(row.get("skillshots_dodged")),
        ))

    conn.commit()


# -----------------------------------------
# Main
# -----------------------------------------

def main():
    conn = init_db()
    df = load_stg_participants()

    upsert_dim_summoner(conn, df)
    upsert_dim_champion(conn, df)
    upsert_dim_queue(conn, df)
    upsert_dim_match(conn, df)
    upsert_fact_participant(conn, df)

    conn.close()
    print(f"Warehouse load complete. DB: {WAREHOUSE_DB}")


if __name__ == "__main__":
    main()
