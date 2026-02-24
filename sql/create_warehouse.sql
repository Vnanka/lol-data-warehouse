-- sql/create_warehouse.sql

PRAGMA foreign_keys = ON;

-- =========================================
-- Dimensions
-- =========================================

CREATE TABLE IF NOT EXISTS dim_summoner (
    summoner_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    puuid           TEXT NOT NULL UNIQUE,
    summoner_name   TEXT,
    last_seen_at    TEXT          -- ISO datetime of latest match we saw
);

CREATE TABLE IF NOT EXISTS dim_champion (
    champion_id     INTEGER PRIMARY KEY,
    champion_name   TEXT
);

CREATE TABLE IF NOT EXISTS dim_queue (
    queue_id            INTEGER PRIMARY KEY,
    queue_description   TEXT
);

CREATE TABLE IF NOT EXISTS dim_match (
    match_id            TEXT PRIMARY KEY,
    game_creation       TEXT,      -- ISO datetime
    game_duration_sec   INTEGER,
    queue_id            INTEGER,
    game_version        TEXT,
    platform            TEXT,
    FOREIGN KEY (queue_id) REFERENCES dim_queue(queue_id)
);

-- =========================================
-- Fact
-- =========================================

CREATE TABLE IF NOT EXISTS fact_participant (
    fact_participant_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id                TEXT NOT NULL,
    summoner_id             INTEGER NOT NULL,
    champion_id             INTEGER,
    team_id                 INTEGER,
    win                     INTEGER,
    kills                   INTEGER,
    deaths                  INTEGER,
    assists                 INTEGER,
    gold_earned             INTEGER,
    damage_to_champs        INTEGER,
    cs                      INTEGER,
    vision_score            INTEGER,
    role                    TEXT,
    team_position           TEXT,
    individual_position     TEXT,
    lane                    TEXT,
    time_played_sec         INTEGER,

    FOREIGN KEY (match_id)    REFERENCES dim_match(match_id),
    FOREIGN KEY (summoner_id) REFERENCES dim_summoner(summoner_id),
    FOREIGN KEY (champion_id) REFERENCES dim_champion(champion_id)
);