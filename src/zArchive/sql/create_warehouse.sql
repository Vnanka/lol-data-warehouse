-- sql/create_warehouse.sql
-- NOTE: if you change this schema, delete data/warehouse/lol_dw.sqlite
--       and re-run the pipeline to rebuild from scratch.

PRAGMA foreign_keys = ON;

-- =========================================
-- Dimensions
-- =========================================

CREATE TABLE IF NOT EXISTS dim_summoner (
    summoner_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    puuid           TEXT NOT NULL UNIQUE,
    riot_id         TEXT,
    summoner_name   TEXT,
    last_seen_at    TEXT
);

CREATE TABLE IF NOT EXISTS dim_champion (
    champion_id     INTEGER PRIMARY KEY,
    champion_name   TEXT,
    champion_key    TEXT,
    title           TEXT,
    tags            TEXT
);

CREATE TABLE IF NOT EXISTS dim_queue (
    queue_id            INTEGER PRIMARY KEY,
    queue_description   TEXT
);

CREATE TABLE IF NOT EXISTS dim_match (
    match_id            TEXT PRIMARY KEY,
    game_creation       TEXT,
    game_duration_sec   INTEGER,
    game_mode           TEXT,
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

    -- Keys
    match_id                TEXT NOT NULL,
    summoner_id             INTEGER NOT NULL,
    champion_id             INTEGER,

    -- Position
    team_id                 INTEGER,
    team_position           TEXT,
    individual_position     TEXT,
    role                    TEXT,
    lane                    TEXT,

    -- Core stats
    win                     INTEGER,
    kills                   INTEGER,
    deaths                  INTEGER,
    assists                 INTEGER,
    gold_earned             INTEGER,
    cs                      INTEGER,
    neutral_minions_killed  INTEGER,
    vision_score            INTEGER,
    champ_level             INTEGER,
    time_played_sec         INTEGER,

    -- Multi-kills & sprees
    double_kills            INTEGER,
    triple_kills            INTEGER,
    quadra_kills            INTEGER,
    penta_kills             INTEGER,
    largest_multi_kill      INTEGER,
    largest_killing_spree   INTEGER,
    killing_sprees          INTEGER,

    -- Milestones
    first_blood_kill        INTEGER,
    first_blood_assist      INTEGER,
    first_tower_kill        INTEGER,

    -- Vision
    wards_placed            INTEGER,
    wards_killed            INTEGER,
    detector_wards_placed   INTEGER,

    -- Damage dealt
    damage_to_champs        INTEGER,
    physical_damage_to_champs INTEGER,
    magic_damage_to_champs  INTEGER,
    true_damage_to_champs   INTEGER,
    damage_to_objectives    INTEGER,
    damage_to_turrets       INTEGER,

    -- Damage taken / survival
    total_damage_taken      INTEGER,
    total_heal              INTEGER,
    total_time_spent_dead   INTEGER,

    -- Objectives
    turret_kills            INTEGER,
    dragon_kills            INTEGER,
    baron_kills             INTEGER,
    objectives_stolen       INTEGER,

    -- Game outcome context
    surrendered             INTEGER,
    early_surrendered       INTEGER,

    -- Challenges (derived metrics from Riot)
    kda                     REAL,
    kill_participation      REAL,
    damage_per_minute       REAL,
    gold_per_minute         REAL,
    solo_kills              INTEGER,
    vision_score_per_minute REAL,
    team_damage_pct         REAL,
    cs_first_10_min         INTEGER,
    skillshots_hit          INTEGER,
    skillshots_dodged       INTEGER,

    FOREIGN KEY (match_id)    REFERENCES dim_match(match_id),
    FOREIGN KEY (summoner_id) REFERENCES dim_summoner(summoner_id),
    FOREIGN KEY (champion_id) REFERENCES dim_champion(champion_id)
);

CREATE TABLE IF NOT EXISTS fact_champion_mastery (
    puuid                       TEXT NOT NULL,
    champion_id                 INTEGER NOT NULL,
    champion_level              INTEGER,
    champion_points             INTEGER,
    last_play_time              TEXT,
    points_since_last_level     INTEGER,
    points_until_next_level     INTEGER,
    chest_granted               INTEGER,
    tokens_earned               INTEGER,
    fetched_at                  TEXT NOT NULL,
    PRIMARY KEY (puuid, champion_id),
    FOREIGN KEY (puuid)        REFERENCES dim_summoner(puuid),
    FOREIGN KEY (champion_id)  REFERENCES dim_champion(champion_id)
);
