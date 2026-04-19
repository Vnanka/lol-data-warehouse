-- fact_participant.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per (match, participant).
--
-- This is the main fact table — all player-game performance metrics. FKs:
--   - match_id    → dim_match
--   - puuid       → dim_summoner
--   - champion_id → dim_champion
--   - queue_id is on dim_match, not here
--
-- Currently materialized as table (full rebuild). For larger data volumes this
-- should become incremental with unique_key=['match_id','puuid']:
--
--   {{ config(materialized='incremental', unique_key=['match_id','puuid']) }}
--   ...
--   {% if is_incremental() %}
--     where match_id not in (select match_id from {{ this }})
--   {% endif %}
-- -----------------------------------------------------------------------------

select
    -- Keys
    match_id,
    puuid,
    champion_id,

    -- Position
    team_id,
    team_position,
    individual_position,
    role,
    lane,

    -- Core stats
    win,
    kills,
    deaths,
    assists,
    gold_earned,
    cs,
    neutral_minions_killed,
    vision_score,
    champ_level,
    time_played_sec,

    -- Multi-kills & sprees
    double_kills,
    triple_kills,
    quadra_kills,
    penta_kills,
    largest_multi_kill,
    largest_killing_spree,
    killing_sprees,

    -- Milestones
    first_blood_kill,
    first_blood_assist,
    first_tower_kill,

    -- Vision
    wards_placed,
    wards_killed,
    detector_wards_placed,

    -- Damage dealt
    damage_to_champs,
    physical_damage_to_champs,
    magic_damage_to_champs,
    true_damage_to_champs,
    damage_to_objectives,
    damage_to_turrets,

    -- Damage taken / survival
    total_damage_taken,
    total_heal,
    total_time_spent_dead,

    -- Objectives
    turret_kills,
    dragon_kills,
    baron_kills,
    objectives_stolen,

    -- Game outcome
    surrendered,
    early_surrendered,

    -- Riot-derived challenges
    kda,
    kill_participation,
    damage_per_minute,
    gold_per_minute,
    solo_kills,
    vision_score_per_minute,
    team_damage_pct,
    cs_first_10_min,
    skillshots_hit,
    skillshots_dodged
from {{ ref('stg_participants') }}
