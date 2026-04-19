-- fact_champion_mastery.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per (puuid, champion_id).
-- Snapshotted each pipeline run — fetched_at records when.
-- -----------------------------------------------------------------------------

select
    puuid,
    champion_id,
    champion_level,
    champion_points,
    last_play_time,
    points_since_last_level,
    points_until_next_level,
    tokens_earned,
    fetched_at
from {{ ref('stg_champion_mastery') }}
