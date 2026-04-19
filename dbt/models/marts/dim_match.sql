-- dim_match.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per match. Match-level context only — per-player stats live
-- in fact_participant.
-- -----------------------------------------------------------------------------

select
    match_id,
    game_created_at,
    game_creation_ms,
    game_duration_sec,
    game_mode,
    queue_id,
    game_version,
    platform
from {{ ref('stg_matches') }}
