-- stg_matches.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per match.
--
-- Derived from stg_participants — every participant row in the same match
-- carries identical match-level context, so we distinct it out. Doing this in
-- dbt (instead of splitting the CSV in Python) keeps the Python load layer
-- "dumb" and all transformation logic in SQL where it's reviewable.
-- -----------------------------------------------------------------------------

select distinct
    match_id,
    game_creation_ms,
    game_created_at,
    game_duration_sec,
    game_mode,
    queue_id,
    game_version,
    platform
from {{ ref('stg_participants') }}
