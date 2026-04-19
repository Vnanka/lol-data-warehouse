-- dim_summoner.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per unique summoner (puuid).
--
-- Design decision: we use `puuid` directly as the primary key instead of a
-- surrogate integer. Reasons:
--   - puuid is already globally unique and stable (Riot guarantees this)
--   - avoids surrogate-key instability across full refreshes
--   - simpler joins downstream
--
-- Built from the UNION of stg_participants and stg_champion_mastery so mastery
-- snapshots for puuids we haven't seen in match history still have a valid
-- FK target.
-- -----------------------------------------------------------------------------

with from_participants as (
    select
        puuid,
        riot_id,
        summoner_name,
        game_created_at as seen_at
    from {{ ref('stg_participants') }}
    where puuid is not null
),

from_mastery as (
    select
        puuid,
        cast(null as varchar) as riot_id,
        cast(null as varchar) as summoner_name,
        fetched_at            as seen_at
    from {{ ref('stg_champion_mastery') }}
    where puuid is not null
),

union_all as (
    select * from from_participants
    union all
    select * from from_mastery
),

ranked as (
    select
        puuid,
        riot_id,
        summoner_name,
        seen_at,
        -- Prefer rows that have a riot_id (participants) over those that don't (mastery-only)
        row_number() over (
            partition by puuid
            order by case when riot_id is not null then 0 else 1 end, seen_at desc
        ) as rn
    from union_all
)

select
    puuid,
    riot_id,
    summoner_name,
    max(seen_at) over (partition by puuid) as last_seen_at
from ranked
where rn = 1
