-- stg_champion_mastery.sql
-- -----------------------------------------------------------------------------
-- Clean per-(puuid, champion) mastery snapshot.
--
-- Source JSON keeps Riot API camelCase; we rename to snake_case and convert
-- lastPlayTime (epoch ms) to a proper TIMESTAMP.
--
-- Note: `chestGranted` was removed from Riot's mastery endpoint (associated
-- with the Hextech Chest system changes) — so this model no longer surfaces
-- that column.
-- -----------------------------------------------------------------------------

with raw as (
    select * from {{ source('raw', 'champion_mastery') }}
)

select
    puuid,
    try_cast("championId" as integer)                             as champion_id,
    try_cast("championLevel" as integer)                          as champion_level,
    try_cast("championPoints" as integer)                         as champion_points,
    to_timestamp(try_cast("lastPlayTime" as bigint) / 1000)       as last_play_time,
    try_cast("championPointsSinceLastLevel" as integer)           as points_since_last_level,
    try_cast("championPointsUntilNextLevel" as integer)           as points_until_next_level,
    try_cast("tokensEarned" as integer)                           as tokens_earned,
    try_cast(fetched_at as timestamp)                             as fetched_at
from raw
