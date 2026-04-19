-- stg_champions.sql
-- -----------------------------------------------------------------------------
-- Clean champion master data from Riot Data Dragon.
--
-- Data Dragon's naming is confusing:
--   - "key" is the numeric champion ID (as a string) — we call this champion_id
--   - "id"  is the string slug like "Aatrox"         — we call this champion_key
-- -----------------------------------------------------------------------------

with raw as (
    select * from {{ source('raw', 'champions') }}
)

select
    try_cast(key as integer)    as champion_id,
    name                        as champion_name,
    id                          as champion_key,
    title,
    array_to_string(tags, ', ') as tags
from raw
