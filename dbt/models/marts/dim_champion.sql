-- dim_champion.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per champion. Sourced from Data Dragon.
-- -----------------------------------------------------------------------------

select
    champion_id,
    champion_name,
    champion_key,
    title,
    tags
from {{ ref('stg_champions') }}
where champion_id is not null
