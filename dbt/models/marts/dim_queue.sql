-- dim_queue.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per queue (game mode).
-- -----------------------------------------------------------------------------

select
    queue_id,
    queue_description,
    map_name,
    notes
from {{ ref('stg_queues') }}
