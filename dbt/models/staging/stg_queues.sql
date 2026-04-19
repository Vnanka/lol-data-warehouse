-- stg_queues.sql
-- -----------------------------------------------------------------------------
-- Clean queue master data from Riot's static queues.json endpoint.
-- -----------------------------------------------------------------------------

with raw as (
    select * from {{ source('raw', 'queues') }}
)

select
    try_cast("queueId" as integer) as queue_id,
    description                    as queue_description,
    map                            as map_name,
    notes
from raw
where "queueId" is not null
