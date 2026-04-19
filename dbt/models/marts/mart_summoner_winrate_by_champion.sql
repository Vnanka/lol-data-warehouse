-- mart_summoner_winrate_by_champion.sql
-- -----------------------------------------------------------------------------
-- "Which champions do I win most with?"
--
-- Grain: one row per (summoner, champion).
-- Aggregates fact_participant and joins to dim_summoner / dim_champion for
-- human-readable names.
-- -----------------------------------------------------------------------------

with participants as (
    select * from {{ ref('fact_participant') }}
),

summoners as (
    select * from {{ ref('dim_summoner') }}
),

champions as (
    select * from {{ ref('dim_champion') }}
)

select
    s.puuid,
    s.summoner_name,
    s.riot_id,
    p.champion_id,
    c.champion_name,

    -- Volume
    count(*)                                as games_played,

    -- Wins / losses
    sum(p.win)                              as wins,
    count(*) - sum(p.win)                   as losses,
    round(sum(p.win) * 1.0 / count(*), 4)   as win_rate,

    -- KDA
    round(avg(p.kda), 2)                    as avg_kda,
    round(avg(p.kills), 2)                  as avg_kills,
    round(avg(p.deaths), 2)                 as avg_deaths,
    round(avg(p.assists), 2)                as avg_assists,

    -- Damage & economy
    round(avg(p.damage_per_minute), 2)      as avg_dpm,
    round(avg(p.gold_per_minute), 2)        as avg_gpm,

    -- Vision
    round(avg(p.vision_score), 2)           as avg_vision_score

from participants p
join summoners s on p.puuid = s.puuid
left join champions c on p.champion_id = c.champion_id
group by 1, 2, 3, 4, 5
order by s.puuid, games_played desc
