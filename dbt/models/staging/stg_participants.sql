-- stg_participants.sql
-- -----------------------------------------------------------------------------
-- Grain: one row per (match, participant).
--
-- Takes the all-VARCHAR raw.participants table and produces a typed, renamed
-- view. Business logic lives downstream (in marts) — here we only:
--   - rename columns to match downstream naming (e.g. total_damage_to_champions
--     → damage_to_champs, time_played → time_played_sec, platform_id → platform)
--   - convert strings to typed columns (try_cast = NULL on failure, never errors)
--   - convert True/False text to INTEGER 0/1 for boolean fields
--   - convert game_creation epoch-ms to a proper TIMESTAMP
--
-- Materialized as a view (see dbt_project.yml) — cheap to rebuild, always current.
-- -----------------------------------------------------------------------------

with raw as (
    select * from {{ source('raw', 'participants') }}
)

select
    -- ── Match context ──────────────────────────────────────────────
    match_id,
    try_cast(game_creation as bigint)                                         as game_creation_ms,
    to_timestamp(try_cast(game_creation as bigint) / 1000)                    as game_created_at,
    try_cast(game_duration as integer)                                        as game_duration_sec,
    game_mode,
    try_cast(queue_id as integer)                                             as queue_id,
    game_version,
    platform_id                                                               as platform,

    -- ── Player identity ────────────────────────────────────────────
    participant_puuid                                                         as puuid,
    riot_id,
    summoner_name,
    try_cast(summoner_level as integer)                                       as summoner_level,
    try_cast(champion_id as integer)                                          as champion_id,
    champion_name,
    try_cast(champ_level as integer)                                          as champ_level,
    try_cast(team_id as integer)                                              as team_id,
    team_position,
    individual_position,
    role,
    lane,

    -- ── Core stats (win is True/False text → 0/1 int) ──────────────
    case lower(win) when 'true' then 1 when 'false' then 0 end                as win,
    try_cast(kills as integer)                                                as kills,
    try_cast(deaths as integer)                                               as deaths,
    try_cast(assists as integer)                                              as assists,
    try_cast(gold_earned as integer)                                          as gold_earned,
    try_cast(cs as integer)                                                   as cs,
    try_cast(neutral_minions_killed as integer)                               as neutral_minions_killed,
    try_cast(vision_score as integer)                                         as vision_score,
    try_cast(time_played as integer)                                          as time_played_sec,

    -- ── Multi-kills & sprees ───────────────────────────────────────
    try_cast(double_kills as integer)                                         as double_kills,
    try_cast(triple_kills as integer)                                         as triple_kills,
    try_cast(quadra_kills as integer)                                         as quadra_kills,
    try_cast(penta_kills as integer)                                          as penta_kills,
    try_cast(largest_multi_kill as integer)                                   as largest_multi_kill,
    try_cast(largest_killing_spree as integer)                                as largest_killing_spree,
    try_cast(killing_sprees as integer)                                       as killing_sprees,

    -- ── Milestones (booleans → 0/1) ────────────────────────────────
    case lower(first_blood_kill)   when 'true' then 1 when 'false' then 0 end as first_blood_kill,
    case lower(first_blood_assist) when 'true' then 1 when 'false' then 0 end as first_blood_assist,
    case lower(first_tower_kill)   when 'true' then 1 when 'false' then 0 end as first_tower_kill,

    -- ── Vision ─────────────────────────────────────────────────────
    try_cast(wards_placed as integer)                                         as wards_placed,
    try_cast(wards_killed as integer)                                         as wards_killed,
    try_cast(detector_wards_placed as integer)                                as detector_wards_placed,

    -- ── Damage dealt (renamed to match fact) ───────────────────────
    try_cast(total_damage_to_champions as integer)                            as damage_to_champs,
    try_cast(physical_damage_to_champions as integer)                         as physical_damage_to_champs,
    try_cast(magic_damage_to_champions as integer)                            as magic_damage_to_champs,
    try_cast(true_damage_to_champions as integer)                             as true_damage_to_champs,
    try_cast(damage_to_objectives as integer)                                 as damage_to_objectives,
    try_cast(damage_to_turrets as integer)                                    as damage_to_turrets,

    -- ── Damage taken / survival ────────────────────────────────────
    try_cast(total_damage_taken as integer)                                   as total_damage_taken,
    try_cast(total_heal as integer)                                           as total_heal,
    try_cast(total_time_spent_dead as integer)                                as total_time_spent_dead,

    -- ── Objectives ─────────────────────────────────────────────────
    try_cast(turret_kills as integer)                                         as turret_kills,
    try_cast(dragon_kills as integer)                                         as dragon_kills,
    try_cast(baron_kills as integer)                                          as baron_kills,
    try_cast(objectives_stolen as integer)                                    as objectives_stolen,

    -- ── Game outcome (booleans → 0/1) ──────────────────────────────
    case lower(surrendered)       when 'true' then 1 when 'false' then 0 end  as surrendered,
    case lower(early_surrendered) when 'true' then 1 when 'false' then 0 end  as early_surrendered,

    -- ── Challenges (Riot-derived metrics) ──────────────────────────
    try_cast(kda as double)                                                   as kda,
    try_cast(kill_participation as double)                                    as kill_participation,
    try_cast(damage_per_minute as double)                                     as damage_per_minute,
    try_cast(gold_per_minute as double)                                       as gold_per_minute,
    try_cast(solo_kills as integer)                                           as solo_kills,
    try_cast(vision_score_per_minute as double)                               as vision_score_per_minute,
    try_cast(team_damage_pct as double)                                       as team_damage_pct,
    try_cast(cs_first_10_min as integer)                                      as cs_first_10_min,
    try_cast(skillshots_hit as integer)                                       as skillshots_hit,
    try_cast(skillshots_dodged as integer)                                    as skillshots_dodged

from raw
