import json
import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MATCH_DIR = PROJECT_ROOT / "data" / "raw" / "matches"
DEFAULT_OUT_FILE  = PROJECT_ROOT / "data" / "stg" / "stg_participants.csv"

FIELDS = [
    # ── Match context ──────────────────────────────────────────────
    "match_id",
    "game_creation",
    "game_duration",
    "game_mode",
    "queue_id",
    "game_version",
    "platform_id",

    # ── Player identity ────────────────────────────────────────────
    "participant_puuid",
    "riot_id",
    "summoner_name",
    "summoner_level",
    "champion_id",
    "champion_name",
    "champ_level",
    "team_id",
    "team_position",
    "individual_position",
    "role",
    "lane",

    # ── Core stats ─────────────────────────────────────────────────
    "win",
    "kills",
    "deaths",
    "assists",
    "gold_earned",
    "cs",
    "neutral_minions_killed",
    "vision_score",
    "time_played",

    # ── Multi-kills & sprees ───────────────────────────────────────
    "double_kills",
    "triple_kills",
    "quadra_kills",
    "penta_kills",
    "largest_multi_kill",
    "largest_killing_spree",
    "killing_sprees",

    # ── Milestones ─────────────────────────────────────────────────
    "first_blood_kill",
    "first_blood_assist",
    "first_tower_kill",

    # ── Vision ─────────────────────────────────────────────────────
    "wards_placed",
    "wards_killed",
    "detector_wards_placed",

    # ── Damage dealt ───────────────────────────────────────────────
    "total_damage_to_champions",
    "physical_damage_to_champions",
    "magic_damage_to_champions",
    "true_damage_to_champions",
    "damage_to_objectives",
    "damage_to_turrets",

    # ── Damage taken / survival ────────────────────────────────────
    "total_damage_taken",
    "total_heal",
    "total_time_spent_dead",

    # ── Objectives ─────────────────────────────────────────────────
    "turret_kills",
    "dragon_kills",
    "baron_kills",
    "objectives_stolen",

    # ── Game outcome context ───────────────────────────────────────
    "surrendered",
    "early_surrendered",

    # ── Challenges (derived metrics from Riot) ─────────────────────
    "kda",
    "kill_participation",
    "damage_per_minute",
    "gold_per_minute",
    "solo_kills",
    "vision_score_per_minute",
    "team_damage_pct",
    "cs_first_10_min",
    "skillshots_hit",
    "skillshots_dodged",
]


def build_stg_participants(
    match_dir: Path = DEFAULT_MATCH_DIR,
    out_file: Path = DEFAULT_OUT_FILE,
) -> None:
    """
    Reads all raw match JSON files and extracts one row per participant.
    Writes the enriched result to a staging CSV file.
    """
    out_file.parent.mkdir(parents=True, exist_ok=True)
    rows = []

    for path in match_dir.glob("*.json"):
        match_id = path.stem

        with open(path, "r", encoding="utf-8") as f:
            match = json.load(f)

        info = match.get("info", {})

        for p in info.get("participants", []):
            ch = p.get("challenges", {})

            row = {
                # ── Match context ──────────────────────────────────
                "match_id":       match_id,
                "game_creation":  info.get("gameCreation"),
                "game_duration":  info.get("gameDuration"),
                "game_mode":      info.get("gameMode"),
                "queue_id":       info.get("queueId"),
                "game_version":   info.get("gameVersion"),
                "platform_id":    info.get("platformId"),

                # ── Player identity ────────────────────────────────
                "participant_puuid": p.get("puuid"),
                "riot_id":           (p.get("riotIdGameName", "") + "#" + p.get("riotIdTagline", "")).strip("#"),
                "summoner_name":     p.get("summonerName"),
                "summoner_level":    p.get("summonerLevel"),
                "champion_id":       p.get("championId"),
                "champion_name":     p.get("championName"),
                "champ_level":       p.get("champLevel"),
                "team_id":           p.get("teamId"),
                "team_position":     p.get("teamPosition"),
                "individual_position": p.get("individualPosition"),
                "role":              p.get("role"),
                "lane":              p.get("lane"),

                # ── Core stats ─────────────────────────────────────
                "win":                   p.get("win"),
                "kills":                 p.get("kills"),
                "deaths":                p.get("deaths"),
                "assists":               p.get("assists"),
                "gold_earned":           p.get("goldEarned"),
                "cs":                    p.get("totalMinionsKilled"),
                "neutral_minions_killed": p.get("neutralMinionsKilled"),
                "vision_score":          p.get("visionScore"),
                "time_played":           p.get("timePlayed"),

                # ── Multi-kills & sprees ───────────────────────────
                "double_kills":         p.get("doubleKills"),
                "triple_kills":         p.get("tripleKills"),
                "quadra_kills":         p.get("quadraKills"),
                "penta_kills":          p.get("pentaKills"),
                "largest_multi_kill":   p.get("largestMultiKill"),
                "largest_killing_spree": p.get("largestKillingSpree"),
                "killing_sprees":       p.get("killingSprees"),

                # ── Milestones ─────────────────────────────────────
                "first_blood_kill":   p.get("firstBloodKill"),
                "first_blood_assist": p.get("firstBloodAssist"),
                "first_tower_kill":   p.get("firstTowerKill"),

                # ── Vision ─────────────────────────────────────────
                "wards_placed":         p.get("wardsPlaced"),
                "wards_killed":         p.get("wardsKilled"),
                "detector_wards_placed": p.get("detectorWardsPlaced"),

                # ── Damage dealt ───────────────────────────────────
                "total_damage_to_champions":    p.get("totalDamageDealtToChampions"),
                "physical_damage_to_champions": p.get("physicalDamageDealtToChampions"),
                "magic_damage_to_champions":    p.get("magicDamageDealtToChampions"),
                "true_damage_to_champions":     p.get("trueDamageDealtToChampions"),
                "damage_to_objectives":         p.get("damageDealtToObjectives"),
                "damage_to_turrets":            p.get("damageDealtToTurrets"),

                # ── Damage taken / survival ────────────────────────
                "total_damage_taken":    p.get("totalDamageTaken"),
                "total_heal":            p.get("totalHeal"),
                "total_time_spent_dead": p.get("totalTimeSpentDead"),

                # ── Objectives ─────────────────────────────────────
                "turret_kills":      p.get("turretKills"),
                "dragon_kills":      p.get("dragonKills"),
                "baron_kills":       p.get("baronKills"),
                "objectives_stolen": p.get("objectivesStolen"),

                # ── Game outcome context ───────────────────────────
                "surrendered":       p.get("gameEndedInSurrender"),
                "early_surrendered": p.get("gameEndedInEarlySurrender"),

                # ── Challenges ─────────────────────────────────────
                "kda":                  ch.get("kda"),
                "kill_participation":   ch.get("killParticipation"),
                "damage_per_minute":    ch.get("damagePerMinute"),
                "gold_per_minute":      ch.get("goldPerMinute"),
                "solo_kills":           ch.get("soloKills"),
                "vision_score_per_minute": ch.get("visionScorePerMinute"),
                "team_damage_pct":      ch.get("teamDamagePercentage"),
                "cs_first_10_min":      ch.get("laneMinionsFirst10Minutes"),
                "skillshots_hit":       ch.get("skillshotsHit"),
                "skillshots_dodged":    ch.get("skillshotsDodged"),
            }
            rows.append(row)

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_file}")


if __name__ == "__main__":
    build_stg_participants()
