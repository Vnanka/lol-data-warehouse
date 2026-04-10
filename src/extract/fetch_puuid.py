import os
import requests
from dotenv import load_dotenv
from urllib.parse import quote


def fetch_puuid() -> str:
    """
    Looks up your PUUID from the Riot API using your game name and tag line.
    Reads RIOT_API_KEY, RIOT_GAME_NAME, RIOT_TAG_LINE from environment variables.
    Returns the PUUID string.
    """
    api_key = os.getenv("RIOT_API_KEY")
    game_name = os.getenv("RIOT_GAME_NAME")
    tag_line = os.getenv("RIOT_TAG_LINE")

    if not api_key:
        raise RuntimeError("RIOT_API_KEY not found in .env")
    if not game_name or not tag_line:
        raise RuntimeError("RIOT_GAME_NAME or RIOT_TAG_LINE not found in .env")

    # URL-encode the game name (handles spaces & special chars)
    game_name_encoded = quote(game_name)

    url = (
        "https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/"
        f"{game_name_encoded}/{tag_line}"
    )

    r = requests.get(url, headers={"X-Riot-Token": api_key}, timeout=30)
    r.raise_for_status()  # raises an error automatically if status != 200

    puuid = r.json()["puuid"]
    return puuid


# -------------------------------------------------------
# This block only runs when you execute THIS file directly:
#   python src/extract/fetch_puuid.py
#
# It does NOT run when another script imports this file.
# -------------------------------------------------------
if __name__ == "__main__":
    load_dotenv()
    puuid = fetch_puuid()
    print("Your PUUID:", puuid)
