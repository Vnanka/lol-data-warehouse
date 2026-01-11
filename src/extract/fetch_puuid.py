import os
import requests
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()

api_key = os.getenv("RIOT_API_KEY")
game_name = os.getenv("RIOT_GAME_NAME")
tag_line = os.getenv("RIOT_TAG_LINE")

if not api_key:
    raise RuntimeError("RIOT_API_KEY not found")
if not game_name or not tag_line:
    raise RuntimeError("RIOT_GAME_NAME or RIOT_TAG_LINE not found")

# 🔑 URL-encode the game name (handles spaces & special chars)
game_name_encoded = quote(game_name)

headers = {"X-Riot-Token": api_key}

url = (
    "https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/"
    f"{game_name_encoded}/{tag_line}"
)

print("Using Riot ID:", f"{game_name}#{tag_line}")
print("Encoded name:", game_name_encoded)

r = requests.get(url, headers=headers, timeout=30)

print("Status code:", r.status_code)
print("Response:", r.text)
