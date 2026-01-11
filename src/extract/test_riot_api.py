import os
import requests
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

api_key = os.getenv("RIOT_API_KEY")
if not api_key:
    raise RuntimeError("RIOT_API_KEY not found. Check your .env file.")

headers = {
    "X-Riot-Token": api_key
}

# Simple endpoint just to verify access
url = "https://euw1.api.riotgames.com/lol/status/v4/platform-data"

response = requests.get(url, headers=headers, timeout=30)

print("Status code:", response.status_code)
print("Response preview:")
print(response.text[:500])
