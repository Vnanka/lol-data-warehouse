import csv
import os
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PARTICIPANTS_FILE = PROJECT_ROOT / "data" / "stg" / "stg_participants.csv"
DEFAULT_OUT_FILE = PROJECT_ROOT / "data" / "stg" / "stg_my_games.csv"


def build_stg_my_games(
    puuid: str,
    participants_file: Path = DEFAULT_PARTICIPANTS_FILE,
    out_file: Path = DEFAULT_OUT_FILE,
) -> None:
    rows = []
    fieldnames = None

    with open(participants_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row["participant_puuid"] == puuid:
                rows.append(row)

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)  # type: ignore
        writer.writeheader()
        writer.writerows(rows)

    print(f"Filtered {len(rows)} rows into {out_file}")


if __name__ == "__main__":
    load_dotenv()
    puuid = os.getenv("PUUID")
    if not puuid:
        raise RuntimeError("PUUID not set in .env")
    build_stg_my_games(puuid)
