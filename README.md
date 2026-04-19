# lol-data-warehouse

A personal League of Legends data warehouse built as a Data Engineering
portfolio project. Pulls my own match history, champion mastery, and reference
data from the Riot API, lands it in DuckDB, and transforms it into a dimensional
model using dbt.

## Architecture

```
  ┌──────────┐    ┌───────────┐    ┌──────────┐    ┌─────────────────────┐
  │ EXTRACT  │ →  │ TRANSFORM │ →  │ LOAD RAW │ →  │         DBT         │
  │ (Python) │    │ (Python)  │    │ (Python) │    │ staging → marts     │
  └──────────┘    └───────────┘    └──────────┘    └─────────────────────┘
  Riot API →      match JSONs →     CSV + JSON      raw.* → stg_* →
  files on disk   flat CSV          → DuckDB raw.*  dim_*, fact_*, mart_*
```

Python handles the "EL" (Extract + Load) — it pulls from Riot's APIs, lands
files on disk, then dumb-loads them into a `raw` schema in DuckDB. **dbt owns
all transformation** — naming, casting, dimensional modelling, and tests live
in SQL models where they're reviewable and auto-documented.

## Data model

Star schema in the `main` schema of DuckDB.

| Table                                    | Grain                           | Purpose                                              |
|------------------------------------------|---------------------------------|------------------------------------------------------|
| `dim_summoner`                           | one row per `puuid`             | Player identity                                      |
| `dim_champion`                           | one row per `champion_id`       | Champion master data (Data Dragon)                   |
| `dim_queue`                              | one row per `queue_id`          | Game mode / queue metadata                           |
| `dim_match`                              | one row per `match_id`          | Match-level context                                  |
| `fact_participant`                       | `(match_id, puuid)`             | Core performance fact — 50+ metrics per player-game  |
| `fact_champion_mastery`                  | `(puuid, champion_id)`          | Mastery level / points snapshot                      |
| `mart_summoner_winrate_by_champion`      | `(puuid, champion_id)`          | Analytics mart — win rate, avg KDA, DPM, etc.        |

Referential integrity is tested by dbt `relationships` tests between every fact
and its dimension.

## Folder layout

```
.
├── dbt/                             dbt project (transformation layer)
│   ├── dbt_project.yml
│   ├── profiles.yml                 — connection to data/warehouse/lol_dw.duckdb
│   └── models/
│       ├── staging/                 views over raw.* (renames, casts)
│       │   ├── sources.yml
│       │   ├── stg_participants.sql
│       │   ├── stg_matches.sql
│       │   ├── stg_champions.sql
│       │   ├── stg_queues.sql
│       │   ├── stg_champion_mastery.sql
│       │   └── schema.yml
│       └── marts/                   tables (dims, facts, analytics)
│           ├── dim_*.sql, fact_*.sql, mart_*.sql
│           └── schema.yml
├── src/
│   ├── extract/                     Python — pull Riot API → files on disk
│   ├── transform/                   Python — flatten match JSONs into CSV
│   ├── load/
│   │   └── load_raw.py              Python — files → DuckDB raw.* tables
│   └── pipeline/
│       └── run_pipeline.py          single entry point (EL + dbt build)
├── data/                            gitignored
│   ├── raw/                         API responses (JSON)
│   ├── stg/                         flattened CSVs
│   └── warehouse/lol_dw.duckdb      the warehouse
├── sql/                             legacy SQLite DDL (superseded by dbt)
├── requirements.txt
├── .env.example                     template — copy to .env and fill in
└── .gitignore
```

## Quickstart

```bash
# 1. Clone and enter the repo
git clone <this-repo>
cd lol-data-warehouse

# 2. Create a virtualenv and install dependencies
python -m venv .venv
.venv\Scripts\activate             # Windows
# source .venv/bin/activate        # macOS/Linux
pip install -r requirements.txt

# 3. Configure credentials
copy .env.example .env             # or `cp` on macOS/Linux
# edit .env and paste your RIOT_API_KEY, RIOT_GAME_NAME, RIOT_TAG_LINE

# 4. (First run only) discover your PUUID and paste it back into .env
python src/extract/fetch_puuid.py

# 5. Run the whole pipeline — extract, load, dbt build + test
python src/pipeline/run_pipeline.py
```

### Useful flags while iterating

```bash
# Re-run without hitting the Riot API (uses data already on disk)
python src/pipeline/run_pipeline.py --skip-extract

# Only re-run dbt (skip every Python step)
python src/pipeline/run_pipeline.py --skip-extract --skip-transform --skip-load

# Work directly with dbt from the dbt/ folder
cd dbt
dbt build --profiles-dir .         # build everything, run tests
dbt test  --profiles-dir .         # tests only
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

## Querying the warehouse

DuckDB is a single file. Open it from anywhere:

```bash
# Via the DuckDB CLI
duckdb data/warehouse/lol_dw.duckdb

# Via Python
python -c "import duckdb; print(duckdb.connect('data/warehouse/lol_dw.duckdb').sql('select * from mart_summoner_winrate_by_champion order by games_played desc limit 10').to_df())"
```

Example queries:

```sql
-- My win rate on each champion (min 5 games)
select champion_name, games_played, wins, win_rate, avg_kda
from mart_summoner_winrate_by_champion
where games_played >= 5
order by win_rate desc;

-- My most-played champions by mastery points
select c.champion_name, m.champion_level, m.champion_points
from fact_champion_mastery m
join dim_champion c on c.champion_id = m.champion_id
order by m.champion_points desc
limit 20;
```

## What's next

Tests, documentation, and the first analytics mart are in place — the
immediately valuable next steps are:

- Add more marts: `mart_player_career`, `mart_champion_mastery_progression`
  (combines mastery with performance), `mart_match_timeline`.
- Add a date dimension (`dim_date`) for time-series analysis.
- Introduce `dbt_utils` via `packages.yml` for richer tests (`unique_combination_of_columns`, `expression_is_true`).
- Make `fact_participant` incremental once the match count grows.
- Wrap it all in a simple Metabase / Streamlit dashboard for "My LoL Career".

## Legacy / superseded files

- `sql/create_warehouse.sql` — SQLite DDL from the pre-dbt era. Kept for history.
- `src/load/load_to_warehouse.py` — old Python upsert logic; replaced by `load_raw.py` + dbt.
- `src/transform/build_stg_my_games_csv.py` — filter for personal games; dbt handles this now.
- `data/warehouse/lol_dw.sqlite` — safe to delete; new warehouse is `lol_dw.duckdb` in the same folder.
