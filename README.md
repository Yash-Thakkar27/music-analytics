# Music Analytics (Spotify Listening History)

Streamlit dashboards and a simple session-based recommender built on top of a Postgres warehouse populated from the Spotify listening history CSV. ETL scripts load the raw CSV into `songs`, `users`, and `listens` tables; the dashboards query that database for KPIs, exports, and recommendation demos.

## Features
- Streamlit dashboards: KPIs, daily listens/skip-rate trends, DAU/MAU, top songs, session duration distribution, skip vs tempo scatter, optional KMeans user segments.
- Session-based recommender: averages the last _N_ songs in a session, ranks candidates by cosine similarity on tempo/valence/energy/danceability, and filters out already-played tracks.
- Data exports: most charts include CSV downloads; PNG downloads available when `kaleido` is installed.
- ETL utilities: chunked loaders to populate `songs`, `users`, and `listens` from `data_raw/data_final.csv` with basic cleaning and type handling.
- Notebooks: exploratory analysis and sessionization prototypes in `notebooks/`.

## Repository Layout
- `app/` – Streamlit apps (`streamlit_dashboards.py`, `streamlit_recommender.py`).
- `src/etl/` – CSV → Postgres loaders (`load_songs_upsert.py`, `load_listens_chunks.py`, `load_main_csv.py`, `upsert_users_from_csv.py`).
- `data_raw/` – expected location for `data_final.csv` (tracked with Git LFS).
- `data_processed/` – placeholder for derived outputs.
- `notebooks/` – exploration and sessionization notebooks.
- `docs/`, `dashboards/`, `sql/` – placeholders for documentation, exported dashboards, or SQL schema (currently empty).

## Prerequisites
- Python 3.10+.
- PostgreSQL instance accessible to the app (default connection points to `localhost:5432`).
- `data_raw/data_final.csv` placed locally (stored via Git LFS in this repo).

## Installation
```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuration
Set database connection via environment variables (override the insecure defaults in code):
- `DB_USER` (default: `postgres`)
- `DB_PASS` (default: `Tom&Jerry2704`)
- `DB_HOST` (default: `localhost`)
- `DB_PORT` (default: `5432`)
- `DB_NAME` (default: `music_analytics`)

Example `.env` (PowerShell):
```bash
setx DB_USER "postgres"
setx DB_PASS "change-me"
setx DB_HOST "localhost"
setx DB_PORT "5432"
setx DB_NAME "music_analytics"
```
Restart the shell after running `setx` so Streamlit picks up the variables.

## Loading the Database
Create the target Postgres database first (e.g., `createdb music_analytics`). Then ingest the CSV:

1) Upsert songs (unique `track_id` rows):
```bash
python src/etl/load_songs_upsert.py
```

2) Upsert users (unique `session_id`):
```bash
python src/etl/upsert_users_from_csv.py
```

3) Load listens in chunks (timestamp derivation and skip inference included):
```bash
python src/etl/load_listens_chunks.py
```

Notes:
- All scripts read `data_raw/data_final.csv`; adjust `CHUNKSIZE` constants if memory is tight.
- `load_main_csv.py` is an alternative end-to-end loader that writes songs, users, and listens via `pandas.to_sql`.
- The loaders expect the CSV columns used in the scripts (e.g., `track_id`, `session_id`, `date`, `hour_of_day`, `duration`, `not_skipped`, `skip_1/2/3`, audio features).

## Running the Apps
Activate your virtualenv, ensure the database is populated, then start Streamlit:
```bash
streamlit run app/streamlit_dashboards.py
```
```bash
streamlit run app/streamlit_recommender.py
```

Dashboards will open in the browser (default `http://localhost:8501`).

## Notebooks
Use Jupyter or VS Code to open the notebooks in `notebooks/` for exploration (`01_exploration.ipynb`, `03_sessionization.ipynb`, etc.). They rely on the same CSV and may expect the virtualenv kernel.

## Troubleshooting
- PNG exports require `kaleido` (`pip install kaleido`).
- If you see connection errors, verify Postgres is running and the `DB_*` env vars match your instance.
- If inserts fail due to duplicates, rerun with the upsert-based scripts (`load_songs_upsert.py`, `upsert_users_from_csv.py`).