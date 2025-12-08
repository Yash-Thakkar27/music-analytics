# src/etl/load_songs_upsert.py
import csv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

# DB config - update if needed
DB_USER = "postgres"
DB_PASS = "Tom&Jerry2704"     # <- replace with your password
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "music_analytics"

CSV = "data_raw/data_final.csv"
CHUNKSIZE = 10000   # small safe chunk for songs pass

# canonical columns we want to insert into songs table
song_cols = [
    'track_id','release_year','duration','track_duration_mins',
    'acousticness','danceability','energy','instrumentalness',
    'liveness','loudness','tempo','valence','us_popularity_estimate'
]

def row_to_tuple(r):
    # convert pandas Series r into tuple matching DB columns order below
    # handle missing gracefully
    duration_ms = None
    if pd.notnull(r.get('duration')):
        try:
            duration_ms = int(float(r.get('duration')))
        except:
            duration_ms = None
    elif pd.notnull(r.get('track_duration_mins')):
        try:
            duration_ms = int(float(r.get('track_duration_mins')) * 60000)
        except:
            duration_ms = None

    return (
        r.get('track_id'),
        int(r.get('release_year')) if pd.notnull(r.get('release_year')) else None,
        duration_ms,
        float(r.get('acousticness')) if pd.notnull(r.get('acousticness')) else None,
        float(r.get('danceability')) if pd.notnull(r.get('danceability')) else None,
        float(r.get('energy')) if pd.notnull(r.get('energy')) else None,
        float(r.get('instrumentalness')) if pd.notnull(r.get('instrumentalness')) else None,
        float(r.get('liveness')) if pd.notnull(r.get('liveness')) else None,
        float(r.get('loudness')) if pd.notnull(r.get('loudness')) else None,
        float(r.get('tempo')) if pd.notnull(r.get('tempo')) else None,
        float(r.get('valence')) if pd.notnull(r.get('valence')) else None,
        int(r.get('us_popularity_estimate')) if pd.notnull(r.get('us_popularity_estimate')) else None
    )

# target DB columns (must match the tuple order above)
db_columns = [
    'song_id','release_year','duration_ms',
    'acousticness','danceability','energy','instrumentalness',
    'liveness','loudness','tempo','valence','popularity'
]

insert_sql = f"""
INSERT INTO songs ({', '.join(db_columns)})
VALUES %s
ON CONFLICT (song_id) DO NOTHING;
"""

def main():
    # collect unique songs in a dict to avoid memory explosion
    seen = set()
    rows_to_insert = []

    # read CSV in chunks and collect unique track rows
    for chunk in pd.read_csv(CSV, chunksize=CHUNKSIZE, low_memory=False):
        # keep only rows that have a track_id
        if 'track_id' not in chunk.columns:
            raise SystemExit("CSV does not contain 'track_id' column")
        for _, r in chunk.iterrows():
            tid = r.get('track_id')
            if pd.isna(tid):
                continue
            if tid in seen:
                continue
            seen.add(tid)
            rows_to_insert.append(row_to_tuple(r))

    print(f"Unique songs discovered: {len(rows_to_insert)}")

    # connect and bulk insert using execute_values
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    try:
        with conn:
            with conn.cursor() as cur:
                # execute_values handles parameter escaping and is fast for bulk inserts
                execute_values(cur, insert_sql, rows_to_insert, page_size=1000)
                print("Inserted songs via upsert (ON CONFLICT DO NOTHING).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
