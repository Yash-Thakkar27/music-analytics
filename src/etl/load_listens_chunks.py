# src/etl/load_listens_chunked.py
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import math

# === DB config - update if different ===
DB_USER = "postgres"
DB_PASS = "Tom&Jerry2704"   # <- your password
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "music_analytics"

CSV = "data_raw/data_final.csv"
CHUNK = 50000   # start small (50000). Lower to 10000 if memory issues.

# columns expected in CSV (we checked these exist)
# We'll select only the columns we need for the listens table
USE_COLS = [
    "session_id", "session_position", "session_length", "track_id",
    "skip_1", "skip_2", "skip_3", "not_skipped",
    "context_switch", "hour_of_day", "date", "premium", "context_type",
    "duration", "session_day", "session_month"
]

INSERT_SQL = """
INSERT INTO listens (
    user_id, song_id, ts, played_ms, event_type, skipped, session_id
) VALUES %s
"""

def make_db_conn():
    dsn = f"dbname={DB_NAME} user={DB_USER} password={DB_PASS} host={DB_HOST} port={DB_PORT}"
    return psycopg2.connect(dsn)

def prepare_chunk(df):
    # keep columns we care about
    # note: we previously verified these columns exist
    df = df.copy()

    # timestamp: date + hour_of_day
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['hour_of_day'] = pd.to_numeric(df.get('hour_of_day', 0), errors='coerce').fillna(0)
    df['ts'] = df['date'] + pd.to_timedelta(df['hour_of_day'], unit='h')

    # user_id and session_id both come from session_id in your CSV
    df['user_id'] = df['session_id'].astype(str)
    df['session_id'] = df['session_id'].astype(str)

    # song_id from track_id
    df['song_id'] = df['track_id'].astype(str)

    # played_ms: we try to infer units robustly
    # if `duration` exists:
    if 'duration' in df.columns:
        # convert to numeric
        df['duration_num'] = pd.to_numeric(df['duration'], errors='coerce')
        # heuristic: if values > 1000 assume already milliseconds, else treat as seconds
        df['played_ms'] = df['duration_num'].apply(
            lambda x: int(x) if pd.isna(x) else (int(x) if x > 1000 else int(x * 1000))
        )
    elif 'track_duration_mins' in df.columns:
        df['played_ms'] = (pd.to_numeric(df['track_duration_mins'], errors='coerce').fillna(0) * 60 * 1000).astype('Int64')
    else:
        df['played_ms'] = None

    # skipped: prefer explicit not_skipped, else fallback to skip_1/2/3
    if 'not_skipped' in df.columns:
        # if not_skipped == 1 -> not skipped (False); 0 -> skipped (True)
        df['skipped'] = df['not_skipped'].apply(lambda x: False if pd.notnull(x) and int(x) == 1 else True)
    else:
        # if any skip_* == 1 -> skipped True, else False
        def infer_skip(row):
            for c in ('skip_1','skip_2','skip_3'):
                if c in row.index and pd.notnull(row[c]) and int(row[c]) == 1:
                    return True
            return False
        df['skipped'] = df.apply(infer_skip, axis=1)

    # event_type: set a default 'play' (you can adjust later)
    df['event_type'] = 'play'

    # final dataframe matching your DB columns:
    final_cols = ['user_id', 'song_id', 'ts', 'played_ms', 'event_type', 'skipped', 'session_id']
    df_final = df[final_cols].copy()

    # convert NaN -> None for psycopg2
    df_final = df_final.where(pd.notnull(df_final), None)

    return df_final

def insert_rows(rows):
    conn = make_db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # execute_values will expand the tuples into the VALUES clause
                execute_values(cur, INSERT_SQL, rows, page_size=1000)
    finally:
        conn.close()

def main():
    total = 0
    chunk_num = 0
    for chunk in pd.read_csv(CSV, chunksize=CHUNK, low_memory=False):
        chunk_num += 1
        print(f"Preparing chunk {chunk_num} (rows in chunk: {len(chunk)}) ...")
        df_p = prepare_chunk(chunk)
        rows = [tuple(x) for x in df_p.to_numpy()]
        if not rows:
            print(f"Chunk {chunk_num} had 0 rows after prepare. Skipping.")
            continue
        print(f"Inserting chunk {chunk_num} rows: {len(rows)} ...")
        insert_rows(rows)
        total += len(rows)
        print(f"Inserted chunk {chunk_num}. Total rows inserted so far: {total}")
    print("Done loading listens table. Total inserted:", total)

if __name__ == "__main__":
    main()
