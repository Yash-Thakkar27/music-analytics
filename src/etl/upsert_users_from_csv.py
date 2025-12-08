# src/etl/upsert_users_from_csv.py
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

CSV = "data_raw/data_final.csv"
CHUNKSIZE = 100000   # adjust down if memory is low

DB_USER = "postgres"
DB_PASS = "Tom&Jerry2704"   # <-- replace with your password if different
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "music_analytics"

INSERT_SQL = """
INSERT INTO users (user_id)
VALUES %s
ON CONFLICT (user_id) DO NOTHING;
"""

def make_conn():
    dsn = f"dbname={DB_NAME} user={DB_USER} password={DB_PASS} host={DB_HOST} port={DB_PORT}"
    return psycopg2.connect(dsn)

def main():
    seen = set()
    rows_to_insert = []
    total_new = 0
    chunk_i = 0

    for chunk in pd.read_csv(CSV, usecols=['session_id'], chunksize=CHUNKSIZE, low_memory=False):
        chunk_i += 1
        # drop NA and duplicates within chunk
        chunk = chunk.dropna(subset=['session_id'])
        unique_ids = chunk['session_id'].astype(str).unique()
        batch = [uid for uid in unique_ids if uid not in seen]
        if not batch:
            print(f"Chunk {chunk_i}: no new users")
            continue
        # add to seen set and prepare tuples
        for uid in batch:
            seen.add(uid)
            rows_to_insert.append((uid,))
        total_new += len(batch)
        print(f"Chunk {chunk_i}: discovered {len(batch)} new users (total new so far {total_new})")

        # flush in batches to DB to avoid huge memory usage
        if len(rows_to_insert) >= 50000:
            conn = make_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        execute_values(cur, INSERT_SQL, rows_to_insert, page_size=1000)
                print(f"Flushed {len(rows_to_insert)} users to DB")
            finally:
                conn.close()
            rows_to_insert = []

    # flush remaining
    if rows_to_insert:
        conn = make_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, INSERT_SQL, rows_to_insert, page_size=1000)
            print(f"Flushed final {len(rows_to_insert)} users to DB")
        finally:
            conn.close()

    print("Done. Total unique users discovered:", len(seen))

if __name__ == "__main__":
    main()
