# src/etl/load_main_csv.py
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

DB_USER = "postgres"
DB_PASS = "Tom&Jerry2704"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "music_analytics"
connection_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_url, pool_size=5, max_overflow=10)

CSV = "data_raw/data_final.csv"
CHUNKSIZE = 5000  # reduce if memory constrained

def prepare_chunk(df):
    # parse dates and timestamp
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['ts'] = df['date'] + pd.to_timedelta(df['hour_of_day'].fillna(0).astype(float), unit='h')

    # derive skip boolean from not_skipped (adjust if column type differs)
    if 'not_skipped' in df.columns:
        df['skipped'] = (~df['not_skipped'].astype(bool)).astype(int)
    else:
        # fallback: consider skip_1/2/3
        df['skipped'] = ((df.get('skip_1',0)==1) | (df.get('skip_2',0)==1) | (df.get('skip_3',0)==1)).astype(int)

    # map columns we will write to listens table
    listens = df[[
        'session_id','track_id','ts','session_position','session_length',
        'skipped','context_type','premium','context_switch','session_day','session_month',
        'release_year','us_popularity_estimate','track_duration_mins'
    ]].copy()

    # rename to match DB
    listens = listens.rename(columns={
        'session_id':'user_id',
        'track_id':'song_id',
        'track_duration_mins':'duration_mins'
    })

    # ensure types
    listens['session_position'] = listens['session_position'].astype('Int64', errors='ignore')
    listens['session_length'] = listens['session_length'].astype('Int64', errors='ignore')

    return df, listens

def load():
    # 1) extract songs metadata (unique)
    # We'll do a first pass to collect unique song rows (small memory)
    song_cols = ['track_id','release_year','duration','track_duration_mins',
                 'acousticness','danceability','energy','instrumentalness',
                 'liveness','loudness','tempo','valence']  # extend as needed

    song_seen = set()
    songs_rows = []

    # stream through CSV once to capture unique songs and small sample of users
    for chunk in pd.read_csv(CSV, chunksize=CHUNKSIZE, low_memory=False):
        # collect unique tracks
        for _, r in chunk.drop_duplicates(subset=['track_id'])[song_cols].dropna(subset=['track_id']).iterrows():
            tid = r['track_id']
            if tid not in song_seen:
                song_seen.add(tid)
                songs_rows.append({
                    'song_id': r['track_id'],
                    'release_year': r.get('release_year'),
                    'duration_ms': int(r.get('duration') or (r.get('track_duration_mins')*60000) if pd.notnull(r.get('track_duration_mins')) else None),
                    'acousticness': r.get('acousticness'),
                    'danceability': r.get('danceability'),
                    'energy': r.get('energy'),
                    'instrumentalness': r.get('instrumentalness'),
                    'liveness': r.get('liveness'),
                    'loudness': r.get('loudness'),
                    'tempo': r.get('tempo'),
                    'valence': r.get('valence')
                })

    songs_df = pd.DataFrame(songs_rows)
    print("Unique songs:", len(songs_df))
    songs_df.to_sql('songs', engine, if_exists='append', index=False, method='multi')

    # 2) stream again to load listens and users
    users_seen = set()
    for chunk in pd.read_csv(CSV, chunksize=CHUNKSIZE, low_memory=False):
        orig, listens_df = prepare_chunk(chunk)

        # load users table: unique session_id in this chunk
        users = listens_df['user_id'].drop_duplicates().to_frame(name='user_id')
        # optional: create minimal columns; to_sql with if_exists='append' and handle conflicts in Postgres
        # We'll write users, but avoid duplicates using ON CONFLICT in a raw query later if needed
        try:
            users.to_sql('users', engine, if_exists='append', index=False, method='multi')
        except Exception as e:
            # duplicates may cause issues; ignore and continue (or implement upsert)
            print("Users insert warning:", e)

        # write listens chunk
        listens_df.to_sql('listens', engine, if_exists='append', index=False, method='multi')
        print("Inserted chunk, rows:", len(listens_df))

if __name__ == "__main__":
    load()
