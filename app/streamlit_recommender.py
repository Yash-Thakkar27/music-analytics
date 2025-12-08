# app/streamlit_recommender.py
import os
import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity

# ---- CONFIG: edit these or set environment variables ----
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "Tom&Jerry2704")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "music_analytics")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ---- CACHED DB loaders ----
@st.cache_data(ttl=3600)
def load_songs():
    engine = create_engine(DB_URL)
    q = "SELECT song_id, release_year, duration_ms, tempo, valence, energy, danceability FROM songs"
    return pd.read_sql(q, engine)

@st.cache_data(ttl=3600)
def load_listens(nrows=None):
    engine = create_engine(DB_URL)
    if nrows:
        q = text("SELECT user_id, session_id, song_id, ts FROM listens ORDER BY user_id, session_id, ts LIMIT :n")
        return pd.read_sql(q, engine, params={"n": nrows})
    q = "SELECT user_id, session_id, song_id, ts FROM listens ORDER BY user_id, session_id, ts"
    return pd.read_sql(q, engine)

# ---- Recommender helper (robust) ----
FEATURE_COLS = ["tempo", "valence", "energy", "danceability"]
LAST_N = 3
TOP_K = 10

@st.cache_data(ttl=3600)
def build_feature_matrix(songs_df):
    # ensure feature cols exist and numeric
    for col in FEATURE_COLS:
        if col not in songs_df.columns:
            songs_df[col] = np.nan
        songs_df[col] = pd.to_numeric(songs_df[col], errors="coerce")
    songs_clean = songs_df.copy()
    songs_clean[FEATURE_COLS] = songs_clean[FEATURE_COLS].fillna(songs_clean[FEATURE_COLS].mean())
    feat_mat = songs_clean[FEATURE_COLS].values.astype(float)
    scaler = StandardScaler().fit(feat_mat)
    feat_scaled = scaler.transform(feat_mat)
    id_to_idx = {sid: i for i, sid in enumerate(songs_clean["song_id"].values)}
    return songs_clean, feat_mat, feat_scaled, scaler, id_to_idx

def get_last_n_song_ids(df_listens, user_id, session_id, n=LAST_N):
    sess = df_listens[(df_listens["user_id"]==user_id) & (df_listens["session_id"]==session_id)].sort_values("ts")
    return list(sess["song_id"].tail(n))

def session_embedding_from_ids(song_ids, id_to_idx, feat_scaled):
    vecs = []
    for sid in song_ids:
        idx = id_to_idx.get(sid)
        if idx is not None:
            vecs.append(feat_scaled[idx])
    if len(vecs) == 0:
        return None
    return np.mean(vecs, axis=0).reshape(1, -1)

def recommend_for_session_simple(user_id, session_id, songs_clean, feat_scaled, id_to_idx, df_listens, top_k=TOP_K, last_n=LAST_N):
    last = get_last_n_song_ids(df_listens, user_id, session_id, n=last_n)
    if not last:
        return pd.DataFrame()
    sess_vec = session_embedding_from_ids(last, id_to_idx, feat_scaled)
    if sess_vec is None:
        return pd.DataFrame()
    sims = cosine_similarity(sess_vec, feat_scaled).flatten()
    cand = songs_clean.copy()
    cand["sim"] = sims
    played = set(df_listens[(df_listens["user_id"]==user_id)&(df_listens["session_id"]==session_id)]["song_id"].unique())
    cand = cand[~cand["song_id"].isin(played)]
    return cand.sort_values("sim", ascending=False).head(top_k)

# ---- Streamlit UI ----
st.set_page_config(layout="wide", page_title="Session-based Recommender Demo")
st.title("Session-based Recommender (last 3 songs) — Demo")

with st.spinner("Loading data from DB..."):
    songs = load_songs()
    listens = load_listens()

st.sidebar.header("Controls")
user_list = listens[["user_id","session_id"]].drop_duplicates()["user_id"].unique().tolist()
selected_user = st.sidebar.selectbox("Choose user_id", options=sorted(user_list))
# derive sessions for selected user
sessions_for_user = listens[listens["user_id"]==selected_user]["session_id"].unique().tolist()
selected_session = st.sidebar.selectbox("Choose session_id", options=sorted(sessions_for_user))

st.sidebar.markdown("**Parameters**")
last_n = st.sidebar.slider("Last N songs for session embedding", 1, 5, value=3)
top_k = st.sidebar.slider("Top K recommendations", 5, 20, value=10)

# Build features
songs_clean, feat_mat, feat_scaled, scaler, id_to_idx = build_feature_matrix(songs)

# Show last 3 listens
st.subheader("Last N listens for this session")
last_n_ids = get_last_n_song_ids(listens, selected_user, selected_session, n=last_n)
last_n_df = listens[(listens["user_id"]==selected_user)&(listens["session_id"]==selected_session)].sort_values("ts").tail(last_n)
# join features for display
last_n_enriched = last_n_df.merge(songs_clean[["song_id"]+FEATURE_COLS], on="song_id", how="left")
st.dataframe(last_n_enriched.rename(columns={"ts":"timestamp"}).reset_index(drop=True))

# Generate recommendations
st.subheader("Top recommendations")
recs = recommend_for_session_simple(selected_user, selected_session, songs_clean, feat_scaled, id_to_idx, listens, top_k=top_k, last_n=last_n)
if recs.empty:
    st.write("No recommendations for this session (not enough data).")
else:
    # show relevant columns
    display_df = recs[["song_id","sim"]+FEATURE_COLS+["release_year","duration_ms"]].reset_index(drop=True)
    st.dataframe(display_df)
    st.markdown("### Why these were chosen")
    st.markdown("- Recommendations are ranked by cosine similarity between the *session embedding* (average of last N songs' feature vectors) and each song's audio features.")
    st.markdown("- Songs already played in this session are filtered out.")
    st.markdown("- You can adjust *N* to make the recommender react faster (small N) or be more stable (large N).")

st.sidebar.markdown("---")