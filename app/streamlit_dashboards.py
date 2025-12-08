# app/streamlit_dashboards.py
import os
import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import plotly.express as px
from sklearn.cluster import KMeans
from datetime import timedelta

# ---- DB config (env override) ----
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "Tom&Jerry2704")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "music_analytics")
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@st.cache_data(ttl=600)
def load_listens(limit=None):
    engine = create_engine(DB_URL)
    if limit:
        q = text("SELECT user_id, session_id, song_id, ts, played_ms, skipped FROM listens ORDER BY ts LIMIT :n")
        return pd.read_sql(q, engine, params={"n": limit})
    q = "SELECT user_id, session_id, song_id, ts, played_ms, skipped FROM listens"
    return pd.read_sql(q, engine)

@st.cache_data(ttl=600)
def load_songs():
    engine = create_engine(DB_URL)
    q = "SELECT song_id, release_year, duration_ms, tempo, valence, energy, danceability FROM songs"
    return pd.read_sql(q, engine)

@st.cache_data(ttl=600)
def load_session_summary():
    engine = create_engine(DB_URL)
    q = """
    SELECT user_id, session_id, MIN(ts) AS session_start, MAX(ts) AS session_end,
           EXTRACT(EPOCH FROM (MAX(ts)-MIN(ts)))/60.0 AS session_duration_min,
           COUNT(*) AS num_events, SUM(CASE WHEN skipped THEN 1 ELSE 0 END) AS num_skipped
    FROM listens
    GROUP BY user_id, session_id
    """
    return pd.read_sql(q, engine)

# ---- load data (small preview by default) ----
st.set_page_config(layout="wide", page_title="Music Analytics Dashboards")
st.title("Music Analytics — Dashboards")

with st.spinner("Loading data..."):
    listens = load_listens()       # remove limit to load full data
    songs = load_songs()
    sessions = load_session_summary()

# Basic KPIs
st.header("Overview KPIs")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total users", listens["user_id"].nunique())
col2.metric("Total songs", songs["song_id"].nunique())
col3.metric("Total listens", len(listens))
avg_skip_rate = listens["skipped"].mean()
col4.metric("Avg skip rate", f"{avg_skip_rate:.2%}")

# Time series: daily listens and skip rate
st.subheader("Time series: daily listens & skip rate")
listens['ts_date'] = pd.to_datetime(listens['ts']).dt.date
daily = listens.groupby('ts_date').agg(
    listens=('song_id','count'),
    skip_rate=('skipped','mean'),
    dau=('user_id', lambda x: x.nunique())
).reset_index().sort_values('ts_date')

date_range = st.date_input("Date range", [daily['ts_date'].min(), daily['ts_date'].max()])
mask = (daily['ts_date'] >= date_range[0]) & (daily['ts_date'] <= date_range[1])
fig1 = px.line(daily[mask], x='ts_date', y='listens', title='Daily listens')
fig2 = px.line(daily[mask], x='ts_date', y='skip_rate', title='Daily skip rate')
# --- Export: daily listens CSV + PNG ---
csv_daily = daily[mask].to_csv(index=False).encode('utf-8')
st.download_button(
    "Download daily data (CSV)",
    data=csv_daily,
    file_name="daily_listens.csv",
    mime="text/csv",
    key="dl_daily_csv_1"
)

# PNG export for the first figure (requires kaleido)
try:
    png_bytes = fig1.to_image(format="png", scale=2)
    st.download_button(
        "Download daily listens chart (PNG)",
        data=png_bytes,
        file_name="daily_listens.png",
        mime="image/png",
        key="dl_daily_png_1"
    )
except Exception as e:
    st.warning("PNG export not available (install kaleido).")

csv_daily = daily[mask].to_csv(index=False).encode('utf-8')
st.download_button(
    "Download daily skip_rate data (CSV)",
    data=csv_daily,
    file_name="daily_skip_rate.csv",
    mime="text/csv",
    key="dl_daily_csv_2"
)

# PNG export for the second figure (requires kaleido)
try:
    png_bytes = fig2.to_image(format="png", scale=2)
    st.download_button(
        "Download daily skip rate chart (PNG)",
        data=png_bytes,
        file_name="daily_skip_rate.png",
        mime="image/png",
        key="dl_daily_png_2"
    )
except Exception as e:
    st.warning("PNG export not available (install kaleido).")

# DAU & MAU
st.subheader("DAU and MAU")
daily_active = daily[['ts_date','dau']].rename(columns={'ts_date':'date','dau':'dau'})
daily_active['month'] = pd.to_datetime(daily_active['date']).dt.to_period('M').dt.to_timestamp()
mau = daily_active.groupby('month')['dau'].sum().reset_index().rename(columns={'dau':'mau_est'})
fig_dau = px.line(daily_active, x='date', y='dau', title='Daily Active Users (DAU)')
fig_mau = px.line(mau, x='month', y='mau_est', title='Monthly Active Users (MAU estimate)')
st.plotly_chart(fig_dau, use_container_width=True)
st.plotly_chart(fig_mau, use_container_width=True)

# Top songs and skip rates
st.subheader("Top songs by plays")
top_n = st.slider("Top N songs", 5, 50, 20)
plays = listens.groupby('song_id').agg(plays=('song_id','count'), skip_rate=('skipped','mean')).reset_index()
top = plays.sort_values('plays', ascending=False).head(top_n)
top = top.merge(songs[['song_id','release_year','tempo','valence','energy','danceability']], on='song_id', how='left')
fig_top = px.bar(top, x='song_id', y='plays', hover_data=['skip_rate','tempo','valence'], title=f"Top {top_n} songs by plays")
st.plotly_chart(fig_top, use_container_width=True)
st.dataframe(top[['song_id','plays','skip_rate','tempo','valence','energy','danceability']].reset_index(drop=True))
# Top songs CSV
csv_top = top[['song_id','plays','skip_rate','tempo','valence','energy','danceability']].to_csv(index=False).encode('utf-8')
st.download_button("Download top songs (CSV)", data=csv_top, file_name="top_songs.csv", mime="text/csv", key="dl_top_csv")

# Top songs chart PNG
try:
    png_top = fig_top.to_image(format="png", scale=2)
    st.download_button("Download top songs chart (PNG)", data=png_top, file_name="top_songs.png", mime="image/png", key="dl_top_png")
except Exception:
    st.warning("Top songs PNG export not available (install kaleido).")


# Session distribution
st.subheader("Session duration distribution")
fig_hist = px.histogram(sessions, x='session_duration_min', nbins=50, title='Session duration (minutes)')
st.plotly_chart(fig_hist, use_container_width=True)
csv_sessions = sessions.to_csv(index=False).encode('utf-8')
st.download_button("Download sessions (CSV)", data=csv_sessions, file_name="sessions.csv", mime="text/csv", key="dl_sessions_csv")

try:
    png_hist = fig_hist.to_image(format="png", scale=2)
    st.download_button("Download session durations (PNG)", data=png_hist, file_name="session_duration_hist.png", mime="image/png", key="dl_hist_png")
except Exception:
    st.warning("Session histogram PNG export not available (install kaleido).")


# Skip vs tempo scatter
st.subheader("Skip vs Tempo")
# compute per-song skip rate to plot aggregated relationship
song_stats = listens.merge(songs[['song_id','tempo','valence','energy','danceability']], on='song_id', how='left')
song_summary = song_stats.groupby('song_id').agg(
    plays=('song_id','count'),
    skip_rate=('skipped','mean'),
    tempo=('tempo','mean')
).reset_index()
fig_scatter = px.scatter(song_summary, x='tempo', y='skip_rate', size='plays', hover_data=['song_id'], title='Per-song skip rate vs tempo')
st.plotly_chart(fig_scatter, use_container_width=True)
# CSV for song_summary (used in scatter)
csv_scatter = song_summary.to_csv(index=False).encode('utf-8')
st.download_button("Download song summary (CSV)", data=csv_scatter, file_name="song_summary.csv", mime="text/csv", key="dl_scatter_csv")

# PNG export of scatter
try:
    png_scatter = fig_scatter.to_image(format="png", scale=2)
    st.download_button("Download skip vs tempo chart (PNG)", data=png_scatter, file_name="skip_vs_tempo.png", mime="image/png", key="dl_scatter_png")
except Exception:
    st.warning("Scatter PNG export not available (install kaleido).")


# User segmentation (optional)
st.subheader("User segmentation (k-means on user features)")
if st.checkbox("Run user clustering (may be slow)", value=False):
    # compute user features quickly
    user_feats = listens.groupby('user_id').agg(
        num_sessions=('session_id','nunique'),
        total_listens=('song_id','count'),
        avg_skip_rate=('skipped','mean'),
        avg_played_ms=('played_ms','mean')
    ).reset_index().fillna(0)
    K = st.slider("k clusters", 2, 10, 4)
    km = KMeans(n_clusters=K, random_state=42).fit(user_feats[['avg_skip_rate','avg_played_ms']])
    user_feats['cluster'] = km.labels_
    fig_clusters = px.scatter(user_feats, x='avg_played_ms', y='avg_skip_rate', color='cluster', hover_data=['user_id'], title='User segments')
    st.plotly_chart(fig_clusters, use_container_width=True)
    st.dataframe(user_feats.head(50))
    csv_users = user_feats.to_csv(index=False).encode('utf-8')
    st.download_button("Download user segments (CSV)", data=csv_users, file_name="user_segments.csv", mime="text/csv", key="dl_users_csv")


st.markdown("---")
