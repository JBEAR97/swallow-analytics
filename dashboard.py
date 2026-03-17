import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
import numpy as np
from datetime import datetime, timedelta

# DATABASE_URL da Railway Variables (${{Postgres.DATABASE_URL}})
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    st.error("❌ DATABASE_URL mancante. Imposta '${{Postgres.DATABASE_URL}}' in Railway > dashboard > Variables.")
    st.stop()

# Engine con pool_pre_ping (fix timeout Railway/Postgres)
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True, 
    pool_recycle=300,  # Ricicla conn ogni 5min
    echo=False
)

st.set_page_config(page_title="Swallow Analytics", layout="wide")

st.title("Swallow's Notes Analytics")
st.markdown("---")

# Sidebar refresh
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

if st.button("🔄 Refresh (auto 10min)") or (datetime.now() - st.session_state.last_refresh).total_seconds() > 600:
    st.session_state.last_refresh = datetime.now()
    st.rerun()

# Query helper SICURA + cache
@st.cache_data(ttl=600)  # 10min cache
def get_data(days=7):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT 
                    date_trunc('minute', ts_utc::timestamptz) as minute,
                    event_type,
                    COUNT(*) as count,
                    COUNT(DISTINCT page_path) as unique_pages
                FROM "swallow-analysis" 
                WHERE ts_utc >= NOW() - INTERVAL :days days
                GROUP BY 1,2 
                ORDER BY 1 DESC
            """), conn, params={'days': days}, parse_dates=['minute'])
        return df
    except Exception as e:
        st.error(f"❌ Query error: {str(e)}")
        return pd.DataFrame()

# Metrics cards
col1, col2, col3, col4 = st.columns(4)
today = get_data(days=1)
total_views = int(today[today.event_type=='page_view']['count'].sum() or 0)
total_impressions = int(today[today.event_type=='impression']['count'].sum() or 0)
pages = int(today['unique_pages'].sum() or 0)

with col1:
    st.metric("👀 Page Views (24h)", total_views)
with col2:
    st.metric("📖 Impressions (24h)", total_impressions)
with col3:
    st.metric("📄 Pagine Uniche", pages)
with col4:
    st.metric("⏰ Ultimo Update", st.session_state.last_refresh.strftime("%H:%M"))

# Grafici
tab1, tab2, tab3 = st.tabs(["📊 Ultima Ora", "📈 24h", "📅 7 Giorni"])

with tab1:
    df_hour = get_data(days=1/24)
    if not df_hour.empty:
        fig = px.line(df_hour, x='minute', y='count', color='event_type', 
                      title="Traffico per Minuto", markers=True)
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    df_day = get_data(1)
    if not df_day.empty:
        fig2 = px.bar(df_day.groupby(['minute', 'event_type'])['count'].sum().reset_index(),
                      x='minute', y='count', color='event_type', title="24h Dettaglio")
        st.plotly_chart(fig2, use_container_width=True)

with tab3:
    df_week = get_data(7)
    if not df_week.empty:
        fig3 = px.area(df_week, x='minute', y='count', color='event_type',
                       title="Trend Settimanale")
        st.plotly_chart(fig3, use_container_width=True)

# Top pages
st.subheader("🥇 Top Pagine (24h)")
try:
    with engine.connect() as conn:
        df_pages = pd.read_sql(text("""
            SELECT page_path, COUNT(*) as views 
            FROM "swallow-analysis" 
            WHERE ts_utc >= NOW() - INTERVAL '1 day' AND event_type='page_view'
            GROUP BY page_path ORDER BY views DESC LIMIT 10
        """), conn)
    if not df_pages.empty:
        st.bar_chart(df_pages.set_index('page_path')['views'])
except Exception as e:
    st.error(f"Top pages error: {str(e)}")

