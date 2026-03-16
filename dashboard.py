import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

# DATABASE_URL da Railway
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

st.set_page_config(page_title="Swallow Analytics", layout="wide")

st.title("🦌 Swallow's Notes Analytics")
st.markdown("---")

# Sidebar refresh
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

if st.button("🔄 Refresh (auto 10min)") or (datetime.now() - st.session_state.last_refresh).total_seconds() > 600:
    st.session_state.last_refresh = datetime.now()
    st.rerun()

# Query helper
@st.cache_data(ttl=600)  # Cache 10min
def get_data(days=7):
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT 
                date_trunc('minute', ts_utc::timestamptz) as minute,
                event_type,
                COUNT(*) as count,
                COUNT(DISTINCT page_path) as unique_pages
            FROM "swallow-analysis" 
            WHERE ts_utc >= NOW() - INTERVAL '{} days'::interval
            GROUP BY 1,2 
            ORDER BY 1 DESC
        """.format(days), conn)
    return df

# Metrics cards
col1, col2, col3, col4 = st.columns(4)
today = get_data(1)
total_views = today[today.event_type=='page_view']['count'].sum()
total_impressions = today[today.event_type=='impression']['count'].sum()
pages = today['unique_pages'].nunique()
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
    fig = px.line(df_hour, x='minute', y='count', color='event_type', 
                  title="Traffico per Minuto", markers=True)
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    df_day = get_data(1)
    fig2 = px.bar(df_day.groupby(['minute', 'event_type'])['count'].sum().reset_index(),
                  x='minute', y='count', color='event_type', title="24h Dettaglio")
    st.plotly_chart(fig2, use_container_width=True)

with tab3:
    df_week = get_data(7)
    fig3 = px.area(df_week, x='minute', y='count', color='event_type',
                   title="Trend Settimanale")
    st.plotly_chart(fig3, use_container_width=True)

# Top pages
st.subheader("🥇 Top Pagine (24h)")
df_pages = pd.read_sql("""
    SELECT page_path, COUNT(*) as views 
    FROM "swallow-analysis" 
    WHERE ts_utc >= NOW() - INTERVAL '1 day' AND event_type='page_view'
    GROUP BY page_path ORDER BY views DESC LIMIT 10
""", engine)
st.bar_chart(df_pages.set_index('page_path')['views'])
