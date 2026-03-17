import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
import numpy as np
from datetime import datetime, timedelta
import time  # ← AGGIUNTO per retry

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

# Query helper RETRY + cache (← FIX PRINCIPALE)
@st.cache_data(ttl=600)  # 10min cache
def get_data(days=7, max_retries=3):
    for attempt in range(max_retries):
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
                """), conn, params={'days': str(days)}, parse_dates=['minute'])
                st.success(f"✅ Dati OK: {len(df)} righe, {df['event_type'].nunique()} eventi")
                return df
        except Exception as e:
            if attempt < max_retries - 1:
                st.warning(f"🔄 DB retry {attempt+1}/3...")
                time.sleep(1)
                continue
            st.error(f"❌ DB fallito: {str(e)}")
            # DATI DEMO per test UI
            return pd.DataFrame({
                'minute': pd.date_range(end=datetime.now(), periods=20, freq='10min'),
                'event_type': ['page_view']*12 + ['impression']*8,
                'count': np.random.randint(1, 50, 20),
                'unique_pages': np.random.randint(1, 3, 20)
            })

# Metrics cards
today = get_data(days=1)
col1, col2, col3, col4 = st.columns(4)

if not today.empty and 'event_type' in today.columns:
    total_views = int(today[today['event_type'] == 'page_view']['count'].sum() or 0)
    total_impressions = int(today[today['event_type'] == 'impression']['count'].sum() or 0)
    pages = int(today['unique_pages'].sum() or 0)
else:
    total_views = total_impressions = pages = 0

with col1: st.metric("👀 Page Views (24h)", total_views)
with col2: st.metric("📖 Impressions (24h)", total_impressions)
with col3: st.metric("📄 Pagine Uniche", pages)
with col4: st.metric("⏰ Ultimo Update", st.session_state.last_refresh.strftime("%H:%M"))

# Debug (opzionale)
if st.checkbox("🔍 Debug dati"):
    st.write("**today shape:**", today.shape)
    st.write("**event_types:**", sorted(today['event_type'].unique()))
    st.dataframe(today.head(10))

# Grafici
tab1, tab2, tab3 = st.tabs(["📊 Ultima Ora", "📈 24h", "📅 7 Giorni"])

with tab1:
    df_hour = get_data(days=1/24)
    if not df_hour.empty:
        fig = px.line(df_hour, x='minute', y='count', color='event_type', 
                     title="Traffico per Minuto", markers=True)
        st.plotly_chart(fig, width='stretch')

with tab2:
    df_day = get_data(1)
    if not df_day.empty:
        fig2 = px.bar(df_day, x='minute', y='count', color='event_type', title="24h Dettaglio")
        st.plotly_chart(fig2, width='stretch')

with tab3:
    df_week = get_data(7)
    if not df_week.empty:
        fig3 = px.area(df_week, x='minute', y='count', color='event_type', title="Trend Settimanale")
        st.plotly_chart(fig3, width='stretch')

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
    else:
        st.info("📭 Nessuna page_view nelle 24h")
except:
    st.info("📭 Top pages temporaneamente non disponibile")

