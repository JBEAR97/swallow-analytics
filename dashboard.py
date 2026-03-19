import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
import numpy as np
from datetime import datetime, timedelta
import time
import urllib.parse

def build_database_url():
    """Build DATABASE_URL da env vars Railway Postgres o fallback"""
    pg_host = os.getenv('PGHOST', 'postgres.railway.internal')
    pg_port = os.getenv('PGPORT', '5432')
    pg_user = os.getenv('PGUSER', os.getenv('USER', 'postgres'))
    pg_password = os.getenv('PGPASSWORD')
    pg_database = os.getenv('PGDATABASE', 'railway')
    
    if all([pg_host, pg_port, pg_user, pg_password, pg_database]):
        encoded_password = urllib.parse.quote_plus(pg_password)
        return f"postgresql://{pg_user}:{encoded_password}@{pg_host}:{pg_port}/{pg_database}"
    
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url
    
    return None

# DATABASE_URL dinamica
DATABASE_URL = build_database_url()
if not DATABASE_URL:
    st.error("❌ DATABASE_URL non trovata.\n\n**Railway:** `${{mainline.DATABASE_URL}}`\n**Locale:** PGHOST=mainline.proxy.rlwy.net:50632 etc.")
    st.stop()
else:
    st.success(f"✅ DATABASE_URL: {DATABASE_URL[:40]}...")

engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True, 
    pool_recycle=300,
    echo=False
)

st.set_page_config(page_title="Swallow Analytics", layout="wide")

st.title("Swallow's Notes Analytics")
st.markdown("---")

if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

if st.button("🔄 Refresh (auto 10min)") or (datetime.now() - st.session_state.last_refresh).total_seconds() > 600:
    st.session_state.last_refresh = datetime.now()
    st.rerun()

# 🔧 FUNZIONE get_data esistente
@st.cache_data(ttl=600)
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
                    WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
                    GROUP BY 1,2 
                    ORDER BY 1 DESC
                """), conn, params={'days': float(days)}, parse_dates=['minute'])
                st.success(f"✅ Dati OK: {len(df)} righe, {df['event_type'].nunique()} eventi")
                return df
        except Exception as e:
            if attempt < max_retries - 1:
                st.warning(f"🔄 DB retry {attempt+1}/3...")
                time.sleep(1)
                continue
            st.error(f"❌ DB fallito: {str(e)}")
            return pd.DataFrame({
                'minute': pd.date_range(end=datetime.now(), periods=20, freq='10min'),
                'event_type': ['page_view']*12 + ['impression']*8,
                'count': np.random.randint(1, 50, 20),
                'unique_pages': np.random.randint(1, 3, 20)
            })

# 🌍 NUOVA: Funzione GeoIP
@st.cache_data(ttl=600)
def get_countries(days):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT country_code, COUNT(*) as count
                FROM "swallow-analysis" 
                WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
                GROUP BY country_code
                ORDER BY count DESC
                LIMIT 10
            """), conn, params={'days': float(days)})
            return df
    except:
        return pd.DataFrame()

# Metriche principali
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

# 🌍 GEOIP - Top Paesi + Mappa
st.subheader("🌍 GeoIP: Paesi Visitatori")
time_filter = st.selectbox("Periodo:", ["1h", "24h", "7d"], index=1)
days_map = {'1h': 1/24, '24h': 1, '7d': 7}[time_filter]

df_countries = get_countries(days_map)

col_geo1, col_geo2 = st.columns(2)
with col_geo1:
    st.metric("Paesi Unici", df_countries['country_code'].nunique() if not df_countries.empty else 0)
with col_geo2:
    top_country = df_countries.iloc[0]['country_code'] if not df_countries.empty else 'ZZ'
    top_count = df_countries.iloc[0]['count'] if not df_countries.empty else 0
    st.metric("🥇 Top Paese", f"{top_country} ({top_count})")

if not df_countries.empty:
    # Bar Chart
    fig_bar = px.bar(
        df_countries.head(10), x='count', y='country_code', orientation='h',
        title=f"Top 10 Paesi ({time_filter})",
        color='count', color_continuous_scale='Viridis'
    )
    fig_bar.update_layout(yaxis={'categoryorder':'total descending'})
    fig_bar.update_traces(texttemplate='%{x}', textposition='outside')
    st.plotly_chart(fig_bar, use_container_width=True)
    
    # Mappa Choropleth
    fig_map = px.choropleth(
        df_countries, locations='country_code', color='count',
        hover_name='country_code', color_continuous_scale='Viridis',
        labels={'count':'Visite'}, title=f"Mappa Mondo ({time_filter})"
    )
    fig_map.update_layout(geo=dict(showframe=False, showcoastlines=True, projection_type="natural earth"))
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info(f"Nessun dato per {time_filter}.")

# Debug
if st.checkbox("🔍 Debug dati"):
    st.write("**today shape:**", today.shape)
    st.write("**Paesi:**", df_countries)
    st.dataframe(today.head(10))

# Tabs esistenti
tab1, tab2, tab3 = st.tabs(["📊 Ultima Ora", "📈 24h", "📅 7 Giorni"])

with tab1:
    df_hour = get_data(1/24)
    if not df_hour.empty:
        fig = px.line(df_hour, x='minute', y='count', color='event_type', title="Traffico per Minuto", markers=True)
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    df_day = get_data(1)
    if not df_day.empty:
        fig2 = px.bar(df_day, x='minute', y='count', color='event_type', title="24h Dettaglio")
        st.plotly_chart(fig2, use_container_width=True)

with tab3:
    df_week = get_data(7)
    if not df_week.empty:
        fig3 = px.area(df_week, x='minute', y='count', color='event_type', title="Trend Settimanale")
        st.plotly_chart(fig3, use_container_width=True)

st.subheader("🥇 Top Pagine (24h)")
try:
    df_pages = get_data(1)
    if not df_pages.empty:
        top_pages = df_pages[df_pages['event_type'] == 'page_view'].groupby('unique_pages')['count'].sum().nlargest(10)
        if not top_pages.empty:
            st.bar_chart(top_pages)
        else:
            st.info("📭 Nessuna page_view nelle 24h")
    else:
        st.info("📭 Dati non disponibili")
except:
    st.info("📭 Top pages non disponibile")
