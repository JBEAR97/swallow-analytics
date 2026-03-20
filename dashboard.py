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
import pycountry  # pip install pycountry


# 🌍 Mapping COMPLETO ISO2 → ISO3 (TUTTI i paesi)
def get_iso2_to_iso3():
    """Genera mapping dinamico TUTTI i paesi mondo"""
    mapping = {}
    for country in pycountry.countries:
        mapping[country.alpha_2] = country.alpha_3
    mapping['ZZ'] = 'ZZZ'  # Unknown
    mapping['EU'] = 'EUR'  # Europe proxy
    return mapping

ISO2_TO_ISO3 = get_iso2_to_iso3()


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
    st.error("❌ DATABASE_URL non trovata.")
    st.stop()
else:
    st.success(f"✅ DATABASE_URL: {DATABASE_URL[:40]}...")


engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300, echo=False)
st.set_page_config(page_title="Swallow Analytics", layout="wide")


st.title("Swallow's Notes Analytics")
st.markdown("---")


if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

if st.button("🔄 Refresh (auto 10min)") or (datetime.now() - st.session_state.last_refresh).total_seconds() > 600:
    st.session_state.last_refresh = datetime.now()
    st.rerun()


@st.cache_data(ttl=600)
def get_data(days=7, max_retries=3):
    for attempt in range(max_retries):
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text("""
                    SELECT date_trunc('minute', ts_utc::timestamptz) as minute,
                           event_type, COUNT(*) as count,
                           COUNT(DISTINCT page_path) as unique_pages
                    FROM "swallow-analysis" 
                    WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
                    GROUP BY 1,2 ORDER BY 1 DESC
                """), conn, params={'days': float(days)}, parse_dates=['minute'])
                return df
        except:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return pd.DataFrame()


@st.cache_data(ttl=600)
def get_countries(days, max_retries=3):
    for attempt in range(max_retries):
        try:
            with engine.connect() as conn:
                if days > 10000:  # All-time
                    query = 'SELECT country_code, COUNT(*) as count FROM "swallow-analysis" GROUP BY 1 ORDER BY 2 DESC LIMIT 50'
                    df = pd.read_sql(text(query), conn)
                else:
                    query = """
                        SELECT country_code, COUNT(*) as count
                        FROM "swallow-analysis" 
                        WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
                        GROUP BY 1 ORDER BY 2 DESC LIMIT 20
                    """
                    df = pd.read_sql(text(query), conn, params={'days': float(days)})
                return df
        except:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return pd.DataFrame()


# Metriche
today = get_data(days=1)
col1, col2, col3, col4 = st.columns(4)
total_views = int(today[today['event_type'] == 'page_view']['count'].sum() or 0) if not today.empty else 0
total_impressions = int(today[today['event_type'] == 'impression']['count'].sum() or 0) if not today.empty else 0
pages = int(today['unique_pages'].sum() or 0) if not today.empty else 0

with col1: st.metric("👀 Page Views (24h)", total_views)
with col2: st.metric("📖 Impressions (24h)", total_impressions)
with col3: st.metric("📄 Pagine Uniche", pages)
with col4: st.metric("⏰ Ultimo Update", st.session_state.last_refresh.strftime("%H:%M"))


# 🌍 GEOIP COMPLETA (TUTTI PAESI)
st.subheader("🌍 GeoIP: Paesi Visitatori")
time_filter = st.selectbox("Periodo:", ["1h", "24h", "7d", "🌍 All-time"], index=3)
days_map = {'1h': 1/24, '24h': 1, '7d': 7, '🌍 All-time': 99999}[time_filter]

df_countries = get_countries(days_map)

col_geo1, col_geo2 = st.columns(2)
with col_geo1:
    st.metric("Paesi Unici", df_countries['country_code'].nunique())
with col_geo2:
    if not df_countries.empty:
        top_country = df_countries.iloc[0]['country_code']
        top_count = df_countries.iloc[0]['count']
        st.metric("🥇 Top Paese", f"{top_country} ({top_count})")

if not df_countries.empty:
    if st.checkbox("🔍 Debug GeoIP"):
        st.write("**Raw:**", df_countries)
    
    # ISO2 → ISO3 per TUTTI i paesi (pycountry)
    df_map = df_countries.copy()
    df_map['iso3'] = df_map['country_code'].map(ISO2_TO_ISO3)
    
    # Bar (tutti)
    fig_bar = px.bar(df_countries.head(15), x='count', y='country_code', orientation='h',
                     title=f"Top Paesi ({time_filter})", color='count', 
                     color_continuous_scale='Viridis')
    fig_bar.update_layout(yaxis={'categoryorder':'total descending'})
    fig_bar.update_traces(texttemplate='%{x}', textposition='outside')
    st.plotly_chart(fig_bar, width=700)
    
    # MAPPA ISO3 (tutti mappati)
    valid_map = df_map.dropna(subset=['iso3'])
    if len(valid_map) > 0:
        fig_map = px.choropleth(valid_map, 
                                locations='iso3', color='count',
                                locationmode='ISO-3',
                                hover_name='country_code', hover_data={'count': ':.0f'},
                                color_continuous_scale='Viridis',
                                range_color=[1, valid_map['count'].max()],
                                title=f"🌍 Mappa Mondo ({time_filter})")
        fig_map.update_layout(geo=dict(showframe=False, showcoastlines=True))
        st.plotly_chart(fig_map, width='stretch')
        st.caption(f"✅ {len(valid_map)}/{len(df_countries)} paesi mappati | Max: {valid_map['count'].max()}")
    else:
        st.error("❌ Nessun paese mappato. pip install pycountry")


# Tabs
tab1, tab2, tab3 = st.tabs(["📊 Ultima Ora", "📈 24h", "📅 7 Giorni"])

for tab_name, days in [("📊 Ultima Ora", 1/24), ("📈 24h", 1), ("📅 7 Giorni", 7)]:
    with st.container():
        df_tab = get_data(days)
        if not df_tab.empty:
            if tab_name == "📊 Ultima Ora":
                fig = px.line(df_tab, x='minute', y='count', color='event_type', markers=True)
            elif tab_name == "📈 24h":
                fig = px.bar(df_tab, x='minute', y='count', color='event_type')
            else:
                fig = px.area(df_tab, x='minute', y='count', color='event_type')
            st.plotly_chart(fig, width='stretch')


st.subheader("🥇 Top Pagine (24h)")
df_pages = get_data(1)
if not df_pages.empty:
    top_pages = df_pages[df_pages['event_type'] == 'page_view'].groupby('unique_pages')['count'].sum().nlargest(10)
    if not top_pages.empty:
        st.bar_chart(top_pages)


