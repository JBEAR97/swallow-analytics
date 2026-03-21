from __future__ import annotations

import os
import time
import urllib.parse
from datetime import datetime

import pandas as pd
import plotly.express as px
import pycountry
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


AUTO_REFRESH_SECONDS = 600
METRIC_WINDOW_DAYS = 1
ALL_TIME_DAYS_THRESHOLD = 10_000
EVENT_TABLE = '"swallow-analysis"'
TIME_FILTERS = {
    "1h": 1 / 24,
    "24h": 1,
    "7d": 7,
    "🌍 All-time": 99_999,
}
TAB_CONFIG = [
    ("📊 Ultima Ora", 1 / 24, px.line, {"markers": True}),
    ("📈 24h", 1, px.bar, {}),
    ("📅 7 Giorni", 7, px.area, {}),
]


def get_iso2_to_iso3() -> dict[str, str]:
    """Build an ISO2 -> ISO3 mapping for all countries plus known fallbacks."""
    mapping = {country.alpha_2: country.alpha_3 for country in pycountry.countries}
    mapping["ZZ"] = "ZZZ"
    mapping["EU"] = "EUR"
    return mapping


ISO2_TO_ISO3 = get_iso2_to_iso3()


def build_database_url() -> str | None:
    """Build DATABASE_URL from PostgreSQL env vars, with DATABASE_URL fallback."""
    pg_host = os.getenv("PGHOST", "postgres.railway.internal")
    pg_port = os.getenv("PGPORT", "5432")
    pg_user = os.getenv("PGUSER", os.getenv("USER", "postgres"))
    pg_password = os.getenv("PGPASSWORD")
    pg_database = os.getenv("PGDATABASE", "railway")

    if all([pg_host, pg_port, pg_user, pg_password, pg_database]):
        encoded_password = urllib.parse.quote_plus(pg_password)
        return f"postgresql://{pg_user}:{encoded_password}@{pg_host}:{pg_port}/{pg_database}"

    return os.getenv("DATABASE_URL")


def run_query(query: str, params: dict | None = None, *, parse_dates: list[str] | None = None) -> pd.DataFrame:
    """Execute a SQL query with simple retry logic and return a DataFrame."""
    for attempt in range(3):
        try:
            with engine.connect() as conn:
                return pd.read_sql(
                    text(query),
                    conn,
                    params=params,
                    parse_dates=parse_dates,
                )
        except SQLAlchemyError as exc:
            if attempt == 2:
                st.warning(f"Database query failed: {exc}")
                return pd.DataFrame()
            time.sleep(1)

    return pd.DataFrame()


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_event_counts(days: float) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT date_trunc('minute', ts_utc::timestamptz) AS minute,
               event_type,
               COUNT(*) AS count,
               COUNT(DISTINCT page_path) AS unique_pages
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
        GROUP BY 1, 2
        ORDER BY 1 DESC
        """,
        params={"days": float(days)},
        parse_dates=["minute"],
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_country_counts(days: float) -> pd.DataFrame:
    if days > ALL_TIME_DAYS_THRESHOLD:
        query = f"""
            SELECT country_code, COUNT(*) AS count
            FROM {EVENT_TABLE}
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 50
        """
        return run_query(query)

    return run_query(
        f"""
        SELECT country_code, COUNT(*) AS count
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 20
        """,
        params={"days": float(days)},
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_top_pages(days: float, limit: int = 10) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT page_path, COUNT(*) AS page_views
        FROM {EVENT_TABLE}
        WHERE event_type = 'page_view'
          AND ts_utc >= NOW() - INTERVAL '1 day' * :days
        GROUP BY 1
        ORDER BY 2 DESC, 1 ASC
        LIMIT :limit
        """,
        params={"days": float(days), "limit": int(limit)},
    )


def render_metric_cards(df: pd.DataFrame) -> None:
    total_views = int(df.loc[df["event_type"] == "page_view", "count"].sum()) if not df.empty else 0
    total_impressions = int(df.loc[df["event_type"] == "impression", "count"].sum()) if not df.empty else 0
    unique_pages = int(df["unique_pages"].max()) if not df.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("👀 Page Views (24h)", total_views)
    with col2:
        st.metric("📖 Impressions (24h)", total_impressions)
    with col3:
        st.metric("📄 Pagine Uniche", unique_pages)
    with col4:
        st.metric("⏰ Ultimo Update", st.session_state.last_refresh.strftime("%H:%M"))


def render_country_section() -> None:
    st.subheader("🌍 GeoIP: Paesi Visitatori")
    time_filter = st.selectbox("Periodo:", list(TIME_FILTERS), index=3)
    df_countries = get_country_counts(TIME_FILTERS[time_filter])

    col_geo1, col_geo2 = st.columns(2)
    with col_geo1:
        unique_countries = df_countries["country_code"].nunique() if not df_countries.empty else 0
        st.metric("Paesi Unici", unique_countries)
    with col_geo2:
        if df_countries.empty:
            st.metric("🥇 Top Paese", "N/A")
        else:
            top_country = df_countries.iloc[0]["country_code"]
            top_count = int(df_countries.iloc[0]["count"])
            st.metric("🥇 Top Paese", f"{top_country} ({top_count})")

    if df_countries.empty:
        st.info("Nessun dato GeoIP disponibile per il periodo selezionato.")
        return

    if st.checkbox("🔍 Debug GeoIP"):
        st.write("**Raw:**", df_countries)

    df_map = df_countries.copy()
    df_map["iso3"] = df_map["country_code"].map(ISO2_TO_ISO3)

    fig_bar = px.bar(
        df_countries.head(15),
        x="count",
        y="country_code",
        orientation="h",
        title=f"Top Paesi ({time_filter})",
        color="count",
        color_continuous_scale="Viridis",
    )
    fig_bar.update_layout(yaxis={"categoryorder": "total descending"})
    fig_bar.update_traces(texttemplate="%{x}", textposition="outside")
    st.plotly_chart(fig_bar, width=700)

    valid_map = df_map.dropna(subset=["iso3"])
    if valid_map.empty:
        st.error("❌ Nessun paese mappato. Verifica `pycountry` e i country code salvati.")
        return

    fig_map = px.choropleth(
        valid_map,
        locations="iso3",
        color="count",
        locationmode="ISO-3",
        hover_name="country_code",
        hover_data={"count": ":.0f"},
        color_continuous_scale="Viridis",
        range_color=[1, valid_map["count"].max()],
        title=f"🌍 Mappa Mondo ({time_filter})",
    )
    fig_map.update_layout(geo={"showframe": False, "showcoastlines": True})
    st.plotly_chart(fig_map, width="stretch")
    st.caption(f"✅ {len(valid_map)}/{len(df_countries)} paesi mappati | Max: {valid_map['count'].max()}")


def render_event_tabs() -> None:
    tabs = st.tabs([label for label, *_ in TAB_CONFIG])

    for tab, (label, days, chart_fn, extra_kwargs) in zip(tabs, TAB_CONFIG):
        with tab:
            df_tab = get_event_counts(days)
            if df_tab.empty:
                st.info("Nessun evento disponibile per questo intervallo.")
                continue

            fig = chart_fn(
                df_tab.sort_values("minute"),
                x="minute",
                y="count",
                color="event_type",
                **extra_kwargs,
            )
            st.plotly_chart(fig, width="stretch")


def render_top_pages() -> None:
    st.subheader("🥇 Top Pagine (24h)")
    df_pages = get_top_pages(METRIC_WINDOW_DAYS)

    if df_pages.empty:
        st.info("Nessuna page view registrata nelle ultime 24 ore.")
        return

    st.bar_chart(df_pages.set_index("page_path")["page_views"])


DATABASE_URL = build_database_url()
if not DATABASE_URL:
    st.error("❌ DATABASE_URL non trovata.")
    st.stop()

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300, echo=False)

st.set_page_config(page_title="Swallow Analytics", layout="wide")
st.title("Swallow's Notes Analytics")
st.markdown("---")

if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = datetime.now()

if st.button("🔄 Refresh (auto 10min)") or (
    datetime.now() - st.session_state.last_refresh
).total_seconds() > AUTO_REFRESH_SECONDS:
    st.session_state.last_refresh = datetime.now()
    st.rerun()

today = get_event_counts(METRIC_WINDOW_DAYS)
render_metric_cards(today)
render_country_section()
render_event_tabs()
render_top_pages()
