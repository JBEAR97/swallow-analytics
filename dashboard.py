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
MONTH_WINDOW_DAYS = 30
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
QUALITY_FILTERS = {
    "Human only": "COALESCE(is_bot, FALSE) = FALSE AND COALESCE(is_internal, FALSE) = FALSE",
    "Include internal": "COALESCE(is_bot, FALSE) = FALSE",
    "All traffic": "TRUE",
}


def get_iso2_to_iso3() -> dict[str, str]:
    mapping = {country.alpha_2: country.alpha_3 for country in pycountry.countries}
    mapping["ZZ"] = "ZZZ"
    mapping["EU"] = "EUR"
    return mapping


ISO2_TO_ISO3 = get_iso2_to_iso3()


def build_database_url() -> str | None:
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
    for attempt in range(3):
        try:
            with engine.connect() as conn:
                return pd.read_sql(text(query), conn, params=params, parse_dates=parse_dates)
        except SQLAlchemyError as exc:
            if attempt == 2:
                st.warning(f"Database query failed: {exc}")
                return pd.DataFrame()
            time.sleep(1)

    return pd.DataFrame()


def traffic_clause() -> str:
    return QUALITY_FILTERS[st.session_state.traffic_quality]


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_overview_metrics(days: float, filter_sql: str) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
            COUNT(*) FILTER (WHERE event_type = 'impression') AS impressions,
            COUNT(*) FILTER (WHERE event_type = 'engagement') AS engagements,
            COUNT(*) FILTER (WHERE event_type = 'heartbeat') AS heartbeats,
            COUNT(DISTINCT page_path) AS unique_pages,
            COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS unique_visitors,
            COUNT(DISTINCT session_id) FILTER (WHERE session_id IS NOT NULL) AS sessions,
            COUNT(DISTINCT item_id) FILTER (WHERE item_id IS NOT NULL) AS tracked_items
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
          AND {filter_sql}
        """,
        params={"days": float(days)},
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_event_counts(days: float, filter_sql: str) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT date_trunc('minute', ts_utc::timestamptz) AS minute,
               event_type,
               COUNT(*) AS count
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
          AND {filter_sql}
        GROUP BY 1, 2
        ORDER BY 1 DESC, 2 ASC
        """,
        params={"days": float(days)},
        parse_dates=["minute"],
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_country_counts(days: float, filter_sql: str) -> pd.DataFrame:
    if days > ALL_TIME_DAYS_THRESHOLD:
        return run_query(
            f"""
            SELECT country_code,
                   COUNT(*) AS events,
                   COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS visitors
            FROM {EVENT_TABLE}
            WHERE {filter_sql}
            GROUP BY 1
            ORDER BY visitors DESC NULLS LAST, events DESC
            LIMIT 50
            """
        )

    return run_query(
        f"""
        SELECT country_code,
               COUNT(*) AS events,
               COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS visitors
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
          AND {filter_sql}
        GROUP BY 1
        ORDER BY visitors DESC NULLS LAST, events DESC
        LIMIT 20
        """,
        params={"days": float(days)},
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_top_pages(days: float, filter_sql: str, limit: int = 10) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT page_path,
               COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
               COUNT(DISTINCT visitor_id) FILTER (WHERE event_type = 'page_view' AND visitor_id IS NOT NULL) AS unique_visitors,
               COUNT(*) FILTER (WHERE event_type = 'impression') AS impressions,
               COUNT(*) FILTER (WHERE event_type = 'engagement') AS engagements
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
          AND {filter_sql}
        GROUP BY 1
        HAVING COUNT(*) FILTER (WHERE event_type = 'page_view') > 0
        ORDER BY page_views DESC, unique_visitors DESC, page_path ASC
        LIMIT :limit
        """,
        params={"days": float(days), "limit": int(limit)},
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_top_items(days: float, filter_sql: str, limit: int = 12) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT COALESCE(item_label, item_id) AS item_name,
               item_id,
               COALESCE(item_type, 'unknown') AS item_type,
               COALESCE(section, 'unassigned') AS section,
               COUNT(*) FILTER (WHERE event_type = 'impression') AS impressions,
               COUNT(*) FILTER (WHERE event_type = 'engagement') AS engagements,
               COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL AND event_type = 'impression') AS unique_viewers
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
          AND {filter_sql}
          AND item_id IS NOT NULL
        GROUP BY 1, 2, 3, 4
        HAVING COUNT(*) FILTER (WHERE event_type = 'impression') > 0
        ORDER BY impressions DESC, engagements DESC, item_name ASC
        LIMIT :limit
        """,
        params={"days": float(days), "limit": int(limit)},
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_engagement_breakdown(days: float, filter_sql: str) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT COALESCE(action_type, 'unknown') AS action_type,
               COUNT(*) AS count
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '1 day' * :days
          AND {filter_sql}
          AND event_type = 'engagement'
        GROUP BY 1
        ORDER BY count DESC, action_type ASC
        """,
        params={"days": float(days)},
    )


def render_metric_cards(df: pd.DataFrame) -> None:
    if df.empty:
        df = pd.DataFrame(
            [
                {
                    "page_views": 0,
                    "impressions": 0,
                    "engagements": 0,
                    "heartbeats": 0,
                    "unique_pages": 0,
                    "unique_visitors": 0,
                    "sessions": 0,
                    "tracked_items": 0,
                }
            ]
        )

    row = df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("👀 Page Views (24h)", int(row["page_views"]))
    with col2:
        st.metric("📖 Impressions (24h)", int(row["impressions"]))
    with col3:
        st.metric("🙋 Unique Visitors", int(row["unique_visitors"]))
    with col4:
        st.metric("🧭 Sessions", int(row["sessions"]))

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric("⚡ Engagements", int(row["engagements"]))
    with col6:
        st.metric("💓 Heartbeats", int(row["heartbeats"]))
    with col7:
        st.metric("📄 Unique Pages", int(row["unique_pages"]))
    with col8:
        st.metric("🧱 Tracked Items", int(row["tracked_items"]))

    with st.expander("Metric legend", expanded=False):
        st.markdown(
            """
            **👀 Page Views (24h)**  
            Total number of `page_view` events recorded in the last 24 hours. A single visitor can generate multiple page views by reloading a page or visiting several pages.

            **📖 Impressions (24h)**  
            Total number of `impression` events recorded in the last 24 hours. These usually represent tracked elements or content blocks that were shown to a visitor.

            **🙋 Unique Visitors**  
            Number of distinct `visitor_id` values seen in the selected time window. This estimates how many individual visitors were tracked, excluding events without a visitor ID.

            **🧭 Sessions**  
            Number of distinct `session_id` values in the selected time window. A session groups events that belong to the same visit or browsing session.

            **⚡ Engagements**  
            Total number of `engagement` events recorded in the selected time window. These are active interactions such as clicks or other tracked user actions.

            **💓 Heartbeats**  
            Total number of `heartbeat` events recorded in the selected time window. Heartbeats usually indicate that a visitor remained active on a page over time.

            **📄 Unique Pages**  
            Number of distinct `page_path` values seen in the selected time window. This shows how many different pages generated tracked activity.

            **🧱 Tracked Items**  
            Number of distinct `item_id` values seen in the selected time window. This represents how many unique tracked elements, components, or content items appeared in the data.
            """
        )


def render_country_section(filter_sql: str) -> None:
    st.subheader("🌍 GeoIP: Human Traffic by Country")
    time_filter = st.selectbox("Periodo:", list(TIME_FILTERS), index=3)
    df_countries = get_country_counts(TIME_FILTERS[time_filter], filter_sql)

    col_geo1, col_geo2 = st.columns(2)
    with col_geo1:
        unique_countries = df_countries["country_code"].nunique() if not df_countries.empty else 0
        st.metric("Paesi Unici", unique_countries)
    with col_geo2:
        if df_countries.empty:
            st.metric("🥇 Top Paese", "N/A")
        else:
            top_country = df_countries.iloc[0]["country_code"]
            top_visitors = int(df_countries.iloc[0]["visitors"] or 0)
            st.metric("🥇 Top Paese", f"{top_country} ({top_visitors} visitors)")

    if df_countries.empty:
        st.info("Nessun dato GeoIP disponibile per il periodo selezionato.")
        return

    if st.checkbox("🔍 Debug GeoIP"):
        st.write("**Raw:**", df_countries)

    df_map = df_countries.copy()
    df_map["iso3"] = df_map["country_code"].map(ISO2_TO_ISO3)

    fig_bar = px.bar(
        df_countries.head(15),
        x="visitors",
        y="country_code",
        orientation="h",
        title=f"Top Paesi ({time_filter})",
        color="events",
        color_continuous_scale="Viridis",
        hover_data={"events": True, "visitors": True},
    )
    fig_bar.update_layout(yaxis={"categoryorder": "total descending"})
    fig_bar.update_traces(texttemplate="%{x}", textposition="outside")
    st.plotly_chart(fig_bar, width="stretch")

    valid_map = df_map.dropna(subset=["iso3"])
    if valid_map.empty:
        st.error("❌ Nessun paese mappato. Verifica `pycountry` e i country code salvati.")
        return

    fig_map = px.choropleth(
        valid_map,
        locations="iso3",
        color="visitors",
        locationmode="ISO-3",
        hover_name="country_code",
        hover_data={"events": True, "visitors": True},
        color_continuous_scale="Viridis",
        range_color=[1, max(int(valid_map["visitors"].max()), 1)],
        title=f"🌍 Mappa Mondo ({time_filter})",
    )
    fig_map.update_layout(geo={"showframe": False, "showcoastlines": True})
    st.plotly_chart(fig_map, width="stretch")
    st.caption(f"✅ {len(valid_map)}/{len(df_countries)} paesi mappati | Max visitors: {int(valid_map['visitors'].max())}")


def render_event_tabs(filter_sql: str) -> None:
    tabs = st.tabs([label for label, *_ in TAB_CONFIG])

    for tab, (label, days, chart_fn, extra_kwargs) in zip(tabs, TAB_CONFIG):
        with tab:
            df_tab = get_event_counts(days, filter_sql)
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


def render_top_pages(filter_sql: str) -> None:
    st.subheader("🥇 Top Pagine (30d)")
    df_pages = get_top_pages(MONTH_WINDOW_DAYS, filter_sql)

    if df_pages.empty:
        st.info("Nessuna page view registrata negli ultimi 30 giorni.")
        return

    st.dataframe(df_pages, width="stretch", hide_index=True)


def render_top_items(filter_sql: str) -> None:
    st.subheader("🧱 Top Items by Impression (30d)")
    df_items = get_top_items(MONTH_WINDOW_DAYS, filter_sql)

    if df_items.empty:
        st.info("Nessuna impression item-level registrata negli ultimi 30 giorni.")
        return

    fig = px.bar(
        df_items.sort_values("impressions", ascending=True),
        x="impressions",
        y="item_name",
        color="item_type",
        orientation="h",
        hover_data={"section": True, "engagements": True, "unique_viewers": True, "item_id": True},
        title="Top Impression Items",
    )
    st.plotly_chart(fig, width="stretch")
    st.dataframe(df_items, width="stretch", hide_index=True)


def render_engagement_section(filter_sql: str) -> None:
    st.subheader("🎯 Engagement Breakdown (30d)")
    df_engagement = get_engagement_breakdown(MONTH_WINDOW_DAYS, filter_sql)
    if df_engagement.empty:
        st.info("Nessun evento di engagement registrato negli ultimi 30 giorni.")
        return

    fig = px.pie(df_engagement, names="action_type", values="count", title="Engagement Types")
    st.plotly_chart(fig, width="stretch")
    st.dataframe(df_engagement, width="stretch", hide_index=True)


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
if "traffic_quality" not in st.session_state:
    st.session_state.traffic_quality = "Human only"

col_filter1, col_filter2 = st.columns([2, 1])
with col_filter1:
    st.session_state.traffic_quality = st.radio(
        "Traffic filter",
        list(QUALITY_FILTERS),
        horizontal=True,
        index=list(QUALITY_FILTERS).index(st.session_state.traffic_quality),
    )
with col_filter2:
    st.metric("⏰ Ultimo Update", st.session_state.last_refresh.strftime("%H:%M"))

if st.button("🔄 Refresh (auto 10min)") or (
    datetime.now() - st.session_state.last_refresh
).total_seconds() > AUTO_REFRESH_SECONDS:
    st.session_state.last_refresh = datetime.now()
    st.rerun()

filter_sql = traffic_clause()
overview = get_overview_metrics(METRIC_WINDOW_DAYS, filter_sql)
render_metric_cards(overview)
render_country_section(filter_sql)
render_event_tabs(filter_sql)
render_top_pages(filter_sql)
render_top_items(filter_sql)
render_engagement_section(filter_sql)
