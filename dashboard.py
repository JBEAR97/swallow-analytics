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
ALL_TIME_DAYS_THRESHOLD = 10_000
EVENT_TABLE = '"swallow-analysis"'
SESSION_KEY_SQL = """
COALESCE(
    NULLIF(session_id, ''),
    CASE
        WHEN visitor_id IS NOT NULL THEN visitor_id || ':' || to_char(date_trunc('hour', ts_utc), 'YYYY-MM-DD HH24')
        ELSE event_id
    END
)
"""
TIME_FILTERS = {
    "24h": 1,
    "7d": 7,
    "30d": 30,
    "90d": 90,
    "All-time": 99_999,
}
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


def build_where_clause(days: float, filter_sql: str) -> tuple[str, dict]:
    conditions = [filter_sql]
    params: dict[str, float] = {}
    if days <= ALL_TIME_DAYS_THRESHOLD:
        conditions.append("ts_utc >= NOW() - INTERVAL '1 day' * :days")
        params["days"] = float(days)
    return " AND ".join(f"({condition})" for condition in conditions), params


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_overview_metrics(days: float, filter_sql: str) -> pd.DataFrame:
    where_clause, params = build_where_clause(days, filter_sql)
    return run_query(
        f"""
        WITH filtered AS (
            SELECT *
            FROM {EVENT_TABLE}
            WHERE {where_clause}
        ),
        session_rollup AS (
            SELECT
                {SESSION_KEY_SQL} AS session_key,
                COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
                COUNT(*) FILTER (WHERE event_type = 'engagement') AS engagements,
                COUNT(*) FILTER (WHERE event_type = 'heartbeat') AS heartbeats,
                COUNT(*) FILTER (WHERE COALESCE(is_conversion, FALSE)) AS conversions
            FROM filtered
            GROUP BY 1
        )
        SELECT
            COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
            COUNT(*) FILTER (WHERE event_type = 'impression') AS impressions,
            COUNT(*) FILTER (WHERE event_type = 'engagement') AS engagements,
            COUNT(*) FILTER (WHERE event_type = 'heartbeat') AS heartbeats,
            COUNT(*) FILTER (WHERE COALESCE(is_conversion, FALSE)) AS conversions,
            COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS users,
            COUNT(DISTINCT {SESSION_KEY_SQL}) AS sessions,
            COUNT(DISTINCT page_path) AS pages,
            (
                SELECT COUNT(*)
                FROM session_rollup
                WHERE page_views >= 2 OR engagements > 0 OR heartbeats > 0 OR conversions > 0
            ) AS engaged_sessions
        FROM filtered
        """,
        params=params,
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_event_trend(days: float, filter_sql: str) -> pd.DataFrame:
    where_clause, params = build_where_clause(days, filter_sql)
    return run_query(
        f"""
        SELECT date_trunc('hour', ts_utc::timestamptz) AS hour,
               event_type,
               COUNT(*) AS count
        FROM {EVENT_TABLE}
        WHERE {where_clause}
        GROUP BY 1, 2
        ORDER BY 1 ASC, 2 ASC
        """,
        params=params,
        parse_dates=["hour"],
    )


@st.cache_data(ttl=60)
def get_realtime_metrics(filter_sql: str) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            COUNT(*) AS events_30m,
            COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views_30m,
            COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS users_30m,
            COUNT(DISTINCT {SESSION_KEY_SQL}) AS sessions_30m
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '30 minutes'
          AND {filter_sql}
        """
    )


@st.cache_data(ttl=60)
def get_realtime_pages(filter_sql: str) -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            page_path,
            COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
            COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS users
        FROM {EVENT_TABLE}
        WHERE ts_utc >= NOW() - INTERVAL '30 minutes'
          AND {filter_sql}
        GROUP BY 1
        HAVING COUNT(*) FILTER (WHERE event_type = 'page_view') > 0
        ORDER BY page_views DESC, users DESC, page_path ASC
        LIMIT 10
        """
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_acquisition(days: float, filter_sql: str) -> pd.DataFrame:
    where_clause, params = build_where_clause(days, filter_sql)
    return run_query(
        f"""
        SELECT
            COALESCE(source, 'direct') AS source,
            COALESCE(medium, '(none)') AS medium,
            COALESCE(campaign, '(not set)') AS campaign,
            COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS users,
            COUNT(DISTINCT {SESSION_KEY_SQL}) AS sessions,
            COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
            COUNT(*) FILTER (WHERE COALESCE(is_conversion, FALSE)) AS conversions
        FROM {EVENT_TABLE}
        WHERE {where_clause}
        GROUP BY 1, 2, 3
        ORDER BY sessions DESC, users DESC, page_views DESC
        LIMIT 25
        """,
        params=params,
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_landing_pages(days: float, filter_sql: str) -> pd.DataFrame:
    where_clause, params = build_where_clause(days, filter_sql)
    return run_query(
        f"""
        WITH filtered AS (
            SELECT *,
                   {SESSION_KEY_SQL} AS session_key
            FROM {EVENT_TABLE}
            WHERE {where_clause}
        ),
        ranked AS (
            SELECT
                session_key,
                page_path,
                source,
                medium,
                ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY ts_utc ASC, id ASC) AS rn
            FROM filtered
            WHERE event_type = 'page_view'
        )
        SELECT
            page_path AS landing_page,
            COALESCE(source, 'direct') AS source,
            COALESCE(medium, '(none)') AS medium,
            COUNT(*) AS sessions
        FROM ranked
        WHERE rn = 1
        GROUP BY 1, 2, 3
        ORDER BY sessions DESC, landing_page ASC
        LIMIT 20
        """,
        params=params,
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_page_report(days: float, filter_sql: str) -> pd.DataFrame:
    where_clause, params = build_where_clause(days, filter_sql)
    return run_query(
        f"""
        WITH filtered AS (
            SELECT *,
                   {SESSION_KEY_SQL} AS session_key
            FROM {EVENT_TABLE}
            WHERE {where_clause}
        ),
        page_sessions AS (
            SELECT
                page_path,
                session_key,
                COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
                COUNT(*) FILTER (WHERE event_type = 'engagement') AS engagements,
                COUNT(*) FILTER (WHERE COALESCE(is_conversion, FALSE)) AS conversions
            FROM filtered
            GROUP BY 1, 2
        )
        SELECT
            page_path,
            SUM(page_views) AS page_views,
            COUNT(DISTINCT session_key) FILTER (WHERE page_views > 0) AS sessions,
            COUNT(DISTINCT session_key) FILTER (WHERE engagements > 0 OR conversions > 0 OR page_views > 1) AS engaged_sessions,
            SUM(engagements) AS engagements,
            SUM(conversions) AS conversions
        FROM page_sessions
        GROUP BY 1
        HAVING SUM(page_views) > 0
        ORDER BY page_views DESC, sessions DESC, page_path ASC
        LIMIT 25
        """,
        params=params,
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_conversion_report(days: float, filter_sql: str) -> pd.DataFrame:
    where_clause, params = build_where_clause(days, filter_sql)
    return run_query(
        f"""
        SELECT
            COALESCE(conversion_name, action_type, event_type) AS conversion_name,
            COUNT(*) AS conversions,
            COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS users,
            COUNT(DISTINCT {SESSION_KEY_SQL}) AS sessions,
            COALESCE(SUM(event_value), 0) AS total_value
        FROM {EVENT_TABLE}
        WHERE {where_clause}
          AND COALESCE(is_conversion, FALSE) = TRUE
        GROUP BY 1
        ORDER BY conversions DESC, total_value DESC, conversion_name ASC
        LIMIT 20
        """,
        params=params,
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_device_report(days: float, filter_sql: str) -> pd.DataFrame:
    where_clause, params = build_where_clause(days, filter_sql)
    return run_query(
        f"""
        SELECT
            COALESCE(device_category, 'unknown') AS device_category,
            COALESCE(browser, 'Other') AS browser,
            COALESCE(operating_system, 'Other') AS operating_system,
            COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS users,
            COUNT(DISTINCT {SESSION_KEY_SQL}) AS sessions,
            COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views
        FROM {EVENT_TABLE}
        WHERE {where_clause}
        GROUP BY 1, 2, 3
        ORDER BY users DESC, sessions DESC, page_views DESC
        LIMIT 25
        """,
        params=params,
    )


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def get_country_counts(days: float, filter_sql: str) -> pd.DataFrame:
    where_clause, params = build_where_clause(days, filter_sql)
    return run_query(
        f"""
        SELECT
            country_code,
            COUNT(*) AS events,
            COUNT(DISTINCT visitor_id) FILTER (WHERE visitor_id IS NOT NULL) AS users,
            COUNT(DISTINCT {SESSION_KEY_SQL}) AS sessions
        FROM {EVENT_TABLE}
        WHERE {where_clause}
        GROUP BY 1
        ORDER BY users DESC NULLS LAST, sessions DESC, events DESC
        LIMIT 50
        """,
        params=params,
    )


def render_overview(days: float, filter_sql: str) -> None:
    overview = get_overview_metrics(days, filter_sql)
    if overview.empty:
        row = {
            "page_views": 0,
            "impressions": 0,
            "engagements": 0,
            "heartbeats": 0,
            "conversions": 0,
            "users": 0,
            "sessions": 0,
            "pages": 0,
            "engaged_sessions": 0,
        }
    else:
        row = overview.iloc[0].fillna(0).to_dict()

    sessions = int(row["sessions"] or 0)
    engaged_sessions = int(row["engaged_sessions"] or 0)
    page_views = int(row["page_views"] or 0)
    conversions = int(row["conversions"] or 0)
    engagement_rate = (engaged_sessions / sessions * 100) if sessions else 0
    pages_per_session = (page_views / sessions) if sessions else 0
    conversion_rate = (conversions / sessions * 100) if sessions else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Users", int(row["users"] or 0))
    col2.metric("Sessions", sessions)
    col3.metric("Engaged Sessions", engaged_sessions)
    col4.metric("Page Views", page_views)

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Engagement Rate", f"{engagement_rate:.1f}%")
    col6.metric("Pages / Session", f"{pages_per_session:.2f}")
    col7.metric("Conversions", conversions)
    col8.metric("Session CVR", f"{conversion_rate:.1f}%")

    with st.expander("Metric definitions", expanded=False):
        st.markdown(
            """
            `Users`: distinct `visitor_id` values in the selected period. Events without a `visitor_id` do not increase this metric.

            `Sessions`: distinct `session_id` values in the selected period. If a session id is missing, the dashboard falls back to a synthetic session key based on `visitor_id` and hour.

            `Engaged Sessions`: sessions with at least 2 page views, or at least 1 engagement event, heartbeat, or conversion.

            `Page Views`: total number of `page_view` events in the selected period.

            `Engagement Rate`: engaged sessions divided by total sessions.

            `Pages / Session`: total page views divided by total sessions.

            `Conversions`: total number of events where `is_conversion = true`. This includes both newly tracked explicit conversions and any historical rows you intentionally backfilled. A single session can contain multiple conversions.

            `Session CVR`: conversions divided by sessions. Because sessions can contain more than one conversion, this is a session-normalized conversion rate, not a unique-session conversion rate.
            """
        )


def render_realtime(filter_sql: str) -> None:
    st.subheader("Realtime")
    realtime = get_realtime_metrics(filter_sql)
    realtime_row = realtime.iloc[0].fillna(0) if not realtime.empty else pd.Series(dtype="float64")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Events (30m)", int(realtime_row.get("events_30m", 0)))
    col2.metric("Page Views (30m)", int(realtime_row.get("page_views_30m", 0)))
    col3.metric("Users (30m)", int(realtime_row.get("users_30m", 0)))
    col4.metric("Sessions (30m)", int(realtime_row.get("sessions_30m", 0)))

    df_pages = get_realtime_pages(filter_sql)
    if df_pages.empty:
        st.info("No page activity in the last 30 minutes.")
    else:
        st.dataframe(df_pages, width="stretch", hide_index=True)


def render_trend(days: float, filter_sql: str) -> None:
    st.subheader("Traffic Trend")
    df_trend = get_event_trend(days, filter_sql)
    if df_trend.empty:
        st.info("No events available for the selected period.")
        return

    fig = px.area(
        df_trend,
        x="hour",
        y="count",
        color="event_type",
        title="Hourly event volume",
    )
    st.plotly_chart(fig, width="stretch")


def render_acquisition(days: float, filter_sql: str) -> None:
    st.subheader("Acquisition")
    df_acquisition = get_acquisition(days, filter_sql)
    if df_acquisition.empty:
        st.info("No acquisition data available.")
        return

    fig = px.bar(
        df_acquisition.head(10),
        x="sessions",
        y="source",
        color="medium",
        orientation="h",
        hover_data={"campaign": True, "users": True, "page_views": True, "conversions": True},
        title="Top traffic sources",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, width="stretch")
    st.dataframe(df_acquisition, width="stretch", hide_index=True)


def render_landing_pages(days: float, filter_sql: str) -> None:
    st.subheader("Landing Pages")
    df_landing = get_landing_pages(days, filter_sql)
    if df_landing.empty:
        st.info("No landing page data available.")
        return
    st.dataframe(df_landing, width="stretch", hide_index=True)


def render_pages(days: float, filter_sql: str) -> None:
    st.subheader("Pages")
    df_pages = get_page_report(days, filter_sql)
    if df_pages.empty:
        st.info("No page report data available.")
        return

    df_pages = df_pages.copy()
    df_pages["engagement_rate"] = (
        (df_pages["engaged_sessions"] / df_pages["sessions"].replace(0, pd.NA)) * 100
    ).fillna(0)
    st.dataframe(df_pages, width="stretch", hide_index=True)


def render_conversions(days: float, filter_sql: str) -> None:
    st.subheader("Conversions")
    df_conversions = get_conversion_report(days, filter_sql)
    if df_conversions.empty:
        st.info("No conversions recorded in the selected period.")
        return

    fig = px.pie(df_conversions, names="conversion_name", values="conversions", title="Conversion mix")
    st.plotly_chart(fig, width="stretch")
    st.dataframe(df_conversions, width="stretch", hide_index=True)


def render_devices(days: float, filter_sql: str) -> None:
    st.subheader("Devices")
    df_devices = get_device_report(days, filter_sql)
    if df_devices.empty:
        st.info("No device data available.")
        return

    fig = px.sunburst(
        df_devices,
        path=["device_category", "browser", "operating_system"],
        values="sessions",
        title="Device mix by sessions",
    )
    st.plotly_chart(fig, width="stretch")
    st.dataframe(df_devices, width="stretch", hide_index=True)


def render_geography(days: float, filter_sql: str) -> None:
    st.subheader("Geography")
    df_countries = get_country_counts(days, filter_sql)
    if df_countries.empty:
        st.info("No geography data available.")
        return

    df_map = df_countries.copy()
    df_map["iso3"] = df_map["country_code"].map(ISO2_TO_ISO3)

    col1, col2 = st.columns(2)
    col1.metric("Countries", int(df_countries["country_code"].nunique()))
    top_country = df_countries.iloc[0]
    col2.metric("Top Country", f"{top_country['country_code']} ({int(top_country['users'] or 0)} users)")

    fig_bar = px.bar(
        df_countries.head(15),
        x="users",
        y="country_code",
        orientation="h",
        color="sessions",
        title="Top countries",
        hover_data={"events": True, "sessions": True},
    )
    fig_bar.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_bar, width="stretch")

    valid_map = df_map.dropna(subset=["iso3"])
    if not valid_map.empty:
        fig_map = px.choropleth(
            valid_map,
            locations="iso3",
            color="users",
            locationmode="ISO-3",
            hover_name="country_code",
            hover_data={"events": True, "sessions": True},
            color_continuous_scale="Viridis",
            title="World map by users",
        )
        fig_map.update_layout(geo={"showframe": False, "showcoastlines": True})
        st.plotly_chart(fig_map, width="stretch")

    st.dataframe(df_countries, width="stretch", hide_index=True)


DATABASE_URL = build_database_url()
if not DATABASE_URL:
    st.error("DATABASE_URL not found.")
    st.stop()

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300, echo=False)

st.set_page_config(page_title="Swallow Analytics", layout="wide")
st.title("Swallow Analytics")
st.caption("GA-style view over your custom event pipeline.")

if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = datetime.now()
if "traffic_quality" not in st.session_state:
    st.session_state.traffic_quality = "Human only"

col1, col2, col3 = st.columns([2, 2, 1])
with col1:
    selected_period = st.selectbox("Date range", list(TIME_FILTERS), index=2)
with col2:
    st.session_state.traffic_quality = st.radio(
        "Traffic filter",
        list(QUALITY_FILTERS),
        horizontal=True,
        index=list(QUALITY_FILTERS).index(st.session_state.traffic_quality),
    )
with col3:
    st.metric("Last refresh", st.session_state.last_refresh.strftime("%H:%M"))

if st.button("Refresh") or (datetime.now() - st.session_state.last_refresh).total_seconds() > AUTO_REFRESH_SECONDS:
    st.session_state.last_refresh = datetime.now()
    st.rerun()

selected_days = TIME_FILTERS[selected_period]
filter_sql = traffic_clause()

render_overview(selected_days, filter_sql)
render_realtime(filter_sql)
render_trend(selected_days, filter_sql)

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Acquisition", "Pages", "Conversions", "Devices", "Geography"])
with tab1:
    render_acquisition(selected_days, filter_sql)
    render_landing_pages(selected_days, filter_sql)
with tab2:
    render_pages(selected_days, filter_sql)
with tab3:
    render_conversions(selected_days, filter_sql)
with tab4:
    render_devices(selected_days, filter_sql)
with tab5:
    render_geography(selected_days, filter_sql)
