from __future__ import annotations

import os
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import geoip2.database
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from geoip2.errors import AddressNotFoundError
from sqlalchemy import create_engine, text

load_dotenv()

TABLE_NAME = '"swallow-analysis"'
BLOCKED_REF_SUBSTR = "https://orca-tetra-d4nz.squarespace.com"
ALLOWED_EVENT_TYPES = {"page_view", "impression", "engagement", "heartbeat"}
INTERNAL_TRAFFIC_SECRET = os.getenv("INTERNAL_TRAFFIC_SECRET", "").strip()
BOT_PATTERNS = (
    "bot",
    "spider",
    "crawler",
    "crawl",
    "slurp",
    "facebookexternalhit",
    "whatsapp",
    "preview",
    "headless",
    "python-requests",
    "curl/",
    "wget/",
    "uptime",
    "monitor",
    "pingdom",
    "checkly",
    "datadog",
    "site24x7",
)
ID_PATTERN = re.compile(r"^[a-zA-Z0-9._:-]{1,128}$")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL mancante!")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
GEOIP_DB_PATH = os.path.join(os.path.dirname(__file__), "data", "GeoLite2-Country.mmdb")
geoip_reader = None


def column_exists(conn, column_name: str) -> bool:
    result = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'swallow-analysis'
              AND column_name = :column_name
            """
        ),
        {"column_name": column_name},
    )
    return result.fetchone() is not None


def index_exists(conn, index_name: str) -> bool:
    result = conn.execute(
        text(
            """
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = 'swallow-analysis'
              AND indexname = :index_name
            """
        ),
        {"index_name": index_name},
    )
    return result.fetchone() is not None


def add_column_if_missing(conn, column_name: str, ddl: str) -> None:
    if not column_exists(conn, column_name):
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {ddl}"))
        print(f"✅ Migration: added {column_name}")


def create_index_if_missing(conn, index_name: str, sql: str) -> None:
    if not index_exists(conn, index_name):
        conn.execute(text(sql))
        print(f"✅ Migration: added index {index_name}")


def ensure_event_type_constraint(conn) -> None:
    allowed_values = "'page_view', 'impression', 'engagement', 'heartbeat'"
    desired_definition = f"CHECK ((event_type = ANY (ARRAY[{allowed_values}])))"
    result = conn.execute(
        text(
            """
            SELECT conname, pg_get_constraintdef(pg_constraint.oid) AS definition
            FROM pg_constraint
            JOIN pg_class ON pg_class.oid = pg_constraint.conrelid
            WHERE pg_class.relname = 'swallow-analysis'
              AND pg_constraint.contype = 'c'
            """
        )
    )

    has_desired_constraint = False
    constraints_to_replace: list[str] = []
    for row in result:
        name = row._mapping["conname"]
        definition = row._mapping["definition"]
        if "event_type" not in definition:
            continue
        if all(event_name in definition for event_name in ("page_view", "impression", "engagement", "heartbeat")):
            has_desired_constraint = True
            continue
        constraints_to_replace.append(name)

    for constraint_name in constraints_to_replace:
        conn.execute(text(f'ALTER TABLE {TABLE_NAME} DROP CONSTRAINT "{constraint_name}"'))
        print(f"✅ Migration: dropped outdated constraint {constraint_name}")

    if not has_desired_constraint:
        conn.execute(
            text(
                f"""
                ALTER TABLE {TABLE_NAME}
                ADD CONSTRAINT chk_event_type
                CHECK (event_type IN ({allowed_values}))
                """
            )
        )
        print("✅ Migration: added chk_event_type")


def run_migrations() -> None:
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                        id BIGSERIAL PRIMARY KEY,
                        event_type TEXT NOT NULL CHECK (event_type IN ('page_view', 'impression', 'engagement', 'heartbeat')),
                        page_path TEXT NOT NULL,
                        referrer TEXT,
                        user_agent TEXT,
                        ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            ensure_event_type_constraint(conn)

            add_column_if_missing(conn, "created_at", "created_at TIMESTAMPTZ DEFAULT NOW()")
            conn.execute(text(f"UPDATE {TABLE_NAME} SET created_at = ts_utc WHERE created_at IS NULL"))

            add_column_if_missing(conn, "country_code", "country_code VARCHAR(2) DEFAULT 'ZZ'")
            add_column_if_missing(conn, "event_id", "event_id VARCHAR(64)")
            add_column_if_missing(conn, "visitor_id", "visitor_id VARCHAR(128)")
            add_column_if_missing(conn, "session_id", "session_id VARCHAR(128)")
            add_column_if_missing(conn, "page_load_id", "page_load_id VARCHAR(128)")
            add_column_if_missing(conn, "item_id", "item_id VARCHAR(255)")
            add_column_if_missing(conn, "item_type", "item_type VARCHAR(100)")
            add_column_if_missing(conn, "item_label", "item_label TEXT")
            add_column_if_missing(conn, "item_position", "item_position INTEGER")
            add_column_if_missing(conn, "section", "section VARCHAR(100)")
            add_column_if_missing(conn, "visibility_threshold", "visibility_threshold DOUBLE PRECISION")
            add_column_if_missing(conn, "action_type", "action_type VARCHAR(100)")
            add_column_if_missing(conn, "action_target", "action_target TEXT")
            add_column_if_missing(conn, "action_value", "action_value TEXT")
            add_column_if_missing(conn, "is_bot", "is_bot BOOLEAN DEFAULT FALSE")
            add_column_if_missing(conn, "bot_reason", "bot_reason VARCHAR(255)")
            add_column_if_missing(conn, "is_internal", "is_internal BOOLEAN DEFAULT FALSE")

            create_index_if_missing(conn, "idx_swallow_ts_utc", f"CREATE INDEX idx_swallow_ts_utc ON {TABLE_NAME}(ts_utc)")
            create_index_if_missing(conn, "idx_swallow_event_type", f"CREATE INDEX idx_swallow_event_type ON {TABLE_NAME}(event_type)")
            create_index_if_missing(conn, "idx_swallow_event_id_unique", f"CREATE UNIQUE INDEX idx_swallow_event_id_unique ON {TABLE_NAME}(event_id)")
            create_index_if_missing(conn, "idx_swallow_visitor_id", f"CREATE INDEX idx_swallow_visitor_id ON {TABLE_NAME}(visitor_id)")
            create_index_if_missing(conn, "idx_swallow_session_id", f"CREATE INDEX idx_swallow_session_id ON {TABLE_NAME}(session_id)")
            create_index_if_missing(conn, "idx_swallow_page_load_id", f"CREATE INDEX idx_swallow_page_load_id ON {TABLE_NAME}(page_load_id)")
            create_index_if_missing(conn, "idx_swallow_item_id", f"CREATE INDEX idx_swallow_item_id ON {TABLE_NAME}(item_id)")
            create_index_if_missing(conn, "idx_swallow_event_ts", f"CREATE INDEX idx_swallow_event_ts ON {TABLE_NAME}(event_type, ts_utc)")
            create_index_if_missing(conn, "idx_swallow_page_event_ts", f"CREATE INDEX idx_swallow_page_event_ts ON {TABLE_NAME}(page_path, event_type, ts_utc)")
            create_index_if_missing(
                conn,
                "idx_swallow_human_ts",
                f"CREATE INDEX idx_swallow_human_ts ON {TABLE_NAME}(ts_utc) WHERE COALESCE(is_bot, FALSE) = FALSE AND COALESCE(is_internal, FALSE) = FALSE",
            )
    except Exception as exc:
        print(f"⚠️ Migration warning: {exc}")


def get_client_ip(request: Request) -> str:
    client_ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "")
    return client_ip.split(",")[0].strip() if client_ip else ""


def get_country_code(client_ip: str) -> str:
    if not geoip_reader or not client_ip:
        return "ZZ"

    try:
        response = geoip_reader.country(client_ip)
        country_code = response.country.iso_code or "ZZ"
        print(f"🌍 {client_ip} → {country_code}")
        return country_code
    except AddressNotFoundError:
        return "ZZ"
    except Exception as exc:
        print(f"⚠️ GeoIP {client_ip}: {exc}")
        return "ZZ"


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def clip(value: object, limit: int) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value[:limit] if text_value else None


def parse_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def parse_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def normalize_identifier(value: object, field_name: str, *, limit: int = 128) -> str | None:
    value = clip(value, limit)
    if value is None:
        return None
    if not ID_PATTERN.fullmatch(value):
        raise HTTPException(422, f"❌ {field_name} contiene caratteri non validi")
    return value


def detect_bot(user_agent: str, referrer: str) -> tuple[bool, str | None]:
    user_agent_lower = user_agent.lower()
    referrer_lower = referrer.lower()

    for pattern in BOT_PATTERNS:
        if pattern in user_agent_lower:
            return True, f"user_agent:{pattern}"

    if "developers.google.com" in referrer_lower:
        return True, "referrer:google_preview"

    return False, None


def detect_internal(request: Request, payload: dict) -> bool:
    if bool(payload.get("internal")):
        return True

    if not INTERNAL_TRAFFIC_SECRET:
        return False

    provided_secret = (
        request.headers.get("x-analytics-secret")
        or request.query_params.get("analytics_secret")
        or str(payload.get("internal_secret") or "").strip()
    )
    return provided_secret == INTERNAL_TRAFFIC_SECRET


def validate_event_payload(data: dict) -> dict:
    event_type = clip(data.get("event_type"), 32)
    if event_type not in ALLOWED_EVENT_TYPES:
        raise HTTPException(400, "❌ event_type non valido")

    page_path = clip(data.get("page_path"), 500)
    if not page_path:
        raise HTTPException(400, "❌ page_path obbligatorio")

    ts_raw = clip(data.get("ts_utc"), 64)
    if not ts_raw:
        raise HTTPException(400, "❌ ts_utc obbligatorio")

    normalized = {
        "event_type": event_type,
        "page_path": page_path,
        "referrer": clip(data.get("referrer"), 500) or "",
        "ts_utc": parse_timestamp(ts_raw),
        "event_id": normalize_identifier(data.get("event_id"), "event_id", limit=64) or str(uuid.uuid4()),
        "visitor_id": normalize_identifier(data.get("visitor_id"), "visitor_id"),
        "session_id": normalize_identifier(data.get("session_id"), "session_id"),
        "page_load_id": normalize_identifier(data.get("page_load_id"), "page_load_id"),
        "item_id": clip(data.get("item_id"), 255),
        "item_type": clip(data.get("item_type"), 100),
        "item_label": clip(data.get("item_label"), 500),
        "item_position": parse_int(data.get("item_position")),
        "section": clip(data.get("section"), 100),
        "visibility_threshold": parse_float(data.get("visibility_threshold")),
        "action_type": clip(data.get("action_type"), 100),
        "action_target": clip(data.get("action_target"), 500),
        "action_value": clip(data.get("action_value"), 500),
    }

    if event_type == "impression" and not normalized["item_id"]:
        raise HTTPException(400, "❌ item_id obbligatorio per impression")

    if event_type == "engagement" and not normalized["action_type"]:
        raise HTTPException(400, "❌ action_type obbligatorio per engagement")

    return normalized


@asynccontextmanager
async def lifespan(app: FastAPI):
    global geoip_reader
    run_migrations()

    try:
        geoip_reader = geoip2.database.Reader(GEOIP_DB_PATH)
        print(f"✅ GeoIP: {GEOIP_DB_PATH}")
    except Exception as exc:
        print(f"❌ GeoIP init error: {exc}")

    yield

    if geoip_reader:
        geoip_reader.close()


app = FastAPI(title="Swallow Analytics + GeoIP", lifespan=lifespan)


@app.get("/")
async def health_check():
    return {
        "status": "🟢 OK",
        "endpoint": "/track (POST)",
        "event_types": sorted(ALLOWED_EVENT_TYPES),
        "geoip": "✅" if geoip_reader else "❌",
        "db_url": DATABASE_URL.split("@")[1].split("/")[0] if "@" in DATABASE_URL else "hidden",
    }


@app.get("/test-geoip")
async def test_geoip(request: Request):
    if not geoip_reader:
        return {"error": "GeoIP non inizializzato"}

    client_ip = get_client_ip(request)
    try:
        response = geoip_reader.country(client_ip)
        return {
            "client_ip": client_ip,
            "country_code": response.country.iso_code or "ZZ",
            "country_name": response.country.name or "Unknown",
        }
    except AddressNotFoundError:
        return {"error": "IP non trovato nel DB", "ip": client_ip}
    except Exception as exc:
        return {"error": str(exc), "ip": client_ip}


@app.post("/track")
async def track_event(request: Request):
    try:
        data = await request.json()
        if not isinstance(data, dict):
            raise HTTPException(400, "❌ payload JSON non valido")

        referrer = (data.get("referrer") or "").lower()
        if BLOCKED_REF_SUBSTR in referrer:
            return {"status": "ignored", "reason": "squarespace_preview"}

        event = validate_event_payload(data)
        client_ip = get_client_ip(request)
        user_agent = clip(request.headers.get("user-agent"), 1000) or ""
        country_code = get_country_code(client_ip)
        is_bot, bot_reason = detect_bot(user_agent, event["referrer"])
        is_internal = detect_internal(request, data)

        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"""
                    INSERT INTO {TABLE_NAME} (
                        event_type,
                        page_path,
                        referrer,
                        user_agent,
                        ts_utc,
                        country_code,
                        event_id,
                        visitor_id,
                        session_id,
                        page_load_id,
                        item_id,
                        item_type,
                        item_label,
                        item_position,
                        section,
                        visibility_threshold,
                        action_type,
                        action_target,
                        action_value,
                        is_bot,
                        bot_reason,
                        is_internal
                    )
                    VALUES (
                        :event_type,
                        :page_path,
                        :referrer,
                        :user_agent,
                        :ts_utc,
                        :country_code,
                        :event_id,
                        :visitor_id,
                        :session_id,
                        :page_load_id,
                        :item_id,
                        :item_type,
                        :item_label,
                        :item_position,
                        :section,
                        :visibility_threshold,
                        :action_type,
                        :action_target,
                        :action_value,
                        :is_bot,
                        :bot_reason,
                        :is_internal
                    )
                    ON CONFLICT (event_id) DO NOTHING
                    """
                ),
                {
                    **event,
                    "user_agent": user_agent,
                    "country_code": country_code,
                    "is_bot": is_bot,
                    "bot_reason": bot_reason,
                    "is_internal": is_internal,
                },
            )

        if result.rowcount == 0:
            return {"status": "ignored", "reason": "duplicate_event", "event_id": event["event_id"]}

        return {
            "status": "✅ OK",
            "event": event["event_type"],
            "country": country_code,
            "is_bot": is_bot,
            "is_internal": is_internal,
        }

    except ValueError as exc:
        raise HTTPException(422, f"❌ Payload non valido: {exc}") from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"❌ Errore: {exc}") from exc


@app.get("/stats/minute")
async def stats_minute():
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"""
                    SELECT date_trunc('minute', ts_utc::timestamptz) AS minute,
                           event_type,
                           COUNT(*) AS count
                    FROM {TABLE_NAME}
                    WHERE ts_utc >= NOW() - INTERVAL '10 minutes'
                      AND COALESCE(is_bot, FALSE) = FALSE
                      AND COALESCE(is_internal, FALSE) = FALSE
                    GROUP BY 1, 2
                    ORDER BY 1 DESC, 2 ASC
                    LIMIT 40
                    """
                )
            )
            return {"stats": [dict(row._mapping) for row in result]}
    except Exception as exc:
        raise HTTPException(500, f"❌ Stats: {exc}") from exc


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
