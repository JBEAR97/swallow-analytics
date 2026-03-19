# Swallow's Notes Analytics API - FIXED GEOIP per Railway
# Usa X-Forwarded-For per IP reali

from fastapi import FastAPI, Request, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import geoip2.database
from geoip2.errors import AddressNotFoundError

load_dotenv()

BLOCKED_REF_SUBSTR = "https://orca-tetra-d4nz.squarespace.com"

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL mancante!")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

GEOIP_DB_PATH = os.path.join(os.path.dirname(__file__), "data", "GeoLite2-Country.mmdb")

def run_migrations():
    """Run database migrations on startup"""
    try:
        with engine.begin() as conn:
            # created_at migration
            result = conn.execute(text("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'swallow-analysis' AND column_name = 'created_at'
            """))
            if not result.fetchone():
                conn.execute(text('ALTER TABLE "swallow-analysis" ADD COLUMN created_at TIMESTAMP DEFAULT NOW()'))
                conn.execute(text('UPDATE "swallow-analysis" SET created_at = ts_utc'))
                print("✅ Migration: added created_at")
            
            # country_code migration
            result = conn.execute(text("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'swallow-analysis' AND column_name = 'country_code'
            """))
            if not result.fetchone():
                conn.execute(text('ALTER TABLE "swallow-analysis" ADD COLUMN country_code VARCHAR(2) DEFAULT \'ZZ\''))
                print("✅ Migration: added country_code")
                
    except Exception as e:
        print(f"⚠️ Migration warning: {e}")

geoip_reader = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global geoip_reader
    run_migrations()
    
    # GeoIP startup
    try:
        geoip_reader = geoip2.database.Reader(GEOIP_DB_PATH)
        print(f"✅ GeoIP: {GEOIP_DB_PATH}")
    except Exception as e:
        print(f"❌ GeoIP init error: {e}")
    
    yield
    
    if geoip_reader:
        geoip_reader.close()

app = FastAPI(title="Swallow Analytics + GeoIP", lifespan=lifespan)

@app.get("/")
async def health_check():
    return {
        "status": "🟢 OK", 
        "endpoint": "/track (POST)",
        "geoip": "✅" if geoip_reader else "❌",
        "db_url": DATABASE_URL.split('@')[1].split('/')[0] if '@' in DATABASE_URL else "hidden"
    }

@app.get("/test-geoip")
async def test_geoip(request: Request):
    """Test GeoIP reale (usa X-Forwarded-For)"""
    global geoip_reader
    if not geoip_reader:
        return {"error": "GeoIP non inizializzato"}
    
    # FIXED: X-Forwarded-For per Railway proxy
    client_ip = request.headers.get("x-forwarded-for", request.client.host)
    if client_ip:
        client_ip = client_ip.split(",")[0].strip()
    
    try:
        response = geoip_reader.country(client_ip)
        return {
            "client_ip": client_ip,
            "country_code": response.country.iso_code or "ZZ",
            "country_name": response.country.name or "Unknown"
        }
    except AddressNotFoundError:
        return {"error": "IP non trovato nel DB", "ip": client_ip}
    except Exception as e:
        return {"error": str(e), "ip": client_ip}

@app.post("/track")
async def track_event(request: Request):
    """Track + COUNTRY reale da X-Forwarded-For"""
    global geoip_reader
    try:
        data = await request.json()
        
        # Block squarespace preview
        ref = (data.get("referrer") or "").lower()
        if BLOCKED_REF_SUBSTR in ref:
            return {"status": "ignored", "reason": "squarespace_preview"}
        
        required = ["event_type", "page_path", "ts_utc"]
        if any(not data.get(k) for k in required):
            raise HTTPException(400, f"❌ Campi mancanti: {required}")
        
        if data["event_type"] not in ["page_view", "impression"]:
            raise HTTPException(400, "❌ event_type: 'page_view' o 'impression'")
        
        # FIXED GEOIP: IP reale da proxy headers
        country_code = "ZZ"
        if geoip_reader:
            client_ip = request.headers.get("x-forwarded-for", request.client.host)
            if client_ip:
                client_ip = client_ip.split(",")[0].strip()
                try:
                    response = geoip_reader.country(client_ip)
                    country_code = response.country.iso_code or "ZZ"
                    print(f"🌍 {client_ip} → {country_code}")  # Railway logs
                except AddressNotFoundError:
                    pass  # ZZ già settato
                except Exception as ge:
                    print(f"⚠️ GeoIP {client_ip}: {ge}")
        
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO "swallow-analysis" 
                    (event_type, page_path, referrer, user_agent, ts_utc, country_code)
                    VALUES (:event_type, :page_path, :referrer, :user_agent, :ts_utc, :country_code)
                """),
                {
                    "event_type": data["event_type"],
                    "page_path": data["page_path"][:500],
                    "referrer": data.get("referrer", "")[:500],
                    "user_agent": request.headers.get("user-agent", "")[:1000],
                    "ts_utc": datetime.fromisoformat(data["ts_utc"].replace("Z", "+00:00")),
                    "country_code": country_code
                }
            )
        
        return {"status": "✅ OK", "event": data["event_type"], "country": country_code}
    
    except ValueError as e:
        raise HTTPException(422, f"❌ Data: {e}")
    except Exception as e:
        raise HTTPException(500, f"❌ Errore: {e}")

@app.get("/stats/minute")
async def stats_minute():
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT date_trunc('minute', ts_utc::timestamp) AS minute,
                       event_type, COUNT(*) AS count
                FROM "swallow-analysis" 
                WHERE ts_utc >= NOW() - INTERVAL '10 minutes'
                GROUP BY 1,2 ORDER BY 1 DESC LIMIT 10
            """))
            return {"stats": [dict(row._mapping) for row in result]}
    except Exception as e:
        raise HTTPException(500, f"❌ Stats: {e}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
