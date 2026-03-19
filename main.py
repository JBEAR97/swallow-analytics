# Swallow's Notes Analytics API
# Railway + FastAPI + Postgres + GeoIP Country Tracking

from fastapi import FastAPI, Request, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import geoip2.database  # Per GeoLite2
from geoip2.errors import AddressNotFoundError

# Carica .env per test locali
load_dotenv()

BLOCKED_REF_SUBSTR = "https://orca-tetra-d4nz.squarespace.com"

# DATABASE_URL: Railway lo imposta automaticamente, .env per locale
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL mancante! Imposta in .env o Railway.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Path al DB GeoLite2 (versionato in data/)
GEOIP_DB_PATH = os.path.join(os.path.dirname(__file__), "data", "GeoLite2-Country.mmdb")

def run_migrations():
    """Run database migrations on startup"""
    try:
        with engine.begin() as conn:
            # Check if created_at column exists
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'swallow-analysis' 
                AND column_name = 'created_at'
            """))
            
            if not result.fetchone():
                # Column doesn't exist, add it
                conn.execute(text("""
                    ALTER TABLE "swallow-analysis"
                    ADD COLUMN created_at TIMESTAMP DEFAULT NOW()
                """))
                
                # Update ALL existing rows to use ts_utc value
                conn.execute(text("""
                    UPDATE "swallow-analysis"
                    SET created_at = ts_utc
                """))
                print("✅ Migration: added created_at column and backfilled ALL rows")
            else:
                print("✅ Migration: created_at column already exists")
                
            # NUOVA MIGRATION: Check if country_code column exists
            result_country = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'swallow-analysis' 
                AND column_name = 'country_code'
            """))
            
            if not result_country.fetchone():
                # Add country_code column
                conn.execute(text("""
                    ALTER TABLE "swallow-analysis"
                    ADD COLUMN country_code VARCHAR(2) DEFAULT 'ZZ'
                """))
                print("✅ Migration: added country_code column")
            else:
                print("✅ Migration: country_code column already exists")
                
    except Exception as e:
        print(f"⚠️ Migration warning: {e}")

# Variabile globale per il reader (inizializzato in lifespan)
geoip_reader = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global geoip_reader
    # Startup
    run_migrations()
    
    # Inizializza GeoIP reader
    try:
        geoip_reader = geoip2.database.Reader(GEOIP_DB_PATH)
        print(f"✅ GeoIP reader inizializzato: {GEOIP_DB_PATH}")
    except FileNotFoundError:
        print(f"❌ File GeoLite2 non trovato: {GEOIP_DB_PATH}. Scaricalo da MaxMind.")
    except Exception as e:
        print(f"❌ Errore inizializzazione GeoIP: {e}")
    
    yield
    # Shutdown
    if geoip_reader:
        geoip_reader.close()
        print("✅ GeoIP reader chiuso")
    pass

app = FastAPI(title="Swallow's Notes Analytics", lifespan=lifespan)

@app.get("/")
async def health_check():
    """Test salute API"""
    return {
        "status": "🟢 OK", 
        "endpoint": "/track (POST)",
        "geoip": "✅" if geoip_reader else "❌",
        "db_url": DATABASE_URL.split('@')[1].split('/')[0] if '@' in DATABASE_URL else "hidden"
    }

@app.post("/track")
async def track_event(request: Request):
    """Registra evento: page_view o impression + COUNTRY da IP"""
    global geoip_reader
    try:
        data = await request.json()
        
        # NUOVO: Filtra Squarespace preview referrer
        ref = (data.get("referrer") or "").lower()
        if BLOCKED_REF_SUBSTR in ref:
            return {"status": "ignored", "reason": "squarespace_preview_referrer"}
        
        # Validazione minima
        required = ["event_type", "page_path", "ts_utc"]
        missing = [k for k in required if not data.get(k)]
        if missing:
            raise HTTPException(400, f"❌ Campi mancanti: {missing}")
        
        # Event types consentiti
        if data["event_type"] not in ["page_view", "impression"]:
            raise HTTPException(400, "❌ event_type: solo 'page_view' o 'impression'")
        
        # GEOIP: Estrai country_code dall'IP del client
        country_code = "ZZ"  # Default sconosciuto
        if geoip_reader:
            client_ip = request.client.host
            try:
                response = geoip_reader.country(client_ip)
                country_code = response.country.iso_code or "ZZ"
            except AddressNotFoundError:
                country_code = "ZZ"
            except Exception as ge:
                print(f"⚠️ GeoIP error per {client_ip}: {ge}")
                country_code = "ZZ"
        
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO "swallow-analysis" (
                        event_type, page_path, referrer, user_agent, ts_utc, country_code
                    ) VALUES (
                        :event_type, :page_path, :referrer, :user_agent, :ts_utc, :country_code
                    )
                """),
                {
                    "event_type": data["event_type"],
                    "page_path": data["page_path"][:500],  # Truncate lungo URLs
                    "referrer": data.get("referrer", "")[:500],
                    "user_agent": request.headers.get("user-agent", "")[:1000],
                    "ts_utc": datetime.fromisoformat(data["ts_utc"].replace("Z", "+00:00")),
                    "country_code": country_code
                }
            )
        
        return {"status": "✅ OK", "event": data["event_type"], "country": country_code}
    
    except ValueError as e:
        raise HTTPException(422, f"❌ Formato data: {e}")
    except SQLAlchemyError as e:
        raise HTTPException(500, f"❌ DB errore: {e}")
    except Exception as e:
        raise HTTPException(500, f"❌ Errore: {e}")

@app.get("/stats/minute")
async def stats_minute():
    """Quick stats ultimi 10 minuti (test)"""
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT 
                    date_trunc('minute', ts_utc::timestamp) AS minute,
                    event_type,
                    COUNT(*) AS count,
                    COUNT(DISTINCT page_path) AS pages
                FROM "swallow-analysis" 
                WHERE ts_utc >= NOW() - INTERVAL '10 minutes'
                GROUP BY 1, 2 
                ORDER BY 1 DESC 
                LIMIT 10
            """))
            
            rows = [dict(row._mapping) for row in result]
        return {"stats": rows}
    except Exception as e:
        raise HTTPException(500, f"❌ Stats errore: {e}")

# NUOVO: Endpoint per testare GeoIP
@app.get("/test-geoip")
async def test_geoip(request: Request):
    """Test GeoIP sul tuo IP"""
    global geoip_reader
    if not geoip_reader:
        return {"error": "GeoIP non inizializzato"}
    
    client_ip = request.client.host
    try:
        response = geoip_reader.country(client_ip)
        country_code = response.country.iso_code or "ZZ"
        country_name = response.country.name or "Unknown"
        return {
            "your_ip": client_ip,
            "country_code": country_code,
            "country_name": country_name
        }
    except Exception as e:
        return {"error": str(e), "ip": client_ip}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
