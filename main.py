# Swallow's Notes Analytics API
# Railway + FastAPI + Postgres

from fastapi import FastAPI, Request, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import os
from dotenv import load_dotenv

# Carica .env per test locali
load_dotenv()

BLOCKED_REF_SUBSTR = "https://orca-tetra-d4nz.squarespace.com"

app = FastAPI(title="Swallow's Notes Analytics")

# DATABASE_URL: Railway lo imposta automaticamente, .env per locale
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL mancante! Imposta in .env o Railway.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

@app.get("/")
async def health_check():
    """Test salute API"""
    return {
        "status": "🟢 OK", 
        "endpoint": "/track (POST)",
        "db_url": DATABASE_URL.split('@')[1].split('/')[0] if '@' in DATABASE_URL else "hidden"
    }

@app.post("/track")
async def track_event(request: Request):
    """Registra evento: page_view o impression"""
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
        
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO "swallow-analysis" (
                        event_type, page_path, referrer, user_agent, ts_utc
                    ) VALUES (
                        :event_type, :page_path, :referrer, :user_agent, :ts_utc
                    )
                """),
                {
                    "event_type": data["event_type"],
                    "page_path": data["page_path"][:500],  # Truncate lungo URLs
                    "referrer": data.get("referrer", "")[:500],
                    "user_agent": request.headers.get("user-agent", "")[:1000],
                    "ts_utc": datetime.fromisoformat(data["ts_utc"].replace("Z", "+00:00"))
                }
            )
        
        return {"status": "✅ OK", "event": data["event_type"]}
    
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
