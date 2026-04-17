CREATE TABLE IF NOT EXISTS "swallow-analysis" (
  id BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL CHECK (event_type IN ('page_view', 'impression', 'engagement', 'heartbeat')),
  page_path TEXT NOT NULL,
  referrer TEXT,
  user_agent TEXT,
  ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  client_ts_utc TIMESTAMPTZ,
  country_code VARCHAR(2) DEFAULT 'ZZ',
  event_id VARCHAR(64),
  visitor_id VARCHAR(128),
  session_id VARCHAR(128),
  page_load_id VARCHAR(128),
  item_id VARCHAR(255),
  item_type VARCHAR(100),
  item_label TEXT,
  item_position INTEGER,
  section VARCHAR(100),
  visibility_threshold DOUBLE PRECISION,
  action_type VARCHAR(100),
  action_target TEXT,
  action_value TEXT,
  page_url TEXT,
  page_title VARCHAR(300),
  hostname VARCHAR(255),
  utm_source VARCHAR(150),
  utm_medium VARCHAR(150),
  utm_campaign VARCHAR(150),
  utm_term VARCHAR(150),
  utm_content VARCHAR(150),
  source VARCHAR(150),
  medium VARCHAR(150),
  campaign VARCHAR(150),
  device_category VARCHAR(32),
  browser VARCHAR(64),
  operating_system VARCHAR(64),
  is_conversion BOOLEAN DEFAULT FALSE,
  conversion_name VARCHAR(150),
  event_value DOUBLE PRECISION,
  is_bot BOOLEAN DEFAULT FALSE,
  bot_reason VARCHAR(255),
  is_internal BOOLEAN DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_swallow_event_id_unique ON "swallow-analysis"(event_id);
CREATE INDEX IF NOT EXISTS idx_swallow_ts_utc ON "swallow-analysis"(ts_utc);
CREATE INDEX IF NOT EXISTS idx_swallow_event_type ON "swallow-analysis"(event_type);
CREATE INDEX IF NOT EXISTS idx_swallow_visitor_id ON "swallow-analysis"(visitor_id);
CREATE INDEX IF NOT EXISTS idx_swallow_session_id ON "swallow-analysis"(session_id);
CREATE INDEX IF NOT EXISTS idx_swallow_page_load_id ON "swallow-analysis"(page_load_id);
CREATE INDEX IF NOT EXISTS idx_swallow_item_id ON "swallow-analysis"(item_id);
CREATE INDEX IF NOT EXISTS idx_swallow_event_ts ON "swallow-analysis"(event_type, ts_utc);
CREATE INDEX IF NOT EXISTS idx_swallow_page_event_ts ON "swallow-analysis"(page_path, event_type, ts_utc);
CREATE INDEX IF NOT EXISTS idx_swallow_session_ts ON "swallow-analysis"(session_id, ts_utc);
CREATE INDEX IF NOT EXISTS idx_swallow_source_medium_ts ON "swallow-analysis"(source, medium, ts_utc);
CREATE INDEX IF NOT EXISTS idx_swallow_conversion_ts ON "swallow-analysis"(is_conversion, ts_utc);
CREATE INDEX IF NOT EXISTS idx_swallow_human_ts
  ON "swallow-analysis"(ts_utc)
  WHERE COALESCE(is_bot, FALSE) = FALSE AND COALESCE(is_internal, FALSE) = FALSE;
