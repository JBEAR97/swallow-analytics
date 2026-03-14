CREATE TABLE IF NOT EXISTS web_events (
  id BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL CHECK (event_type IN ('page_view', 'impression')),
  page_path TEXT NOT NULL,
  referrer TEXT,
  user_agent TEXT,
  ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ts_utc ON web_events(ts_utc);
CREATE INDEX IF NOT EXISTS idx_minute ON web_events(date_trunc('minute', ts_utc));
