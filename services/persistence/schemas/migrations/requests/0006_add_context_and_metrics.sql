ALTER TABLE requests RENAME COLUMN source TO peer_addr;
ALTER TABLE requests ADD COLUMN user TEXT;
ALTER TABLE requests ADD COLUMN local_addr TEXT;
ALTER TABLE requests ADD COLUMN mode TEXT;
ALTER TABLE requests ADD COLUMN behave_as TEXT;

CREATE TABLE IF NOT EXISTS metrics (
  id INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL,
  request_id TEXT,
  user TEXT,
  peer_addr TEXT,
  local_addr TEXT,
  mode TEXT,
  behave_as TEXT,
  method TEXT,
  path TEXT,
  url TEXT,
  status INTEGER,
  request_error TEXT,
  account_id TEXT,
  provider_id TEXT,
  latency_ms INTEGER,

  inbound_req_method   TEXT,
  inbound_req_url      TEXT,
  inbound_req_headers  BLOB,

  inbound_resp_status  INTEGER,
  inbound_resp_headers BLOB,
  inbound_resp_body    BLOB
);

CREATE INDEX IF NOT EXISTS idx_metrics_ts       ON metrics(ts);
CREATE INDEX IF NOT EXISTS idx_metrics_local_addr ON metrics(local_addr);
CREATE INDEX IF NOT EXISTS idx_metrics_provider ON metrics(provider_id);
CREATE INDEX IF NOT EXISTS idx_metrics_account  ON metrics(account_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_request_id ON metrics(request_id) WHERE request_id IS NOT NULL;
