-- Squashed requests migrations from snapshot v0.0.0 to snapshot v0.1.1.
-- Covers schema versions 0001 through 0006.

ALTER TABLE requests ADD COLUMN request_id TEXT;
ALTER TABLE requests ADD COLUMN request_error TEXT;

-- Rename prompt_tok/completion_tok to input_tok/output_tok and add
-- cached_tok and reasoning_tok columns for breakdown of usage tokens.
ALTER TABLE requests RENAME COLUMN prompt_tok TO input_tok;
ALTER TABLE requests RENAME COLUMN completion_tok TO output_tok;
ALTER TABLE requests ADD COLUMN cached_tok INTEGER;
ALTER TABLE requests ADD COLUMN reasoning_tok INTEGER;

ALTER TABLE requests ADD COLUMN latency_ms_nullable INTEGER;
UPDATE requests SET latency_ms_nullable = latency_ms;
ALTER TABLE requests DROP COLUMN latency_ms;
ALTER TABLE requests RENAME COLUMN latency_ms_nullable TO latency_ms;

ALTER TABLE requests ADD COLUMN status_nullable INTEGER;
UPDATE requests SET status_nullable = status;
ALTER TABLE requests DROP COLUMN status;
ALTER TABLE requests RENAME COLUMN status_nullable TO status;

ALTER TABLE requests ADD COLUMN stream_nullable INTEGER;
UPDATE requests SET stream_nullable = stream;
ALTER TABLE requests DROP COLUMN stream;
ALTER TABLE requests RENAME COLUMN stream_nullable TO stream;

DROP INDEX idx_requests_session;
ALTER TABLE requests ADD COLUMN session_id_nullable TEXT;
UPDATE requests SET session_id_nullable = session_id;
ALTER TABLE requests DROP COLUMN session_id;
ALTER TABLE requests RENAME COLUMN session_id_nullable TO session_id;
CREATE INDEX idx_requests_session ON requests(session_id);

ALTER TABLE requests ADD COLUMN inbound_req_headers_nullable BLOB;
UPDATE requests SET inbound_req_headers_nullable = inbound_req_headers;
ALTER TABLE requests DROP COLUMN inbound_req_headers;
ALTER TABLE requests RENAME COLUMN inbound_req_headers_nullable TO inbound_req_headers;

ALTER TABLE requests ADD COLUMN inbound_req_body_nullable BLOB;
UPDATE requests SET inbound_req_body_nullable = inbound_req_body;
ALTER TABLE requests DROP COLUMN inbound_req_body;
ALTER TABLE requests RENAME COLUMN inbound_req_body_nullable TO inbound_req_body;

ALTER TABLE requests ADD COLUMN inbound_resp_headers_nullable BLOB;
UPDATE requests SET inbound_resp_headers_nullable = inbound_resp_headers;
ALTER TABLE requests DROP COLUMN inbound_resp_headers;
ALTER TABLE requests RENAME COLUMN inbound_resp_headers_nullable TO inbound_resp_headers;

ALTER TABLE requests ADD COLUMN inbound_resp_body_nullable BLOB;
UPDATE requests SET inbound_resp_body_nullable = inbound_resp_body;
ALTER TABLE requests DROP COLUMN inbound_resp_body;
ALTER TABLE requests RENAME COLUMN inbound_resp_body_nullable TO inbound_resp_body;

ALTER TABLE requests ADD COLUMN latency_header_ms INTEGER;
CREATE UNIQUE INDEX idx_requests_request_id ON requests(request_id);

ALTER TABLE requests ADD COLUMN source TEXT;
ALTER TABLE requests ADD COLUMN method TEXT;

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
