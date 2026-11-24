-- Canonical current schema for requests/<YYYY-MM-DD>.db.
-- Regenerated whenever a new NNN_*.sql migration is added so that fresh
-- day files can jump straight here instead of replaying history.
-- Must remain equivalent to the cumulative effect of 001..NNN.

CREATE TABLE requests (
  id INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL,
  session_id TEXT,
  user TEXT,
  peer_addr TEXT,
  local_addr TEXT,
  mode TEXT,
  behave_as TEXT,
  method TEXT,
  request_id TEXT,
  request_error TEXT,
  endpoint TEXT NOT NULL,
  account_id TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  model TEXT NOT NULL,
  initiator TEXT NOT NULL,
  status INTEGER,
  stream INTEGER,
  latency_ms INTEGER,
  latency_header_ms INTEGER,
  input_tok INTEGER,
  output_tok INTEGER,
  cached_tok INTEGER,
  reasoning_tok INTEGER,

  inbound_req_method   TEXT,
  inbound_req_url      TEXT,
  inbound_req_headers  BLOB,
  inbound_req_body     BLOB,

  outbound_req_method  TEXT,
  outbound_req_url     TEXT,
  outbound_req_headers BLOB,
  outbound_req_body    BLOB,

  outbound_resp_status  INTEGER,
  outbound_resp_headers BLOB,
  outbound_resp_body    BLOB,

  inbound_resp_status  INTEGER,
  inbound_resp_headers BLOB,
  inbound_resp_body    BLOB
);
CREATE INDEX idx_requests_ts      ON requests(ts);
CREATE INDEX idx_requests_session ON requests(session_id);
CREATE INDEX idx_requests_account ON requests(account_id);
CREATE UNIQUE INDEX idx_requests_request_id ON requests(request_id);

CREATE TABLE metrics (
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
CREATE INDEX idx_metrics_ts       ON metrics(ts);
CREATE INDEX idx_metrics_local_addr ON metrics(local_addr);
CREATE INDEX idx_metrics_provider ON metrics(provider_id);
CREATE INDEX idx_metrics_account  ON metrics(account_id);
CREATE UNIQUE INDEX idx_metrics_request_id ON metrics(request_id) WHERE request_id IS NOT NULL;
