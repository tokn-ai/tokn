-- Historical initial schema for requests/<YYYY-MM-DD>.db. Frozen on first
-- release; never edited. Existing user databases walk forward from here
-- via 002+.

CREATE TABLE requests (
  id INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL,
  session_id TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  account_id TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  model TEXT NOT NULL,
  initiator TEXT NOT NULL,
  status INTEGER NOT NULL,
  stream INTEGER NOT NULL,
  latency_ms INTEGER NOT NULL,
  prompt_tok INTEGER,
  completion_tok INTEGER,

  inbound_req_method   TEXT,
  inbound_req_url      TEXT,
  inbound_req_headers  BLOB NOT NULL,
  inbound_req_body     BLOB NOT NULL,

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
