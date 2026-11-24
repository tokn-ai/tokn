-- Canonical current schema for usage.db.
-- Regenerated whenever a new NNN_*.sql migration is added so that fresh
-- installs can jump straight here instead of replaying history.
-- Must remain equivalent to the cumulative effect of 001..003.

CREATE TABLE requests (
  id             INTEGER PRIMARY KEY,
  ts             INTEGER NOT NULL,
  session_id     TEXT,
  request_id     TEXT,
  project_id     TEXT,
  endpoint       TEXT,
  account_id     TEXT,
  provider_id    TEXT,
  model          TEXT    NOT NULL,
  initiator      TEXT    NOT NULL DEFAULT 'user',
  input_tok      INTEGER,
  output_tok     INTEGER,
  cached_tok     INTEGER,
  reasoning_tok  INTEGER,
  latency_ms     INTEGER,
  status         INTEGER,
  stream         INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_requests_ts      ON requests(ts);
CREATE INDEX idx_requests_session ON requests(session_id);
CREATE UNIQUE INDEX idx_requests_request ON requests(request_id);
CREATE INDEX idx_requests_project ON requests(project_id);
CREATE INDEX idx_requests_account ON requests(account_id);
