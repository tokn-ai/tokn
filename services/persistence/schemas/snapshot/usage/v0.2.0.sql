-- Canonical current schema for usage.db.
-- Regenerated whenever a new NNN_*.sql migration is added so that fresh
-- installs can jump straight here instead of replaying history.
-- Must remain equivalent to the cumulative effect of 001..005.

CREATE TABLE requests (
  id            INTEGER PRIMARY KEY,
  ts            INTEGER NOT NULL,
  session_id    TEXT,
  request_id    TEXT,
  project_id    TEXT,
  ver           TEXT,
  request_error TEXT,
  endpoint      TEXT,
  account_id    TEXT,
  provider_id   TEXT,
  model         TEXT    NOT NULL,
  params_json   TEXT,
  usage_json    TEXT,
  ctx_json      TEXT,
  status        INTEGER
);
CREATE INDEX idx_requests_ts      ON requests(ts);
CREATE INDEX idx_requests_session ON requests(session_id);
CREATE UNIQUE INDEX idx_requests_request ON requests(request_id);
CREATE INDEX idx_requests_project ON requests(project_id);
CREATE INDEX idx_requests_account ON requests(account_id);
