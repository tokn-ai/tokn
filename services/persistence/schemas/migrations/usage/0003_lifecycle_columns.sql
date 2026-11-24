-- Make columns nullable for INSERT+UPDATE lifecycle pattern.
-- Add endpoint column.

-- SQLite doesn't support ALTER COLUMN, so we recreate the table.
CREATE TABLE requests_new (
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
  prompt_tok     INTEGER,
  completion_tok INTEGER,
  latency_ms     INTEGER,
  status         INTEGER,
  stream         INTEGER NOT NULL DEFAULT 0
);

INSERT INTO requests_new (id, ts, session_id, request_id, project_id, account_id, provider_id, model, initiator, prompt_tok, completion_tok, latency_ms, status, stream)
  SELECT id, ts, session_id, request_id, project_id, account_id, provider_id, model, initiator, prompt_tok, completion_tok, latency_ms, status, stream FROM requests;

DROP TABLE requests;
ALTER TABLE requests_new RENAME TO requests;

CREATE INDEX idx_requests_ts      ON requests(ts);
CREATE INDEX idx_requests_session ON requests(session_id);
CREATE UNIQUE INDEX idx_requests_request ON requests(request_id);
CREATE INDEX idx_requests_project ON requests(project_id);
CREATE INDEX idx_requests_account ON requests(account_id);
