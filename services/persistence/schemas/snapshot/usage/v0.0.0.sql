-- Historical initial schema for usage.db. Frozen on first release; never
-- edited. Existing user databases walk forward from here via 002+.

CREATE TABLE requests (
  id             INTEGER PRIMARY KEY,
  ts             INTEGER NOT NULL,
  account_id     TEXT    NOT NULL,
  provider_id    TEXT    NOT NULL DEFAULT '',
  model          TEXT    NOT NULL,
  initiator      TEXT    NOT NULL DEFAULT 'user',
  prompt_tok     INTEGER,
  completion_tok INTEGER,
  latency_ms     INTEGER NOT NULL,
  status         INTEGER NOT NULL,
  stream         INTEGER NOT NULL
);
CREATE INDEX idx_requests_ts      ON requests(ts);
CREATE INDEX idx_requests_account ON requests(account_id);
