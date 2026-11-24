-- Canonical current schema for sessions.db.
-- Regenerated whenever a new NNN_*.sql migration is added so that fresh
-- installs can jump straight here instead of replaying history.
-- Must remain equivalent to the cumulative effect of 001..NNN.
--
-- Mental model: a session is an ordered list of *parts*. A "message" is
-- merely a logical grouping of consecutive parts that share a role / turn,
-- exposed via the `message_seq` column rather than a separate table.

CREATE TABLE sessions (
  id            TEXT PRIMARY KEY,
  first_seen_ts INTEGER NOT NULL,
  last_seen_ts  INTEGER NOT NULL,
  source        TEXT    NOT NULL,        -- 'header' | 'auto'
  account_id    TEXT,
  provider_id   TEXT,
  model         TEXT,
  message_count INTEGER NOT NULL DEFAULT 0,
  part_count    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_sessions_last ON sessions(last_seen_ts);

-- Content-addressed part store; identical parts dedupe across the whole DB.
CREATE TABLE part_blobs (
  hash      TEXT PRIMARY KEY,            -- sha256(part_type || 0x00 || content)
  part_type TEXT NOT NULL,               -- 'text' | 'image_url' | 'tool_use' | …
  content   BLOB NOT NULL
);

-- Each row = one part of a session, in order.
CREATE TABLE session_parts (
  session_id  TEXT    NOT NULL REFERENCES sessions(id),
  part_seq    INTEGER NOT NULL,          -- monotonic 0..N within the session
  message_seq INTEGER NOT NULL,          -- groups parts of the same message
  part_index  INTEGER NOT NULL,          -- position within the message
  ts          INTEGER NOT NULL,
  endpoint    TEXT    NOT NULL,
  role        TEXT    NOT NULL,
  status      INTEGER,
  part_hash   TEXT    NOT NULL REFERENCES part_blobs(hash),
  PRIMARY KEY (session_id, part_seq)
);
CREATE INDEX idx_session_parts_msg  ON session_parts(session_id, message_seq, part_index);
CREATE INDEX idx_session_parts_hash ON session_parts(part_hash);
