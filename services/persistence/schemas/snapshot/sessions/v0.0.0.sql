-- Historical initial schema for sessions.db. Frozen on first release;
-- never edited. Existing user databases walk forward from here via 002+.
--
-- See 000_bootstrap.sql for the current canonical shape and the rationale.

CREATE TABLE sessions (
  id            TEXT PRIMARY KEY,
  first_seen_ts INTEGER NOT NULL,
  last_seen_ts  INTEGER NOT NULL,
  source        TEXT    NOT NULL,
  account_id    TEXT,
  provider_id   TEXT,
  model         TEXT,
  message_count INTEGER NOT NULL DEFAULT 0,
  part_count    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_sessions_last ON sessions(last_seen_ts);

CREATE TABLE part_blobs (
  hash      TEXT PRIMARY KEY,
  part_type TEXT NOT NULL,
  content   BLOB NOT NULL
);

CREATE TABLE session_parts (
  session_id  TEXT    NOT NULL REFERENCES sessions(id),
  part_seq    INTEGER NOT NULL,
  message_seq INTEGER NOT NULL,
  part_index  INTEGER NOT NULL,
  ts          INTEGER NOT NULL,
  endpoint    TEXT    NOT NULL,
  role        TEXT    NOT NULL,
  status      INTEGER,
  part_hash   TEXT    NOT NULL REFERENCES part_blobs(hash),
  PRIMARY KEY (session_id, part_seq)
);
CREATE INDEX idx_session_parts_msg  ON session_parts(session_id, message_seq, part_index);
CREATE INDEX idx_session_parts_hash ON session_parts(part_hash);
