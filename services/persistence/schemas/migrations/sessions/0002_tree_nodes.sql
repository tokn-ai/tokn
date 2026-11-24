DROP INDEX IF EXISTS idx_sessions_last;
DROP INDEX IF EXISTS idx_session_parts_msg;
DROP INDEX IF EXISTS idx_session_parts_hash;
DROP TABLE IF EXISTS session_parts;
ALTER TABLE sessions RENAME TO sessions_legacy;

CREATE TABLE sessions (
  id            TEXT PRIMARY KEY,
  first_seen_ts INTEGER NOT NULL,
  last_seen_ts  INTEGER NOT NULL,
  source        TEXT    NOT NULL,
  account_id    TEXT,
  provider_id   TEXT,
  model         TEXT
);
INSERT INTO sessions (id, first_seen_ts, last_seen_ts, source, account_id, provider_id, model)
SELECT id, first_seen_ts, last_seen_ts, source, account_id, provider_id, model
FROM sessions_legacy;
DROP TABLE sessions_legacy;
CREATE INDEX idx_sessions_last ON sessions(last_seen_ts);

CREATE TABLE session_nodes (
  id                     TEXT PRIMARY KEY,
  session_id             TEXT    NOT NULL REFERENCES sessions(id),
  parent_id              TEXT    REFERENCES session_nodes(id),
  request_id             TEXT    NOT NULL,
  ts                     INTEGER NOT NULL,
  endpoint               TEXT    NOT NULL,
  status                 INTEGER,
  account_id             TEXT,
  provider_id            TEXT,
  model                  TEXT,
  reduction_kind         TEXT    NOT NULL,
  parent_source          TEXT    NOT NULL,
  common_prefix_messages INTEGER NOT NULL DEFAULT 0,
  request_message_count  INTEGER NOT NULL DEFAULT 0,
  response_message_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE(session_id, request_id)
);
CREATE INDEX idx_session_nodes_session_parent ON session_nodes(session_id, parent_id);
CREATE INDEX idx_session_nodes_session_ts ON session_nodes(session_id, ts);

CREATE TABLE node_messages (
  id          TEXT PRIMARY KEY,
  node_id     TEXT    NOT NULL REFERENCES session_nodes(id),
  side        TEXT    NOT NULL,
  message_seq INTEGER NOT NULL,
  role        TEXT    NOT NULL,
  status      INTEGER,
  UNIQUE(node_id, side, message_seq)
);
CREATE INDEX idx_node_messages_node_side ON node_messages(node_id, side, message_seq);

CREATE TABLE node_parts (
  message_id TEXT    NOT NULL REFERENCES node_messages(id),
  part_index INTEGER NOT NULL,
  part_hash  TEXT    NOT NULL REFERENCES part_blobs(hash),
  PRIMARY KEY(message_id, part_index)
);
CREATE INDEX idx_node_parts_hash ON node_parts(part_hash);

CREATE TABLE session_heads (
  session_id TEXT    PRIMARY KEY REFERENCES sessions(id),
  node_id    TEXT    NOT NULL REFERENCES session_nodes(id),
  updated_ts INTEGER NOT NULL
);

CREATE TABLE session_relations (
  parent_session_id TEXT    NOT NULL,
  child_session_id  TEXT    NOT NULL,
  relation_kind     TEXT    NOT NULL,
  first_seen_ts     INTEGER NOT NULL,
  last_seen_ts      INTEGER NOT NULL,
  source            TEXT    NOT NULL,
  PRIMARY KEY(parent_session_id, child_session_id, relation_kind)
);
