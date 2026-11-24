-- Squashed sessions migrations from snapshot v0.1.1 to snapshot v0.2.0.
-- Covers schema versions 0001 through 0004.

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
  thread_id               TEXT,
  UNIQUE(session_id, request_id)
);
CREATE INDEX idx_session_nodes_session_parent ON session_nodes(session_id, parent_id);
CREATE INDEX idx_session_nodes_session_ts ON session_nodes(session_id, ts);
CREATE INDEX idx_session_nodes_session_thread_ts
  ON session_nodes(session_id, thread_id, ts)
  WHERE thread_id IS NOT NULL;

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

CREATE TABLE session_threads (
  session_id       TEXT    NOT NULL REFERENCES sessions(id),
  thread_id        TEXT    NOT NULL,
  parent_thread_id TEXT,
  first_seen_ts    INTEGER NOT NULL,
  last_seen_ts     INTEGER NOT NULL,
  source           TEXT    NOT NULL,
  PRIMARY KEY(session_id, thread_id)
);
CREATE INDEX idx_session_threads_parent ON session_threads(session_id, parent_thread_id);

CREATE VIEW session_current AS
SELECT
  s.id AS session_id,
  s.first_seen_ts,
  s.last_seen_ts,
  s.source,
  h.node_id AS head_node_id,
  h.updated_ts AS head_updated_ts,
  n.request_id AS head_request_id,
  n.ts AS head_ts,
  n.endpoint AS head_endpoint,
  n.status AS head_status,
  COALESCE(n.account_id, s.account_id) AS account_id,
  COALESCE(n.provider_id, s.provider_id) AS provider_id,
  COALESCE(n.model, s.model) AS model,
  n.parent_id AS head_parent_id,
  n.thread_id AS head_thread_id,
  t.parent_thread_id AS head_parent_thread_id,
  n.reduction_kind AS head_reduction_kind,
  n.parent_source AS head_parent_source,
  n.common_prefix_messages AS head_common_prefix_messages,
  n.request_message_count AS head_request_message_count,
  n.response_message_count AS head_response_message_count
FROM sessions s
LEFT JOIN session_heads h ON h.session_id = s.id
LEFT JOIN session_nodes n ON n.id = h.node_id
LEFT JOIN session_threads t ON t.session_id = n.session_id AND t.thread_id = n.thread_id;

CREATE VIEW session_messages AS
SELECT
  s.id AS session_id,
  s.first_seen_ts,
  s.last_seen_ts,
  s.source,
  n.id AS node_id,
  n.parent_id,
  n.thread_id,
  t.parent_thread_id,
  n.request_id,
  n.ts AS node_ts,
  n.endpoint,
  n.status AS node_status,
  COALESCE(n.account_id, s.account_id) AS account_id,
  COALESCE(n.provider_id, s.provider_id) AS provider_id,
  COALESCE(n.model, s.model) AS model,
  n.reduction_kind,
  n.parent_source,
  CASE WHEN h.node_id = n.id THEN 1 ELSE 0 END AS is_head,
  m.side,
  m.message_seq,
  m.role,
  m.status AS message_status,
  p.part_index,
  p.part_hash,
  b.part_type,
  b.content
FROM sessions s
JOIN session_nodes n ON n.session_id = s.id
LEFT JOIN session_heads h ON h.session_id = s.id
LEFT JOIN session_threads t ON t.session_id = n.session_id AND t.thread_id = n.thread_id
JOIN node_messages m ON m.node_id = n.id
JOIN node_parts p ON p.message_id = m.id
JOIN part_blobs b ON b.hash = p.part_hash;
