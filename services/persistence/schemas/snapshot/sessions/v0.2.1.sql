-- Canonical sessions.db schema for the v0.2.1 release line.
-- Fresh installs apply this snapshot directly; existing databases reach the
-- same shape through migrations 001..005.
--
-- Mental model: immutable messages form one content-addressed prefix tree.
-- A session node is an observed request/result that points to the final
-- message reached by that observation. Legacy node-local deltas remain only
-- so rows written before v0.2.1 can be read without being rewritten.

CREATE TABLE sessions (
  id            TEXT PRIMARY KEY,
  first_seen_ts INTEGER NOT NULL,
  last_seen_ts  INTEGER NOT NULL,
  source        TEXT    NOT NULL,        -- 'header' | 'auto'
  account_id    TEXT,
  provider_id   TEXT,
  model         TEXT
);
CREATE INDEX idx_sessions_last ON sessions(last_seen_ts);

-- Content-addressed part store; identical parts dedupe across the whole DB.
CREATE TABLE part_blobs (
  hash      TEXT PRIMARY KEY,            -- sha256(part_type || 0x00 || content)
  part_type TEXT NOT NULL,               -- 'text' | 'image_url' | 'tool_use' | ...
  content   BLOB NOT NULL
);

-- One row is one message at one immutable prefix. `id` hashes the parent
-- prefix and semantic message hash, so retries share paths while divergent
-- input or output creates a branch naturally.
CREATE TABLE message_tree (
  id           BLOB    PRIMARY KEY CHECK(typeof(id) = 'blob' AND length(id) = 32),
  parent_id    BLOB    REFERENCES message_tree(id)
                         CHECK(parent_id IS NULL OR (typeof(parent_id) = 'blob' AND length(parent_id) = 32)),
  depth        INTEGER NOT NULL CHECK(depth > 0),
  message_hash BLOB    NOT NULL CHECK(typeof(message_hash) = 'blob' AND length(message_hash) = 32),
  role         TEXT    NOT NULL,
  status       INTEGER
);
CREATE INDEX idx_message_tree_parent ON message_tree(parent_id);

CREATE TABLE message_parts (
  message_id BLOB    NOT NULL REFERENCES message_tree(id),
  part_index INTEGER NOT NULL,
  part_hash  TEXT    NOT NULL REFERENCES part_blobs(hash),
  PRIMARY KEY(message_id, part_index)
);
CREATE INDEX idx_message_parts_hash ON message_parts(part_hash);

-- Each node is an observation bookmark. `message_id` is the final output
-- message when output exists and otherwise the final input message. Input and
-- output counts divide that one prefix path at the request/response boundary.
-- Legacy rows retain their reduction metadata. New rows use
-- reduction_kind = 'message_tree' with a null parent_id. For those rows,
-- common_prefix_messages is the leading input already present in the global
-- message tree at first write, and request_message_count is the remaining
-- suffix. Observation ancestry is derived when sessions are read.
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
  thread_id               TEXT, -- null only for rows written before thread-aware lineage
  message_id              BLOB REFERENCES message_tree(id)
                                  CHECK(message_id IS NULL OR (typeof(message_id) = 'blob' AND length(message_id) = 32)),
  input_message_count     INTEGER CHECK(input_message_count >= 0),
  output_message_count    INTEGER CHECK(output_message_count >= 0),
  UNIQUE(session_id, request_id)
);
CREATE INDEX idx_session_nodes_session_parent ON session_nodes(session_id, parent_id);
CREATE INDEX idx_session_nodes_session_ts ON session_nodes(session_id, ts);
CREATE INDEX idx_session_nodes_session_thread_ts
  ON session_nodes(session_id, thread_id, ts)
  WHERE thread_id IS NOT NULL;
CREATE INDEX idx_session_nodes_message ON session_nodes(message_id) WHERE message_id IS NOT NULL;

-- Legacy storage for nodes written before v0.2.1. New writes use message_tree
-- and message_parts exclusively.
CREATE TABLE node_messages (
  id          TEXT PRIMARY KEY,
  node_id     TEXT    NOT NULL REFERENCES session_nodes(id),
  side        TEXT    NOT NULL,           -- 'request' | 'response'
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

-- Subagent/session topology metadata. This is deliberately separate from
-- session_nodes.parent_id, which links observed request nodes.
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
  source           TEXT    NOT NULL, -- 'thread-header' | 'session-fallback'
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
  n.response_message_count AS head_response_message_count,
  CASE WHEN n.message_id IS NULL THEN NULL ELSE lower(hex(n.message_id)) END AS head_message_id,
  n.input_message_count AS head_input_message_count,
  n.output_message_count AS head_output_message_count
FROM sessions s
LEFT JOIN session_heads h ON h.session_id = s.id
LEFT JOIN session_nodes n ON n.id = h.node_id
LEFT JOIN session_threads t ON t.session_id = n.session_id AND t.thread_id = n.thread_id;

CREATE VIEW session_messages AS
WITH RECURSIVE message_paths(node_id, message_id) AS (
  SELECT id, message_id
  FROM session_nodes
  WHERE message_id IS NOT NULL
  UNION
  SELECT path.node_id, message.parent_id
  FROM message_paths path
  JOIN message_tree message ON message.id = path.message_id
  WHERE message.parent_id IS NOT NULL
)
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
  legacy.side,
  legacy.message_seq,
  legacy.role,
  legacy.status AS message_status,
  part.part_index,
  part.part_hash,
  blob.part_type,
  blob.content
FROM sessions s
JOIN session_nodes n ON n.session_id = s.id
LEFT JOIN session_heads h ON h.session_id = s.id
LEFT JOIN session_threads t ON t.session_id = n.session_id AND t.thread_id = n.thread_id
JOIN node_messages legacy ON legacy.node_id = n.id
JOIN node_parts part ON part.message_id = legacy.id
JOIN part_blobs blob ON blob.hash = part.part_hash
WHERE n.message_id IS NULL
UNION ALL
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
  CASE WHEN message.depth <= n.input_message_count THEN 'request' ELSE 'response' END AS side,
  CASE
    WHEN message.depth <= n.input_message_count THEN message.depth - 1
    ELSE message.depth - n.input_message_count - 1
  END AS message_seq,
  message.role,
  message.status AS message_status,
  part.part_index,
  part.part_hash,
  blob.part_type,
  blob.content
FROM sessions s
JOIN session_nodes n ON n.session_id = s.id
LEFT JOIN session_heads h ON h.session_id = s.id
LEFT JOIN session_threads t ON t.session_id = n.session_id AND t.thread_id = n.thread_id
JOIN message_paths path ON path.node_id = n.id
JOIN message_tree message ON message.id = path.message_id
LEFT JOIN message_parts part ON part.message_id = message.id
LEFT JOIN part_blobs blob ON blob.hash = part.part_hash
WHERE n.message_id IS NOT NULL;
