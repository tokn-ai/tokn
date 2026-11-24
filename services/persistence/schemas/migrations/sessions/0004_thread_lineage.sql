-- Thread identity is intentionally nullable on existing nodes. Historical
-- rows keep their inferred session-wide parentage until an explicit repair.
ALTER TABLE session_nodes ADD COLUMN thread_id TEXT;
CREATE INDEX idx_session_nodes_session_thread_ts
  ON session_nodes(session_id, thread_id, ts)
  WHERE thread_id IS NOT NULL;

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

DROP VIEW session_current;
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

DROP VIEW session_messages;
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
