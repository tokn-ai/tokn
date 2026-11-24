-- Message content and request observations are separate concepts from this
-- version forward. Existing node-local deltas remain untouched and readable;
-- new nodes point into an immutable, content-addressed prefix tree.
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

-- Null identifies an untouched legacy node. For a message-tree node,
-- message_id is the final output message when output exists and otherwise the
-- final input message. The counts divide that one prefix path into input and
-- output ranges.
ALTER TABLE session_nodes ADD COLUMN message_id BLOB REFERENCES message_tree(id)
  CHECK(message_id IS NULL OR (typeof(message_id) = 'blob' AND length(message_id) = 32));
ALTER TABLE session_nodes ADD COLUMN input_message_count INTEGER CHECK(input_message_count >= 0);
ALTER TABLE session_nodes ADD COLUMN output_message_count INTEGER CHECK(output_message_count >= 0);
CREATE INDEX idx_session_nodes_message ON session_nodes(message_id) WHERE message_id IS NOT NULL;

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
  n.response_message_count AS head_response_message_count,
  CASE WHEN n.message_id IS NULL THEN NULL ELSE lower(hex(n.message_id)) END AS head_message_id,
  n.input_message_count AS head_input_message_count,
  n.output_message_count AS head_output_message_count
FROM sessions s
LEFT JOIN session_heads h ON h.session_id = s.id
LEFT JOIN session_nodes n ON n.id = h.node_id
LEFT JOIN session_threads t ON t.session_id = n.session_id AND t.thread_id = n.thread_id;

DROP VIEW session_messages;
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
