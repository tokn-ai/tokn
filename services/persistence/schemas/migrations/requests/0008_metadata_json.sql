DROP VIEW requests;

DROP INDEX IF EXISTS idx_request_metadata_session;
DROP INDEX IF EXISTS idx_request_metadata_account;
DROP INDEX IF EXISTS idx_request_metadata_provider;
DROP INDEX IF EXISTS idx_request_connection_ts;
DROP INDEX IF EXISTS idx_request_connection_local_addr;

ALTER TABLE request_connection RENAME TO request_connection_legacy;

CREATE TABLE request_connection (
  request_id TEXT PRIMARY KEY,
  ts INTEGER NOT NULL,
  ver TEXT,
  endpoint TEXT,
  status INTEGER,
  request_error TEXT,
  user TEXT,
  ctx_json TEXT
);
CREATE INDEX idx_request_connection_ts ON request_connection(ts);

INSERT INTO request_connection (
  rowid,
  request_id,
  ts,
  ver,
  endpoint,
  status,
  request_error,
  user,
  ctx_json
)
SELECT
  rowid,
  request_id,
  ts,
  NULL,
  endpoint,
  status,
  request_error,
  user,
  CASE
    WHEN latency_ms IS NULL
      AND latency_header_ms IS NULL
      AND peer_addr IS NULL
      AND local_addr IS NULL
      AND mode IS NULL
      AND behave_as IS NULL
      AND method IS NULL
    THEN NULL
    ELSE json_remove(
      json_set(
        '{}',
        '$.latency_ms', latency_ms,
        '$.latency_header_ms', latency_header_ms,
        '$.peer_addr', peer_addr,
        '$.local_addr', local_addr,
        '$.mode', mode,
        '$.agent_id', behave_as,
        '$.pipeline_id', method
      ),
      CASE WHEN latency_ms IS NULL THEN '$.latency_ms' ELSE '$.__noop__' END,
      CASE WHEN latency_header_ms IS NULL THEN '$.latency_header_ms' ELSE '$.__noop__' END,
      CASE WHEN peer_addr IS NULL THEN '$.peer_addr' ELSE '$.__noop__' END,
      CASE WHEN local_addr IS NULL THEN '$.local_addr' ELSE '$.__noop__' END,
      CASE WHEN mode IS NULL THEN '$.mode' ELSE '$.__noop__' END,
      CASE WHEN behave_as IS NULL THEN '$.agent_id' ELSE '$.__noop__' END,
      CASE WHEN method IS NULL THEN '$.pipeline_id' ELSE '$.__noop__' END
    )
  END
FROM request_connection_legacy;

DROP TABLE request_connection_legacy;

ALTER TABLE request_metadata RENAME TO request_metadata_legacy;

CREATE TABLE request_metadata (
  request_id TEXT PRIMARY KEY,
  session_id TEXT,
  account_id TEXT,
  provider_id TEXT,
  model TEXT,
  params_json TEXT,
  usage_json TEXT
);
CREATE INDEX idx_request_metadata_session ON request_metadata(session_id);
CREATE INDEX idx_request_metadata_account ON request_metadata(account_id);
CREATE INDEX idx_request_metadata_provider ON request_metadata(provider_id);

INSERT INTO request_metadata (
  request_id,
  session_id,
  account_id,
  provider_id,
  model,
  params_json,
  usage_json
)
SELECT
  request_id,
  session_id,
  account_id,
  provider_id,
  model,
  CASE
    WHEN initiator IS NULL AND stream IS NULL THEN NULL
    ELSE json_remove(
      json_set(
        '{}',
        '$.initiator', initiator,
        '$.stream', CASE WHEN stream = 0 THEN json('false') ELSE json('true') END
      ),
      CASE WHEN initiator IS NULL THEN '$.initiator' ELSE '$.__noop__' END,
      CASE WHEN stream IS NULL THEN '$.stream' ELSE '$.__noop__' END
    )
  END,
  CASE
    WHEN input_tok IS NULL
      AND output_tok IS NULL
      AND cached_tok IS NULL
      AND reasoning_tok IS NULL
    THEN NULL
    ELSE json_remove(
      json_set(
        '{}',
        '$.input', input_tok,
        '$.output', output_tok,
        '$.cache_read', cached_tok,
        '$.reasoning', reasoning_tok
      ),
      CASE WHEN input_tok IS NULL THEN '$.input' ELSE '$.__noop__' END,
      CASE WHEN output_tok IS NULL THEN '$.output' ELSE '$.__noop__' END,
      CASE WHEN cached_tok IS NULL THEN '$.cache_read' ELSE '$.__noop__' END,
      CASE WHEN reasoning_tok IS NULL THEN '$.reasoning' ELSE '$.__noop__' END
    )
  END
FROM request_metadata_legacy;

DROP TABLE request_metadata_legacy;

CREATE VIEW requests AS
SELECT
  c.rowid AS idx,
  c.ts,
  m.session_id,
  c.request_id,
  c.ver,
  c.request_error,
  c.endpoint,
  m.account_id,
  m.provider_id,
  m.model,
  m.params_json,
  c.status,
  c.ctx_json,
  m.usage_json,
  c.user,
  d.inbound_req_method,
  d.inbound_req_url,
  d.inbound_req_headers,
  d.inbound_req_body,
  u.outbound_req_method,
  u.outbound_req_url,
  u.outbound_req_headers,
  u.outbound_req_body,
  u.outbound_resp_status,
  u.outbound_resp_headers,
  u.outbound_resp_body,
  d.inbound_resp_status,
  d.inbound_resp_headers,
  d.inbound_resp_body
FROM request_connection c
LEFT JOIN request_metadata m ON m.request_id = c.request_id
LEFT JOIN request_downstream d ON d.request_id = c.request_id
LEFT JOIN request_upstream u ON u.request_id = c.request_id;
