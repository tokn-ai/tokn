ALTER TABLE requests RENAME TO requests_legacy;
DROP INDEX IF EXISTS idx_requests_ts;
DROP INDEX IF EXISTS idx_requests_session;
DROP INDEX IF EXISTS idx_requests_request;
DROP INDEX IF EXISTS idx_requests_project;
DROP INDEX IF EXISTS idx_requests_account;

CREATE TABLE requests (
  id            INTEGER PRIMARY KEY,
  ts            INTEGER NOT NULL,
  session_id    TEXT,
  request_id    TEXT,
  project_id    TEXT,
  ver           TEXT,
  request_error TEXT,
  endpoint      TEXT,
  account_id    TEXT,
  provider_id   TEXT,
  model         TEXT NOT NULL,
  params_json   TEXT,
  usage_json    TEXT,
  ctx_json      TEXT,
  status        INTEGER
);
CREATE INDEX idx_requests_ts ON requests(ts);
CREATE INDEX idx_requests_session ON requests(session_id);
CREATE UNIQUE INDEX idx_requests_request ON requests(request_id);
CREATE INDEX idx_requests_project ON requests(project_id);
CREATE INDEX idx_requests_account ON requests(account_id);

INSERT INTO requests (
  id,
  ts,
  session_id,
  request_id,
  project_id,
  ver,
  request_error,
  endpoint,
  account_id,
  provider_id,
  model,
  params_json,
  usage_json,
  ctx_json,
  status
)
SELECT
  id,
  ts,
  session_id,
  request_id,
  project_id,
  NULL,
  NULL,
  endpoint,
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
    WHEN input_tok IS NOT NULL
      OR output_tok IS NOT NULL
      OR cached_tok IS NOT NULL
      OR reasoning_tok IS NOT NULL
      THEN json_patch(
        json_patch(
          json_patch(
            json_patch(
              '{}',
              CASE WHEN input_tok IS NULL THEN '{}' ELSE json_object('input', input_tok) END
            ),
            CASE WHEN output_tok IS NULL THEN '{}' ELSE json_object('output', output_tok) END
          ),
          CASE WHEN cached_tok IS NULL THEN '{}' ELSE json_object('cache_read', cached_tok) END
        ),
        CASE WHEN reasoning_tok IS NULL THEN '{}' ELSE json_object('reasoning', reasoning_tok) END
      )
    ELSE NULL
  END,
  CASE
    WHEN latency_ms IS NOT NULL THEN json_object('latency_ms', latency_ms)
    ELSE NULL
  END,
  status
FROM requests_legacy;

DROP TABLE requests_legacy;
