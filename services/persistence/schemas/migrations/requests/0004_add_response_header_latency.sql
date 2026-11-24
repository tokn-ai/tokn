ALTER TABLE requests ADD COLUMN latency_ms_nullable INTEGER;
UPDATE requests SET latency_ms_nullable = latency_ms;
ALTER TABLE requests DROP COLUMN latency_ms;
ALTER TABLE requests RENAME COLUMN latency_ms_nullable TO latency_ms;

ALTER TABLE requests ADD COLUMN status_nullable INTEGER;
UPDATE requests SET status_nullable = status;
ALTER TABLE requests DROP COLUMN status;
ALTER TABLE requests RENAME COLUMN status_nullable TO status;

ALTER TABLE requests ADD COLUMN stream_nullable INTEGER;
UPDATE requests SET stream_nullable = stream;
ALTER TABLE requests DROP COLUMN stream;
ALTER TABLE requests RENAME COLUMN stream_nullable TO stream;

DROP INDEX idx_requests_session;
ALTER TABLE requests ADD COLUMN session_id_nullable TEXT;
UPDATE requests SET session_id_nullable = session_id;
ALTER TABLE requests DROP COLUMN session_id;
ALTER TABLE requests RENAME COLUMN session_id_nullable TO session_id;
CREATE INDEX idx_requests_session ON requests(session_id);

ALTER TABLE requests ADD COLUMN inbound_req_headers_nullable BLOB;
UPDATE requests SET inbound_req_headers_nullable = inbound_req_headers;
ALTER TABLE requests DROP COLUMN inbound_req_headers;
ALTER TABLE requests RENAME COLUMN inbound_req_headers_nullable TO inbound_req_headers;

ALTER TABLE requests ADD COLUMN inbound_req_body_nullable BLOB;
UPDATE requests SET inbound_req_body_nullable = inbound_req_body;
ALTER TABLE requests DROP COLUMN inbound_req_body;
ALTER TABLE requests RENAME COLUMN inbound_req_body_nullable TO inbound_req_body;

ALTER TABLE requests ADD COLUMN inbound_resp_headers_nullable BLOB;
UPDATE requests SET inbound_resp_headers_nullable = inbound_resp_headers;
ALTER TABLE requests DROP COLUMN inbound_resp_headers;
ALTER TABLE requests RENAME COLUMN inbound_resp_headers_nullable TO inbound_resp_headers;

ALTER TABLE requests ADD COLUMN inbound_resp_body_nullable BLOB;
UPDATE requests SET inbound_resp_body_nullable = inbound_resp_body;
ALTER TABLE requests DROP COLUMN inbound_resp_body;
ALTER TABLE requests RENAME COLUMN inbound_resp_body_nullable TO inbound_resp_body;

ALTER TABLE requests ADD COLUMN latency_header_ms INTEGER;
CREATE UNIQUE INDEX idx_requests_request_id ON requests(request_id);
