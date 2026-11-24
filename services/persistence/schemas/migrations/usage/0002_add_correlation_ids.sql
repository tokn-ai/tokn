ALTER TABLE requests ADD COLUMN session_id TEXT;
ALTER TABLE requests ADD COLUMN request_id TEXT;
ALTER TABLE requests ADD COLUMN project_id TEXT;
CREATE INDEX idx_requests_session ON requests(session_id);
CREATE INDEX idx_requests_request ON requests(request_id);
CREATE INDEX idx_requests_project ON requests(project_id);
