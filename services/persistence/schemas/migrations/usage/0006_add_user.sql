ALTER TABLE requests ADD COLUMN user TEXT;

CREATE INDEX idx_requests_user ON requests(user);
