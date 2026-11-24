-- Squashed usage migrations from snapshot v0.2.0 to snapshot v0.2.1.
-- Covers schema versions 0005 through 0006.

ALTER TABLE requests ADD COLUMN user TEXT;

CREATE INDEX idx_requests_user ON requests(user);
