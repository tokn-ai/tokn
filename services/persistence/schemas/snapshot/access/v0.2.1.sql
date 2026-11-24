-- Canonical initial schema for access.db, introduced in v0.2.1.

CREATE TABLE api_keys (
  id                TEXT PRIMARY KEY,
  name              TEXT    NOT NULL,
  secret_hash       BLOB    NOT NULL,
  allowed_providers TEXT    NOT NULL,
  created_at        INTEGER NOT NULL,
  revoked_at        INTEGER
);
