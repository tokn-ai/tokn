use rusqlite::Connection;

use crate::{migrate, Result};

const CURRENT_TS_MILLIS_SCHEMA_VERSION: u32 = 8;
const SPLIT_REQUESTS_SCHEMA_VERSION: u32 = 7;
const REQUEST_PARAMS_JSON_SCHEMA_VERSION: u32 = 8;
const REQUEST_ID_SCHEMA_VERSION: u32 = 2;
pub(super) const SESSION_TREE_SCHEMA_VERSION: u32 = 2;
pub(super) const SESSION_MESSAGE_TREE_SCHEMA_VERSION: u32 = 5;

#[derive(Debug, Clone, Copy)]
pub(super) struct RequestSchema {
  version: u32,
}

impl RequestSchema {
  pub(super) fn read(conn: &Connection) -> Result<Self> {
    Ok(Self {
      version: read_schema_version(conn)?,
    })
  }

  pub(super) fn is_split(self) -> bool {
    self.version >= SPLIT_REQUESTS_SCHEMA_VERSION
  }

  pub(super) fn has_request_id(self) -> bool {
    self.version >= REQUEST_ID_SCHEMA_VERSION
  }

  pub(super) fn has_params_json(self) -> bool {
    self.version >= REQUEST_PARAMS_JSON_SCHEMA_VERSION
  }

  pub(super) fn row_id_column(self) -> &'static str {
    if self.is_split() {
      "idx"
    } else {
      "id"
    }
  }

  pub(super) fn legacy_request_id_sql(self) -> &'static str {
    if self.has_request_id() {
      "CASE WHEN request_id IS NULL OR request_id = '' THEN 'legacy:' || id ELSE request_id END"
    } else {
      "'legacy:' || id"
    }
  }

  pub(super) fn normalized_timestamp(self, ts: i64) -> i64 {
    if self.version < CURRENT_TS_MILLIS_SCHEMA_VERSION {
      ts.saturating_mul(1_000)
    } else {
      ts
    }
  }

  pub(super) fn timestamp_sql(self, column: &str) -> String {
    if self.version < CURRENT_TS_MILLIS_SCHEMA_VERSION {
      format!("({column} * 1000)")
    } else {
      column.to_string()
    }
  }
}

pub(super) fn read_schema_version(conn: &Connection) -> Result<u32> {
  migrate::read_current_version(conn)
}
