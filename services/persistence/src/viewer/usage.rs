use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::path::Path;

use super::database::open_readonly;
use super::schema::read_schema_version;
use crate::Result;

const SESSION_ID_SCHEMA_VERSION: u32 = 2;
const USAGE_BREAKDOWN_SCHEMA_VERSION: u32 = 4;
const USAGE_JSON_SCHEMA_VERSION: u32 = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionUsage {
  pub session_id: String,
  pub request_count: u64,
  pub requests_with_usage: u64,
  pub input_tokens: Option<u64>,
  pub output_tokens: Option<u64>,
  pub total_tokens: Option<u64>,
  pub cache_read_tokens: Option<u64>,
  pub cache_write_tokens: Option<u64>,
  pub reasoning_tokens: Option<u64>,
  pub requests: Vec<SessionRequestUsage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionRequestUsage {
  pub request_id: String,
  /// All provider-reported input tokens for this request.
  pub context_tokens: Option<u64>,
  /// Input tokens not covered by the provider-reported cache-read prefix.
  pub input_delta_tokens: Option<u64>,
  pub output_tokens: Option<u64>,
}

/// Load aggregate and per-request provider-reported usage for one session.
///
/// A missing database or a session without usage rows returns `None`. The
/// viewer deliberately opens usage storage read-only and never migrates it.
pub fn get_session_usage(usage_db: &Path, session_id: &str) -> Result<Option<SessionUsage>> {
  let Some(conn) = open_readonly(usage_db)? else {
    return Ok(None);
  };
  let schema_version = read_schema_version(&conn)?;
  if schema_version < SESSION_ID_SCHEMA_VERSION {
    return Err(crate::Error::UnsupportedUsageSchema {
      version: schema_version,
    });
  }

  let totals = if schema_version >= USAGE_JSON_SCHEMA_VERSION {
    select_json_usage(&conn, session_id)?
  } else {
    select_legacy_usage(&conn, session_id, schema_version)?
  };
  let Some(totals) = totals else {
    return Ok(None);
  };
  let requests = if schema_version >= USAGE_JSON_SCHEMA_VERSION {
    select_json_request_usage(&conn, session_id)?
  } else {
    select_legacy_request_usage(&conn, session_id, schema_version)?
  };
  Ok(Some(SessionUsage {
    session_id: session_id.to_string(),
    request_count: totals.request_count,
    requests_with_usage: totals.requests_with_usage,
    input_tokens: totals.input_tokens,
    output_tokens: totals.output_tokens,
    total_tokens: totals.total_tokens,
    cache_read_tokens: totals.cache_read_tokens,
    cache_write_tokens: totals.cache_write_tokens,
    reasoning_tokens: totals.reasoning_tokens,
    requests,
  }))
}

#[derive(Debug)]
struct UsageTotals {
  request_count: u64,
  requests_with_usage: u64,
  input_tokens: Option<u64>,
  output_tokens: Option<u64>,
  total_tokens: Option<u64>,
  cache_read_tokens: Option<u64>,
  cache_write_tokens: Option<u64>,
  reasoning_tokens: Option<u64>,
}

fn select_json_usage(conn: &Connection, session_id: &str) -> Result<Option<UsageTotals>> {
  let sql = format!(
    "SELECT
       COUNT(*),
       SUM(CASE WHEN usage_json IS NOT NULL AND json_valid(usage_json) THEN 1 ELSE 0 END),
       {input},
       {output},
       {total},
       {cache_read},
       {cache_write},
       {reasoning}
     FROM requests
     WHERE session_id = ?1
     HAVING COUNT(*) > 0",
    input = json_token_sum("$.input"),
    output = json_token_sum("$.output"),
    total = json_total_sum(),
    cache_read = json_token_sum("$.cache_read"),
    cache_write = json_token_sum("$.cache_write"),
    reasoning = json_token_sum("$.reasoning"),
  );
  select_usage_totals(conn, &sql, session_id)
}

fn select_json_request_usage(conn: &Connection, session_id: &str) -> Result<Vec<SessionRequestUsage>> {
  let mut stmt = conn.prepare(
    "SELECT
       request_id,
       CASE
         WHEN json_valid(usage_json) AND json_type(usage_json, '$.input') IN ('integer', 'real')
         THEN MAX(CAST(json_extract(usage_json, '$.input') AS INTEGER), 0)
       END,
       CASE
         WHEN json_valid(usage_json) AND json_type(usage_json, '$.input') IN ('integer', 'real')
         THEN MAX(
           CAST(json_extract(usage_json, '$.input') AS INTEGER)
             - CASE
               WHEN json_type(usage_json, '$.cache_read') IN ('integer', 'real')
               THEN MAX(CAST(json_extract(usage_json, '$.cache_read') AS INTEGER), 0)
               ELSE 0
             END,
           0
         )
       END,
       CASE
         WHEN json_valid(usage_json) AND json_type(usage_json, '$.output') IN ('integer', 'real')
         THEN MAX(CAST(json_extract(usage_json, '$.output') AS INTEGER), 0)
       END
     FROM requests
     WHERE session_id = ?1 AND request_id IS NOT NULL AND request_id != ''",
  )?;
  let rows = stmt.query_map(params![session_id], request_usage_from_row)?;
  rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

fn json_total_sum() -> String {
  "SUM(CASE
     WHEN json_valid(usage_json) AND json_type(usage_json, '$.total') IN ('integer', 'real')
     THEN MAX(CAST(json_extract(usage_json, '$.total') AS INTEGER), 0)
     WHEN json_valid(usage_json)
       AND (
         json_type(usage_json, '$.input') IN ('integer', 'real')
         OR json_type(usage_json, '$.output') IN ('integer', 'real')
       )
     THEN
       CASE WHEN json_type(usage_json, '$.input') IN ('integer', 'real')
         THEN MAX(CAST(json_extract(usage_json, '$.input') AS INTEGER), 0)
         ELSE 0
       END
       + CASE WHEN json_type(usage_json, '$.output') IN ('integer', 'real')
         THEN MAX(CAST(json_extract(usage_json, '$.output') AS INTEGER), 0)
         ELSE 0
       END
   END)"
    .to_string()
}

fn json_token_sum(path: &str) -> String {
  format!(
    "SUM(CASE
       WHEN json_valid(usage_json) AND json_type(usage_json, '{path}') IN ('integer', 'real')
       THEN MAX(CAST(json_extract(usage_json, '{path}') AS INTEGER), 0)
     END)"
  )
}

fn select_legacy_usage(conn: &Connection, session_id: &str, schema_version: u32) -> Result<Option<UsageTotals>> {
  let (input_column, output_column, cache_read_column, reasoning_column) =
    if schema_version >= USAGE_BREAKDOWN_SCHEMA_VERSION {
      ("input_tok", "output_tok", "cached_tok", "reasoning_tok")
    } else {
      ("prompt_tok", "completion_tok", "NULL", "NULL")
    };
  let sql = format!(
    "SELECT
       COUNT(*),
       SUM(CASE WHEN {input_column} IS NOT NULL OR {output_column} IS NOT NULL THEN 1 ELSE 0 END),
       SUM(MAX({input_column}, 0)),
       SUM(MAX({output_column}, 0)),
       SUM(CASE
         WHEN {input_column} IS NOT NULL OR {output_column} IS NOT NULL
         THEN MAX(COALESCE({input_column}, 0), 0) + MAX(COALESCE({output_column}, 0), 0)
       END),
       SUM(MAX({cache_read_column}, 0)),
       NULL,
       SUM(MAX({reasoning_column}, 0))
     FROM requests
     WHERE session_id = ?1
     HAVING COUNT(*) > 0"
  );
  select_usage_totals(conn, &sql, session_id)
}

fn select_legacy_request_usage(
  conn: &Connection,
  session_id: &str,
  schema_version: u32,
) -> Result<Vec<SessionRequestUsage>> {
  let (input_column, output_column, cache_read_column) = if schema_version >= USAGE_BREAKDOWN_SCHEMA_VERSION {
    ("input_tok", "output_tok", "cached_tok")
  } else {
    ("prompt_tok", "completion_tok", "NULL")
  };
  let sql = format!(
    "SELECT
       request_id,
       MAX({input_column}, 0),
       CASE
         WHEN {input_column} IS NOT NULL
         THEN MAX({input_column} - COALESCE(MAX({cache_read_column}, 0), 0), 0)
       END,
       MAX({output_column}, 0)
     FROM requests
     WHERE session_id = ?1 AND request_id IS NOT NULL AND request_id != ''"
  );
  let mut stmt = conn.prepare(&sql)?;
  let rows = stmt.query_map(params![session_id], request_usage_from_row)?;
  rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

fn request_usage_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRequestUsage> {
  Ok(SessionRequestUsage {
    request_id: row.get(0)?,
    context_tokens: optional_nonnegative(row.get(1)?),
    input_delta_tokens: optional_nonnegative(row.get(2)?),
    output_tokens: optional_nonnegative(row.get(3)?),
  })
}

fn select_usage_totals(conn: &Connection, sql: &str, session_id: &str) -> Result<Option<UsageTotals>> {
  conn
    .query_row(sql, params![session_id], |row| {
      Ok(UsageTotals {
        request_count: nonnegative(row.get(0)?),
        requests_with_usage: nonnegative(row.get(1)?),
        input_tokens: optional_nonnegative(row.get(2)?),
        output_tokens: optional_nonnegative(row.get(3)?),
        total_tokens: optional_nonnegative(row.get(4)?),
        cache_read_tokens: optional_nonnegative(row.get(5)?),
        cache_write_tokens: optional_nonnegative(row.get(6)?),
        reasoning_tokens: optional_nonnegative(row.get(7)?),
      })
    })
    .optional()
    .map_err(Into::into)
}

fn nonnegative(value: i64) -> u64 {
  value.max(0) as u64
}

fn optional_nonnegative(value: Option<i64>) -> Option<u64> {
  value.map(nonnegative)
}
