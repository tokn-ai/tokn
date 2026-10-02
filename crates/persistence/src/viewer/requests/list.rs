use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection};
use std::path::Path;

use super::url_paths::register_url_path_function;
use super::{LatestRequests, RequestCursor, RequestListOptions, RequestPage, RequestSummary};
use crate::viewer::database::open_readonly;
use crate::viewer::days::{request_day_files, DayFile};
use crate::viewer::schema::RequestSchema;
use crate::viewer::value::sqlite_status;
use crate::Result;

/// Return the most recent request rows across every existing request-day DB.
pub fn list_requests(requests_dir: &Path, options: &RequestListOptions) -> Result<RequestPage> {
  let limit = options.effective_limit();
  let mut requests = Vec::new();
  let fetch_limit = limit.saturating_add(1);

  let day_files = request_day_files(requests_dir)?;
  if let Some(day) = options.day.as_deref() {
    if let Some(day_file) = day_files.iter().find(|day_file| day_file.day == day) {
      requests.extend(read_day_requests(day_file, options, Some(fetch_limit))?);
    }
  } else {
    for day_file in &day_files {
      requests.extend(list_day_requests_best_effort(day_file, options, Some(fetch_limit)));
    }
  }

  requests.sort_by(|left, right| {
    right
      .ts
      .cmp(&left.ts)
      .then_with(|| right.day.cmp(&left.day))
      .then_with(|| right.row_id.cmp(&left.row_id))
  });
  Ok(request_page(requests, limit))
}

/// Return requests from the most recent non-empty, readable request day.
pub fn list_latest_requests(
  requests_dir: &Path,
  limit: Option<usize>,
  cursor: Option<RequestCursor>,
) -> Result<LatestRequests> {
  if let Some(cursor) = cursor {
    let day = cursor.day().to_string();
    let page = list_requests(
      requests_dir,
      &RequestListOptions {
        day: Some(day.clone()),
        limit,
        cursor: Some(cursor),
        ..RequestListOptions::default()
      },
    )?;
    return Ok(LatestRequests {
      day: Some(day),
      requests: page.requests,
      next_cursor: page.next_cursor,
    });
  }

  let options = RequestListOptions {
    limit,
    ..RequestListOptions::default()
  };
  let limit = options.effective_limit();
  let fetch_limit = limit.saturating_add(1);

  for day_file in request_day_files(requests_dir)? {
    match read_day_requests(&day_file, &options, Some(fetch_limit)) {
      Ok(requests) if !requests.is_empty() => {
        let page = request_page(requests, limit);
        return Ok(LatestRequests {
          day: Some(day_file.day),
          requests: page.requests,
          next_cursor: page.next_cursor,
        });
      }
      Ok(_) => {}
      Err(error) => log_day_read_failure(&day_file, &error),
    }
  }

  Ok(LatestRequests {
    day: None,
    requests: Vec::new(),
    next_cursor: None,
  })
}

pub(in crate::viewer) fn list_day_requests_best_effort(
  day_file: &DayFile,
  options: &RequestListOptions,
  limit: Option<usize>,
) -> Vec<RequestSummary> {
  match read_day_requests(day_file, options, limit) {
    Ok(requests) => requests,
    Err(error) => {
      log_day_read_failure(day_file, &error);
      Vec::new()
    }
  }
}

fn read_day_requests(
  day_file: &DayFile,
  options: &RequestListOptions,
  limit: Option<usize>,
) -> Result<Vec<RequestSummary>> {
  let Some(conn) = open_readonly(&day_file.path)? else {
    return Ok(Vec::new());
  };
  list_day_requests(&conn, &day_file.day, options, limit)
}

fn log_day_read_failure(day_file: &DayFile, error: &crate::Error) {
  tracing::warn!(
    path = %day_file.path.display(),
    error = %error,
    "skipping request history database after read failure"
  );
}

fn list_day_requests(
  conn: &Connection,
  day: &str,
  options: &RequestListOptions,
  limit: Option<usize>,
) -> Result<Vec<RequestSummary>> {
  let schema = RequestSchema::read(conn)?;
  register_url_path_function(conn)?;
  if schema.is_split() {
    list_split_day_requests(conn, day, options, limit, schema)
  } else {
    list_legacy_day_requests(conn, day, options, limit, schema)
  }
}

fn list_split_day_requests(
  conn: &Connection,
  day: &str,
  options: &RequestListOptions,
  limit: Option<usize>,
  schema: RequestSchema,
) -> Result<Vec<RequestSummary>> {
  let params_json = if schema.has_params_json() {
    "m.params_json"
  } else {
    "NULL"
  };
  let mut sql = format!(
    "SELECT c.request_id, c.ts, c.endpoint, c.status, c.request_error, m.session_id, m.account_id,
            m.provider_id, m.model, d.inbound_req_method, d.inbound_req_url, u.outbound_resp_status,
            d.inbound_resp_status, c.rowid, {params_json}
     FROM request_connection c
     LEFT JOIN request_metadata m ON m.request_id = c.request_id
     LEFT JOIN request_downstream d ON d.request_id = c.request_id
     LEFT JOIN request_upstream u ON u.request_id = c.request_id
     WHERE 1 = 1"
  );
  let mut values = Vec::new();

  if let Some(session_id) = options.session_id.as_deref() {
    sql.push_str(" AND m.session_id = ?");
    values.push(SqlValue::Text(session_id.to_string()));
  }
  if let Some(provider_id) = options.provider_id.as_deref() {
    sql.push_str(" AND m.provider_id = ?");
    values.push(SqlValue::Text(provider_id.to_string()));
  }
  append_url_path_filter(&mut sql, &mut values, "d.inbound_req_url", options.url_path.as_deref())?;
  if let Some(status) = options.status {
    sql.push_str(" AND COALESCE(d.inbound_resp_status, u.outbound_resp_status, c.status) = ?");
    values.push(SqlValue::Integer(i64::from(status)));
  }
  if options.errors_only {
    sql.push_str(
      " AND (c.request_error IS NOT NULL OR c.status >= 400 OR u.outbound_resp_status >= 400
             OR d.inbound_resp_status >= 400)",
    );
  }
  if let Some(query) = options.query.as_deref().filter(|value| !value.is_empty()) {
    sql.push_str(" AND (c.request_id LIKE ? OR m.session_id LIKE ? OR m.model LIKE ?)");
    let pattern = format!("%{query}%");
    values.extend([
      SqlValue::Text(pattern.clone()),
      SqlValue::Text(pattern.clone()),
      SqlValue::Text(pattern),
    ]);
  }
  append_cursor_condition(
    &mut sql,
    &mut values,
    "c.ts",
    "c.rowid",
    day,
    schema,
    options.cursor.as_ref(),
  );
  sql.push_str(" ORDER BY c.ts DESC, c.rowid DESC");
  if let Some(limit) = limit {
    sql.push_str(" LIMIT ?");
    values.push(SqlValue::Integer(limit as i64));
  }

  let mut stmt = conn.prepare(&sql)?;
  let rows = stmt
    .query_map(params_from_iter(values.iter()), |row| {
      request_summary_from_row(row, day, schema)
    })?
    .collect::<rusqlite::Result<Vec<_>>>()?;
  Ok(rows)
}

fn list_legacy_day_requests(
  conn: &Connection,
  day: &str,
  options: &RequestListOptions,
  limit: Option<usize>,
  schema: RequestSchema,
) -> Result<Vec<RequestSummary>> {
  let request_id = schema.legacy_request_id_sql();
  let request_error = if schema.has_request_id() {
    "request_error"
  } else {
    "NULL"
  };
  let mut sql = format!(
    "SELECT {request_id}, ts, endpoint, status, {request_error}, session_id, account_id, provider_id,
            model, inbound_req_method, inbound_req_url, outbound_resp_status, inbound_resp_status, id, NULL
     FROM requests
     WHERE 1 = 1"
  );
  let mut values = Vec::new();

  if let Some(session_id) = options.session_id.as_deref() {
    sql.push_str(" AND session_id = ?");
    values.push(SqlValue::Text(session_id.to_string()));
  }
  if let Some(provider_id) = options.provider_id.as_deref() {
    sql.push_str(" AND provider_id = ?");
    values.push(SqlValue::Text(provider_id.to_string()));
  }
  append_url_path_filter(&mut sql, &mut values, "inbound_req_url", options.url_path.as_deref())?;
  if let Some(status) = options.status {
    sql.push_str(" AND COALESCE(inbound_resp_status, outbound_resp_status, status) = ?");
    values.push(SqlValue::Integer(i64::from(status)));
  }
  if options.errors_only {
    sql.push_str(&format!(
      " AND ({request_error} IS NOT NULL OR status >= 400 OR outbound_resp_status >= 400
             OR inbound_resp_status >= 400)"
    ));
  }
  if let Some(query) = options.query.as_deref().filter(|value| !value.is_empty()) {
    sql.push_str(&format!(
      " AND ({request_id} LIKE ? OR session_id LIKE ? OR model LIKE ?)"
    ));
    let pattern = format!("%{query}%");
    values.extend([
      SqlValue::Text(pattern.clone()),
      SqlValue::Text(pattern.clone()),
      SqlValue::Text(pattern),
    ]);
  }
  append_cursor_condition(&mut sql, &mut values, "ts", "id", day, schema, options.cursor.as_ref());
  sql.push_str(" ORDER BY ts DESC, id DESC");
  if let Some(limit) = limit {
    sql.push_str(" LIMIT ?");
    values.push(SqlValue::Integer(limit as i64));
  }

  let mut stmt = conn.prepare(&sql)?;
  let rows = stmt
    .query_map(params_from_iter(values.iter()), |row| {
      request_summary_from_row(row, day, schema)
    })?
    .collect::<rusqlite::Result<Vec<_>>>()?;
  Ok(rows)
}

fn append_url_path_filter(
  sql: &mut String,
  values: &mut Vec<SqlValue>,
  column: &str,
  url_path: Option<&str>,
) -> Result<()> {
  let Some(url_path) = url_path.filter(|value| !value.is_empty()) else {
    return Ok(());
  };
  sql.push_str(&format!(" AND tokn_url_path({column}) = ?"));
  values.push(SqlValue::Text(url_path.to_string()));
  Ok(())
}

fn append_cursor_condition(
  sql: &mut String,
  values: &mut Vec<SqlValue>,
  ts_sql: &str,
  row_id_sql: &str,
  day: &str,
  schema: RequestSchema,
  cursor: Option<&RequestCursor>,
) {
  let Some(cursor) = cursor else {
    return;
  };
  // Legacy schemas persisted whole seconds. Compare in the normalized
  // millisecond domain so a cursor can also safely traverse mixed-version
  // day files in the aggregate listing.
  let ts_sql = schema.timestamp_sql(ts_sql);
  let cursor_ts = cursor.timestamp();

  match day.cmp(cursor.day()) {
    // At the cursor timestamp, a newer day sorts before the cursor and must
    // not be returned. Only strictly older timestamps remain eligible.
    std::cmp::Ordering::Greater => {
      sql.push_str(&format!(" AND {ts_sql} < ?"));
      values.push(SqlValue::Integer(cursor_ts));
    }
    // On the cursor day, the SQLite row identity is the stable tie-breaker.
    std::cmp::Ordering::Equal => {
      sql.push_str(&format!(" AND ({ts_sql} < ? OR ({ts_sql} = ? AND {row_id_sql} < ?))"));
      values.extend([
        SqlValue::Integer(cursor_ts),
        SqlValue::Integer(cursor_ts),
        SqlValue::Integer(cursor.row_id()),
      ]);
    }
    // An older day sorts after the cursor when timestamps are equal.
    std::cmp::Ordering::Less => {
      sql.push_str(&format!(" AND {ts_sql} <= ?"));
      values.push(SqlValue::Integer(cursor_ts));
    }
  }
}

fn request_page(mut requests: Vec<RequestSummary>, limit: usize) -> RequestPage {
  let has_more = requests.len() > limit;
  requests.truncate(limit);
  let next_cursor = has_more
    .then(|| {
      requests
        .last()
        .map(|request| RequestCursor::from_position(&request.day, request.ts, request.row_id))
    })
    .flatten()
    .map(|cursor| cursor.encode());
  RequestPage { requests, next_cursor }
}

fn request_summary_from_row(
  row: &rusqlite::Row<'_>,
  day: &str,
  schema: RequestSchema,
) -> rusqlite::Result<RequestSummary> {
  let params_json: Option<String> = row.get(14)?;
  let request_classification = params_json
    .as_deref()
    .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
    .and_then(|params| params.get("request_classification").cloned())
    .and_then(|classification| serde_json::from_value(classification).ok());
  Ok(RequestSummary {
    row_id: row.get(13)?,
    day: day.to_string(),
    request_id: row.get(0)?,
    ts: schema.normalized_timestamp(row.get(1)?),
    endpoint: row.get(2)?,
    request_classification,
    status: sqlite_status(row.get(3)?),
    request_error: row.get(4)?,
    session_id: row.get(5)?,
    account_id: row.get(6)?,
    provider_id: row.get(7)?,
    model: row.get(8)?,
    inbound_req_method: row.get(9)?,
    inbound_req_url: row.get(10)?,
    outbound_resp_status: sqlite_status(row.get(11)?),
    inbound_resp_status: sqlite_status(row.get(12)?),
  })
}
