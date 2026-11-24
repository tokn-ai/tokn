use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection};
use serde_json::{Map, Value};
use std::path::Path;

use super::{RequestDetail, RequestPayload, RequestPayloadField};
use crate::viewer::database::open_readonly;
use crate::viewer::days::request_day_files;
use crate::viewer::schema::RequestSchema;
use crate::viewer::value::sqlite_value_to_json;
use crate::Result;

/// Return request metadata without eagerly loading large network payloads.
pub fn get_request(
  requests_dir: &Path,
  day: &str,
  request_id: &str,
  row_id: Option<i64>,
) -> Result<Option<RequestDetail>> {
  let Some(day_file) = request_day_files(requests_dir)?
    .into_iter()
    .find(|file| file.day == day)
  else {
    return Ok(None);
  };
  let Some(conn) = open_readonly(&day_file.path)? else {
    return Ok(None);
  };

  let schema = RequestSchema::read(&conn)?;
  let request_condition = request_lookup_condition(schema, row_id.is_some());
  let overview_columns = request_overview_columns(&conn)?;
  let mut projection = vec![quote_identifier(schema.row_id_column())];
  projection.extend(overview_columns.iter().map(|column| quote_identifier(column)));
  let projection = projection.join(", ");
  let mut stmt = conn.prepare(&format!(
    "SELECT {projection} FROM requests WHERE {request_condition} LIMIT 1"
  ))?;
  let mut values = vec![SqlValue::Text(request_id.to_string())];
  if let Some(row_id) = row_id {
    values.push(SqlValue::Integer(row_id));
  }
  let mut rows = stmt.query(params_from_iter(values.iter()))?;
  let Some(row) = rows.next()? else {
    return Ok(None);
  };
  let row_id = row.get(0)?;

  let mut request = Map::with_capacity(overview_columns.len());
  for (index, name) in overview_columns.iter().enumerate() {
    request.insert(name.clone(), sqlite_value_to_json(row.get_ref(index + 1)?, name));
  }
  if !schema.is_split() && !matches!(request.get("request_id"), Some(Value::String(value)) if !value.is_empty()) {
    request.insert("request_id".to_string(), Value::String(request_id.to_string()));
  }
  normalize_timestamp(&mut request, schema);

  Ok(Some(RequestDetail {
    day: day_file.day,
    row_id,
    request,
  }))
}

/// Return one explicitly selected request payload field without mutating its database.
pub fn get_request_payload(
  requests_dir: &Path,
  day: &str,
  request_id: &str,
  row_id: Option<i64>,
  field: RequestPayloadField,
) -> Result<Option<RequestPayload>> {
  let Some(day_file) = request_day_files(requests_dir)?
    .into_iter()
    .find(|file| file.day == day)
  else {
    return Ok(None);
  };
  let Some(conn) = open_readonly(&day_file.path)? else {
    return Ok(None);
  };

  let schema = RequestSchema::read(&conn)?;
  let request_condition = request_lookup_condition(schema, row_id.is_some());
  let field_name = field.as_str();
  let mut stmt = conn.prepare(&format!(
    "SELECT {} FROM requests WHERE {request_condition} LIMIT 1",
    quote_identifier(field_name)
  ))?;
  let mut values = vec![SqlValue::Text(request_id.to_string())];
  if let Some(row_id) = row_id {
    values.push(SqlValue::Integer(row_id));
  }
  let mut rows = stmt.query(params_from_iter(values.iter()))?;
  let Some(row) = rows.next()? else {
    return Ok(None);
  };

  Ok(Some(RequestPayload {
    field: field_name.to_string(),
    value: sqlite_value_to_json(row.get_ref(0)?, field_name),
  }))
}

fn request_overview_columns(conn: &Connection) -> Result<Vec<String>> {
  let stmt = conn.prepare("SELECT * FROM requests LIMIT 0")?;
  Ok(
    stmt
      .column_names()
      .into_iter()
      .filter(|column| !RequestPayloadField::is_payload_column(column))
      .map(str::to_string)
      .collect(),
  )
}

pub(super) fn request_lookup_condition(schema: RequestSchema, include_row_id: bool) -> String {
  let request_id = if schema.is_split() {
    "request_id".to_string()
  } else {
    schema.legacy_request_id_sql().to_string()
  };
  if include_row_id {
    format!("{request_id} = ?1 AND {} = ?2", schema.row_id_column())
  } else {
    format!("{request_id} = ?1")
  }
}

pub(super) fn quote_identifier(identifier: &str) -> String {
  format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn normalize_timestamp(request: &mut Map<String, Value>, schema: RequestSchema) {
  let Some(ts) = request.get("ts").and_then(Value::as_i64) else {
    return;
  };
  request.insert("ts".to_string(), Value::from(schema.normalized_timestamp(ts)));
}
