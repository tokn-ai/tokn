use rusqlite::functions::FunctionFlags;
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use std::path::Path;

use super::RequestUrlPath;
use crate::viewer::database::open_readonly;
use crate::viewer::days::request_day_files;
use crate::viewer::schema::RequestSchema;
use crate::Result;

pub fn list_request_url_paths(requests_dir: &Path, day: &str) -> Result<Vec<RequestUrlPath>> {
  let Some(day_file) = request_day_files(requests_dir)?
    .into_iter()
    .find(|day_file| day_file.day == day)
  else {
    return Ok(Vec::new());
  };
  let Some(conn) = open_readonly(&day_file.path)? else {
    return Ok(Vec::new());
  };
  register_url_path_function(&conn)?;
  let table = if RequestSchema::read(&conn)?.is_split() {
    "request_downstream"
  } else {
    "requests"
  };
  let mut stmt = conn.prepare(&format!(
    "SELECT tokn_url_path(inbound_req_url), COUNT(*) FROM {table}
     WHERE inbound_req_url IS NOT NULL AND inbound_req_url != ''
     GROUP BY tokn_url_path(inbound_req_url)"
  ))?;
  let rows = stmt
    .query_map([], |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?)))?
    .collect::<rusqlite::Result<Vec<_>>>()?;
  let mut paths = rows
    .into_iter()
    .filter_map(|(url_path, request_count)| {
      Some(RequestUrlPath {
        url_path: url_path?,
        request_count: u64::try_from(request_count).expect("SQLite COUNT cannot be negative"),
      })
    })
    .collect::<Vec<_>>();
  paths.sort_by(|left, right| {
    right
      .request_count
      .cmp(&left.request_count)
      .then_with(|| left.url_path.cmp(&right.url_path))
  });
  Ok(paths)
}

pub(super) fn register_url_path_function(conn: &Connection) -> Result<()> {
  conn.create_scalar_function(
    "tokn_url_path",
    1,
    FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
    |context| {
      let raw_url = match context.get_raw(0) {
        ValueRef::Text(value) | ValueRef::Blob(value) => std::str::from_utf8(value).ok(),
        ValueRef::Null | ValueRef::Integer(_) | ValueRef::Real(_) => None,
      };
      Ok(raw_url.and_then(normalize_url_path))
    },
  )?;
  Ok(())
}

fn normalize_url_path(raw_url: &str) -> Option<String> {
  let raw_url = raw_url.trim();
  if raw_url.is_empty() {
    return None;
  }
  let path_and_suffix = if let Some((_, authority_and_path)) = raw_url.split_once("://") {
    authority_and_path
      .find('/')
      .map_or("/", |index| &authority_and_path[index..])
  } else {
    raw_url
  };
  let path_end = path_and_suffix.find(['?', '#']).unwrap_or(path_and_suffix.len());
  let path = &path_and_suffix[..path_end];
  if path.is_empty() {
    Some("/".to_string())
  } else if path.starts_with('/') {
    Some(path.to_string())
  } else {
    Some(format!("/{path}"))
  }
}

#[cfg(test)]
mod tests {
  use super::normalize_url_path;

  #[test]
  fn normalizes_relative_and_absolute_urls_without_query_or_fragment() {
    for (url, expected) in [
      ("/v1/responses?stream=true", Some("/v1/responses")),
      (
        "https://chatgpt.com/backend-api/codex/alpha/search#results",
        Some("/backend-api/codex/alpha/search"),
      ),
      ("https://chatgpt.com?client=codex", Some("/")),
      ("v1/models", Some("/v1/models")),
      ("  ", None),
    ] {
      assert_eq!(normalize_url_path(url).as_deref(), expected);
    }
  }
}
