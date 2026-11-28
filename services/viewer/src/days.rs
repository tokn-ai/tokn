use rusqlite::Connection;
use serde::Serialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokn_persistence::indexed_archive::IndexedArchiveFormat;
use tokn_persistence::request_files::ArchiveFormat;

use super::schema::RequestSchema;
use crate::Result;

const DAY_PROBE_BUSY_TIMEOUT: Duration = Duration::from_millis(100);

/// The availability of a request history database for one UTC day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestDayState {
  Available,
  Empty,
  Unavailable,
}

/// A request history day and the state of its backing database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RequestDay {
  pub day: String,
  pub state: RequestDayState,
}

#[derive(Debug, Clone)]
pub(super) struct DayFile {
  pub(super) day: String,
  pub(super) path: PathBuf,
}

pub use tokn_persistence::requests::is_valid_request_day;

/// List all request-day databases from newest to oldest with their availability.
pub fn list_request_days(requests_dir: &Path) -> Result<Vec<RequestDay>> {
  Ok(
    request_day_files(requests_dir)?
      .into_iter()
      .map(|day_file| RequestDay {
        state: probe_request_day_state_best_effort(&day_file),
        day: day_file.day,
      })
      .collect(),
  )
}

pub(super) fn request_day_files(requests_dir: &Path) -> Result<Vec<DayFile>> {
  let entries = match std::fs::read_dir(requests_dir) {
    Ok(entries) => entries,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
    Err(error) => return Err(error.into()),
  };
  let mut days = BTreeSet::new();
  for entry in entries {
    let entry = entry?;
    if !entry.file_type()?.is_file() {
      continue;
    }
    let mut path = entry.path();
    if let Some(day) = IndexedArchiveFormat::day_from_path(&path) {
      if is_valid_request_day(day) {
        days.insert(day.to_owned());
      }
      continue;
    }
    if ArchiveFormat::from_path(&path).is_some() {
      path.set_extension("");
    }
    if path.extension().is_none_or(|extension| extension != "db") {
      continue;
    }
    let Some(day) = path.file_stem().and_then(|value| value.to_str()) else {
      continue;
    };
    if !is_valid_request_day(day) {
      continue;
    }
    days.insert(day.to_owned());
  }
  Ok(
    days
      .into_iter()
      .rev()
      .map(|day| DayFile {
        path: requests_dir.join(format!("{day}.db")),
        day,
      })
      .collect(),
  )
}

fn probe_request_day_state_best_effort(day_file: &DayFile) -> RequestDayState {
  let result = (|| -> Result<RequestDayState> {
    let Some(conn) = day_file.open_with_timeout(DAY_PROBE_BUSY_TIMEOUT)? else {
      tracing::warn!(
        path = %day_file.path.display(),
        "request history database disappeared while checking its availability"
      );
      return Ok(RequestDayState::Unavailable);
    };
    let state = if day_has_requests(&conn)? {
      RequestDayState::Available
    } else {
      RequestDayState::Empty
    };
    Ok(state)
  })();

  match result {
    Ok(state) => state,
    Err(error) => {
      tracing::warn!(
        path = %day_file.path.display(),
        error = %error,
        "marking request history database unavailable after read failure"
      );
      RequestDayState::Unavailable
    }
  }
}

fn day_has_requests(conn: &Connection) -> Result<bool> {
  let sql = if RequestSchema::read(conn)?.is_split() {
    "SELECT EXISTS(SELECT 1 FROM request_connection)"
  } else {
    "SELECT EXISTS(SELECT 1 FROM requests)"
  };
  let has_requests = conn.query_row(sql, [], |row| row.get::<_, i64>(0))?;
  Ok(has_requests != 0)
}
