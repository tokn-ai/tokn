use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use std::time::Duration;

use crate::Result;

const READ_BUSY_TIMEOUT: Duration = Duration::from_millis(2_500);

pub(super) fn open_readonly(path: &Path) -> Result<Option<Connection>> {
  open_readonly_with_timeout(path, READ_BUSY_TIMEOUT)
}

pub(super) fn open_readonly_with_timeout(path: &Path, busy_timeout: Duration) -> Result<Option<Connection>> {
  if !path.exists() {
    return Ok(None);
  }
  let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
  conn.busy_timeout(busy_timeout)?;
  conn.execute_batch("PRAGMA query_only = ON;")?;
  Ok(Some(conn))
}
