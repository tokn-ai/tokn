pub mod access;
#[cfg(feature = "lzma")]
pub mod archive_xz;
pub mod archive_zstd;
pub mod history_import;
pub mod indexed_archive;
pub mod migrate;
pub mod request_archive;
pub mod request_files;
pub mod requests;
pub mod sessions;
pub mod usage;

pub use access::{AccessDb, ApiKeyRecord, ApiKeySummaryRecord, NewApiKeyRecord};
pub use requests::{is_valid_request_day, read_request_row, RequestEventHandler, RequestPersistenceOptions};
pub use sessions::SessionEventHandler;

use bytes::Bytes;
use snafu::Snafu;
pub use tokn_core::db::{DbPaths, HttpSnapshot, MessageRecord, PartRecord};
#[allow(unused_imports)]
pub(crate) use tokn_core::db::{Usage, UsageDetails};
pub use usage::{UsageDb, UsageEventHandler};

/// Open an existing SQLite database for queries without creating or migrating it.
pub fn open_readonly(
  path: &std::path::Path,
  busy_timeout: std::time::Duration,
) -> rusqlite::Result<rusqlite::Connection> {
  let conn = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
  conn.busy_timeout(busy_timeout)?;
  conn.execute_batch("PRAGMA query_only = ON;")?;
  Ok(conn)
}

/// Serialise an HTTP header map to JSON bytes, redacting values whose name
/// is sensitive (`authorization`, `proxy-authorization`, `x-zcode-authorization`,
/// `cookie`, anything containing `api-key`). Public so both inbound (server::forward) and
/// outbound (db::requests) capture paths share the same redaction policy.
pub fn headers_json(headers: &tokn_headers::HeaderMap) -> Bytes {
  use serde_json::{Map, Value};
  let mut out = Map::new();
  for (name, value) in headers {
    let key = name.as_str().to_ascii_lowercase();
    let value = if is_sensitive_header(&key) {
      "<redacted>".to_string()
    } else {
      value.as_str().to_string()
    };
    out.insert(key, Value::String(value));
  }
  serde_json::to_vec(&Value::Object(out)).unwrap_or_default().into()
}

pub fn is_sensitive_header(name: &str) -> bool {
  matches!(
    name,
    "authorization" | "proxy-authorization" | "x-zcode-authorization" | "cookie"
  ) || name.contains("api-key")
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
  #[snafu(display("db io"))]
  Io { source: std::io::Error },

  #[snafu(display("sqlite"))]
  Sqlite { source: rusqlite::Error },

  #[snafu(display("session message tree is invalid at {message_id}"))]
  InvalidMessageTree { message_id: String },

  #[snafu(display("db writer channel closed"))]
  ChannelClosed,
}

impl From<std::io::Error> for Error {
  fn from(source: std::io::Error) -> Self {
    Error::Io { source }
  }
}

impl From<rusqlite::Error> for Error {
  fn from(source: rusqlite::Error) -> Self {
    Error::Sqlite { source }
  }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
