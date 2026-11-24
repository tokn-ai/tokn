use bytes::Bytes;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct DbPaths {
  pub usage_db: PathBuf,
  pub sessions_db: PathBuf,
  pub requests_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionSource {
  Header,
  Auto,
}

impl SessionSource {
  pub fn as_str(self) -> &'static str {
    match self {
      SessionSource::Header => "header",
      SessionSource::Auto => "auto",
    }
  }
}

pub use crate::usage::{Usage, UsageDetails, UsageType};

#[derive(Debug, Clone)]
pub struct MessageRecord {
  pub role: String,
  pub status: Option<u16>,
  pub parts: Vec<PartRecord>,
}

#[derive(Debug, Clone)]
pub struct PartRecord {
  pub part_type: String,
  pub content: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct HttpSnapshot {
  pub url: Option<String>,
  pub method: Option<String>,
  /// Response status (req side has no status).
  pub status: Option<u16>,
  pub req_headers: crate::headers::HeaderMap,
  pub req_body: Bytes,
  pub resp_headers: crate::headers::HeaderMap,
  pub resp_body: Bytes,
}

pub type OutboundSnapshot = HttpSnapshot;
