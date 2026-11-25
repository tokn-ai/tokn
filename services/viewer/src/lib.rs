//! Read-only queries over recorded requests, session trees and usage.
//!
//! The gateway writes request history into one SQLite database per UTC day.
//! Queries open live databases read-only, without migrating them. Archived
//! days are decompressed into private temporary databases for the duration of
//! a query, so pruned history can be browsed with the gateway offline.

mod database;
mod days;
mod requests;
mod schema;
mod sessions;
mod usage;
mod value;

pub use days::{is_valid_request_day, list_request_days, RequestDay, RequestDayState};
pub use requests::{
  get_request, get_request_llm_message, get_request_llm_summary, get_request_llm_tool_definition, get_request_payload,
  list_latest_requests, list_request_url_paths, list_requests, InvalidRequestCursor, InvalidRequestPayloadField,
  LatestRequests, LlmItemDetail, LlmMessageSummary, LlmRequestContentSummary, LlmToolDefinitionSummary, RequestCursor,
  RequestDetail, RequestListOptions, RequestPage, RequestPayload, RequestPayloadField, RequestSummary, RequestUrlPath,
};
pub use sessions::{
  get_session, get_session_from_db, get_session_node_from_db, list_sessions, list_sessions_from_db, SessionDetail,
  SessionMessage, SessionMessageTruncation, SessionNodeDetail, SessionNodeDetailTruncation, SessionNodeSummary,
  SessionPart, SessionPartContent, SessionPartEncoding, SessionPartOmissionReason, SessionSummary, StoredSessionDetail,
};
pub use tokn_persistence::usage::{RowSummary as UsageSummary, SummaryError as UsageSummaryError};
pub use tokn_persistence::DbPaths;
pub use usage::{get_session_usage, usage_summary, SessionRequestUsage, SessionUsage};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
  #[snafu(transparent)]
  Io { source: std::io::Error },
  #[snafu(transparent)]
  Sqlite { source: rusqlite::Error },
  #[snafu(transparent)]
  Database { source: tokn_persistence::Error },
  #[snafu(display(
    "sessions database schema version {version} does not support session viewing; version 2 or newer is required"
  ))]
  UnsupportedSessionSchema { version: u32 },
  #[snafu(display(
    "usage database schema version {version} does not support session usage; version 2 or newer is required"
  ))]
  UnsupportedUsageSchema { version: u32 },
  #[snafu(display("session message tree is invalid at {message_id}"))]
  InvalidMessageTree { message_id: String },
}

pub type Result<T> = std::result::Result<T, Error>;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 500;

fn effective_limit(limit: Option<usize>) -> usize {
  limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

#[cfg(test)]
mod tests;
