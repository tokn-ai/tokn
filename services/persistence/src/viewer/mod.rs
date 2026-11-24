//! Read-only queries for the local request and session history viewer.
//!
//! The gateway writes request history into one SQLite database per UTC day.
//! Viewer queries deliberately open those files in read-only mode: displaying
//! history must never create a database, apply a migration, or take ownership
//! of a writer connection.

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
pub use usage::{get_session_usage, SessionRequestUsage, SessionUsage};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 500;

fn effective_limit(limit: Option<usize>) -> usize {
  limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

#[cfg(test)]
mod tests;
