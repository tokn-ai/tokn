use serde::Serialize;
use serde_json::Value;

/// A session represented either by semantic storage or inferred request rows.
#[derive(Debug, Clone, Serialize)]
pub struct SessionSummary {
  pub session_id: String,
  pub source: Option<String>,
  pub first_ts: i64,
  pub last_ts: i64,
  pub request_count: u64,
  pub last_request_day: String,
  pub last_request_id: String,
  pub endpoint: Option<String>,
  pub status: Option<u16>,
  pub account_id: Option<String>,
  pub provider_id: Option<String>,
  pub model: Option<String>,
}

/// A session timeline inferred directly from request-day databases.
#[derive(Debug, Clone, Serialize)]
pub struct SessionDetail {
  pub session: SessionSummary,
  pub requests: Vec<super::super::requests::RequestSummary>,
}

/// Read-only semantic metadata for one stored session.
#[derive(Debug, Clone, Serialize)]
pub struct StoredSessionDetail {
  pub session: SessionSummary,
  pub head_node_id: Option<String>,
  pub nodes: Vec<SessionNodeSummary>,
  pub nodes_truncated: bool,
}

/// Compact metadata for one node in a semantic session tree.
#[derive(Debug, Clone, Serialize)]
pub struct SessionNodeSummary {
  pub node_id: String,
  pub parent_node_id: Option<String>,
  pub request_id: String,
  pub ts: i64,
  pub endpoint: String,
  pub status: Option<u16>,
  pub account_id: Option<String>,
  pub provider_id: Option<String>,
  pub model: Option<String>,
  pub reduction_kind: String,
  pub parent_source: String,
  pub common_prefix_messages: u64,
  pub request_message_count: u64,
  pub response_message_count: u64,
  /// Hex-encoded prefix hash for message-tree nodes; null for untouched legacy nodes.
  pub message_id: Option<String>,
  /// Total input messages on the path, not only a node-local legacy delta.
  pub input_message_count: u64,
  /// Messages appended after the input boundary by this observation.
  pub output_message_count: u64,
  pub is_head: bool,
}

/// Full stored input prefix and captured output suffix for one observation node.
#[derive(Debug, Clone, Serialize)]
pub struct SessionNodeDetail {
  pub node: SessionNodeSummary,
  pub request_messages: Vec<SessionMessage>,
  pub response_messages: Vec<SessionMessage>,
  pub truncation: SessionNodeDetailTruncation,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionMessage {
  pub role: String,
  pub status: Option<u16>,
  pub parts: Vec<SessionPart>,
  pub parts_total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionPart {
  pub part_type: String,
  pub byte_length: u64,
  pub content: SessionPartContent,
}

/// Exact totals describing which semantic content was bounded for the viewer.
#[derive(Debug, Clone, Serialize)]
pub struct SessionNodeDetailTruncation {
  pub request_messages: SessionMessageTruncation,
  pub response_messages: SessionMessageTruncation,
  pub parts_total: u64,
  pub parts_returned: u64,
  pub parts_omitted: u64,
  pub content_bytes_total: u64,
  pub content_bytes_returned: u64,
  pub content_parts_truncated: u64,
  pub binary_parts_elided: u64,
}

/// The retained range for one ordered side of a semantic node.
#[derive(Debug, Clone, Serialize)]
pub struct SessionMessageTruncation {
  pub messages_total: u64,
  pub messages_returned: u64,
  pub messages_omitted_before: u64,
  pub messages_omitted_after: u64,
}

/// A JSON-safe representation of arbitrary semantic part bytes.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "encoding", rename_all = "snake_case")]
pub enum SessionPartContent {
  Text {
    value: String,
    truncated: bool,
  },
  Json {
    value: Value,
  },
  Encrypted {
    byte_length: u64,
  },
  Binary {
    byte_length: u64,
  },
  Omitted {
    original_encoding: SessionPartEncoding,
    reason: SessionPartOmissionReason,
  },
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPartEncoding {
  Text,
  Json,
  Binary,
  Unknown,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPartOmissionReason {
  /// The stored content exceeded the byte limit for one part.
  PartLimit,
  /// Returning the content would exceed the byte limit for this message side.
  AggregateLimit,
}
