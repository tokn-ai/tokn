use serde::Serialize;
use serde_json::{Map, Value};
use tokn_core::request_classification::RequestClassification;

use super::RequestCursor;
use crate::viewer::{effective_limit, value::serialize_i64_as_string};

/// Query options accepted by the request list and inferred session timeline.
#[derive(Debug, Clone, Default)]
pub struct RequestListOptions {
  pub day: Option<String>,
  pub limit: Option<usize>,
  pub cursor: Option<RequestCursor>,
  pub session_id: Option<String>,
  pub provider_id: Option<String>,
  pub url_path: Option<String>,
  pub status: Option<u16>,
  pub errors_only: bool,
  pub query: Option<String>,
}

/// One normalized inbound URL path available on a request day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RequestUrlPath {
  pub url_path: String,
  pub request_count: u64,
}

impl RequestListOptions {
  pub(super) fn effective_limit(&self) -> usize {
    effective_limit(self.limit)
  }
}

/// A compact request row suitable for a viewer list or session timeline.
#[derive(Debug, Clone, Serialize)]
pub struct RequestSummary {
  #[serde(serialize_with = "serialize_i64_as_string")]
  pub row_id: i64,
  pub day: String,
  pub request_id: String,
  pub ts: i64,
  pub endpoint: Option<String>,
  pub request_classification: Option<RequestClassification>,
  pub status: Option<u16>,
  pub request_error: Option<String>,
  pub session_id: Option<String>,
  pub account_id: Option<String>,
  pub provider_id: Option<String>,
  pub model: Option<String>,
  pub inbound_req_method: Option<String>,
  pub inbound_req_url: Option<String>,
  pub outbound_resp_status: Option<u16>,
  pub inbound_resp_status: Option<u16>,
}

/// One page of request summaries in stable, newest-first order.
#[derive(Debug, Clone, Serialize)]
pub struct RequestPage {
  pub requests: Vec<RequestSummary>,
  pub next_cursor: Option<String>,
}

/// The newest available request day and its latest request rows.
#[derive(Debug, Clone, Serialize)]
pub struct LatestRequests {
  pub day: Option<String>,
  pub requests: Vec<RequestSummary>,
  pub next_cursor: Option<String>,
}

/// Decoded request metadata for one identity (`day`, `request_id`), excluding
/// large network header/body payloads.
#[derive(Debug, Clone, Serialize)]
pub struct RequestDetail {
  pub day: String,
  #[serde(serialize_with = "serialize_i64_as_string")]
  pub row_id: i64,
  pub request: Map<String, Value>,
}

/// One lazily fetched request payload field.
#[derive(Debug, Clone, Serialize)]
pub struct RequestPayload {
  pub field: String,
  pub value: Value,
}

/// Payload fields that may be fetched separately from the request overview.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestPayloadField {
  InboundReqHeaders,
  InboundReqBody,
  InboundRespHeaders,
  InboundRespBody,
  OutboundReqHeaders,
  OutboundReqBody,
  OutboundRespHeaders,
  OutboundRespBody,
}

impl RequestPayloadField {
  pub fn as_str(self) -> &'static str {
    match self {
      Self::InboundReqHeaders => "inbound_req_headers",
      Self::InboundReqBody => "inbound_req_body",
      Self::InboundRespHeaders => "inbound_resp_headers",
      Self::InboundRespBody => "inbound_resp_body",
      Self::OutboundReqHeaders => "outbound_req_headers",
      Self::OutboundReqBody => "outbound_req_body",
      Self::OutboundRespHeaders => "outbound_resp_headers",
      Self::OutboundRespBody => "outbound_resp_body",
    }
  }

  pub(super) fn is_payload_column(value: &str) -> bool {
    value.parse::<Self>().is_ok()
  }
}

impl std::str::FromStr for RequestPayloadField {
  type Err = InvalidRequestPayloadField;

  fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
    match value {
      "inbound_req_headers" => Ok(Self::InboundReqHeaders),
      "inbound_req_body" => Ok(Self::InboundReqBody),
      "inbound_resp_headers" => Ok(Self::InboundRespHeaders),
      "inbound_resp_body" => Ok(Self::InboundRespBody),
      "outbound_req_headers" => Ok(Self::OutboundReqHeaders),
      "outbound_req_body" => Ok(Self::OutboundReqBody),
      "outbound_resp_headers" => Ok(Self::OutboundRespHeaders),
      "outbound_resp_body" => Ok(Self::OutboundRespBody),
      _ => Err(InvalidRequestPayloadField),
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRequestPayloadField;

impl std::fmt::Display for InvalidRequestPayloadField {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.write_str("invalid request payload field")
  }
}

impl std::error::Error for InvalidRequestPayloadField {}
