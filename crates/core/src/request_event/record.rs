//! Request-side records captured alongside pipeline execution.
//!
//! Distinct from the upstream-shaped *intent* values carried on stage
//! summaries (`ConvertedRequestSummary`, `SentSummary`):
//!
//! - intent values describe what requests *prepared*;
//! - records describe transport-adjacent facts the pipeline itself does
//!   not retain: inbound connection metadata, outbound wire-truth after
//!   `Provider::patch_headers` auth injection / `Host`+`Content-Length`
//!   stripping in `tokn_core::util::http::send`, and parsed usage.
//!
//! Persistence uses records to populate connection, usage, outbound,
//! and fully-converted response body columns; intent values still flow through the per-stage
//! stage events for diagnostics (and for dry-run profiles whose Send stage is a
//! no-op).
//!
//! Records ride a dedicated [`RequestEventPayload::Record`] variant
//! (peer of `Stage` and `Custom`) rather than nesting inside
//! `StageEvent`, so subscribers that only care about lifecycle/error
//! observation don't pay a match-arm tax for wire-truth captures, and
//! vice versa.
//!
//! [`RequestEventPayload::Record`]: super::RequestEventPayload::Record

use crate::headers::HeaderMap;
use crate::types::{Bytes, SmolStr};
use crate::usage::Usage;
use serde::Serialize;

/// A local rejection returned before the request pipeline takes ownership.
///
/// The enclosing [`super::RequestEvent::ts`] is the completion time. Rejected
/// bodies are deliberately absent: a failed body read may be partial, exceed
/// the configured limit, or contain data that was never admitted for execution.
/// Emit this record only on local ingress failures, never for requests already
/// dispatched to a pipeline, whose lifecycle supplies its own records.
#[derive(Debug, Clone, Serialize)]
pub struct IngressRejection {
  pub started_ts: i64,
  pub request_endpoint: super::RequestEndpoint,
  pub status: u16,
  pub error: SmolStr,
  pub mode: SmolStr,
  pub pipeline_id: SmolStr,
  pub method: SmolStr,
  pub url: Option<SmolStr>,
  /// Original inbound headers. Persistence must apply its normal redaction.
  pub headers: HeaderMap,
  pub user: Option<SmolStr>,
  pub api_key_id: Option<SmolStr>,
  pub local_addr: Option<SmolStr>,
  pub peer_addr: Option<SmolStr>,
}

/// Transport facts recorded alongside request execution.
///
/// Ingress failures are self-contained. Pipeline records split captures so
/// subscribers can write each piece as soon as it is known: inbound connection
/// facts, outbound request bytes, response status and headers, accumulated
/// bodies, and parsed usage. Providers emit outbound captures through
/// `tokn_core::provider::OutboundCapture` in standard builds.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum RecordEvent {
  /// Complete record for a request rejected before pipeline dispatch.
  IngressRejected(IngressRejection),
  /// Inbound client->router connection facts captured by the transport
  /// before the pipeline starts. Emitted outside the runner so callers can
  /// supply whatever connection context they have without widening
  /// [`RawInbound`](tokn_requests::RawInbound).
  InboundConnection {
    /// Authenticated client label. This is the API-key name for managed
    /// requests and is absent when client authentication is bypassed.
    user: Option<SmolStr>,
    /// Stable identifier of the client API key used for authentication.
    api_key_id: Option<SmolStr>,
    local_addr: Option<SmolStr>,
    peer_addr: Option<SmolStr>,
    mode: SmolStr,
    method: SmolStr,
    inbound_method: SmolStr,
    url: Option<SmolStr>,
  },
  /// Outbound request as it left reqwest. Headers reflect post-strip,
  /// post-patch state; `body` is the exact bytes handed to reqwest.
  UpstreamReq {
    method: SmolStr,
    url: SmolStr,
    headers: HeaderMap,
    body: Bytes,
  },
  /// Upstream response status + headers, captured as soon as they
  /// arrive. Headers reflect reqwest's post-decompression view (the
  /// `Content-Encoding` and `Content-Length` headers are stripped when
  /// reqwest decompresses).
  UpstreamResp { status: u16, headers: HeaderMap },
  /// Materialized upstream response body. Emitted only for buffered
  /// responses or after streaming accumulation finishes. Streaming paths may
  /// carry a partial body plus the stream termination error.
  UpstreamBody { body: Bytes, error: Option<SmolStr> },
  /// Materialized client-facing response bytes after any endpoint translation.
  /// Standard recording emits buffered bytes or the accumulated streaming body;
  /// these bytes can also represent empty or opaque non-JSON responses.
  ConvertedBody { body: Bytes, error: Option<SmolStr> },
  /// Parsed token usage attributed to the request. Added now so callers and
  /// persistence can converge on a stable shape before every execution path
  /// emits it.
  Usage(Usage),
}
