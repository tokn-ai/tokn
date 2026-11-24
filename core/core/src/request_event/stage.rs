//! Closed set of pipeline-observation events emitted by standard and injected
//! execution in `tokn-requests`.
//!
//! These types live in `tokn-core` (rather than `tokn-requests`) so that the
//! workspace's standard `tokn_core::event::Event` enum can embed request events
//! without inverting the dep graph (requests depends on tokn-core).
//!
//! `StageEvent` carries **lossy summaries** of each stage's output rather than
//! the full stage-output structs. The full structs in `tokn-requests` embed
//! types (`AccountHandle`, internal codec enums, etc.) that should not leak
//! into tokn-core's public surface. Summaries are cheap to clone and carry the
//! fields subscribers actually need (status, body bytes, headers, model id,
//! endpoint, request_id, etc.).
//!
//! Each `*Summary` struct corresponds 1:1 with a requests stage output
//! (`Extracted`, `Resolved`, `BuiltHeaders`, `ConvertedRequest`,
//! `SentResponse`, `ConvertedResponse`); requests provides `From` impls.

use crate::headers::{HeaderMap, TemplateVars};
use crate::provider::Endpoint;
use crate::request_classification::RequestClassification;
use crate::types::{Bytes, Shared, SmolStr};
use crate::AgentId;
use serde::{Serialize, Serializer};
use serde_json::Value;

// Preserve the established event-module path for standard consumers.
pub use crate::endpoint::RequestEndpoint;

/// Identifies which pipeline stage produced an event. Used both as a tag on
/// success variants (implicit via the variant name) and as a field on
/// [`StageEvent::Error`] so subscribers can filter without pattern-matching
/// on N error variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
  Extract,
  Resolve,
  BuildHeaders,
  ConvertRequest,
  Send,
  ConvertResponse,
}

impl Stage {
  pub fn as_str(self) -> &'static str {
    match self {
      Stage::Extract => "extract",
      Stage::Resolve => "resolve",
      Stage::BuildHeaders => "build_headers",
      Stage::ConvertRequest => "convert_request",
      Stage::Send => "send",
      Stage::ConvertResponse => "convert_response",
    }
  }
}

impl core::fmt::Display for Stage {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str(self.as_str())
  }
}

/// Cloneable summary of `tokn_requests::pipeline::stages::Extracted`. Drops
/// the requests-internal `content_encoding` enum (kept inside requests only).
#[derive(Debug, Clone, Serialize)]
pub struct ExtractedSummary {
  pub agent_id: Option<AgentId>,
  pub request_classification: Option<RequestClassification>,
  pub model: SmolStr,
  pub stream: bool,
  pub session_id: Option<SmolStr>,
  pub project_id: Option<SmolStr>,
  pub initiator: Option<SmolStr>,
  pub header_initiator: Option<SmolStr>,
  pub route_mode_hint: Option<SmolStr>,
  pub headers: HeaderMap,
  pub raw_body: Bytes,
  pub decoded_body: Bytes,
  #[serde(serialize_with = "serialize_shared")]
  pub body_json: Shared<Value>,
}

/// Cloneable summary of `tokn_requests::pipeline::stages::Resolved`. Drops
/// the typed `AccountHandle` (which would require tokn-core to depend on
/// tokn-accounts).
#[derive(Debug, Clone, Serialize)]
pub struct ResolvedSummary {
  pub agent_id: Option<AgentId>,
  pub model: SmolStr,
  pub resolved_endpoint: Option<Endpoint>,
  pub upstream_model: SmolStr,
  pub upstream_endpoint: Option<Endpoint>,
  pub account_id: SmolStr,
  pub provider_id: SmolStr,
}

/// Cloneable summary of `tokn_requests::pipeline::stages::BuiltHeaders`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BuiltHeadersSummary {
  pub headers: HeaderMap,
  pub vars: TemplateVars,
  pub agent_id: AgentId,
}

/// Cloneable summary of `tokn_requests::pipeline::stages::ConvertedRequest`.
/// `content_encoding` is the wire token (e.g. `"gzip"`/`"zstd"`); the
/// requests-internal codec enum is intentionally not exposed here to keep
/// tokn-core free of requests's `utils::codec` types.
#[derive(Debug, Clone, Serialize)]
pub struct ConvertedRequestSummary {
  #[serde(serialize_with = "serialize_shared")]
  pub upstream_body: Shared<Value>,
  pub upstream_wire_body: Bytes,
  pub debug_outbound_body: Bytes,
  pub content_encoding: Option<SmolStr>,
}

/// Cloneable summary of `tokn_requests::pipeline::stages::SentResponse`.
/// The full struct can't be cloned (it owns a single-shot `reqwest::Response`),
/// so the `Send` event carries this summary instead.
#[derive(Debug, Clone, Serialize)]
pub struct SentSummary {
  pub status: u16,
  pub headers: HeaderMap,
  pub upstream_endpoint: Option<Endpoint>,
  /// Mirrors the inbound `stream` flag — true iff the client asked for SSE.
  pub stream: bool,
}

/// Cloneable summary of a converted response. Parsed buffered JSON bodies are
/// shared. Streams and buffered responses without parsed JSON set `body` to `None`.
#[derive(Debug, Clone, Serialize)]
pub struct ConvertedResponseSummary {
  pub status: u16,
  pub headers: HeaderMap,
  /// Parsed buffered JSON, when available. `None` for streams and unparsed
  /// buffered bodies, such as empty or opaque bytes.
  #[serde(serialize_with = "serialize_optional_shared")]
  pub body: Option<Shared<Value>>,
}

// Serialize the underlying values directly: serde's `rc` feature also enables
// Arc support, which is unavailable on targets without pointer-width atomics.
fn serialize_shared<T: Serialize + ?Sized, S: Serializer>(value: &Shared<T>, serializer: S) -> Result<S::Ok, S::Error> {
  value.as_ref().serialize(serializer)
}

fn serialize_optional_shared<T: Serialize + ?Sized, S: Serializer>(
  value: &Option<Shared<T>>,
  serializer: S,
) -> Result<S::Ok, S::Error> {
  value.as_deref().serialize(serializer)
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum StageEvent {
  /// Emitted once at the start of each attempt, before any stage has produced
  /// output. A retry starts another attempt with the same request identifier.
  Started { request_endpoint: RequestEndpoint },
  /// Extract stage completed successfully.
  Extract(ExtractedSummary),
  /// Resolve stage completed successfully.
  Resolve(ResolvedSummary),
  /// BuildHeaders stage completed successfully.
  BuildHeaders(BuiltHeadersSummary),
  /// ConvertRequest stage completed successfully.
  ConvertRequest(ConvertedRequestSummary),
  /// Send stage completed successfully.
  Send(SentSummary),
  /// Response conversion produced a buffered response or a live stream.
  /// Parsed buffered JSON is shared; a live stream's final outcome is reported
  /// later by `Completed` after delivery, failure, or cancellation.
  ConvertResponse(ConvertedResponseSummary),
  /// Any stage failure (or deliberate stop). `recoverable` and `stop` are
  /// propagated verbatim from the requests `PipelineError`. Subscribers that
  /// need partial state fold prior per-stage events themselves.
  Error {
    stage: Stage,
    message: SmolStr,
    recoverable: bool,
    stop: bool,
  },
  /// Emitted once when an attempt finishes. Buffered attempts finish after
  /// response conversion; live streams finish after their last output is
  /// delivered, or on failure or cancellation. `success` is false for failures,
  /// cancellation, and deliberate stops. `attempts` counts attempts so far,
  /// including the one that just finished.
  Completed { success: bool, attempts: u32 },
}
