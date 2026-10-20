use alloc::string::String;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_macros::LenientFields;

use crate::request::MessagesToolChoice;

/// Scalar / small-enum generation knobs accepted by the Anthropic
/// Messages API request body (`POST /v1/messages`).
///
/// Only fields that appear in **requests** are included.
///
/// Excluded (kept on [`MessagesRequest`](crate::MessagesRequest)
/// directly):
/// - Content: `messages`, `system`, `stop_sequences`
/// - Tools: `tools`, `tool_choice`
/// - Structured config: `metadata`, `thinking`
/// - Required scalar: `max_tokens` (Anthropic requires it; kept at
///   top level so the type system enforces presence)
/// - Streaming: `stream`
///
/// Vendor-specific scalar fields go in
/// [`MessagesExtraParameters`];
/// unknown JSON keys are captured by the parent's `extras` field.
#[derive(Clone, Debug, Default, Serialize, Deserialize, LenientFields)]
pub struct MessagesRequestParameters {
  /// Sampling temperature in `[0, 1]` (Anthropic clamps higher
  /// values).
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub temperature: Option<f64>,

  /// Nucleus sampling probability mass.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub top_p: Option<f64>,

  /// Top-k sampling: restrict candidate tokens to the `k` most
  /// likely.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub top_k: Option<u64>,

  /// Service tier for processing the request. Anthropic-defined
  /// values: `"auto"`, `"standard_only"`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub service_tier: Option<String>,

  /// How the model should select tools. Anthropic uses an object
  /// form like `{"type": "auto"|"any"|"tool"|"none", ...}`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub tool_choice: Option<MessagesToolChoice>,

  /// Extended-thinking configuration blob (Claude 3.5+).
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub thinking: Option<Value>,
}

/// Vendor-specific scalar parameters that may be sent on Messages
/// requests by non-Anthropic backends (Bedrock, Vertex AI proxies,
/// etc.).
///
/// Currently empty — the Anthropic Messages dialect has been very
/// stable in scalar parameters. Add fields here as the harness
/// reports new vendor extensions. Embed alongside
/// [`MessagesRequestParameters`] on the request struct via
/// `#[serde(flatten)]`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, LenientFields)]
pub struct MessagesExtraParameters {}
