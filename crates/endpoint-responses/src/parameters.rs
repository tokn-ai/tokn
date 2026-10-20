use alloc::string::String;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::ToolChoice;
use tokn_endpoint_macros::LenientFields;

/// Scalar / small-enum generation knobs accepted by the OpenAI Responses
/// API request body (`POST /v1/responses`).
///
/// Sourced from the official OpenAPI spec (`CreateResponse`) plus
/// newer fields documented at platform.openai.com that are not yet in
/// the public OpenAPI release. Only fields that appear in **requests**
/// are included; result-only fields (`completed_at`, `incomplete_details`,
/// `error`, `output`, `output_text`, `usage`, `id`, `object`,
/// `created_at`, `status`) stay on
/// [`ResponsesResponse`](crate::ResponsesResponse).
///
/// Excluded (kept on [`ResponsesRequest`](crate::ResponsesRequest)
/// directly):
/// - Content: `input`, `instructions`
/// - Tools: `tools`, `tool_choice`
/// - Structured config: `text`, `reasoning`, `metadata`,
///   `client_metadata`
/// - Lists: `include`, `stop`
/// - Streaming: `stream`
///
/// Vendor-specific scalar fields go in
/// [`ResponsesExtraParameters`];
/// unknown JSON keys are captured by the parent's `extras` field.
#[derive(Clone, Debug, Default, Serialize, Deserialize, LenientFields)]
pub struct ResponsesRequestParameters {
  /// Sampling temperature in `[0, 2]`. Higher values increase
  /// randomness.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub temperature: Option<f64>,

  /// Nucleus sampling probability mass.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub top_p: Option<f64>,

  /// Upper bound on tokens generated for a response, including any
  /// reasoning tokens.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub max_output_tokens: Option<u64>,

  /// Maximum number of tool calls the model may make in a single
  /// response (newer Responses-only knob).
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub max_tool_calls: Option<u64>,

  /// Number of most-likely tokens to return at each output position.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub top_logprobs: Option<u32>,

  /// Whether to allow the model to run tool calls in parallel.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub parallel_tool_calls: Option<bool>,

  /// How the model should select tools, if any. Either a string mode
  /// (`"none"`, `"auto"`, `"required"`) or a structured selector
  /// object.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub tool_choice: Option<ToolChoice>,

  /// Reasoning configuration blob (effort, summary, etc.).
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub reasoning: Option<Value>,

  /// Structured-output / response-format controls
  /// (`text.format`, `text.verbosity`). This is a control blob, not
  /// model output content; the actual generated text lives on
  /// [`ResponsesResponse`](crate::ResponsesResponse).
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub text: Option<Value>,

  /// Truncation strategy applied when the input exceeds the context
  /// window. Common values: `"auto"`, `"disabled"`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub truncation: Option<String>,

  /// Service tier for processing the request. Common values:
  /// `"auto"`, `"default"`, `"flex"`, `"scale"`, `"priority"`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub service_tier: Option<String>,

  /// Whether to store the generated model response for later
  /// retrieval.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub store: Option<bool>,

  /// Run the response generation in the background and return an
  /// in-progress response immediately.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub background: Option<bool>,

  /// Unique ID of the previous response to continue. Enables
  /// multi-turn conversations on the server.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub previous_response_id: Option<String>,

  /// Opaque routing/caching key used to bias requests with a similar
  /// prefix toward the same backend cache.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub prompt_cache_key: Option<String>,

  /// How long the prompt cache entry should be retained, e.g.
  /// `"in_memory"` or a duration string. Provider-specific values.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub prompt_cache_retention: Option<String>,

  /// Stable identifier used by safety/abuse-monitoring systems.
  /// Distinct from [`user`](Self::user); meant to remain stable
  /// across sessions.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub safety_identifier: Option<String>,

  /// A unique identifier representing the end-user.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub user: Option<String>,
}

/// Vendor-specific scalar parameters that may be sent on Responses
/// requests by non-OpenAI providers.
///
/// Currently empty — the Responses API has not yet seen significant
/// non-OpenAI adoption with extra scalar dials. Add fields here as
/// the harness reports new vendor extensions. Embed alongside
/// [`ResponsesRequestParameters`] on the request struct via
/// `#[serde(flatten)]`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, LenientFields)]
pub struct ResponsesExtraParameters {}
