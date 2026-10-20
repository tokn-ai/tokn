use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::ToolChoice;
use tokn_endpoint_macros::LenientFields;

/// Scalar / small-enum generation knobs accepted by the OpenAI Chat
/// Completions API request body (`POST /v1/chat/completions`).
///
/// Sourced from the official OpenAPI spec (`CreateChatCompletionRequest`).
/// Only fields that appear in **requests** are included here; result-only
/// fields stay on [`ChatResponse`](crate::ChatResponse).
///
/// Excluded (kept on [`ChatRequest`](crate::ChatRequest) directly):
/// - Content: `messages`
/// - Tools: `tools`, `tool_choice`, deprecated `functions`/`function_call`
/// - Structured output: `response_format`, `prediction`, `audio`,
///   `web_search_options`
/// - Sampling structures: `stop`, `logit_bias`
/// - Streaming/metadata: `stream`, `stream_options`, `metadata`
///
/// Vendor-specific scalar fields go in
/// [`ChatExtraParameters`]; unknown JSON
/// keys are captured by the parent's `extras` field.
#[derive(Clone, Debug, Default, Serialize, Deserialize, LenientFields)]
pub struct ChatRequestParameters {
  /// Sampling temperature in `[0, 2]`. Higher values make output more
  /// random; lower values make it more deterministic.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub temperature: Option<f64>,

  /// Nucleus sampling: only consider tokens with cumulative probability
  /// mass `top_p`. Generally use either `temperature` or `top_p`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub top_p: Option<f64>,

  /// Frequency penalty in `[-2, 2]`. Positive values discourage
  /// verbatim repetition of previously generated tokens.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub frequency_penalty: Option<f64>,

  /// Presence penalty in `[-2, 2]`. Positive values encourage the model
  /// to talk about new topics.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub presence_penalty: Option<f64>,

  /// Upper bound on tokens generated for a completion, including any
  /// reasoning tokens. Replaces the deprecated `max_tokens` field for
  /// reasoning-capable models.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub max_completion_tokens: Option<u64>,

  /// Maximum number of tokens to generate. Deprecated by OpenAI in
  /// favor of [`max_completion_tokens`](Self::max_completion_tokens),
  /// but still accepted by most providers.
  #[deprecated(note = "Use `max_completion_tokens` instead.")]
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub max_tokens: Option<u64>,

  /// How many independent chat completion choices to generate per
  /// input.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub n: Option<u32>,

  /// Best-effort deterministic sampling seed. Repeated requests with
  /// the same seed and parameters should return the same result on a
  /// best-effort basis.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub seed: Option<i64>,

  /// Whether to return log-probabilities of output tokens.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub logprobs: Option<bool>,

  /// Number of most-likely tokens to return at each position when
  /// [`logprobs`](Self::logprobs) is true. Range `[0, 20]`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub top_logprobs: Option<u32>,

  /// Whether to enable parallel function calling during tool use.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub parallel_tool_calls: Option<bool>,

  /// How the model should select tools, if any. Either a string mode
  /// (`"none"`, `"auto"`, `"required"`) or a structured selector
  /// object.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub tool_choice: Option<ToolChoice>,

  /// Provider extension. Some gateways accept a `reasoning` config
  /// blob alongside chat completions to control reasoning behavior.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub reasoning: Option<Value>,

  /// Provider extension. Anthropic-style `thinking` config blob,
  /// accepted by some gateways that bridge to Claude.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub thinking: Option<Value>,

  /// Constrains effort on reasoning for reasoning models. Common
  /// values: `"minimal"`, `"low"`, `"medium"`, `"high"`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub reasoning_effort: Option<String>,

  /// Output modalities the model should generate, e.g.
  /// `["text"]` or `["text", "audio"]`. Small enum array; kept here
  /// rather than as a structured payload.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub modalities: Option<Vec<String>>,

  /// Service tier for processing the request. Common values:
  /// `"auto"`, `"default"`, `"flex"`, `"scale"`, `"priority"`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub service_tier: Option<String>,

  /// A unique identifier representing the end-user. Used by OpenAI for
  /// abuse monitoring.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub user: Option<String>,

  /// Whether to store the output for later retrieval, distillation, or
  /// evaluations.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub store: Option<bool>,
}

/// Vendor-specific scalar parameters frequently sent on Chat
/// Completions requests by non-OpenAI providers (vLLM, OpenRouter,
/// xAI, Mistral, etc.).
///
/// These are not part of the official OpenAI spec but are commonly
/// understood across multiple OpenAI-compatible backends. Embed
/// alongside [`ChatRequestParameters`] on the request struct via
/// `#[serde(flatten)]`.
///
/// Unknown JSON keys still fall through to the parent's `extras`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, LenientFields)]
pub struct ChatExtraParameters {
  /// Repetition penalty (xAI, vLLM, llama.cpp, others). Multiplicative
  /// penalty applied to previously generated tokens; values `> 1.0`
  /// discourage repetition.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub repetition_penalty: Option<f64>,

  /// Top-k sampling: restrict the candidate set to the `k` most-likely
  /// tokens. Exposed by Anthropic, vLLM, llama.cpp, xAI, and other
  /// OpenAI-compatible backends.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub top_k: Option<u64>,

  /// Min-p sampling threshold (vLLM, llama.cpp). Drops tokens whose
  /// probability is less than `min_p` times the most-likely token's
  /// probability.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub min_p: Option<f64>,

  /// Mistral safe-prompt flag: prepend a safety system prompt.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub safe_prompt: Option<bool>,

  /// OpenRouter routing hint (`"fallback"` etc.).
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub route: Option<String>,
}
