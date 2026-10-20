use alloc::string::String;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::Extras;

use crate::response::ResponsesResponse;

/// Fields shared by most Responses streaming events. Stored on every
/// variant so consumers can route without matching on the variant first.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResponsesEventCommon {
  /// Monotonic sequence id; OpenAI emits this on every streaming event.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub sequence_number: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub response_id: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub item_id: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub output_index: Option<u32>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub content_index: Option<u32>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub summary_index: Option<u32>,
}

/// Streaming events emitted by the Responses API. Variants cover the
/// commonly used types; everything else falls into [`ResponsesEvent::Other`].
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsesEvent {
  #[serde(rename = "response.created")]
  Created {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<ResponsesResponse>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.in_progress")]
  InProgress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<ResponsesResponse>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.output_item.added")]
  OutputItemAdded {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    #[serde(default)]
    item: Value,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.output_item.done")]
  OutputItemDone {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    #[serde(default)]
    item: Value,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.content_part.added")]
  ContentPartAdded {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    #[serde(default)]
    part: Value,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.content_part.done")]
  ContentPartDone {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    #[serde(default)]
    part: Value,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.output_text.delta")]
  OutputTextDelta {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    delta: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    obfuscation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    logprobs: Option<Value>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.output_text.done")]
  OutputTextDone {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    logprobs: Option<Value>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.reasoning_text.delta")]
  ReasoningTextDelta {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    delta: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    obfuscation: Option<String>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.reasoning_text.done")]
  ReasoningTextDone {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    text: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.reasoning_summary_part.added")]
  ReasoningSummaryPartAdded {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    #[serde(default)]
    part: Value,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.reasoning_summary_text.delta")]
  ReasoningSummaryTextDelta {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    delta: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    obfuscation: Option<String>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.reasoning_summary_text.done")]
  ReasoningSummaryTextDone {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    text: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.function_call_arguments.delta")]
  FunctionCallArgumentsDelta {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    call_id: Option<String>,
    delta: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.function_call_arguments.done")]
  FunctionCallArgumentsDone {
    #[serde(default, flatten)]
    common: ResponsesEventCommon,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    call_id: Option<String>,
    arguments: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.completed")]
  Completed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<ResponsesResponse>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.failed")]
  Failed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<ResponsesResponse>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "response.incomplete")]
  Incomplete {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<ResponsesResponse>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(rename = "error")]
  Error {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequence_number: Option<u64>,
    #[serde(default)]
    error: Value,
    #[serde(default, flatten)]
    extras: Extras,
  },
  /// Catch-all for less common or future event types.
  #[serde(untagged)]
  Other(Value),
}

impl ResponsesEvent {
  pub fn kind(&self) -> &str {
    match self {
      Self::Created { .. } => "response.created",
      Self::InProgress { .. } => "response.in_progress",
      Self::OutputItemAdded { .. } => "response.output_item.added",
      Self::OutputItemDone { .. } => "response.output_item.done",
      Self::ContentPartAdded { .. } => "response.content_part.added",
      Self::ContentPartDone { .. } => "response.content_part.done",
      Self::OutputTextDelta { .. } => "response.output_text.delta",
      Self::OutputTextDone { .. } => "response.output_text.done",
      Self::ReasoningTextDelta { .. } => "response.reasoning_text.delta",
      Self::ReasoningTextDone { .. } => "response.reasoning_text.done",
      Self::ReasoningSummaryPartAdded { .. } => "response.reasoning_summary_part.added",
      Self::ReasoningSummaryTextDelta { .. } => "response.reasoning_summary_text.delta",
      Self::ReasoningSummaryTextDone { .. } => "response.reasoning_summary_text.done",
      Self::FunctionCallArgumentsDelta { .. } => "response.function_call_arguments.delta",
      Self::FunctionCallArgumentsDone { .. } => "response.function_call_arguments.done",
      Self::Completed { .. } => "response.completed",
      Self::Failed { .. } => "response.failed",
      Self::Incomplete { .. } => "response.incomplete",
      Self::Error { .. } => "error",
      Self::Other(v) => v.get("type").and_then(Value::as_str).unwrap_or(""),
    }
  }
}
