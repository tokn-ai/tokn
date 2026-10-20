use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::Extras;

/// Content part for an input message.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputContentPart {
  InputText {
    text: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  InputImage {
    #[serde(default, flatten)]
    fields: Extras,
  },
  InputAudio {
    #[serde(default, flatten)]
    fields: Extras,
  },
  InputFile {
    #[serde(default, flatten)]
    fields: Extras,
  },
  #[serde(other)]
  Other,
}

/// Content part attached to an assistant `message` output item.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputContentPart {
  OutputText {
    text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    annotations: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    logprobs: Option<Value>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  Refusal {
    refusal: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(other)]
  Other,
}

/// One element of a `reasoning` item's `content` or `summary` arrays.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReasoningPart {
  ReasoningText {
    text: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  SummaryText {
    text: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  Text {
    text: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(other)]
  Other,
}
