use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::Extras;

/// Chat content can be either a plain string or a list of typed parts.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChatContent {
  Text(String),
  Parts(Vec<ContentPart>),
}

impl Default for ChatContent {
  fn default() -> Self {
    Self::Text(String::new())
  }
}

/// One element of a structured chat message content array.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
  Text {
    text: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  ImageUrl {
    image_url: Value,
    #[serde(default, flatten)]
    extras: Extras,
  },
  InputAudio {
    input_audio: Value,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(other)]
  Other,
}
