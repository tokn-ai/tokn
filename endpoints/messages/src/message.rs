use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

use tokn_endpoint_core::{Extras, Role};

use crate::content::ContentBlock;

/// One entry in the request `messages[]` array.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
  pub role: Role,
  pub content: MessageContent,
  #[serde(default, flatten)]
  pub extras: Extras,
}

/// Messages API accepts a string or a list of typed blocks.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
  Text(String),
  Blocks(Vec<ContentBlock>),
}

impl Default for MessageContent {
  fn default() -> Self {
    Self::Blocks(Vec::new())
  }
}
