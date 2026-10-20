//! Typed wire schemas for the OpenAI Chat Completions API.
//!
//! Covers request, response, message item, streaming chunk and event
//! shapes. Conversion between endpoints lives elsewhere; this crate
//! only provides serde-friendly types.
//!
//! Disable the default `std` feature to use these types with `no_std + alloc`.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod content;
pub mod event;
pub mod message;
pub mod parameters;
pub mod request;
pub mod response;
pub mod usage;

#[cfg(debug_assertions)]
mod extra_keys_impls;

pub use content::{ChatContent, ContentPart};
pub use event::{ChatChunk, ChatDelta, ChatEvent, ChunkChoice};
pub use message::{ChatMessage, ChatToolCall, ChatToolFunction};
pub use parameters::{ChatExtraParameters, ChatRequestParameters};
pub use request::{ChatRequest, ChatToolChoice, ChatToolDef};
pub use response::{ChatChoice, ChatResponse};
pub use usage::ChatUsage;

use tokn_endpoint_core::{Endpoint, EndpointEvent, EndpointItem, EndpointRequest, EndpointResponse};

impl EndpointRequest for ChatRequest {
  const ENDPOINT: Endpoint = Endpoint::ChatCompletions;
}

impl EndpointResponse for ChatResponse {
  const ENDPOINT: Endpoint = Endpoint::ChatCompletions;
}

impl EndpointItem for ChatMessage {
  const ENDPOINT: Endpoint = Endpoint::ChatCompletions;
}

impl EndpointEvent for ChatEvent {
  const ENDPOINT: Endpoint = Endpoint::ChatCompletions;

  fn event_name(&self) -> &str {
    match self {
      ChatEvent::Chunk(_) => "chat.completion.chunk",
      ChatEvent::Done => "[DONE]",
    }
  }
}
