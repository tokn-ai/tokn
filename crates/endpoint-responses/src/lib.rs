//! Typed wire schemas for the OpenAI Responses API.
//!
//! Covers request, response, input/output items, content parts and
//! streaming event shapes.
//!
//! Disable the default `std` feature to use these types with `no_std + alloc`.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod content;
pub mod event;
pub mod item;
pub mod parameters;
pub mod request;
pub mod response;
pub mod usage;

#[cfg(debug_assertions)]
mod extra_keys_impls;

pub use content::{InputContentPart, OutputContentPart, ReasoningPart};
pub use event::{ResponsesEvent, ResponsesEventCommon};
pub use item::{
  FunctionCallItem, FunctionCallOutputItem, InputItem, InputMessage, InputMessageContent, OutputItem, OutputMessage,
  ReasoningItem, TaggedFunctionCall, TaggedFunctionCallOutput, TaggedOutputMessage, TaggedReasoning,
};
pub use parameters::{ResponsesExtraParameters, ResponsesRequestParameters};
pub use request::{ResponsesInput, ResponsesRequest, ResponsesToolChoice, ResponsesToolDef};
pub use response::ResponsesResponse;
pub use usage::ResponsesUsage;

use tokn_endpoint_core::{Endpoint, EndpointEvent, EndpointItem, EndpointRequest, EndpointResponse};

impl EndpointRequest for ResponsesRequest {
  const ENDPOINT: Endpoint = Endpoint::Responses;
}

impl EndpointResponse for ResponsesResponse {
  const ENDPOINT: Endpoint = Endpoint::Responses;
}

impl EndpointItem for InputItem {
  const ENDPOINT: Endpoint = Endpoint::Responses;
}

impl EndpointItem for OutputItem {
  const ENDPOINT: Endpoint = Endpoint::Responses;
}

impl EndpointEvent for ResponsesEvent {
  const ENDPOINT: Endpoint = Endpoint::Responses;

  fn event_name(&self) -> &str {
    self.kind()
  }
}
