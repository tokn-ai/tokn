//! LLM requests through a configured gateway runtime or an injected transport.
//!
//! With the default `std` feature, `Client` loads the same configuration and credential files as the
//! `tokn-gateway` runtime, then executes requests in-process through the shared
//! account pool, routing, conversion, retry, and provider implementations.
//!
//! [`DirectClient`] sends typed OpenAI requests through a caller-supplied
//! [`direct::Transport`] or [`direct::StreamingTransport`]. Disable default
//! features to use it with `no_std + alloc`.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(feature = "std")]
mod client;
pub mod direct;
#[cfg(feature = "std")]
mod endpoint;
#[cfg(feature = "std")]
mod error;
#[cfg(feature = "std")]
mod generate;
#[cfg(feature = "std")]
mod response;

#[cfg(feature = "std")]
pub use client::{Client, ClientBuilder, RequestOptions};
pub use direct::DirectClient;
#[cfg(feature = "std")]
pub use endpoint::{ChatCompletions, Messages, Responses};
#[cfg(feature = "std")]
pub use error::{Error, Result};
#[cfg(feature = "std")]
pub use generate::{
  GenerateCall, GenerateEvent, GenerateRequest, GenerateRequestBuilder, GenerateResponse, GenerateStream, Message,
  Role, TextStream, Tool, ToolCall, ToolChoice, Usage,
};
#[cfg(feature = "std")]
pub use response::{BufferedResponse, ByteStream, RawResponse, ResponseBody, StreamResponse};

#[cfg(feature = "std")]
pub use tokn_core::event::Event;
#[cfg(feature = "std")]
pub use tokn_core::generation::{
  GenerationOptions, GenerationOptionsError, ReasoningEffort, ReasoningMode, ReasoningOptions, ReasoningSummary,
};
#[cfg(feature = "std")]
pub use tokn_core::provider::Endpoint;
#[cfg(feature = "std")]
pub use tokn_core::request_event::{RecordEvent, RequestEvent, RequestEventPayload, StageEvent};
pub use tokn_endpoint_chat_completions as chat_completions;
pub use tokn_endpoint_messages as messages;
pub use tokn_endpoint_responses as responses;
