//! One configured client and request pipeline for standard and embedded applications.
//!
//! [`Client`] owns execution configuration and state and creates shared request
//! sessions. Standard and custom adapters supply HTTP exchanges, time, and events.
//! The standard builder prepares configuration and credentials from disk, including
//! legacy normalization; [`Client::new`] accepts in-memory rules and accounts with
//! either standard services or a local [`Adapter`].

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod client;
mod endpoint;
#[cfg(feature = "std")]
mod error;
#[cfg(feature = "std")]
mod generate;
#[cfg(feature = "std")]
mod response;
#[cfg(feature = "std")]
mod std_client;

pub use client::{
  BufferedResponse, Client, PlatformFactory, PlatformResponse, PlatformServices, RequestOptions, ResponseAdapter,
  Services,
};
pub use endpoint::{ChatCompletions, Messages, Responses};
#[cfg(feature = "std")]
pub use error::{Error, Result};
#[cfg(feature = "std")]
pub use generate::{
  GenerateCall, GenerateEvent, GenerateRequest, GenerateRequestBuilder, GenerateResponse, GenerateStream, Message,
  Role, TextStream, Tool, ToolCall, ToolChoice, Usage,
};
#[cfg(feature = "std")]
pub use response::{ByteStream, RawResponse, ResponseBody, StreamResponse};
#[cfg(feature = "std")]
pub use std_client::{ClientBuilder, StdPlatform};
pub use tokn_config as config;
pub use tokn_core::{account, settings, usage};
pub use tokn_policy as policy;
pub use tokn_requests::configured::{Adapter, EventRuntime, Lifecycle, Limits, Runtime};
pub use tokn_requests::session;
pub use tokn_requests::session::{ModelCapabilities, ModelInventory};
pub use tokn_service::transport;

#[cfg(feature = "std")]
pub use tokn_router_legacy_config::v2::V2ProjectionWarning as ConfigWarning;

#[cfg(feature = "std")]
pub use tokn_core::event::Event;
#[cfg(feature = "std")]
pub use tokn_core::generation::{
  GenerationOptions, GenerationOptionsError, ReasoningEffort, ReasoningMode, ReasoningOptions, ReasoningSummary,
};
pub use tokn_core::provider::Endpoint;
pub use tokn_core::request_event::{
  RecordEvent, RequestEvent, RequestEventPayload, RequestEventSink, Stage, StageEvent,
};
pub use tokn_endpoint_chat_completions as chat_completions;
pub use tokn_endpoint_messages as messages;
pub use tokn_endpoint_responses as responses;
