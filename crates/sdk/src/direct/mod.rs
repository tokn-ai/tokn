//! Buffered and streaming native API requests with caller-supplied networking.
//!
//! This module only requires `core` and `alloc`. It does not load configuration,
//! select accounts, convert between provider protocols, or start an executor.
//! Requests use native Responses, Chat Completions, or Messages schemas,
//! including their generation controls. Each endpoint has its own URL and headers.
//!
//! Implement [`Transport`] using the device's HTTP/TLS stack. The transport is
//! responsible for certificate validation, DNS, timeouts, and limiting buffered
//! response sizes. Implement [`StreamingTransport`] for incremental typed SSE
//! events with configurable per-event limits.

mod config;
mod error;
mod sse;
mod stream;
mod stream_error;
mod transport;

pub use config::{
  Anthropic, ConfigError, EndpointConfig, OpenAi, ProviderConfig, ANTHROPIC_BASE_URL, ANTHROPIC_VERSION,
  OPENAI_BASE_URL,
};
pub use error::Error;
pub use sse::DecodeError as SseError;
pub use stream::{EventStream, StreamOptions};
pub use stream_error::StreamError;
pub use transport::{
  BufferedResponse, Header, Request, Response, StreamBody, StreamingResponse, StreamingTransport, Transport,
};

pub use tokn_endpoint_core::Endpoint;

use alloc::vec::Vec;
use core::fmt;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokn_endpoint_chat_completions::{ChatRequest, ChatResponse};
use tokn_endpoint_messages::{MessagesRequest, MessagesResponse};
use tokn_endpoint_responses::{ResponsesRequest, ResponsesResponse};

/// A native API client that borrows no executor or operating-system services.
///
/// Methods take `&mut self` so a transport can own a single socket or an
/// embedded network stack. Neither the transport nor its futures must be
/// `Send`, and no reference counting or atomic operations are required.
pub struct DirectClient<T> {
  transport: T,
  provider: ProviderConfig,
}

impl<T> DirectClient<T> {
  /// Accepts explicit endpoint routes or a convenience configuration such as
  /// [`OpenAi`] or [`Anthropic`].
  pub fn new(transport: T, provider: impl Into<ProviderConfig>) -> Self {
    Self {
      transport,
      provider: provider.into(),
    }
  }

  pub fn transport(&self) -> &T {
    &self.transport
  }

  pub fn transport_mut(&mut self) -> &mut T {
    &mut self.transport
  }

  pub fn into_transport(self) -> T {
    self.transport
  }

  pub fn provider(&self) -> &ProviderConfig {
    &self.provider
  }

  /// Replace routes or credentials while retaining the transport. Credential
  /// acquisition and expiry handling remain the caller's responsibility.
  pub fn set_provider(&mut self, provider: impl Into<ProviderConfig>) {
    self.provider = provider.into();
  }

  fn http_request(&self, endpoint: Endpoint, body: Vec<u8>, streaming: bool) -> Result<Request, Endpoint> {
    let config = self.provider.endpoint(endpoint).ok_or(endpoint)?;
    let mut headers = config.headers().to_vec();
    headers.push(Header::new("content-type", "application/json"));
    headers.push(Header::new(
      "accept",
      if streaming {
        "text/event-stream"
      } else {
        "application/json"
      },
    ));
    Ok(Request {
      method: "POST",
      url: config.url().into(),
      headers,
      body,
    })
  }
}

impl<T> fmt::Debug for DirectClient<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("DirectClient")
      .field("provider", &self.provider)
      .finish_non_exhaustive()
  }
}

impl<T: Transport> DirectClient<T> {
  /// Send a buffered request to the configured Responses endpoint.
  ///
  /// Uses the model and OpenAI-native parameters exactly as supplied. A
  /// `stream: true` request is rejected before invoking the transport.
  pub async fn responses(
    &mut self,
    request: &ResponsesRequest,
  ) -> Result<BufferedResponse<ResponsesResponse>, Error<T::Error>> {
    self.execute(Endpoint::Responses, request, request.stream).await
  }

  /// Send a buffered request to the configured Chat Completions endpoint.
  ///
  /// Uses the model and OpenAI-native parameters exactly as supplied. A
  /// `stream: true` request is rejected before invoking the transport.
  pub async fn chat_completions(
    &mut self,
    request: &ChatRequest,
  ) -> Result<BufferedResponse<ChatResponse>, Error<T::Error>> {
    self.execute(Endpoint::ChatCompletions, request, request.stream).await
  }

  /// Send a buffered request to the configured Messages endpoint.
  ///
  /// Uses native parameters such as `max_tokens`, `system`, `thinking`, and
  /// `tools` as supplied. A `stream: true` request is rejected before transport.
  pub async fn messages(
    &mut self,
    request: &MessagesRequest,
  ) -> Result<BufferedResponse<MessagesResponse>, Error<T::Error>> {
    self.execute(Endpoint::Messages, request, request.stream).await
  }

  async fn execute<Q: Serialize, R: DeserializeOwned>(
    &mut self,
    endpoint: Endpoint,
    request: &Q,
    stream: Option<bool>,
  ) -> Result<BufferedResponse<R>, Error<T::Error>> {
    if stream == Some(true) {
      return Err(Error::StreamingUnsupported);
    }
    let mut body = serde_json::to_value(request).map_err(Error::Serialize)?;
    // Endpoint schemas allow flattened extras. Check the final wire value too,
    // so an extras entry cannot turn this buffered API into an SSE request.
    if body.get("stream") == Some(&Value::Bool(true)) {
      return Err(Error::StreamingUnsupported);
    }
    body["stream"] = Value::Bool(false);
    let body = serde_json::to_vec(&body).map_err(Error::Serialize)?;
    let request = self
      .http_request(endpoint, body, false)
      .map_err(Error::UnsupportedEndpoint)?;
    let response = self.transport.send(request).await.map_err(Error::Transport)?;
    if !(200..300).contains(&response.status) {
      return Err(Error::HttpStatus { response });
    }
    if is_event_stream(&response.headers) {
      return Err(Error::UnexpectedStream);
    }
    let data = serde_json::from_slice(&response.body).map_err(Error::Deserialize)?;
    Ok(BufferedResponse {
      status: response.status,
      headers: response.headers,
      data,
    })
  }
}

fn is_event_stream(headers: &[Header]) -> bool {
  headers.iter().any(|header| {
    header.name.eq_ignore_ascii_case("content-type")
      && header
        .value
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
  })
}
