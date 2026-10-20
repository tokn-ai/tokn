//! Buffered OpenAI requests with caller-supplied networking.
//!
//! This module only requires `core` and `alloc`. It does not load configuration,
//! select accounts, convert between provider protocols, or start an executor.
//! Requests use the exact OpenAI endpoint schemas, including their native
//! generation controls.
//!
//! Implement [`Transport`] using the device's HTTP/TLS stack. The transport is
//! responsible for certificate validation, DNS, timeouts, and limiting buffered
//! response sizes. Streaming requests are rejected by this first buffered API.

mod config;
mod error;
mod transport;

pub use config::{ConfigError, OpenAi, OPENAI_BASE_URL};
pub use error::Error;
pub use transport::{BufferedResponse, Header, Request, Response, Transport};

use alloc::{format, vec};
use core::fmt;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokn_endpoint_chat_completions::{ChatRequest, ChatResponse};
use tokn_endpoint_responses::{ResponsesRequest, ResponsesResponse};

/// A direct OpenAI client that borrows no executor or operating-system services.
///
/// Methods take `&mut self` so a transport can own a single socket or an
/// embedded network stack. Neither the transport nor its futures must be
/// `Send`, and no reference counting or atomic operations are required.
pub struct DirectClient<T> {
  transport: T,
  provider: OpenAi,
}

impl<T> DirectClient<T> {
  pub fn new(transport: T, provider: OpenAi) -> Self {
    Self { transport, provider }
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
}

impl<T> fmt::Debug for DirectClient<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("DirectClient")
      .field("provider", &self.provider)
      .finish_non_exhaustive()
  }
}

impl<T: Transport> DirectClient<T> {
  /// Send a buffered request to `/responses`.
  ///
  /// Uses the model and OpenAI-native parameters exactly as supplied. A
  /// `stream: true` request is rejected before invoking the transport.
  pub async fn responses(
    &mut self,
    request: &ResponsesRequest,
  ) -> Result<BufferedResponse<ResponsesResponse>, Error<T::Error>> {
    self.execute("responses", request, request.stream).await
  }

  /// Send a buffered request to `/chat/completions`.
  ///
  /// Uses the model and OpenAI-native parameters exactly as supplied. A
  /// `stream: true` request is rejected before invoking the transport.
  pub async fn chat_completions(
    &mut self,
    request: &ChatRequest,
  ) -> Result<BufferedResponse<ChatResponse>, Error<T::Error>> {
    self.execute("chat/completions", request, request.stream).await
  }

  async fn execute<Q: Serialize, R: DeserializeOwned>(
    &mut self,
    operation: &str,
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
    let response = self
      .transport
      .send(Request {
        method: "POST",
        url: format!("{}/{operation}", self.provider.base_url()),
        headers: vec![
          Header::new("authorization", self.provider.authorization()),
          Header::new("content-type", "application/json"),
          Header::new("accept", "application/json"),
        ],
        body,
      })
      .await
      .map_err(Error::Transport)?;
    if !(200..300).contains(&response.status) {
      return Err(Error::HttpStatus { response });
    }
    if response.headers.iter().any(|header| {
      header.name.eq_ignore_ascii_case("content-type")
        && header
          .value
          .split(';')
          .next()
          .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
    }) {
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
