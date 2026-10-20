use alloc::vec::Vec;
use core::fmt;
use serde_json::Value;

use super::{Header, Response, SseError};

/// Streaming failures. After an error the event stream is closed.
///
/// HTTP errors retain status, headers, and at most the configured error-body
/// limit. Provider error payloads can be inspected explicitly; Debug omits them.
pub enum StreamError<E> {
  Transport(E),
  Serialize(serde_json::Error),
  Deserialize(serde_json::Error),
  HttpStatus { response: Response, truncated: bool },
  UnexpectedContentType { status: u16, headers: Vec<Header> },
  Sse(SseError),
  InvalidEvent(&'static str),
  Provider { data: Value },
  UnexpectedEof,
  InvalidReadSize,
}

impl<E: fmt::Debug> fmt::Debug for StreamError<E> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Transport(error) => f.debug_tuple("Transport").field(error).finish(),
      Self::Serialize(error) => f.debug_tuple("Serialize").field(error).finish(),
      Self::Deserialize(error) => f.debug_tuple("Deserialize").field(error).finish(),
      Self::HttpStatus { response, truncated } => f
        .debug_struct("HttpStatus")
        .field("response", response)
        .field("truncated", truncated)
        .finish(),
      Self::UnexpectedContentType { status, headers } => f
        .debug_struct("UnexpectedContentType")
        .field("status", status)
        .field("headers", headers)
        .finish(),
      Self::Sse(error) => f.debug_tuple("Sse").field(error).finish(),
      Self::InvalidEvent(reason) => f.debug_tuple("InvalidEvent").field(reason).finish(),
      Self::Provider { .. } => f.debug_struct("Provider").finish_non_exhaustive(),
      Self::UnexpectedEof => f.write_str("UnexpectedEof"),
      Self::InvalidReadSize => f.write_str("InvalidReadSize"),
    }
  }
}

impl<E: fmt::Display> fmt::Display for StreamError<E> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Transport(error) => write!(f, "transport failed: {error}"),
      Self::Serialize(error) => write!(f, "failed to serialize request: {error}"),
      Self::Deserialize(error) => write!(f, "failed to deserialize stream event: {error}"),
      Self::HttpStatus { response, .. } => write!(f, "OpenAI request failed with HTTP status {}", response.status),
      Self::UnexpectedContentType { .. } => f.write_str("streaming response requires text/event-stream content type"),
      Self::Sse(error) => error.fmt(f),
      Self::InvalidEvent(reason) => write!(f, "invalid stream event: {reason}"),
      Self::Provider { .. } => f.write_str("provider reported a streaming error"),
      Self::UnexpectedEof => f.write_str("stream ended before an endpoint terminal event"),
      Self::InvalidReadSize => f.write_str("transport returned a read size larger than the supplied buffer"),
    }
  }
}

impl<E: core::error::Error + 'static> core::error::Error for StreamError<E> {
  fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
    match self {
      Self::Transport(error) => Some(error),
      Self::Serialize(error) | Self::Deserialize(error) => Some(error),
      Self::Sse(error) => Some(error),
      _ => None,
    }
  }
}
