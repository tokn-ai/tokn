//! HTTP transport contract shared by standard and embedded request sessions.

use alloc::string::String;
use core::fmt::Display;
use tokn_core::{headers::HeaderMap, types::Bytes};

#[derive(Clone, Debug)]
pub struct Request {
  pub method: &'static str,
  pub url: String,
  pub headers: HeaderMap,
  pub body: Bytes,
}

#[derive(Clone, Debug)]
pub struct ResponseHead {
  pub status: u16,
  pub headers: HeaderMap,
}

/// One HTTP exchange at a time, with streaming reads of its active response.
///
/// `send` replaces the previous exchange, including an unread response. `read`
/// returns zero only at EOF. Reads expose decoded HTTP entity bytes; the
/// implementation owns HTTP framing, content decoding, and TLS;
/// Tokn owns provider authentication, routing, conversion, and retries. A
/// response borrows its session until EOF or drop, so a driver need not allocate
/// or erase a socket lifetime. There are no thread-safety requirements here.
#[allow(async_fn_in_trait)]
pub trait Transport {
  type Error: Display;

  async fn send(&mut self, request: Request) -> Result<ResponseHead, Self::Error>;
  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error>;

  /// Content codings this adapter can decode before yielding entity bytes.
  ///
  /// The request pipeline overrides persona-generated `Accept-Encoding` with
  /// this capability. Small transports safely use identity; adapters with
  /// decoders may opt into, for example, `"gzip, br"`.
  fn accept_encoding(&self) -> &str {
    "identity"
  }

  /// Release or abort the active response, including its socket/body resources.
  /// Called on completion, cancellation, and errors. Stateless drivers may
  /// keep the default implementation.
  fn close_response(&mut self) {}

  /// Only errors for which replay is appropriate should return true.
  fn recoverable(&self, _error: &Self::Error) -> bool {
    false
  }
}
