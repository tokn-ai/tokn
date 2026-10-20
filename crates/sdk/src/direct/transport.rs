use alloc::{string::String, vec::Vec};
use core::fmt;

/// An owned HTTP header. Repeated names are represented by repeated entries.
///
/// Debug output omits values, which may contain credentials or cookies.
#[derive(Clone, PartialEq, Eq)]
pub struct Header {
  pub name: String,
  pub value: String,
}

impl Header {
  pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      value: value.into(),
    }
  }
}

impl fmt::Debug for Header {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Header")
      .field("name", &self.name)
      .field("value", &"<redacted>")
      .finish()
  }
}

/// A complete request for an HTTP transport.
///
/// `body` is UTF-8 JSON. The transport must transmit these bytes without
/// modifying the endpoint payload. The client currently always uses `POST`.
/// Debug output omits header values and body contents.
pub struct Request {
  pub method: &'static str,
  pub url: String,
  pub headers: Vec<Header>,
  pub body: Vec<u8>,
}

impl fmt::Debug for Request {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Request")
      .field("method", &self.method)
      .field("url", &self.url)
      .field("headers", &self.headers)
      .field("body_bytes", &self.body.len())
      .finish()
  }
}

/// A buffered HTTP response with its body decoded from HTTP transfer/content
/// encodings by the transport.
///
/// Debug output omits header values and body contents. A transport must
/// enforce its response-size budget while reading, before allocating an
/// arbitrarily large body.
pub struct Response {
  pub status: u16,
  pub headers: Vec<Header>,
  pub body: Vec<u8>,
}

impl fmt::Debug for Response {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Response")
      .field("status", &self.status)
      .field("headers", &self.headers)
      .field("body_bytes", &self.body.len())
      .finish()
  }
}

/// A decoded endpoint response and its HTTP metadata.
#[derive(Debug)]
pub struct BufferedResponse<T> {
  pub status: u16,
  pub headers: Vec<Header>,
  pub data: T,
}

/// Networking supplied by the application.
///
/// Implementations own DNS, TCP, TLS with certificate verification, timeouts,
/// and response-size limits. Return non-2xx HTTP responses normally: the client
/// preserves them in [`super::Error::HttpStatus`]. Automatic redirects must not
/// forward the authorization header to a different origin or downgrade HTTPS.
/// Prefer returning redirects to the client as HTTP responses.
///
/// Futures intentionally have no `Send` bound, allowing a single-threaded
/// embedded executor and a transport that borrows a device driver. Neither a
/// boxed future nor an async-trait macro is required.
#[allow(async_fn_in_trait)]
pub trait Transport {
  type Error;

  async fn send(&mut self, request: Request) -> Result<Response, Self::Error>;
}

/// A response body read incrementally into caller-provided storage.
///
/// Return the number of bytes written, at most `buffer.len()`. For a nonempty
/// buffer, zero means permanent EOF; wait for data instead of returning zero
/// when the connection is merely idle. Decode HTTP transfer/content encodings
/// before returning bytes. The SDK supplies a nonempty buffer on every call.
///
/// Reads must be cancellation safe: dropping a pending read must not consume
/// bytes that a later read cannot recover. Dropping the body must release or
/// abort the response without requiring it to be drained. A borrowed connection
/// must not be reused with unread response bytes still on it.
#[allow(async_fn_in_trait)]
pub trait StreamBody {
  type Error;

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error>;
}

/// HTTP status and headers available before reading a streaming body.
pub struct StreamingResponse<B> {
  pub status: u16,
  pub headers: Vec<Header>,
  pub body: B,
}

impl<B> fmt::Debug for StreamingResponse<B> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("StreamingResponse")
      .field("status", &self.status)
      .field("headers", &self.headers)
      .finish_non_exhaustive()
  }
}

/// Injected HTTP streaming, independent of the buffered [`Transport`] contract.
///
/// The body may borrow a socket or driver from `self`; it needs neither `Send`
/// nor `'static`, and requires no boxed future, executor, or atomics. Implement
/// both transport traits only if the application needs both request modes.
///
/// As with [`Transport`], implementations own DNS, TLS certificate validation,
/// timeouts, header-size limits, and safe redirect handling. Return non-2xx
/// responses normally so the SDK can preserve HTTP metadata and a bounded
/// error body. Do not buffer the entire body before returning.
#[allow(async_fn_in_trait)]
pub trait StreamingTransport {
  type Error;
  type Body<'a>: StreamBody<Error = Self::Error>
  where
    Self: 'a;

  async fn send_stream(&mut self, request: Request) -> Result<StreamingResponse<Self::Body<'_>>, Self::Error>;
}
