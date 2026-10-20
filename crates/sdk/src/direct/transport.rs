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
