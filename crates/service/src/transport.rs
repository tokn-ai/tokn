//! HTTP transport contract shared by standard and embedded request sessions.

use alloc::string::String;
use core::fmt::Display;
use tokn_core::{headers::HeaderMap, types::Bytes};

#[derive(Debug)]
pub struct EncodeError(pub String);

impl core::fmt::Display for EncodeError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str(&self.0)
  }
}
impl core::error::Error for EncodeError {}

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
/// returns zero only at EOF. Managed reads expose decoded HTTP entity bytes;
/// opaque exchanges may preserve content encoding and report that through
/// `response_is_decoded`. The implementation owns framing, codecs, and TLS;
/// Tokn owns provider authentication, routing, conversion, and retries.
/// Responses can own or borrow the transport until completion or drop, so an
/// injected driver need not allocate or erase a socket lifetime. There are no
/// thread-safety requirements here.
#[allow(async_fn_in_trait)]
pub trait Transport {
  type Error: Display;

  async fn send(&mut self, request: Request) -> Result<ResponseHead, Self::Error>;
  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error>;

  /// Select opaque forwarding or managed entity decoding for this exchange.
  fn configure_body_mode(&mut self, _preserve_encoding: bool) {}

  /// Whether response reads have removed HTTP content encoding.
  fn response_is_decoded(&self) -> bool {
    true
  }

  /// Encode a changed request body using the admitted inbound content coding.
  fn encode_body(&self, body: &[u8], encoding: Option<&str>) -> Result<Bytes, EncodeError> {
    match encoding {
      None | Some("identity") => Ok(Bytes::copy_from_slice(body)),
      Some(encoding) => Err(EncodeError(alloc::format!("unsupported content encoding: {encoding}"))),
    }
  }

  /// Content codings this adapter can decode before yielding entity bytes.
  ///
  /// The request pipeline overrides persona-generated `Accept-Encoding` with
  /// this capability for decoded exchanges. Opaque exchanges preserve the
  /// caller's negotiation. Small transports safely use identity; adapters with
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
