//! Content-encoding helpers (gzip/zstd) for request and response bodies.
//!
//! Ported from `services/router/src/api/codec.rs`. The legacy version was
//! coupled to `axum::http::HeaderMap` and the gateway `ApiError`; this
//! port reuses `tokn_headers::HeaderMap` + `tokn_headers::HeaderValue` and
//! exposes a self-contained [`CodecError`] so requests stages stay free
//! of HTTP-server framework imports.
//!
//! Behaviour parity with the legacy module is preserved (and exercised
//! by the unit tests at the bottom of the file):
//!
//! * `request_content_encoding` parses the inbound `Content-Encoding`
//!   header, treating `identity` as absent and rejecting any value with
//!   more than one non-identity encoding (gzip/zstd are the only ones
//!   we accept; everything else is `UnsupportedEncoding`).
//! * `negotiate_response_encoding` picks the best codec the client
//!   advertised in `Accept-Encoding`, preferring higher `q=` values
//!   and breaking ties in favour of zstd.
//! * `encode_body_bytes` / `decode_body_bytes` are the symmetric
//!   transcoding helpers used both by the request-side
//!   `DecodedJsonRequest` flow and the response-side
//!   `maybe_compress_buffered_response` helper.

use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::Value;
use snafu::Snafu;
use std::io::{Read, Write};
use tokn_headers::keys::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH};
use tokn_headers::{HeaderMap, HeaderValue};

/// `Vary` is not currently exported from `tokn_headers::keys`; keep a
/// module-local constant so we stay aligned with the legacy router
/// behaviour without taking a churny dependency on the headers crate.
const VARY_HEADER: &str = "vary";

/// Codecs the router knows how to inflate and re-deflate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentEncodingKind {
  Gzip,
  Zstd,
}

impl ContentEncodingKind {
  /// Wire-form token (e.g. `"gzip"`), suitable for use as a
  /// `Content-Encoding` value.
  pub fn as_str(self) -> &'static str {
    match self {
      ContentEncodingKind::Gzip => "gzip",
      ContentEncodingKind::Zstd => "zstd",
    }
  }
}

/// Failures the codec helpers can produce. Stages are expected to map
/// these onto their own error variants (typically a permanent stage
/// error tagged with `recoverable: false`).
#[derive(Debug, Snafu)]
pub enum CodecError {
  #[snafu(display("invalid content-encoding header: not valid UTF-8"))]
  InvalidEncodingHeader,
  #[snafu(display("multiple content-encodings are not supported"))]
  MultipleEncodings,
  #[snafu(display("unsupported content-encoding '{encoding}'"))]
  UnsupportedEncoding { encoding: String },
  #[snafu(display("gzip {direction} failed: {source}"))]
  Gzip {
    direction: &'static str,
    source: std::io::Error,
  },
  #[snafu(display("zstd {direction} failed: {source}"))]
  Zstd {
    direction: &'static str,
    source: std::io::Error,
  },
  #[snafu(display("invalid JSON request body: {source}"))]
  InvalidJson { source: serde_json::Error },
}

/// A request body that has been (optionally) decompressed and parsed
/// into a `serde_json::Value`.
///
/// `raw_body` is the bytes as received off the wire; `decoded_body` is
/// the post-decompression payload. When no `Content-Encoding` was
/// applied the two are identical (and share allocations via
/// `Bytes::clone`).
#[derive(Clone, Debug)]
pub struct DecodedJsonRequest {
  pub raw_body: Bytes,
  pub decoded_body: Bytes,
  pub value: Value,
  pub encoding: Option<ContentEncodingKind>,
}

/// Decode an inbound JSON request, transparently handling gzip/zstd.
pub fn decode_json_request(headers: &HeaderMap, raw_body: Bytes) -> Result<DecodedJsonRequest, CodecError> {
  let encoding = request_content_encoding(headers)?;
  let decoded = decode_body_bytes(raw_body.clone(), encoding)?;
  let value: Value = serde_json::from_slice(&decoded).map_err(|source| CodecError::InvalidJson { source })?;
  Ok(DecodedJsonRequest {
    raw_body,
    decoded_body: decoded,
    value,
    encoding,
  })
}

/// Encode `body` using `encoding`. `None` is a no-op (and avoids any
/// allocation churn beyond a single `Bytes::copy_from_slice`).
pub fn encode_body_bytes(body: &[u8], encoding: Option<ContentEncodingKind>) -> Result<Bytes, CodecError> {
  match encoding {
    None => Ok(Bytes::copy_from_slice(body)),
    Some(ContentEncodingKind::Gzip) => {
      let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
      encoder.write_all(body).map_err(|source| CodecError::Gzip {
        direction: "encode",
        source,
      })?;
      let out = encoder.finish().map_err(|source| CodecError::Gzip {
        direction: "encode",
        source,
      })?;
      Ok(Bytes::from(out))
    }
    Some(ContentEncodingKind::Zstd) => {
      zstd::stream::encode_all(body, 0)
        .map(Bytes::from)
        .map_err(|source| CodecError::Zstd {
          direction: "encode",
          source,
        })
    }
  }
}

/// Inverse of [`encode_body_bytes`].
pub fn decode_body_bytes(body: Bytes, encoding: Option<ContentEncodingKind>) -> Result<Bytes, CodecError> {
  match encoding {
    None => Ok(body),
    Some(ContentEncodingKind::Gzip) => {
      let mut decoder = GzDecoder::new(body.as_ref());
      let mut out = Vec::new();
      decoder.read_to_end(&mut out).map_err(|source| CodecError::Gzip {
        direction: "decode",
        source,
      })?;
      Ok(Bytes::from(out))
    }
    Some(ContentEncodingKind::Zstd) => zstd::stream::decode_all(body.as_ref())
      .map(Bytes::from)
      .map_err(|source| CodecError::Zstd {
        direction: "decode",
        source,
      }),
  }
}

/// Parse the inbound `Content-Encoding` header.
///
/// Returns `Ok(None)` when the header is absent or contains only
/// `identity` tokens. Multiple non-identity encodings are rejected
/// because we don't currently chain decoders.
pub fn request_content_encoding(headers: &HeaderMap) -> Result<Option<ContentEncodingKind>, CodecError> {
  let Some(value) = headers.get(CONTENT_ENCODING.clone()) else {
    return Ok(None);
  };
  let value = value.as_str();
  if value.is_empty() {
    return Ok(None);
  }
  let mut encodings = value
    .split(',')
    .map(str::trim)
    .filter(|part| !part.is_empty() && !part.eq_ignore_ascii_case("identity"));
  let first = match encodings.next() {
    Some(first) => first,
    None => return Ok(None),
  };
  if encodings.next().is_some() {
    return Err(CodecError::MultipleEncodings);
  }
  match first.to_ascii_lowercase().as_str() {
    "gzip" => Ok(Some(ContentEncodingKind::Gzip)),
    "zstd" => Ok(Some(ContentEncodingKind::Zstd)),
    other => Err(CodecError::UnsupportedEncoding {
      encoding: other.to_string(),
    }),
  }
}

/// Pick the best response codec the client advertised.
///
/// Higher `q=` wins. On ties, prefer zstd over gzip (matches legacy
/// router behaviour and the empirical observation that zstd is both
/// faster and tighter on JSON payloads).
pub fn negotiate_response_encoding(headers: &HeaderMap) -> Option<ContentEncodingKind> {
  let value = headers.get(ACCEPT_ENCODING.clone())?.as_str();
  let mut best: Option<(ContentEncodingKind, f32)> = None;
  for entry in value.split(',') {
    let mut parts = entry.split(';').map(str::trim);
    let token = parts.next()?.to_ascii_lowercase();
    let mut q = 1.0_f32;
    for param in parts {
      if let Some(rest) = param.strip_prefix("q=") {
        q = rest.parse::<f32>().ok().unwrap_or(0.0);
      }
    }
    if q <= 0.0 {
      continue;
    }
    let Some(encoding) = (match token.as_str() {
      "gzip" => Some(ContentEncodingKind::Gzip),
      "zstd" => Some(ContentEncodingKind::Zstd),
      _ => None,
    }) else {
      continue;
    };
    match best {
      None => best = Some((encoding, q)),
      Some((_, best_q)) if q > best_q => best = Some((encoding, q)),
      Some((ContentEncodingKind::Gzip, best_q)) if q == best_q && encoding == ContentEncodingKind::Zstd => {
        best = Some((encoding, q))
      }
      _ => {}
    }
  }
  best.map(|(encoding, _)| encoding)
}

/// Compress a fully-buffered response body when the client requested
/// (and we support) compression. Mutates `response_headers` to reflect
/// the chosen encoding (and drops a now-stale `Content-Length`).
///
/// Returns `body` unchanged if the body is empty, the response already
/// has a `Content-Encoding`, or the client didn't advertise any codec
/// we know how to produce.
pub fn maybe_compress_buffered_response(
  request_headers: &HeaderMap,
  response_headers: &mut HeaderMap,
  body: Bytes,
) -> Result<Bytes, CodecError> {
  if body.is_empty() || response_headers.contains_key(CONTENT_ENCODING.clone()) {
    return Ok(body);
  }
  let Some(encoding) = negotiate_response_encoding(request_headers) else {
    return Ok(body);
  };
  let compressed = encode_body_bytes(body.as_ref(), Some(encoding))?;
  response_headers.insert(CONTENT_ENCODING.clone(), HeaderValue::from_static(encoding.as_str()));
  response_headers.remove(CONTENT_LENGTH.clone());
  response_headers.insert(VARY_HEADER, HeaderValue::from_static("accept-encoding"));
  Ok(compressed)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn negotiate_prefers_highest_q_then_zstd() {
    let mut headers = HeaderMap::new();
    headers.insert(
      ACCEPT_ENCODING.clone(),
      HeaderValue::from_static("gzip;q=0.8, zstd;q=0.8"),
    );
    assert_eq!(negotiate_response_encoding(&headers), Some(ContentEncodingKind::Zstd));
  }

  #[test]
  fn gzip_round_trip() {
    let body = br#"{"model":"gpt-5","input":"hi"}"#;
    let encoded = encode_body_bytes(body, Some(ContentEncodingKind::Gzip)).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_ENCODING.clone(), HeaderValue::from_static("gzip"));
    let decoded = decode_json_request(&headers, encoded).unwrap();
    assert_eq!(decoded.value["model"], "gpt-5");
  }

  #[test]
  fn zstd_round_trip() {
    let body = br#"{"model":"gpt-5","input":"hi"}"#;
    let encoded = encode_body_bytes(body, Some(ContentEncodingKind::Zstd)).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_ENCODING.clone(), HeaderValue::from_static("zstd"));
    let decoded = decode_json_request(&headers, encoded).unwrap();
    assert_eq!(decoded.value["model"], "gpt-5");
  }

  #[test]
  fn rejects_unsupported_content_encoding() {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_ENCODING.clone(), HeaderValue::from_static("br"));
    let err = decode_json_request(&headers, Bytes::from_static(br#"{}"#)).unwrap_err();
    assert!(matches!(err, CodecError::UnsupportedEncoding { .. }));
  }

  #[test]
  fn rejects_multiple_encodings() {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_ENCODING.clone(), HeaderValue::from_static("gzip, zstd"));
    let err = decode_json_request(&headers, Bytes::from_static(br#"{}"#)).unwrap_err();
    assert!(matches!(err, CodecError::MultipleEncodings));
  }

  #[test]
  fn identity_is_treated_as_absent() {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_ENCODING.clone(), HeaderValue::from_static("identity"));
    assert_eq!(request_content_encoding(&headers).unwrap(), None);
  }

  #[test]
  fn compresses_buffered_response_for_supported_accept_encoding() {
    let mut request_headers = HeaderMap::new();
    request_headers.insert(ACCEPT_ENCODING.clone(), HeaderValue::from_static("gzip"));
    let mut response_headers = HeaderMap::new();
    let body = Bytes::from_static(br#"{"ok":true}"#);
    let compressed = maybe_compress_buffered_response(&request_headers, &mut response_headers, body.clone()).unwrap();
    assert_ne!(compressed, body);
    assert_eq!(
      response_headers.get(CONTENT_ENCODING.clone()).map(|v| v.as_str()),
      Some("gzip")
    );
    assert_eq!(
      response_headers.get(VARY_HEADER).map(|v| v.as_str()),
      Some("accept-encoding")
    );
  }

  #[test]
  fn skips_compression_when_already_encoded() {
    let mut request_headers = HeaderMap::new();
    request_headers.insert(ACCEPT_ENCODING.clone(), HeaderValue::from_static("gzip"));
    let mut response_headers = HeaderMap::new();
    response_headers.insert(CONTENT_ENCODING.clone(), HeaderValue::from_static("gzip"));
    let body = Bytes::from_static(br#"{"ok":true}"#);
    let out = maybe_compress_buffered_response(&request_headers, &mut response_headers, body.clone()).unwrap();
    assert_eq!(out, body);
  }
}
