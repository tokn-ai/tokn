//! Pure provider request assembly shared by standard and embedded execution.

use alloc::{string::ToString, vec::Vec};
use core::fmt;
use serde_json::Value;

use crate::{
  headers::HeaderMap,
  types::Bytes,
  upstream_url::{CanonicalUpstreamUrl, CleartextHttpPolicy, InvalidOperationPath, InvalidUpstreamUrl},
  Endpoint,
};

use super::PreparedRequest;

/// Invalid provider configuration or request encoding.
#[derive(Debug)]
pub enum PrepareError {
  UnsupportedEndpoint(Endpoint),
  InvalidCredential,
  InvalidBaseUrl(InvalidUpstreamUrl),
  InvalidOperationPath(InvalidOperationPath),
  Json(serde_json::Error),
}

impl fmt::Display for PrepareError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::UnsupportedEndpoint(endpoint) => write!(f, "unsupported provider endpoint: {endpoint}"),
      Self::InvalidCredential => f.write_str("credential must contain only non-whitespace printable ASCII"),
      Self::InvalidBaseUrl(source) => write!(f, "invalid provider base URL: {source}"),
      Self::InvalidOperationPath(source) => write!(f, "invalid provider operation path: {source}"),
      Self::Json(source) => write!(f, "provider request encoding failed: {source}"),
    }
  }
}

impl core::error::Error for PrepareError {
  fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
    match self {
      Self::InvalidBaseUrl(source) => Some(source),
      Self::InvalidOperationPath(source) => Some(source),
      Self::Json(source) => Some(source),
      _ => None,
    }
  }
}

/// Validate a token before placing it in a credential-bearing header.
pub fn validate_credential(credential: &str) -> Result<(), PrepareError> {
  if credential.is_empty() || !credential.bytes().all(|byte| (b'!'..=b'~').contains(&byte)) {
    return Err(PrepareError::InvalidCredential);
  }
  Ok(())
}

/// Assemble a request at a strictly admitted provider URL.
///
/// Cleartext HTTP is admitted only for loopback providers. Configurations that
/// deliberately permit remote cleartext HTTP must use the explicit policy form.
pub fn prepare_request(
  base_url: &str,
  segments: &[&str],
  endpoint: Endpoint,
  body: &Value,
  headers: HeaderMap,
) -> Result<PreparedRequest, PrepareError> {
  prepare_request_with_policy(
    base_url,
    segments,
    endpoint,
    body,
    headers,
    CleartextHttpPolicy::LoopbackOnly,
  )
}

pub fn prepare_request_with_policy(
  base_url: &str,
  segments: &[&str],
  endpoint: Endpoint,
  body: &Value,
  headers: HeaderMap,
  cleartext: CleartextHttpPolicy,
) -> Result<PreparedRequest, PrepareError> {
  let base = CanonicalUpstreamUrl::parse(base_url, cleartext).map_err(PrepareError::InvalidBaseUrl)?;
  let url = base
    .operation_url(segments.iter().copied())
    .map_err(PrepareError::InvalidOperationPath)?;
  let body: Vec<u8> = serde_json::to_vec(body).map_err(PrepareError::Json)?;
  Ok(PreparedRequest {
    url: url.to_string(),
    headers,
    body: Bytes::from(body),
    endpoint,
  })
}
