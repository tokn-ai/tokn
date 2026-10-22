//! Pure native request preparation. Transport, credential refresh, and timing
//! belong to the caller's platform services.

use alloc::string::ToString;
use serde_json::Value;
use tokn_core::{
  provider::{prepare_request_with_policy, validate_credential, PrepareError, PreparedRequest},
  upstream_url::CleartextHttpPolicy,
  Endpoint,
};
use tokn_headers::{
  keys::{ACCEPT, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE},
  HeaderMap, HeaderValue,
};

/// Operation path relative to the configured, validated provider base URL.
pub fn operation_segments(endpoint: Endpoint, _base_url: &str) -> Option<&'static [&'static str]> {
  match endpoint {
    Endpoint::ChatCompletions => Some(&["chat", "completions"]),
    Endpoint::Responses | Endpoint::Messages => None,
  }
}

/// Install caller-supplied credentials using this provider's native scheme.
pub fn inject_credentials(headers: &mut HeaderMap, _endpoint: Endpoint, credential: &str) {
  if !credential.is_empty() {
    headers.insert(&AUTHORIZATION, HeaderValue::from_string(format!("Bearer {credential}")));
  }
}

/// Apply the provider's request-body contract independently of transport.
pub fn transform_input(_endpoint: Endpoint, body: Value) -> Value {
  body
}

pub fn prepare(
  base_url: &str,
  endpoint: Endpoint,
  body: Value,
  credential: &str,
  headers: HeaderMap,
) -> Result<PreparedRequest, PrepareError> {
  prepare_with_policy(
    base_url,
    endpoint,
    body,
    credential,
    headers,
    CleartextHttpPolicy::LoopbackOnly,
  )
}

pub fn prepare_with_policy(
  base_url: &str,
  endpoint: Endpoint,
  body: Value,
  credential: &str,
  mut headers: HeaderMap,
  cleartext: CleartextHttpPolicy,
) -> Result<PreparedRequest, PrepareError> {
  if !credential.is_empty() {
    validate_credential(credential)?;
  }
  let segments = operation_segments(endpoint, base_url).ok_or(PrepareError::UnsupportedEndpoint(endpoint))?;
  let body = transform_input(endpoint, body);
  inject_credentials(&mut headers, endpoint, credential);
  headers.insert(&CONTENT_TYPE, HeaderValue::from_static("application/json"));
  headers.insert(
    &ACCEPT,
    HeaderValue::from_static(if body["stream"].as_bool().unwrap_or(false) {
      "text/event-stream"
    } else {
      "application/json"
    }),
  );
  prepare_request_with_policy(base_url, segments, endpoint, &body, headers, cleartext)
}

pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8080/v1";

/// Per-model operation availability used by the shared route resolver.
pub fn supports_model_endpoint(_model: &str, endpoint: Endpoint, base_url: &str) -> bool {
  operation_segments(endpoint, base_url).is_some()
}

/// Apply the exact header normalization used by the standard provider.
pub fn normalize_headers(headers: &mut HeaderMap, ctx: &tokn_core::provider::HeaderPatchCtx<'_>) -> Option<HeaderMap> {
  headers.insert(
    &ACCEPT,
    HeaderValue::from_static(if ctx.stream {
      "text/event-stream"
    } else {
      "application/json"
    }),
  );
  headers.insert(&CONTENT_TYPE, HeaderValue::from_static("application/json"));
  if let Some(encoding) = ctx.content_encoding {
    headers.insert(&CONTENT_ENCODING, HeaderValue::from_string(encoding.to_string()));
  }
  None
}
