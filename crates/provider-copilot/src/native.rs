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
  HeaderMap, HeaderName, HeaderValue,
};

/// Operation path relative to the configured, validated provider base URL.
pub fn operation_segments(endpoint: Endpoint, _base_url: &str) -> Option<&'static [&'static str]> {
  match endpoint {
    Endpoint::ChatCompletions => Some(&["chat", "completions"]),
    Endpoint::Responses => Some(&["responses"]),
    Endpoint::Messages => Some(&["v1", "messages"]),
  }
}

/// Install caller-supplied credentials using this provider's native scheme.
pub fn inject_credentials(headers: &mut HeaderMap, _endpoint: Endpoint, credential: &str) {
  headers.insert(&AUTHORIZATION, HeaderValue::from_string(format!("Bearer {credential}")));
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
  validate_credential(credential)?;
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

pub const COPILOT_BASE_URL: &str = "https://api.githubcopilot.com";

/// Per-model operation availability used by the shared route resolver.
pub fn supports_model_endpoint(model: &str, endpoint: Endpoint, base_url: &str) -> bool {
  tokn_core::endpoint::match_endpoint_rule(MODEL_ENDPOINT_RULES, model, endpoint)
    .unwrap_or_else(|| operation_segments(endpoint, base_url).is_some())
}

pub static MODEL_ENDPOINT_RULES: &[tokn_core::endpoint::EndpointRule] = &[
  tokn_core::endpoint::EndpointRule {
    pattern: "claude-*",
    endpoints: &[Endpoint::Messages, Endpoint::ChatCompletions],
  },
  tokn_core::endpoint::EndpointRule {
    pattern: "gpt-5-mini",
    endpoints: &[Endpoint::ChatCompletions, Endpoint::Responses],
  },
  tokn_core::endpoint::EndpointRule {
    pattern: "gpt-5*",
    endpoints: &[Endpoint::Responses],
  },
  tokn_core::endpoint::EndpointRule {
    pattern: "o1*",
    endpoints: &[Endpoint::Responses, Endpoint::ChatCompletions],
  },
  tokn_core::endpoint::EndpointRule {
    pattern: "o3*",
    endpoints: &[Endpoint::Responses, Endpoint::ChatCompletions],
  },
  tokn_core::endpoint::EndpointRule {
    pattern: "o4*",
    endpoints: &[Endpoint::Responses, Endpoint::ChatCompletions],
  },
];

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
  headers.insert(
    HeaderName::new("x-initiator"),
    HeaderValue::from_string(ctx.initiator.to_string()),
  );
  if let Some(encoding) = ctx.content_encoding {
    headers.insert(&CONTENT_ENCODING, HeaderValue::from_string(encoding.to_string()));
  }
  None
}
