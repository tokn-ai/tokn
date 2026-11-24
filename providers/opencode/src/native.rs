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
  keys::{
    ACCEPT, ANTHROPIC_VERSION, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, USER_AGENT, X_API_KEY, X_OPENCODE_SESSION,
  },
  HeaderMap, HeaderValue,
};

/// Operation path relative to the configured, validated provider base URL.
pub fn operation_segments(endpoint: Endpoint, _base_url: &str) -> Option<&'static [&'static str]> {
  match endpoint {
    Endpoint::ChatCompletions => Some(&["chat", "completions"]),
    Endpoint::Responses => Some(&["responses"]),
    Endpoint::Messages => Some(&["messages"]),
  }
}

/// Install caller-supplied credentials using this provider's native scheme.
pub fn inject_credentials(headers: &mut HeaderMap, endpoint: Endpoint, credential: &str) {
  if endpoint == Endpoint::Messages {
    headers.remove(&AUTHORIZATION);
    headers.insert(
      &tokn_headers::keys::X_API_KEY,
      HeaderValue::from_string(credential.into()),
    );
  } else {
    headers.remove(&tokn_headers::keys::X_API_KEY);
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

pub const OPENCODE_GO_BASE_URL: &str = "https://opencode.ai/zen/go/v1";

/// Per-model operation availability used by the shared route resolver.
pub fn supports_model_endpoint(model: &str, endpoint: Endpoint, base_url: &str) -> bool {
  supports_model_endpoint_with_catalogue(model, endpoint, base_url, None)
}

/// Refreshed catalogue metadata is supplied by the platform rather than loaded
/// implicitly. The standard provider and embedded selector use this same rule.
pub fn supports_model_endpoint_with_catalogue(
  model: &str,
  endpoint: Endpoint,
  base_url: &str,
  catalogue_endpoint: Option<Endpoint>,
) -> bool {
  operation_segments(endpoint, base_url).is_some()
    && tokn_core::endpoint::match_endpoint_rule(MODEL_ENDPOINT_RULES, model, endpoint)
      .unwrap_or_else(|| endpoint == catalogue_endpoint.unwrap_or(Endpoint::ChatCompletions))
}

pub static MODEL_ENDPOINT_RULES: &[tokn_core::endpoint::EndpointRule] = &[
  tokn_core::endpoint::EndpointRule {
    pattern: "gpt-*",
    endpoints: &[Endpoint::Responses],
  },
  tokn_core::endpoint::EndpointRule {
    pattern: "grok-*",
    endpoints: &[Endpoint::Responses],
  },
  tokn_core::endpoint::EndpointRule {
    pattern: "muse-*",
    endpoints: &[Endpoint::Responses],
  },
  tokn_core::endpoint::EndpointRule {
    pattern: "minimax-*",
    endpoints: &[Endpoint::Messages],
  },
  tokn_core::endpoint::EndpointRule {
    pattern: "qwen*",
    endpoints: &[Endpoint::Messages],
  },
];

/// Apply the exact header normalization used by the standard provider.
pub fn normalize_headers(headers: &mut HeaderMap, ctx: &tokn_core::provider::HeaderPatchCtx<'_>) -> Option<HeaderMap> {
  let authorization = headers.get(&AUTHORIZATION).cloned();
  let api_key = headers.get(&X_API_KEY).cloned();
  let session = ctx
    .vars
    .session_id
    .as_deref()
    .or(ctx.vars.interaction_id.as_deref())
    .or(ctx.vars.request_id.as_deref())
    .map(str::to_string);
  let mut normalized = HeaderMap::new();
  if ctx.endpoint() == Some(Endpoint::Messages) {
    if let Some(api_key) = api_key {
      normalized.insert(&X_API_KEY, api_key);
    }
  } else if let Some(authorization) = authorization {
    normalized.insert(&AUTHORIZATION, authorization);
  }
  normalized.insert(
    &ACCEPT,
    HeaderValue::from_static(if ctx.stream {
      "text/event-stream"
    } else {
      "application/json"
    }),
  );
  normalized.insert(&CONTENT_TYPE, HeaderValue::from_static("application/json"));
  normalized.insert(
    &USER_AGENT,
    HeaderValue::from_string(tokn_core::util::version::tokn_router_user_agent()),
  );
  if ctx.endpoint() == Some(Endpoint::Messages) {
    let version = headers
      .get(&ANTHROPIC_VERSION)
      .cloned()
      .unwrap_or_else(|| HeaderValue::from_static("2023-06-01"));
    normalized.insert(&ANTHROPIC_VERSION, version);
  }
  if let Some(session) = session {
    normalized.insert(&X_OPENCODE_SESSION, HeaderValue::from_string(session));
  }
  if let Some(encoding) = ctx.content_encoding {
    normalized.insert(&CONTENT_ENCODING, HeaderValue::from_string(encoding.to_string()));
  }
  Some(normalized)
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn native_protocol_selects_its_path_and_credential_scheme() {
    for (endpoint, suffix) in [
      (Endpoint::ChatCompletions, "chat/completions"),
      (Endpoint::Responses, "responses"),
      (Endpoint::Messages, "messages"),
    ] {
      let prepared = prepare(
        "https://example.com/v1",
        endpoint,
        json!({"model":"test"}),
        "test-key",
        HeaderMap::new(),
      )
      .unwrap();
      assert_eq!(prepared.url, format!("https://example.com/v1/{suffix}"));
      if endpoint == Endpoint::Messages {
        assert_eq!(
          prepared.headers.get(&tokn_headers::keys::X_API_KEY).unwrap().as_str(),
          "test-key"
        );
        assert!(prepared.headers.get(&AUTHORIZATION).is_none());
      } else {
        assert_eq!(
          prepared.headers.get(&AUTHORIZATION).unwrap().as_str(),
          "Bearer test-key"
        );
      }
    }
  }
}
