//! Pure native request preparation. Transport, credential refresh, and timing
//! belong to the caller's platform services.

use alloc::{
  string::{String, ToString},
  vec::Vec,
};
use serde_json::Value;
use tokn_core::{
  provider::{prepare_request_with_policy, validate_credential, PrepareError, PreparedRequest},
  upstream_url::CleartextHttpPolicy,
  Endpoint,
};
use tokn_headers::{
  keys::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
  HeaderMap, HeaderValue,
};

/// Operation path relative to the configured, validated provider base URL.
pub fn operation_segments(endpoint: Endpoint, _base_url: &str) -> Option<&'static [&'static str]> {
  match endpoint {
    Endpoint::ChatCompletions => Some(&["chat", "completions"]),
    Endpoint::Responses => Some(&["responses"]),
    Endpoint::Messages => None,
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

pub fn transform_codex_input(endpoint: Endpoint, mut body: Value) -> Value {
  if endpoint == Endpoint::Responses {
    if let Some(obj) = body.as_object_mut() {
      obj.remove("max_output_tokens");
      obj.insert("store".into(), Value::Bool(false));
      obj.insert("stream".into(), Value::Bool(true));
      normalize_verbosity(obj);
      if let Some(input) = obj.get_mut("input") {
        normalize_codex_input(input);
      }
      if !obj.get("instructions").map(Value::is_string).unwrap_or(false) {
        obj.insert("instructions".into(), Value::String(String::new()));
      }
      normalize_reasoning_effort(obj);
    }
  }
  body
}

fn normalize_reasoning_effort(obj: &mut serde_json::Map<String, Value>) {
  let Some(effort) = obj
    .remove("reasoning_effort")
    .and_then(|v| v.as_str().map(str::to_string))
  else {
    return;
  };

  match obj.get_mut("reasoning") {
    Some(Value::Object(reasoning)) => {
      reasoning.entry("effort").or_insert_with(|| Value::String(effort));
    }
    Some(_) => {}
    None => {
      obj.insert("reasoning".into(), serde_json::json!({ "effort": effort }));
    }
  }
}

fn normalize_verbosity(obj: &mut serde_json::Map<String, Value>) {
  let Some(verbosity) = obj.remove("verbosity") else {
    return;
  };

  match obj.get_mut("text") {
    Some(Value::Object(text)) => {
      text.entry("verbosity").or_insert(verbosity);
    }
    Some(_) => {}
    None => {
      obj.insert("text".into(), serde_json::json!({ "verbosity": verbosity }));
    }
  }
}

fn normalize_codex_input(input: &mut Value) {
  let replacement = match input.clone() {
    Value::Array(_) => return,
    Value::String(text) => {
      serde_json::json!([{"role": "user", "content": [{"type": "input_text", "text": text.clone()}]}])
    }
    Value::Null => Value::Array(Vec::new()),
    other => Value::Array(vec![other]),
  };
  *input = replacement;
}

pub fn inject_codex_credentials(headers: &mut HeaderMap, token: &str, account_id: Option<&str>) {
  inject_credentials(headers, Endpoint::Responses, token);
  if let Some(account_id) = account_id.filter(|id| !id.trim().is_empty()) {
    headers.insert(
      &tokn_headers::keys::CHATGPT_ACCOUNT_ID,
      HeaderValue::from_string(account_id.into()),
    );
  }
}

pub fn prepare_codex(
  base_url: &str,
  endpoint: Endpoint,
  body: Value,
  credential: &str,
  headers: HeaderMap,
) -> Result<PreparedRequest, PrepareError> {
  prepare_codex_with_policy(
    base_url,
    endpoint,
    body,
    credential,
    headers,
    CleartextHttpPolicy::LoopbackOnly,
  )
}

pub fn prepare_codex_with_policy(
  base_url: &str,
  endpoint: Endpoint,
  body: Value,
  credential: &str,
  headers: HeaderMap,
  cleartext: CleartextHttpPolicy,
) -> Result<PreparedRequest, PrepareError> {
  if endpoint != Endpoint::Responses {
    return Err(PrepareError::UnsupportedEndpoint(endpoint));
  }
  prepare_with_policy(
    base_url,
    endpoint,
    transform_codex_input(endpoint, body),
    credential,
    headers,
    cleartext,
  )
}

pub const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
pub const CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";

/// Per-model operation availability used by the shared route resolver.
pub fn supports_model_endpoint(_model: &str, endpoint: Endpoint, base_url: &str) -> bool {
  operation_segments(endpoint, base_url).is_some()
}

/// Apply the same provider-owned header policy as the standard provider.
pub fn normalize_headers(headers: &mut HeaderMap, ctx: &tokn_core::provider::HeaderPatchCtx<'_>) -> Option<HeaderMap> {
  Some(crate::common::normalize_openai_platform_headers(headers, ctx))
}

pub fn normalize_codex_headers(
  headers: &mut HeaderMap,
  ctx: &tokn_core::provider::HeaderPatchCtx<'_>,
) -> Option<HeaderMap> {
  Some(crate::common::normalize_codex_headers(headers, ctx))
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn native_preparation_preserves_prefix_and_credentials() {
    let prepared = prepare(
      "https://example.com/compatible/v1",
      Endpoint::Responses,
      json!({"model":"test","input":"hello"}),
      "test-key",
      HeaderMap::new(),
    )
    .unwrap();
    assert_eq!(prepared.url, "https://example.com/compatible/v1/responses");
    assert_eq!(
      prepared.headers.get(&AUTHORIZATION).unwrap().as_str(),
      "Bearer test-key"
    );
    assert_eq!(
      serde_json::from_slice::<Value>(&prepared.body).unwrap()["model"],
      "test"
    );
    assert!(matches!(
      prepare(
        "https://example.com/v1",
        Endpoint::Messages,
        json!({}),
        "test-key",
        HeaderMap::new()
      ),
      Err(PrepareError::UnsupportedEndpoint(Endpoint::Messages))
    ));
  }

  #[test]
  fn rejects_unsafe_urls_and_header_credentials() {
    for url in [
      "https://user:password@example.com/v1",
      "https://example.com/a/../v1",
      "https://example.com/v1?key=x",
      "http://example.com/v1",
    ] {
      assert!(prepare(url, Endpoint::Responses, json!({}), "key", HeaderMap::new()).is_err());
    }
    for credential in ["", "a\r\nx-api-key: bad", "a b"] {
      assert!(matches!(
        prepare(
          "https://example.com/v1",
          Endpoint::Responses,
          json!({}),
          credential,
          HeaderMap::new()
        ),
        Err(PrepareError::InvalidCredential)
      ));
    }
  }

  #[test]
  fn codex_preparation_uses_the_same_input_contract_as_standard_provider() {
    let prepared = prepare_codex(
      "https://example.com/codex",
      Endpoint::Responses,
      json!({"model":"test","input":"hello","max_output_tokens":32,"reasoning_effort":"high"}),
      "access-token",
      HeaderMap::new(),
    )
    .unwrap();
    let value: Value = serde_json::from_slice(&prepared.body).unwrap();
    assert_eq!(value["stream"], true);
    assert_eq!(value["store"], false);
    assert_eq!(value["reasoning"]["effort"], "high");
    assert!(value["input"].is_array());
    assert!(value.get("max_output_tokens").is_none());
  }
}
