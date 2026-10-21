//! Pure native request preparation. Transport, credential refresh, and timing
//! belong to the caller's platform services.

use alloc::string::{String, ToString};
use serde_json::{json, Value};
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
pub fn operation_segments(endpoint: Endpoint, base_url: &str) -> Option<&'static [&'static str]> {
  match endpoint {
    Endpoint::ChatCompletions => Some(&["chat", "completions"]),
    Endpoint::Messages if base_url.trim_end_matches('/').ends_with("/anthropic") => Some(&["v1", "messages"]),
    Endpoint::Messages => Some(&["anthropic", "v1", "messages"]),
    Endpoint::Responses => None,
  }
}

/// Install caller-supplied credentials using this provider's native scheme.
pub fn inject_credentials(headers: &mut HeaderMap, _endpoint: Endpoint, credential: &str) {
  headers.insert(&AUTHORIZATION, HeaderValue::from_string(format!("Bearer {credential}")));
}

/// Apply the provider's request-body contract independently of transport.
pub fn transform_input(endpoint: Endpoint, body: Value) -> Value {
  let mut out = body;
  let Some(obj) = out.as_object_mut() else {
    return out;
  };

  // Keep normalized controls authoritative, but also accept native endpoint
  // fields. Otherwise raw requests (and already-shaped requests) lose effort.
  let effort = extract_effort(obj.get("thinking"))
    .or_else(|| extract_effort(obj.get("reasoning")))
    .or_else(|| obj.get("reasoning_effort").and_then(Value::as_str).map(str::to_string))
    .or_else(|| extract_effort(obj.get("output_config")));
  let thinking_mode = extract_thinking_mode(obj.get("thinking"), obj.get("reasoning"), effort.is_some());
  obj.insert("thinking".into(), json!({ "type": thinking_mode }));
  obj.remove("reasoning");

  match endpoint {
    Endpoint::ChatCompletions => {
      obj.remove("output_config");
      if let Some(effort) = effort {
        obj.insert("reasoning_effort".into(), Value::String(effort));
      } else {
        obj.remove("reasoning_effort");
      }
    }
    Endpoint::Messages => {
      obj.remove("reasoning_effort");
      if let Some(effort) = effort {
        obj.insert("output_config".into(), json!({ "effort": effort }));
      } else {
        obj.remove("output_config");
      }
    }
    Endpoint::Responses => {}
  }

  out
}

fn extract_thinking_mode(thinking: Option<&Value>, reasoning: Option<&Value>, has_effort: bool) -> &'static str {
  match thinking
    .and_then(explicit_thinking_mode)
    .or_else(|| reasoning.and_then(explicit_thinking_mode))
  {
    Some(mode) => mode,
    None if thinking.is_some() || reasoning.is_some() || has_effort => "enabled",
    None => "disabled",
  }
}

fn explicit_thinking_mode(value: &Value) -> Option<&'static str> {
  match value {
    Value::Bool(true) => Some("enabled"),
    Value::Bool(false) => Some("disabled"),
    Value::String(s) if s.eq_ignore_ascii_case("enabled") => Some("enabled"),
    Value::String(s) if s.eq_ignore_ascii_case("disabled") => Some("disabled"),
    Value::Object(map) => match map.get("type").and_then(Value::as_str) {
      Some("enabled") => Some("enabled"),
      Some("disabled") => Some("disabled"),
      _ => None,
    },
    _ => None,
  }
}

fn extract_effort(value: Option<&Value>) -> Option<String> {
  match value {
    Some(Value::Object(map)) => map.get("effort").and_then(Value::as_str).map(str::to_string),
    _ => None,
  }
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

pub const DEFAULT_BASE_URL: &str = "https://api.deepseek.com";

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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn messages_paths_and_request_controls_are_shared() {
    for base in ["https://api.deepseek.com", "https://api.deepseek.com/anthropic"] {
      let request = prepare(
        base,
        Endpoint::Messages,
        json!({"model":"test", "reasoning_effort":"high"}),
        "test-key",
        HeaderMap::new(),
      )
      .unwrap();
      assert_eq!(request.url, "https://api.deepseek.com/anthropic/v1/messages");
      let body: Value = serde_json::from_slice(&request.body).unwrap();
      assert_eq!(body["thinking"]["type"], "enabled");
      assert_eq!(body["output_config"]["effort"], "high");
      assert!(body.get("reasoning_effort").is_none());
    }
  }
}
