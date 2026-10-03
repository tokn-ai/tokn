//! Codex compaction requests use priority processing across managed and relay paths.

use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::stages::Resolved;
use serde_json::Value;
use tokn_core::provider::ID_CODEX;
use tokn_core::request_classification::{RequestClassification, RequestPurpose};
use tokn_headers::{keys::X_CODEX_ROUTING_HINT, HeaderMap};

pub(crate) fn compaction_provider_id<'a>(ctx: &'a PipelineCtx, resolved: &'a Resolved) -> &'a str {
  ctx
    .config
    .get_str(crate::stages::resolve::proxy::keys::PROVIDER_DRIVER_ID)
    .unwrap_or_else(|| resolved.account_handle.provider.info().id.as_str())
}

pub(super) fn requires_compaction_priority(provider_id: &str, classification: Option<RequestClassification>) -> bool {
  provider_id == ID_CODEX && classification.is_some_and(|value| value.purpose == RequestPurpose::Compaction)
}

/// Force priority processing for a detected compaction sent to Codex.
/// Returns whether the body changed, so relay callers can preserve untouched wire bytes.
pub fn apply_compaction_priority(
  provider_id: &str,
  classification: Option<RequestClassification>,
  body: &mut Value,
) -> bool {
  if !requires_compaction_priority(provider_id, classification) {
    return false;
  }
  let Some(object) = body.as_object_mut() else {
    return false;
  };
  if object.get("service_tier").and_then(Value::as_str) == Some("priority") {
    return false;
  }
  object.insert("service_tier".into(), Value::String("priority".into()));
  true
}

/// Match the Codex routing hint to a compaction's finalized priority body.
/// The body is decoded JSON, even when the outgoing wire bytes are compressed.
/// Other routing directives survive; duplicate model/tier directives are replaced.
pub fn apply_compaction_priority_routing_hint(
  provider_id: &str,
  classification: Option<RequestClassification>,
  decoded_body: &[u8],
  headers: &mut HeaderMap,
) {
  if !requires_compaction_priority(provider_id, classification)
    || decoded_body.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{')
  {
    return;
  }
  #[derive(serde::Deserialize)]
  struct RoutingHintRequest {
    model: Option<String>,
    service_tier: Option<String>,
  }
  let Ok(body) = serde_json::from_slice::<RoutingHintRequest>(decoded_body) else {
    return;
  };
  if body.service_tier.as_deref() != Some("priority") {
    return;
  }
  let Some(model) = body
    .model
    .filter(|model| !model.is_empty() && !model.bytes().any(|byte| byte.is_ascii_control() || byte == b';'))
  else {
    return;
  };
  let mut hint = format!("model={model};tier=priority");
  for value in headers.get_all(&X_CODEX_ROUTING_HINT) {
    for directive in value.as_str().split(';') {
      if directive.trim().is_empty()
        || directive
          .split_once('=')
          .is_some_and(|(key, _)| key.trim().eq_ignore_ascii_case("model") || key.trim().eq_ignore_ascii_case("tier"))
      {
        continue;
      }
      hint.push(';');
      hint.push_str(directive);
    }
  }
  headers.insert(&X_CODEX_ROUTING_HINT, hint);
}

#[cfg(test)]
mod tests {
  use super::*;
  use tokn_core::request_classification::RequestClassificationSource;

  fn compaction() -> Option<RequestClassification> {
    Some(RequestClassification {
      purpose: RequestPurpose::Compaction,
      source: RequestClassificationSource::RequestField,
    })
  }

  #[test]
  fn priority_hint_uses_final_model_and_preserves_other_directives() {
    let mut headers = HeaderMap::new();
    headers.append(&X_CODEX_ROUTING_HINT, "model=old;tier=default;region=west");
    headers.append(&X_CODEX_ROUTING_HINT, "tier=flex;model=also-old;opaque");
    let body = br#"{"model":"gpt-6.1-sol","service_tier":"priority"}"#;
    apply_compaction_priority_routing_hint("codex", compaction(), body, &mut headers);
    assert_eq!(headers.get_all(&X_CODEX_ROUTING_HINT).count(), 1);
    assert_eq!(
      headers.get(&X_CODEX_ROUTING_HINT).unwrap().as_str(),
      "model=gpt-6.1-sol;tier=priority;region=west;opaque"
    );
    let first = headers.clone();
    apply_compaction_priority_routing_hint("codex", compaction(), body, &mut headers);
    assert_eq!(headers, first);
  }

  #[test]
  fn priority_hint_requires_valid_final_priority_json_and_model() {
    for body in [
      br#"{"model":"gpt-6.1-sol","service_tier":"default"}"#.as_slice(),
      br#"{"model":"gpt-6.1-sol"}"#,
      br#"{"service_tier":"priority"}"#,
      br#"{"model":"","service_tier":"priority"}"#,
      br#"{"model":"gpt;extra=bad","service_tier":"priority"}"#,
      br#"{"model":"gpt\n","service_tier":"priority"}"#,
      br#"{"model":false,"service_tier":"priority"}"#,
      br#"["gpt-6.1-sol","priority"]"#,
      br#"{"model":"gpt-6.1-sol","service_tier":"priority""#,
    ] {
      let mut headers = HeaderMap::new();
      headers.insert(&X_CODEX_ROUTING_HINT, "unchanged");
      apply_compaction_priority_routing_hint("codex", compaction(), body, &mut headers);
      assert_eq!(headers.get(&X_CODEX_ROUTING_HINT).unwrap().as_str(), "unchanged");
    }
  }
}
