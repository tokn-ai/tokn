//! Codex compaction requests use priority processing across managed and relay paths.

use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::stages::Resolved;
use serde_json::Value;
use tokn_core::provider::ID_CODEX;
use tokn_core::request_classification::{RequestClassification, RequestPurpose};

pub(super) fn compaction_provider_id<'a>(ctx: &'a PipelineCtx, resolved: &'a Resolved) -> &'a str {
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
