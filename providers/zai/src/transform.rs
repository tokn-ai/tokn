//! Request body shaping for Z.ai chat completions.
//!
//! Mirrors the contract enforced by opencode's
//! `packages/opencode/test/provider/transform.test.ts`: when the target model
//! advertises `capabilities.reasoning = true` and the caller has not already
//! supplied a `thinking` field, inject:
//!
//! ```json
//! { "thinking": { "type": "enabled", "clear_thinking": false } }
//! ```
//!
//! Reasoning is **off-by-default at the wire level**, so omitting this block
//! silently disables chain-of-thought for GLM models that would otherwise
//! produce it. Conversely, if the caller already sent their own `thinking`
//! object, we never overwrite it — that's a deliberate opt-out path for
//! debugging.

use alloc::string::ToString;
use serde_json::{json, Value};

/// Returns a (possibly cloned) request body with the `thinking` block applied
/// when appropriate. The original body is never mutated in place.
pub fn shape_request(body: &Value, reasoning_enabled: bool) -> Value {
  if !reasoning_enabled {
    return body.clone();
  }
  let mut out = body.clone();
  let obj = match out.as_object_mut() {
    Some(o) => o,
    // Non-object body (shouldn't happen for chat/completions) — return
    // unchanged rather than panicking.
    None => return out,
  };
  if obj.contains_key("thinking") {
    return out;
  }
  obj.insert(
    "thinking".to_string(),
    json!({ "type": "enabled", "clear_thinking": false }),
  );
  out
}

#[cfg(test)]
mod tests {
  use super::*;

  fn body() -> Value {
    json!({
        "model": "glm-4.6",
        "messages": [{"role":"user","content":"hi"}]
    })
  }

  #[test]
  fn injects_thinking_when_reasoning_enabled() {
    let out = shape_request(&body(), true);
    let t = out.get("thinking").expect("thinking block present");
    assert_eq!(t.get("type").and_then(|v| v.as_str()), Some("enabled"));
    assert_eq!(t.get("clear_thinking").and_then(|v| v.as_bool()), Some(false));
  }

  #[test]
  fn skips_when_reasoning_disabled() {
    let out = shape_request(&body(), false);
    assert!(out.get("thinking").is_none(), "must not inject when reasoning=false");
  }

  #[test]
  fn does_not_overwrite_caller_thinking() {
    let mut b = body();
    b.as_object_mut()
      .unwrap()
      .insert("thinking".into(), json!({"type":"disabled"}));
    let out = shape_request(&b, true);
    assert_eq!(
      out.get("thinking").and_then(|t| t.get("type")).and_then(|v| v.as_str()),
      Some("disabled"),
      "caller-supplied thinking must win"
    );
  }

  #[test]
  fn preserves_other_fields() {
    let out = shape_request(&body(), true);
    assert_eq!(out.get("model").and_then(|v| v.as_str()), Some("glm-4.6"));
    assert!(out.get("messages").and_then(|v| v.as_array()).is_some());
  }

  #[test]
  fn non_object_body_is_returned_unchanged() {
    let v = json!("not an object");
    let out = shape_request(&v, true);
    assert_eq!(out, v);
  }

  #[test]
  fn does_not_mutate_input() {
    let b = body();
    let _ = shape_request(&b, true);
    assert!(b.get("thinking").is_none(), "input body must not be mutated");
  }
}
