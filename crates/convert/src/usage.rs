//! Best-effort `Usage` extraction from upstream LLM response bodies.
//!
//! Handles the three shapes we see in the wild:
//!
//! - **OpenAI chat completions**: `usage.{prompt_tokens, completion_tokens,
//!   prompt_tokens_details.cached_tokens,
//!   completion_tokens_details.reasoning_tokens}`
//! - **OpenAI responses / Codex**: `usage.{input_tokens, output_tokens,
//!   input_tokens_details.cached_tokens,
//!   output_tokens_details.reasoning_tokens}` (also accepts the same shape
//!   nested under `/response/usage` for SSE `response.completed` frames)
//! - **Anthropic** (messages / SSE `message_start`):
//!   `[message.]usage.{input_tokens, output_tokens,
//!   cache_creation_input_tokens, cache_read_input_tokens}`. Anthropic's
//!   `input_tokens` excludes cached portions, so we normalize
//!   `Usage.input_tokens` to the total (input + cache_creation + cache_read).

use serde_json::Value;
use tokn_core::usage::{Usage, UsageDetails, UsageType};

/// Extract `Usage` from an upstream response body. Returns an empty `Usage`
/// (all `None`) when no recognizable shape is found.
pub fn parse_usage_any_value(v: &Value) -> Usage {
  // Anthropic streaming shape: message.usage
  if let Some(usage) = parse_anthropic_usage(v.pointer("/message/usage")) {
    return usage;
  }
  // OpenAI Responses API SSE: response.usage
  if let Some(usage) = parse_openai_responses_usage(v.pointer("/response/usage")) {
    return usage;
  }
  // Top-level "usage" appears in multiple APIs, so detect first and then
  // dispatch to the matching parser rather than relying on parser order.
  match detect_usage_type(v.pointer("/usage")) {
    Some(UsageType::Chat) => {
      if let Some(usage) = parse_openai_chat_usage(v.pointer("/usage")) {
        return usage;
      }
    }
    Some(UsageType::Responses) => {
      if let Some(usage) = parse_openai_responses_usage(v.pointer("/usage")) {
        return usage;
      }
    }
    Some(UsageType::Messages) => {
      if let Some(usage) = parse_anthropic_usage(v.pointer("/usage")) {
        return usage;
      }
    }
    None => {}
  }
  // Fallbacks for odd mixed shapes.
  if let Some(usage) = parse_openai_chat_usage(v.pointer("/usage")) {
    return usage;
  }
  if let Some(usage) = parse_openai_responses_usage(v.pointer("/usage")) {
    return usage;
  }
  // Could be Anthropic-style at top-level "usage" too
  if let Some(usage) = parse_anthropic_usage(v.pointer("/usage")) {
    return usage;
  }
  Usage::default()
}

/// `true` when any field on `usage` carries a value.
pub fn usage_has_any(usage: &Usage) -> bool {
  usage.input_tokens.is_some()
    || usage.output_tokens.is_some()
    || usage.total_tokens.is_some()
    || usage.details.cache_read.is_some()
    || usage.details.cache_write.is_some()
    || usage.details.reasoning.is_some()
}

fn ptr_u64(v: Option<&Value>, path: &str) -> Option<u64> {
  v.and_then(|value| value.pointer(path)).and_then(Value::as_u64)
}

fn detect_usage_type(u: Option<&Value>) -> Option<UsageType> {
  let u = u?;
  if has_anthropic_usage_markers(u) {
    return Some(UsageType::Messages);
  }
  if ptr_u64(Some(u), "/prompt_tokens").is_some() || ptr_u64(Some(u), "/completion_tokens").is_some() {
    return Some(UsageType::Chat);
  }
  if ptr_u64(Some(u), "/input_tokens").is_some() || ptr_u64(Some(u), "/output_tokens").is_some() {
    return Some(UsageType::Responses);
  }
  None
}

fn has_anthropic_usage_markers(u: &Value) -> bool {
  ptr_u64(Some(u), "/cache_creation_input_tokens").is_some() || ptr_u64(Some(u), "/cache_read_input_tokens").is_some()
}

/// Parse OpenAI chat-completions usage block.
fn parse_openai_chat_usage(u: Option<&Value>) -> Option<Usage> {
  let input_tokens = ptr_u64(u, "/prompt_tokens");
  let output_tokens = ptr_u64(u, "/completion_tokens");
  let total_tokens = ptr_u64(u, "/total_tokens");
  if input_tokens.is_none() && output_tokens.is_none() {
    return None;
  }
  let cache_read = ptr_u64(u, "/prompt_tokens_details/cached_tokens");
  let reasoning = ptr_u64(u, "/completion_tokens_details/reasoning_tokens");
  Some(Usage {
    input_tokens,
    output_tokens,
    total_tokens,
    usage_type: Some(UsageType::Chat),
    details: UsageDetails {
      cache_read,
      cache_write: None,
      reasoning,
    },
  })
}

/// Parse OpenAI responses / Codex usage block.
fn parse_openai_responses_usage(u: Option<&Value>) -> Option<Usage> {
  let input_tokens = ptr_u64(u, "/input_tokens");
  let output_tokens = ptr_u64(u, "/output_tokens");
  let total_tokens = ptr_u64(u, "/total_tokens");
  if input_tokens.is_none() && output_tokens.is_none() {
    return None;
  }
  let cache_read = ptr_u64(u, "/input_tokens_details/cached_tokens");
  let reasoning = ptr_u64(u, "/output_tokens_details/reasoning_tokens");
  Some(Usage {
    input_tokens,
    output_tokens,
    total_tokens,
    usage_type: Some(UsageType::Responses),
    details: UsageDetails {
      cache_read,
      cache_write: None,
      reasoning,
    },
  })
}

/// Parse Anthropic-style usage block. Recognized by presence of any
/// `cache_*_input_tokens` field. Normalizes `Usage.input_tokens` to the
/// total (input + cache_creation + cache_read) since Anthropic's
/// `input_tokens` excludes cached content.
fn parse_anthropic_usage(u: Option<&Value>) -> Option<Usage> {
  let raw_input = ptr_u64(u, "/input_tokens");
  let cache_write = ptr_u64(u, "/cache_creation_input_tokens");
  let cache_read = ptr_u64(u, "/cache_read_input_tokens");
  // Require at least one Anthropic-specific marker.
  if !has_anthropic_usage_markers(u?) {
    return None;
  }
  let output_tokens = ptr_u64(u, "/output_tokens");
  let total_input = match (raw_input, cache_write, cache_read) {
    (None, None, None) => None,
    (a, b, c) => Some(a.unwrap_or(0) + b.unwrap_or(0) + c.unwrap_or(0)),
  };
  Some(Usage {
    input_tokens: total_input,
    output_tokens,
    total_tokens: ptr_u64(u, "/total_tokens"),
    usage_type: Some(UsageType::Messages),
    details: UsageDetails {
      cache_read,
      cache_write,
      reasoning: None,
    },
  })
}

/// Convenience: parse a JSON byte slice and extract usage. Returns
/// `Usage::default()` on parse error or unrecognized shape.
pub fn parse_usage_any_json(bytes: &[u8]) -> Usage {
  let v: Value = match serde_json::from_slice(bytes) {
    Ok(v) => v,
    Err(_) => return Usage::default(),
  };
  parse_usage_any_value(&v)
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn parses_openai_chat_usage() {
    let v = json!({ "usage": { "prompt_tokens": 11, "completion_tokens": 22 }});
    let u = parse_usage_any_value(&v);
    assert_eq!(u.input_tokens, Some(11));
    assert_eq!(u.output_tokens, Some(22));
    assert_eq!(u.total_tokens, None);
    assert_eq!(u.usage_type, Some(UsageType::Chat));
    assert_eq!(u.details.cache_read, None);
    assert_eq!(u.details.cache_write, None);
    assert_eq!(u.details.reasoning, None);
  }

  #[test]
  fn parses_responses_usage_shape() {
    let v = json!({ "usage": { "input_tokens": 5, "output_tokens": 7 }});
    let u = parse_usage_any_value(&v);
    assert_eq!(u.input_tokens, Some(5));
    assert_eq!(u.output_tokens, Some(7));
    assert_eq!(u.total_tokens, None);
    assert_eq!(u.usage_type, Some(UsageType::Responses));
  }

  #[test]
  fn parses_anthropic_message_start_nested_usage() {
    // Anthropic input_tokens excludes cache portions; total = 9 + 4 + 2 = 15
    let v = json!({
        "type": "message_start",
        "message": { "usage": {
          "input_tokens": 9,
          "output_tokens": 1,
          "cache_creation_input_tokens": 4,
          "cache_read_input_tokens": 2
        }}
    });
    let u = parse_usage_any_value(&v);
    assert_eq!(u.input_tokens, Some(15));
    assert_eq!(u.output_tokens, Some(1));
    assert_eq!(u.total_tokens, None);
    assert_eq!(u.usage_type, Some(UsageType::Messages));
    assert_eq!(u.details.cache_read, Some(2));
    assert_eq!(u.details.cache_write, Some(4));
  }

  #[test]
  fn parses_responses_response_completed_nested_usage() {
    let v = json!({
        "type": "response.completed",
        "response": { "usage": { "input_tokens": 3, "output_tokens": 4 }}
    });
    let u = parse_usage_any_value(&v);
    assert_eq!(u.input_tokens, Some(3));
    assert_eq!(u.output_tokens, Some(4));
    assert_eq!(u.total_tokens, None);
    assert_eq!(u.usage_type, Some(UsageType::Responses));
  }

  #[test]
  fn parses_openai_cached_and_reasoning_tokens() {
    let v = json!({ "usage": {
      "prompt_tokens": 100,
      "completion_tokens": 50,
      "prompt_tokens_details": { "cached_tokens": 30 },
      "completion_tokens_details": { "reasoning_tokens": 20 }
    }});
    let u = parse_usage_any_value(&v);
    assert_eq!(u.input_tokens, Some(100));
    assert_eq!(u.output_tokens, Some(50));
    assert_eq!(u.total_tokens, None);
    assert_eq!(u.usage_type, Some(UsageType::Chat));
    assert_eq!(u.details.cache_read, Some(30));
    assert_eq!(u.details.cache_write, None);
    assert_eq!(u.details.reasoning, Some(20));
  }

  #[test]
  fn parses_codex_usage_details_shape() {
    let v = json!({ "usage": {
      "input_tokens": 35973,
      "input_tokens_details": { "cached_tokens": 34176 },
      "output_tokens": 989,
      "output_tokens_details": { "reasoning_tokens": 11 },
      "total_tokens": 36962
    }});
    let u = parse_usage_any_value(&v);
    assert_eq!(u.input_tokens, Some(35973));
    assert_eq!(u.output_tokens, Some(989));
    assert_eq!(u.total_tokens, Some(36962));
    assert_eq!(u.usage_type, Some(UsageType::Responses));
    assert_eq!(u.details.cache_read, Some(34176));
    assert_eq!(u.details.cache_write, None);
    assert_eq!(u.details.reasoning, Some(11));
  }

  #[test]
  fn top_level_anthropic_usage_is_detected_before_openai_responses() {
    let v = json!({
      "type": "message_delta",
      "usage": {
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "input_tokens": 8,
        "output_tokens": 23
      }
    });
    let u = parse_usage_any_value(&v);
    assert_eq!(u.input_tokens, Some(8));
    assert_eq!(u.output_tokens, Some(23));
    assert_eq!(u.usage_type, Some(UsageType::Messages));
    assert_eq!(u.details.cache_read, Some(0));
    assert_eq!(u.details.cache_write, Some(0));
  }

  #[test]
  fn returns_default_on_unknown_shape() {
    let v = json!({ "foo": "bar" });
    let u = parse_usage_any_value(&v);
    assert!(!usage_has_any(&u));
  }

  #[test]
  fn json_helper_handles_invalid_input() {
    let u = parse_usage_any_json(b"not json");
    assert!(!usage_has_any(&u));
  }
}
