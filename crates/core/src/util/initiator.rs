use serde_json::Value;

/// Classify any supported request shape, including the legacy tools-only hint.
pub fn classify_initiator_any(body: &Value) -> Option<&'static str> {
  if body.get("input").is_some() {
    classify_initiator_responses(body)
  } else {
    classify_initiator(body).or_else(|| {
      body
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty())
        .then_some("agent")
    })
  }
}

/// Best-effort initiator classifier for chat-style payloads.
///
/// Contract:
/// - `Some("user")`: the payload carries concrete evidence of a direct user
///   turn.
/// - `Some("agent")`: the payload carries concrete evidence that this is
///   part of an agent/tool loop.
/// - `None`: the shape is unknown or insufficiently structured, so we avoid
///   asserting either initiator.
pub fn classify_initiator(body: &Value) -> Option<&'static str> {
  let msgs = body.get("messages").and_then(|v| v.as_array())?;
  for m in msgs.iter().rev() {
    match m.get("role").and_then(|r| r.as_str()) {
      Some("system") => continue,
      Some("tool") => return Some("agent"),
      Some("assistant") => return Some("agent"),
      Some("user") => return Some("user"),
      _ => return None,
    }
  }
  None
}

/// Responses-API variant of [`classify_initiator`].
///
/// Shares the same contract:
/// - `Some("user")`: concrete user-turn evidence.
/// - `Some("agent")`: concrete tool/assistant continuation evidence.
/// - `None`: unknown or insufficiently structured input.
pub fn classify_initiator_responses(body: &Value) -> Option<&'static str> {
  let input = body.get("input")?;
  if input.is_string() {
    return Some("user");
  }
  if let Some(items) = input.as_array() {
    for it in items.iter().rev() {
      let classified = classify_initiator_response_item(it);
      if classified.is_some() {
        return classified;
      }
    }
    return None;
  }
  classify_initiator_response_item(input)
}

fn classify_initiator_response_item(item: &Value) -> Option<&'static str> {
  let typ = item.get("type").and_then(|t| t.as_str());
  if let Some(t) = typ {
    match t {
      "function_call_output" | "tool_result" | "computer_call_output" => return Some("agent"),
      "function_call" | "tool_call" | "reasoning" => return Some("agent"),
      "message" => {}
      _ => return None,
    }
  }
  match item.get("role").and_then(|r| r.as_str()) {
    Some("system") | Some("developer") => None,
    Some("tool") => Some("agent"),
    Some("assistant") => Some("agent"),
    Some("user") => Some("user"),
    _ => None,
  }
}

#[cfg(test)]
mod responses_tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn bare_string_input_is_user() {
    let b = json!({ "input": "hello" });
    assert_eq!(classify_initiator_responses(&b), Some("user"));
  }

  #[test]
  fn missing_input_is_unknown() {
    assert_eq!(classify_initiator_responses(&json!({})), None);
  }

  #[test]
  fn user_message_array_is_user() {
    let b = json!({ "input": [
        { "role": "system", "content": "x" },
        { "role": "user", "content": "hi" }
    ]});
    assert_eq!(classify_initiator_responses(&b), Some("user"));
  }

  #[test]
  fn tool_followup_is_agent() {
    let b = json!({ "input": [
        { "role": "user", "content": "x" },
        { "type": "function_call", "name": "f" },
        { "type": "function_call_output", "output": "42" }
    ]});
    assert_eq!(classify_initiator_responses(&b), Some("agent"));
  }

  #[test]
  fn non_array_input_detects_user_role() {
    let b = json!({ "input": {"role": "user", "content": "x"} });
    assert_eq!(classify_initiator_responses(&b), Some("user"));
  }

  #[test]
  fn non_array_input_detects_agent_type() {
    let b = json!({ "input": {"type": "function_call_output", "output": "x"} });
    assert_eq!(classify_initiator_responses(&b), Some("agent"));
  }

  #[test]
  fn unknown_response_type_is_unknown() {
    let b = json!({ "input": [{ "type": "mystery" }] });
    assert_eq!(classify_initiator_responses(&b), None);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn user_turn() {
    let b = json!({"messages":[
        {"role":"system","content":"x"},
        {"role":"user","content":"hi"}
    ]});
    assert_eq!(classify_initiator(&b), Some("user"));
  }

  #[test]
  fn tool_followup_is_agent() {
    let b = json!({"messages":[
        {"role":"user","content":"do x"},
        {"role":"assistant","tool_calls":[{"id":"1"}]},
        {"role":"tool","tool_call_id":"1","content":"42"}
    ]});
    assert_eq!(classify_initiator(&b), Some("agent"));
  }

  #[test]
  fn after_assistant_is_agent() {
    let b = json!({"messages":[
        {"role":"user","content":"hi"},
        {"role":"assistant","content":"ok"}
    ]});
    assert_eq!(classify_initiator(&b), Some("agent"));
  }

  #[test]
  fn empty_is_unknown() {
    let b = json!({});
    assert_eq!(classify_initiator(&b), None);
  }

  #[test]
  fn unknown_chat_role_is_unknown() {
    let b = json!({"messages":[{"role":"mystery","content":"hi"}]});
    assert_eq!(classify_initiator(&b), None);
  }
}
