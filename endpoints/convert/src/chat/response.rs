use crate::error::Result;
use crate::ir::IrResponse;
use tokn_endpoint_chat_completions::ChatResponse;

pub fn response_from_endpoint(response: &ChatResponse) -> Result<IrResponse> {
  let value = serde_json::to_value(response)?;
  crate::value::chat::response_from_value(&value)
}

pub fn response_to_endpoint(response: &IrResponse) -> Result<ChatResponse> {
  let value = crate::value::chat::response_to_value(response)?;
  Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn endpoint_response_matches_value_response_ir() {
    let value = json!({
      "id": "chatcmpl_1",
      "model": "gpt-4.1",
      "choices": [{
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "hello",
          "reasoning_content": "thinking",
          "tool_calls": [{
            "id": "call_1",
            "type": "function",
            "function": {"name": "lookup", "arguments": "{\"q\":\"x\"}"}
          }]
        },
        "finish_reason": "tool_calls"
      }],
      "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
    });
    let endpoint: ChatResponse = serde_json::from_value(value.clone()).unwrap();

    let from_value = crate::value::chat::response_from_value(&value).unwrap();
    let from_endpoint = response_from_endpoint(&endpoint).unwrap();

    assert_eq!(
      serde_json::to_value(from_endpoint).unwrap(),
      serde_json::to_value(from_value).unwrap()
    );
  }

  #[test]
  fn endpoint_response_renders_from_ir() {
    let value = json!({
      "id": "chatcmpl_1",
      "model": "gpt-4.1",
      "choices": [{
        "index": 0,
        "message": {"role": "assistant", "content": "hello"},
        "finish_reason": "stop"
      }]
    });
    let ir = crate::value::chat::response_from_value(&value).unwrap();

    let endpoint = response_to_endpoint(&ir).unwrap();

    assert_eq!(endpoint.id.as_deref(), Some("chatcmpl_1"));
    assert_eq!(endpoint.choices.len(), 1);
  }
}
