use crate::error::Result;
use crate::ir::IrResponse;
use tokn_endpoint_messages::MessagesResponse;

pub fn response_from_endpoint(response: &MessagesResponse) -> Result<IrResponse> {
  let value = serde_json::to_value(response)?;
  crate::value::messages::response_from_value(&value)
}

pub fn response_to_endpoint(response: &IrResponse) -> Result<MessagesResponse> {
  let value = crate::value::messages::response_to_value(response)?;
  Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn endpoint_response_matches_value_response_ir() {
    let value = json!({
      "id": "msg_1",
      "type": "message",
      "role": "assistant",
      "model": "claude-sonnet",
      "content": [
        {"type": "text", "text": "hello"},
        {"type": "thinking", "thinking": "thinking"},
        {"type": "tool_use", "id": "call_1", "name": "lookup", "input": {"q": "x"}}
      ],
      "stop_reason": "tool_use",
      "usage": {"input_tokens": 1, "output_tokens": 2}
    });
    let endpoint: MessagesResponse = serde_json::from_value(value.clone()).unwrap();

    let from_value = crate::value::messages::response_from_value(&value).unwrap();
    let from_endpoint = response_from_endpoint(&endpoint).unwrap();

    assert_eq!(
      serde_json::to_value(from_endpoint).unwrap(),
      serde_json::to_value(from_value).unwrap()
    );
  }
}
