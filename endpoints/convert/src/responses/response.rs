use crate::error::Result;
use crate::ir::IrResponse;
use tokn_endpoint_responses::ResponsesResponse;

pub fn response_from_endpoint(response: &ResponsesResponse) -> Result<IrResponse> {
  let value = serde_json::to_value(response)?;
  crate::value::responses::response_from_value(&value)
}

pub fn response_to_endpoint(response: &IrResponse) -> Result<ResponsesResponse> {
  let value = crate::value::responses::response_to_value(response)?;
  Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn endpoint_response_matches_value_response_ir() {
    let value = json!({
      "id": "resp_1",
      "model": "gpt-4.1",
      "status": "completed",
      "output": [
        {
          "type": "message",
          "id": "msg_1",
          "role": "assistant",
          "content": [{"type": "output_text", "text": "hello", "annotations": []}]
        },
        {"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{\"q\":\"x\"}"}
      ],
      "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
    });
    let endpoint: ResponsesResponse = serde_json::from_value(value.clone()).unwrap();

    let from_value = crate::value::responses::response_from_value(&value).unwrap();
    let from_endpoint = response_from_endpoint(&endpoint).unwrap();

    assert_eq!(
      serde_json::to_value(from_endpoint).unwrap(),
      serde_json::to_value(from_value).unwrap()
    );
  }
}
