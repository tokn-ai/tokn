use crate::error::Result;
use crate::ir::IrRequest;
use tokn_endpoint_responses::ResponsesRequest;

pub fn request_from_endpoint(request: &ResponsesRequest) -> Result<IrRequest> {
  let value = serde_json::to_value(request)?;
  crate::value::responses::request_from_value(&value)
}

pub fn request_to_endpoint(request: &IrRequest) -> Result<ResponsesRequest> {
  let value = crate::value::responses::request_to_value(request)?;
  Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn endpoint_request_matches_value_request_ir() {
    let value = json!({
      "model": "gpt-4.1",
      "instructions": "rules",
      "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
      "tools": [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}],
      "stream": true
    });
    let endpoint: ResponsesRequest = serde_json::from_value(value.clone()).unwrap();

    let from_value = crate::value::responses::request_from_value(&value).unwrap();
    let from_endpoint = request_from_endpoint(&endpoint).unwrap();

    assert_eq!(
      serde_json::to_value(from_endpoint).unwrap(),
      serde_json::to_value(from_value).unwrap()
    );
  }
}
