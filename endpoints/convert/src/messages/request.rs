use crate::error::Result;
use crate::ir::IrRequest;
use tokn_endpoint_messages::MessagesRequest;

pub fn request_from_endpoint(request: &MessagesRequest) -> Result<IrRequest> {
  let value = serde_json::to_value(request)?;
  crate::value::messages::request_from_value(&value)
}

pub fn request_to_endpoint(request: &IrRequest) -> Result<MessagesRequest> {
  let value = crate::value::messages::request_to_value(request)?;
  Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn endpoint_request_matches_value_request_ir() {
    let value = json!({
      "model": "claude-sonnet",
      "system": "rules",
      "max_tokens": 1024,
      "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
      "tools": [{"name": "lookup", "input_schema": {"type": "object"}}],
      "stream": true
    });
    let endpoint: MessagesRequest = serde_json::from_value(value.clone()).unwrap();

    let from_value = crate::value::messages::request_from_value(&value).unwrap();
    let from_endpoint = request_from_endpoint(&endpoint).unwrap();

    assert_eq!(
      serde_json::to_value(from_endpoint).unwrap(),
      serde_json::to_value(from_value).unwrap()
    );
  }
}
