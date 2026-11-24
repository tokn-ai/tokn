use crate::error::Result;
use crate::ir::IrRequest;
use tokn_endpoint_chat_completions::ChatRequest;

pub fn request_from_endpoint(request: &ChatRequest) -> Result<IrRequest> {
  let value = serde_json::to_value(request)?;
  crate::value::chat::request_from_value(&value)
}

pub fn request_to_endpoint(request: &IrRequest) -> Result<ChatRequest> {
  let value = crate::value::chat::request_to_value(request)?;
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
      "messages": [
        {"role": "system", "content": "rules"},
        {"role": "user", "content": [{"type": "text", "text": "hi"}]}
      ],
      "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}],
      "stream": true,
      "temperature": 0.2
    });
    let endpoint: ChatRequest = serde_json::from_value(value.clone()).unwrap();

    let from_value = crate::value::chat::request_from_value(&value).unwrap();
    let from_endpoint = request_from_endpoint(&endpoint).unwrap();

    assert_eq!(
      serde_json::to_value(from_endpoint).unwrap(),
      serde_json::to_value(from_value).unwrap()
    );
  }

  #[test]
  fn endpoint_request_renders_from_ir() {
    let value = json!({
      "model": "gpt-4.1",
      "messages": [{"role": "user", "content": "hi"}],
      "stream": true
    });
    let ir = crate::value::chat::request_from_value(&value).unwrap();

    let endpoint = request_to_endpoint(&ir).unwrap();

    assert_eq!(endpoint.model, "gpt-4.1");
    assert_eq!(endpoint.messages.len(), 1);
    assert_eq!(endpoint.stream, Some(true));
  }
}
