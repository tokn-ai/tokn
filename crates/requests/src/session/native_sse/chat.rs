//! Faithful buffered Chat Completions reconstruction for native checkpoints.

use alloc::{collections::BTreeMap, string::String, vec::Vec};
use serde_json::{json, Map, Value};

use crate::session::Error;

#[derive(Default)]
pub(super) struct Chat {
  metadata: Map<String, Value>,
  message: Map<String, Value>,
  calls: Vec<ToolCall>,
  call_indices: BTreeMap<u64, usize>,
  choice_metadata: Map<String, Value>,
  finish_reason: Option<String>,
  seen_choice: bool,
}

#[derive(Default)]
struct ToolCall {
  id: String,
  kind: Option<String>,
  name: String,
  arguments: String,
  extras: Map<String, Value>,
  function_extras: Map<String, Value>,
}

fn error(message: &'static str) -> Error {
  super::invalid(message)
}

impl Chat {
  pub(super) fn push(&mut self, frame: &Value) -> Result<(), Error> {
    let frame = frame
      .as_object()
      .ok_or_else(|| error("native Chat stream record is not an object"))?;
    for (key, value) in frame {
      match key.as_str() {
        "choices" | "object" => {}
        "usage" => {
          if !value.is_null() {
            let usage = value
              .as_object()
              .ok_or_else(|| error("native Chat usage is not an object"))?;
            let destination = self.metadata.entry("usage").or_insert_with(|| json!({}));
            destination
              .as_object_mut()
              .expect("usage initialized as object")
              .extend(usage.clone());
          }
        }
        _ => preserve(&mut self.metadata, key, value)?,
      }
    }
    let choices = frame
      .get("choices")
      .and_then(Value::as_array)
      .ok_or_else(|| error("native Chat stream record has no choices array"))?;
    if choices.len() > 1 {
      return Err(error("native continuation cannot reconstruct multiple Chat choices"));
    }
    for choice in choices {
      let choice = choice
        .as_object()
        .ok_or_else(|| error("native Chat choice is not an object"))?;
      if choice.get("index").is_some_and(|index| index.as_u64() != Some(0)) {
        return Err(error("native continuation requires Chat choice zero"));
      }
      self.seen_choice = true;
      let delta = choice
        .get("delta")
        .and_then(Value::as_object)
        .ok_or_else(|| error("native Chat stream choice has no delta object"))?;
      if self.finish_reason.is_some() && delta.values().any(|value| !value.is_null()) {
        return Err(error("native Chat content arrived after its finish reason"));
      }
      self.push_delta(delta)?;
      if let Some(reason) = choice.get("finish_reason").filter(|reason| !reason.is_null()) {
        let reason = reason
          .as_str()
          .ok_or_else(|| error("native Chat finish reason is not a string"))?;
        if self.finish_reason.as_deref().is_some_and(|current| current != reason) {
          return Err(error("native Chat finish reason changed"));
        }
        self.finish_reason = Some(reason.into());
      }
      for (key, value) in choice {
        match key.as_str() {
          "index" | "delta" | "finish_reason" => {}
          "logprobs" if !value.is_null() => self.push_logprobs(value)?,
          _ => preserve(&mut self.choice_metadata, key, value)?,
        }
      }
    }
    Ok(())
  }

  fn push_delta(&mut self, delta: &Map<String, Value>) -> Result<(), Error> {
    for (key, value) in delta {
      if value.is_null() {
        continue;
      }
      match key.as_str() {
        "role" if value == "assistant" => {
          self.message.insert(key.clone(), value.clone());
        }
        "content" | "reasoning_content" | "reasoning" | "refusal" => {
          let text = value
            .as_str()
            .ok_or_else(|| error("native Chat text delta is not a string"))?;
          let destination = self
            .message
            .entry(key.clone())
            .or_insert_with(|| Value::String(String::new()));
          let Value::String(destination) = destination else {
            return Err(error("native Chat text field changed shape"));
          };
          destination.push_str(text);
        }
        "tool_calls" => self.push_tools(value)?,
        // An opaque array can be retained exactly when supplied as one native
        // value. Conflicting later fragments need a provider-specific codec.
        "reasoning_details" if value.is_array() => preserve(&mut self.message, key, value)?,
        _ => return Err(error("native Chat delta has an unsupported field or shape")),
      }
    }
    Ok(())
  }

  fn push_tools(&mut self, value: &Value) -> Result<(), Error> {
    for call in value
      .as_array()
      .ok_or_else(|| error("native Chat tool deltas are not an array"))?
    {
      let call = call
        .as_object()
        .ok_or_else(|| error("native Chat tool delta is not an object"))?;
      let source = call
        .get("index")
        .and_then(Value::as_u64)
        .ok_or_else(|| error("native Chat tool delta has no index"))?;
      let index = if let Some(index) = self.call_indices.get(&source) {
        *index
      } else {
        let index = self.calls.len();
        self.calls.push(ToolCall::default());
        self.call_indices.insert(source, index);
        index
      };
      let destination = &mut self.calls[index];
      for (key, value) in call {
        if value.is_null() {
          continue;
        }
        match key.as_str() {
          "index" => {}
          "id" => {
            let id = value.as_str().ok_or_else(|| error("native tool id is not a string"))?;
            if !destination.id.is_empty() && destination.id != id {
              return Err(error("native tool id changed across stream fragments"));
            }
            destination.id = id.into();
          }
          "type" => {
            if value != "function" {
              return Err(error("native Chat tool type is unsupported"));
            }
            destination.kind = Some("function".into());
          }
          "function" => {
            let function = value
              .as_object()
              .ok_or_else(|| error("native Chat function delta is not an object"))?;
            for (key, value) in function {
              if value.is_null() {
                continue;
              }
              match key.as_str() {
                "name" => destination.name.push_str(
                  value
                    .as_str()
                    .ok_or_else(|| error("native tool name is not a string"))?,
                ),
                "arguments" => destination.arguments.push_str(
                  value
                    .as_str()
                    .ok_or_else(|| error("native tool arguments delta is not a string"))?,
                ),
                _ => preserve(&mut destination.function_extras, key, value)?,
              }
            }
          }
          _ => preserve(&mut destination.extras, key, value)?,
        }
      }
    }
    Ok(())
  }

  fn push_logprobs(&mut self, value: &Value) -> Result<(), Error> {
    let fields = value
      .as_object()
      .ok_or_else(|| error("native Chat logprobs are not an object"))?;
    let destination = self.choice_metadata.entry("logprobs").or_insert_with(|| json!({}));
    if destination.is_null() {
      *destination = json!({});
    }
    let destination = destination
      .as_object_mut()
      .ok_or_else(|| error("native Chat logprobs changed shape"))?;
    for (key, value) in fields {
      if value.is_null() {
        continue;
      }
      match key.as_str() {
        "content" | "refusal" => {
          let values = value
            .as_array()
            .ok_or_else(|| error("native Chat logprob values are not an array"))?;
          let output = destination.entry(key.clone()).or_insert_with(|| json!([]));
          output
            .as_array_mut()
            .expect("logprob output initialized as array")
            .extend(values.clone());
        }
        _ => preserve(destination, key, value)?,
      }
    }
    Ok(())
  }

  pub(super) fn finish(mut self) -> Result<Value, Error> {
    if !self.seen_choice || !matches!(self.finish_reason.as_deref(), Some("stop" | "tool_calls")) {
      return Err(error("native Chat stream has no complete assistant choice"));
    }
    self.message.entry("role").or_insert_with(|| json!("assistant"));
    self.message.entry("content").or_insert(Value::Null);
    if !self.calls.is_empty() {
      let mut calls = Vec::with_capacity(self.calls.len());
      for mut call in self.calls {
        if call.id.is_empty() || call.name.is_empty() || call.kind.is_none() {
          return Err(error("native Chat tool call metadata is incomplete"));
        }
        if call.arguments.trim().is_empty() {
          call.arguments = "{}".into();
        }
        let arguments: Value =
          serde_json::from_str(&call.arguments).map_err(|_| error("native Chat tool arguments are incomplete JSON"))?;
        if !arguments.is_object() {
          return Err(error("native Chat tool arguments are not an object"));
        }
        let mut function = call.function_extras;
        function.insert("name".into(), Value::String(call.name));
        function.insert("arguments".into(), Value::String(call.arguments));
        let mut value = call.extras;
        value.insert("id".into(), Value::String(call.id));
        value.insert("type".into(), json!("function"));
        value.insert("function".into(), Value::Object(function));
        calls.push(Value::Object(value));
      }
      self.message.insert("tool_calls".into(), Value::Array(calls));
    }
    self.choice_metadata.insert("index".into(), json!(0));
    self
      .choice_metadata
      .insert("finish_reason".into(), json!(self.finish_reason));
    self
      .choice_metadata
      .insert("message".into(), Value::Object(self.message));
    self.metadata.insert("object".into(), json!("chat.completion"));
    self.metadata.insert("choices".into(), json!([self.choice_metadata]));
    Ok(Value::Object(self.metadata))
  }
}

fn preserve(destination: &mut Map<String, Value>, key: &str, value: &Value) -> Result<(), Error> {
  if value.is_null() {
    return Ok(());
  }
  if destination.get(key).is_some_and(|previous| previous != value) {
    return Err(error("native Chat opaque metadata changed across stream fragments"));
  }
  destination.insert(key.into(), value.clone());
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn reconstructs_sparse_parallel_tools_with_native_reasoning_and_late_usage() {
    let mut chat = Chat::default();
    let reasoning =
      json!([{"type": "reasoning.encrypted", "id": "reasoning-1", "data": "opaque", "signature": "signature"}]);
    for frame in [
      json!({"id": "response-1", "object": "chat.completion.chunk", "model": "model", "created": 123,
        "system_fingerprint": "native-fingerprint", "provider_extension": {"retain": true},
        "choices": [{"index": 0, "delta": {"role": "assistant", "reasoning_content": "think ", "reasoning_details": reasoning}, "finish_reason": null}]}),
      json!({"choices": [{"index": 0, "delta": {"reasoning_content": "carefully", "content": "checking", "tool_calls": [
        {"index": 9, "id": "call-a", "type": "function", "function": {"name": "look", "arguments": "{ \"q\" : "}},
        {"index": 3, "id": "call-b", "type": "function", "function": {"name": "sum", "arguments": "{\"a\":"}}
      ]}, "finish_reason": null}]}),
      json!({"choices": [{"index": 0, "delta": {"tool_calls": [
        {"index": 3, "function": {"arguments": "2}"}},
        {"index": 9, "function": {"name": "up", "arguments": "1 }"}}
      ]}, "finish_reason": "tool_calls"}]}),
      json!({"choices": [], "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10}}),
    ] {
      chat.push(&frame).unwrap();
    }
    let response = chat.finish().unwrap();
    assert_eq!(response["provider_extension"], json!({"retain": true}));
    assert_eq!(response["system_fingerprint"], "native-fingerprint");
    assert_eq!(response["object"], "chat.completion");
    assert_eq!(response["usage"]["total_tokens"], 10);
    let message = &response["choices"][0]["message"];
    assert_eq!(message["reasoning_content"], "think carefully");
    assert_eq!(message["reasoning_details"], reasoning);
    assert_eq!(message["content"], "checking");
    assert_eq!(message["tool_calls"].as_array().unwrap().len(), 2);
    assert_eq!(message["tool_calls"][0]["id"], "call-a");
    assert_eq!(
      message["tool_calls"][0]["function"],
      json!({"name": "lookup", "arguments": "{ \"q\" : 1 }"})
    );
    assert_eq!(message["tool_calls"][1]["id"], "call-b");
    assert_eq!(message["tool_calls"][1]["function"]["arguments"], "{\"a\":2}");
  }

  #[test]
  fn unsupported_or_partial_streams_fail_without_discarding_native_fields() {
    for frame in [
      json!({"choices": [{"index": 1, "delta": {"content": "choice one"}}]}),
      json!({"choices": [{"index": 0, "delta": {"audio": {"data": "opaque audio fragment"}}}]}),
      json!({"choices": [{"index": 0, "delta": {"content": [{"type": "text", "text": "structured"}]}}]}),
    ] {
      assert!(Chat::default().push(&frame).is_err());
    }
    let mut partial = Chat::default();
    partial
      .push(&json!({"choices": [{"delta": {"tool_calls": [
      {"index": 7, "id": "call", "type": "function", "function": {"name": "lookup", "arguments": "{"}}
    ]}, "finish_reason": "tool_calls"}]}))
      .unwrap();
    assert!(partial.finish().is_err());
    let mut opaque = Chat::default();
    opaque
      .push(&json!({"choices": [{"delta": {"reasoning_details": [{"data": "first"}]}}]}))
      .unwrap();
    assert!(opaque
      .push(&json!({"choices": [{"delta": {"reasoning_details": [{"data": "next"}]}}]}))
      .is_err());
    let mut incomplete = Chat::default();
    incomplete
      .push(&json!({"choices": [{"delta": {"content": "partial"}, "finish_reason": "length"}]}))
      .unwrap();
    assert!(incomplete.finish().is_err());
  }

  #[test]
  fn empty_tool_argument_fragments_are_complete_zero_argument_calls() {
    for arguments in ["", " \t\n"] {
      let mut chat = Chat::default();
      chat
        .push(&json!({"choices": [{"delta": {"tool_calls": [
        {"index": 4, "id": "call", "type": "function", "function": {"name": "clock", "arguments": arguments}}
      ]}, "finish_reason": "tool_calls"}]}))
        .unwrap();
      let output = chat.finish().unwrap();
      assert_eq!(
        output["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
        "{}"
      );
    }
  }
}
