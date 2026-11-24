use super::{invalid, Error};
use alloc::{collections::BTreeMap, string::String};
use serde_json::Value;

/// Indexed blocks stay native. Only protocol deltas with known merge
/// semantics may advance a continuation checkpoint.
#[derive(Default)]
pub(super) struct Messages {
  message: Option<Value>,
  blocks: BTreeMap<u64, Block>,
}

struct Block {
  value: Value,
  json_fragments: Option<String>,
  closed: bool,
}

impl Block {
  fn delta(&mut self, delta: &Value) -> Result<(), Error> {
    if self.closed {
      return Err(invalid("delta for a closed Messages block"));
    }
    let key = match (self.value["type"].as_str(), delta["type"].as_str()) {
      (Some("text"), Some("text_delta")) => "text",
      (Some("thinking"), Some("thinking_delta")) => "thinking",
      (Some("thinking"), Some("signature_delta")) => "signature",
      (Some("tool_use"), Some("input_json_delta")) => {
        let part = delta["partial_json"]
          .as_str()
          .ok_or_else(|| invalid("invalid tool input fragment"))?;
        self.json_fragments.get_or_insert_with(String::new).push_str(part);
        return Ok(());
      }
      (Some("text"), Some("citations_delta")) => {
        let citation = delta
          .get("citation")
          .filter(|value| value.is_object())
          .ok_or_else(|| invalid("invalid citation delta"))?;
        if self.value["citations"].is_null() {
          self.value["citations"] = Value::Array(alloc::vec![]);
        }
        self.value["citations"]
          .as_array_mut()
          .ok_or_else(|| invalid("invalid text citations"))?
          .push(citation.clone());
        return Ok(());
      }
      _ => return Err(invalid("unsupported native Messages block delta")),
    };
    let part = delta[key]
      .as_str()
      .ok_or_else(|| invalid("invalid string delta in Messages block"))?;
    if self.value.get(key).is_none() {
      self.value[key] = Value::String(String::new());
    }
    let Value::String(text) = &mut self.value[key] else {
      return Err(invalid("invalid Messages block field"));
    };
    text.push_str(part);
    Ok(())
  }

  fn close(&mut self) -> Result<(), Error> {
    if self.closed {
      return Err(invalid("repeated Messages block stop"));
    }
    if let Some(parts) = self.json_fragments.take().filter(|s| !s.trim().is_empty()) {
      let input: Value = serde_json::from_str(&parts).map_err(|_| invalid("incomplete native tool input"))?;
      if !input.is_object() || self.value["input"].as_object().is_some_and(|object| !object.is_empty()) {
        return Err(invalid("ambiguous native tool input"));
      }
      self.value["input"] = input;
    }
    self.closed = true;
    Ok(())
  }
}

impl Messages {
  pub(super) fn push(&mut self, frame: Value) -> Result<(), Error> {
    let index = || {
      frame["index"]
        .as_u64()
        .ok_or_else(|| invalid("missing Messages block index"))
    };
    match frame["type"].as_str() {
      Some("ping") => {}
      Some("message_start") => {
        let msg = frame
          .get("message")
          .filter(|v| v["content"].as_array().is_some_and(|c| c.is_empty()))
          .ok_or_else(|| invalid("invalid Messages stream start"))?;
        if self.message.replace(msg.clone()).is_some() {
          return Err(invalid("repeated Messages stream start"));
        }
      }
      _ if self.message.is_none() => return Err(invalid("Messages event before message_start")),
      Some("content_block_start") => {
        let value = frame
          .get("content_block")
          .filter(|v| v["type"].is_string())
          .ok_or_else(|| invalid("missing native Messages block"))?;
        if self
          .blocks
          .insert(
            index()?,
            Block {
              value: value.clone(),
              json_fragments: None,
              closed: false,
            },
          )
          .is_some()
        {
          return Err(invalid("repeated Messages block start"));
        }
      }
      Some("content_block_delta" | "content_block_stop") => {
        let block = self
          .blocks
          .get_mut(&index()?)
          .ok_or_else(|| invalid("Messages block was never opened"))?;
        if frame["type"] == "content_block_stop" {
          block.close()?;
        } else {
          block.delta(&frame["delta"])?;
        }
      }
      Some("message_delta") => {
        let message = self.message.as_mut().expect("message started");
        let delta = frame["delta"]
          .as_object()
          .ok_or_else(|| invalid("invalid native message_delta"))?;
        for (key, value) in delta {
          if key == "content" {
            return Err(invalid("message_delta contains conflicting content"));
          }
          message[key] = value.clone();
        }
        if let Some(usage) = frame["usage"].as_object() {
          if message["usage"].is_null() {
            message["usage"] = serde_json::json!({});
          }
          let previous = message["usage"]
            .as_object_mut()
            .ok_or_else(|| invalid("invalid Messages usage"))?;
          for (key, value) in usage {
            previous.insert(key.clone(), value.clone());
          }
        }
      }
      Some("message_stop") => {}
      _ => return Err(invalid("unrecognized Messages stream event")),
    }
    Ok(())
  }

  pub(super) fn finish(self) -> Result<Value, Error> {
    let mut message = self.message.ok_or_else(|| invalid("missing Messages stream start"))?;
    let content = message["content"]
      .as_array_mut()
      .ok_or_else(|| invalid("invalid Messages content"))?;
    for (index, block) in self.blocks {
      if !block.closed || index != content.len() as u64 {
        return Err(invalid("incomplete native Messages content"));
      }
      content.push(block.value);
    }
    Ok(message)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn empty_input_fragments_keep_the_original_zero_argument_object() {
    let mut block = Block {
      value: json!({"type":"tool_use","id":"c","name":"list","input":{}}),
      json_fragments: None,
      closed: false,
    };
    for fragment in ["", " "] {
      block
        .delta(&json!({"type":"input_json_delta","partial_json":fragment}))
        .unwrap();
    }
    block.close().unwrap();
    assert_eq!(block.value["input"], json!({}));
  }
}
