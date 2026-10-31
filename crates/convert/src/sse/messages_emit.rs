//! Stateful Messages SSE emission for cross-endpoint conversion.
//!
//! Content blocks are serialized in first-appearance order. `IrDelta` has no
//! per-tool completion signal, so later blocks wait while a tool is streaming;
//! its remaining arguments can arrive without reopening a closed block.

use super::SseEvent;
use crate::ir::{IrDelta, Usage};
use alloc::collections::BTreeMap;
use alloc::{string::String, vec::Vec};
use serde_json::{json, Value};

enum BlockKind {
  Text,
  Thinking,
  Tool { id: Option<String>, name: Option<String> },
}

struct Block {
  kind: BlockKind,
  pending: String,
  started: bool,
  arguments_seen: bool,
}

/// Retain one emitter for the lifetime of a converted Messages stream.
pub struct MessagesEmitter {
  id: String,
  model: String,
  blocks: Vec<Block>,
  tools: BTreeMap<usize, usize>,
  active: Option<usize>,
  started: bool,
  finished: bool,
  usage: Option<Usage>,
  finish_reason: Option<String>,
}

impl MessagesEmitter {
  pub fn new(id: String, model: String) -> Self {
    Self {
      id,
      model,
      blocks: Vec::new(),
      tools: BTreeMap::new(),
      active: None,
      started: false,
      finished: false,
      usage: None,
      finish_reason: None,
    }
  }

  pub fn update_id(&mut self, id: String) {
    self.id = id;
  }

  pub fn update_model(&mut self, model: String) {
    self.model = model;
  }

  pub fn emit(&mut self, deltas: &[IrDelta]) -> Vec<SseEvent> {
    if self.finished || deltas.is_empty() {
      return Vec::new();
    }
    // Include initial usage in message_start. Later cumulative updates are
    // delivered after all blocks close, including updates after Finish.
    for delta in deltas {
      if let IrDelta::Usage(usage) = delta {
        self.usage.get_or_insert_with(Usage::default).merge(usage.clone());
      }
    }
    let mut out = Vec::new();
    self.start(&mut out);
    for delta in deltas {
      match delta {
        IrDelta::Text(text) | IrDelta::Reasoning(text) if text.is_empty() => {}
        IrDelta::Text(text) | IrDelta::Reasoning(text) => {
          let thinking = matches!(delta, IrDelta::Reasoning(_));
          let same_kind = self.blocks.last().is_some_and(|block| {
            matches!(
              (&block.kind, thinking),
              (BlockKind::Text, false) | (BlockKind::Thinking, true)
            )
          });
          let index = if same_kind {
            self.blocks.len() - 1
          } else {
            self.push_block(if thinking { BlockKind::Thinking } else { BlockKind::Text })
          };
          self.blocks[index].pending.push_str(text);
          self.flush_content(index, &mut out);
        }
        IrDelta::ToolCall {
          index,
          id,
          name,
          arguments_delta,
        } => {
          let block_index = if let Some(index) = self.tools.get(index) {
            *index
          } else {
            let block_index = self.push_block(BlockKind::Tool { id: None, name: None });
            self.tools.insert(*index, block_index);
            block_index
          };
          let block = &mut self.blocks[block_index];
          if !block.started {
            if let BlockKind::Tool {
              id: current_id,
              name: current_name,
            } = &mut block.kind
            {
              if id.as_ref().is_some_and(|id| !id.is_empty()) {
                *current_id = id.clone();
              }
              if name.as_ref().is_some_and(|name| !name.is_empty()) {
                *current_name = name.clone();
              }
            }
          }
          block.pending.push_str(arguments_delta);
          block.arguments_seen |= !arguments_delta.trim().is_empty();
          self.flush_content(block_index, &mut out);
        }
        IrDelta::Usage(_) => {}
        IrDelta::Finish(reason) => self.finish_reason = reason.clone(),
      }
    }
    out
  }

  pub fn finish(&mut self) -> Vec<SseEvent> {
    if self.finished {
      return Vec::new();
    }
    self.finished = true;
    let mut out = Vec::new();
    self.start(&mut out);
    if let Some(active) = self.active.take() {
      for index in active..self.blocks.len() {
        self.flush_block(index, true, &mut out);
        self.stop_block(index, &mut out);
      }
    }
    let reason = self
      .finish_reason
      .as_deref()
      .or_else(|| (!self.tools.is_empty()).then_some("tool_calls"));
    out.push(SseEvent::json(
      Some("message_delta"),
      json!({
        "type": "message_delta",
        "delta": {"stop_reason": crate::value::messages::finish_reason_to_messages(reason), "stop_sequence": null},
        "usage": self.usage_value(false),
      }),
    ));
    out.push(SseEvent::json(Some("message_stop"), json!({"type": "message_stop"})));
    out
  }

  fn push_block(&mut self, kind: BlockKind) -> usize {
    let index = self.blocks.len();
    self.blocks.push(Block {
      kind,
      pending: String::new(),
      started: false,
      arguments_seen: false,
    });
    index
  }

  fn start(&mut self, out: &mut Vec<SseEvent>) {
    if self.started {
      return;
    }
    self.started = true;
    out.push(SseEvent::json(
      Some("message_start"),
      json!({
        "type": "message_start",
        "message": {
          "id": self.id, "type": "message", "role": "assistant", "model": self.model,
          "content": [], "stop_reason": null, "stop_sequence": null, "usage": self.usage_value(true),
        },
      }),
    ));
  }

  fn usage_value(&self, initial: bool) -> Value {
    let mut value = serde_json::Map::new();
    if let Some(usage) = &self.usage {
      value.extend(usage.extras.clone());
      if initial || usage.input_tokens.is_some() {
        value.insert("input_tokens".into(), json!(usage.input_tokens.unwrap_or(0)));
      }
      value.insert("output_tokens".into(), json!(usage.output_tokens.unwrap_or(0)));
    } else {
      if initial {
        value.insert("input_tokens".into(), json!(0));
      }
      value.insert("output_tokens".into(), json!(0));
    }
    Value::Object(value)
  }

  fn flush_content(&mut self, index: usize, out: &mut Vec<SseEvent>) {
    let active = *self.active.get_or_insert(0);
    if active != index && !matches!(self.blocks[active].kind, BlockKind::Tool { .. }) {
      self.stop_block(active, out);
      self.active = Some(active + 1);
    }
    self.flush_block(self.active.unwrap(), false, out);
  }

  fn flush_block(&mut self, index: usize, finishing: bool, out: &mut Vec<SseEvent>) {
    let block = &mut self.blocks[index];
    if !block.started {
      let content = match &block.kind {
        BlockKind::Text => json!({"type": "text", "text": ""}),
        BlockKind::Thinking => json!({"type": "thinking", "thinking": ""}),
        BlockKind::Tool { id, name } => {
          if !finishing && (id.is_none() || name.is_none()) {
            return;
          }
          json!({
            "type": "tool_use", "id": id.clone().unwrap_or_else(|| format!("toolu_converted_{index}")),
            "name": name.as_deref().unwrap_or_default(), "input": {},
          })
        }
      };
      out.push(SseEvent::json(
        Some("content_block_start"),
        json!({
          "type": "content_block_start", "index": index, "content_block": content,
        }),
      ));
      block.started = true;
    }
    if !block.pending.is_empty() {
      let pending = core::mem::take(&mut block.pending);
      let delta = match &block.kind {
        BlockKind::Text => json!({"type": "text_delta", "text": pending}),
        BlockKind::Thinking => json!({"type": "thinking_delta", "thinking": pending}),
        BlockKind::Tool { .. } => json!({"type": "input_json_delta", "partial_json": pending}),
      };
      out.push(SseEvent::json(
        Some("content_block_delta"),
        json!({
          "type": "content_block_delta", "index": index, "delta": delta,
        }),
      ));
    }
  }

  fn stop_block(&self, index: usize, out: &mut Vec<SseEvent>) {
    let block = &self.blocks[index];
    if matches!(block.kind, BlockKind::Tool { .. }) && !block.arguments_seen {
      out.push(SseEvent::json(
        Some("content_block_delta"),
        json!({
          "type": "content_block_delta", "index": index,
          "delta": {"type": "input_json_delta", "partial_json": "{}"},
        }),
      ));
    }
    out.push(SseEvent::json(
      Some("content_block_stop"),
      json!({"type": "content_block_stop", "index": index}),
    ));
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::provider::Endpoint;
  use crate::sse::SseAccumulator;

  fn tool(index: usize, id: Option<&str>, name: Option<&str>, arguments: &str) -> IrDelta {
    IrDelta::ToolCall {
      index,
      id: id.map(String::from),
      name: name.map(String::from),
      arguments_delta: arguments.into(),
    }
  }

  fn kinds(events: &[SseEvent]) -> Vec<&str> {
    events.iter().map(|event| event.event.as_deref().unwrap()).collect()
  }

  #[test]
  fn tool_only_stream_has_one_tool_block_and_no_fabricated_text() {
    let mut emitter = MessagesEmitter::new("msg-1".into(), "model".into());
    let mut events = emitter.emit(&[tool(11, Some("call-1"), Some("lookup"), "{\"q\":")]);
    assert_eq!(
      kinds(&events),
      ["message_start", "content_block_start", "content_block_delta"]
    );
    events.extend(emitter.emit(&[tool(11, None, None, "\"x\"}")]));
    events.extend(emitter.finish());
    assert_eq!(
      kinds(&events),
      [
        "message_start",
        "content_block_start",
        "content_block_delta",
        "content_block_delta",
        "content_block_stop",
        "message_delta",
        "message_stop",
      ]
    );
    assert_eq!(
      events[1].json.as_ref().unwrap(),
      &json!({
        "type": "content_block_start", "index": 0,
        "content_block": {"type": "tool_use", "id": "call-1", "name": "lookup", "input": {}},
      })
    );
    assert_eq!(events[5].json.as_ref().unwrap()["delta"]["stop_reason"], "tool_use");
    assert!(emitter.finish().is_empty());
    assert!(emitter.emit(&[IrDelta::Text("late".into())]).is_empty());
  }

  #[test]
  fn late_identity_and_interleaved_arguments_keep_serialized_blocks() {
    let mut emitter = MessagesEmitter::new("msg-1".into(), "model".into());
    let mut events = emitter.emit(&[tool(7, None, None, "{\"x\":")]);
    assert_eq!(kinds(&events), ["message_start"]);
    assert!(emitter
      .emit(&[tool(19, Some("call-b"), Some("second"), "{\"y\":3}")])
      .is_empty());
    assert!(emitter
      .emit(&[IrDelta::Text("tail".into()), IrDelta::Reasoning("why".into())])
      .is_empty());
    let resumed = emitter.emit(&[tool(7, Some("call-a"), Some("first"), "2}")]);
    assert_eq!(kinds(&resumed), ["content_block_start", "content_block_delta"]);
    assert_eq!(resumed[1].json.as_ref().unwrap()["delta"]["partial_json"], "{\"x\":2}");
    events.extend(resumed);
    events.extend(emitter.finish());

    let mut active = None;
    let mut next_index = 0;
    let mut accumulator = SseAccumulator::new(Endpoint::Messages);
    for event in &events {
      let value = event.json.as_ref().unwrap();
      match event.event.as_deref().unwrap() {
        "content_block_start" => {
          assert!(active.is_none());
          assert_eq!(value["index"], next_index);
          active = Some(next_index);
          next_index += 1;
        }
        "content_block_delta" => assert_eq!(Some(value["index"].as_u64().unwrap()), active),
        "content_block_stop" => assert_eq!(Some(value["index"].as_u64().unwrap()), active.take()),
        "message_delta" | "message_stop" => assert!(active.is_none()),
        _ => {}
      }
      accumulator.push_value(value);
    }
    assert_eq!(next_index, 4);
    let response = accumulator.finish();
    assert_eq!(response.tool_calls.len(), 2);
    assert_eq!(response.tool_calls[0].id.as_deref(), Some("call-a"));
    assert_eq!(response.tool_calls[0].name, "first");
    assert_eq!(response.tool_calls[0].arguments, json!({"x": 2}));
    assert_eq!(response.tool_calls[1].id.as_deref(), Some("call-b"));
    assert_eq!(response.tool_calls[1].arguments, json!({"y": 3}));
    assert_eq!(crate::ir::text_from_parts(&response.content), "tail");
    assert_eq!(
      crate::ir::reasoning_from_parts(&response.content).as_deref(),
      Some("why")
    );
  }

  #[test]
  fn empty_arguments_and_usage_after_finish_are_preserved() {
    for arguments in ["", " \n\t"] {
      let mut emitter = MessagesEmitter::new("msg-1".into(), "model".into());
      let mut events = emitter.emit(&[
        IrDelta::Usage(Usage {
          input_tokens: Some(2),
          output_tokens: Some(0),
          ..Usage::default()
        }),
        tool(25, Some("call-1"), Some("status"), arguments),
        IrDelta::Finish(Some("length".into())),
      ]);
      assert!(emitter
        .emit(&[IrDelta::Usage(Usage {
          output_tokens: Some(8),
          ..Usage::default()
        })])
        .is_empty());
      events.extend(emitter.finish());
      let mut accumulator = SseAccumulator::new(Endpoint::Messages);
      for event in &events {
        accumulator.push_value(event.json.as_ref().unwrap());
      }
      let response = accumulator.finish();
      assert_eq!(response.tool_calls[0].arguments, json!({}));
      assert_eq!(response.usage.as_ref().unwrap().input_tokens, Some(2));
      assert_eq!(response.usage.as_ref().unwrap().output_tokens, Some(8));
      let terminal = events[events.len() - 2].json.as_ref().unwrap();
      assert_eq!(terminal["delta"]["stop_reason"], "max_tokens");
      assert_eq!(terminal["usage"], json!({"input_tokens": 2, "output_tokens": 8}));
    }
  }

  #[test]
  fn empty_stream_has_no_content_block_events() {
    let mut emitter = MessagesEmitter::new("msg-1".into(), "model".into());
    assert!(emitter.emit(&[]).is_empty());
    let mut events = emitter.emit(&[IrDelta::Text(String::new()), IrDelta::Reasoning(String::new())]);
    events.extend(emitter.finish());
    assert_eq!(kinds(&events), ["message_start", "message_delta", "message_stop"]);
    assert_eq!(events[1].json.as_ref().unwrap()["delta"]["stop_reason"], "end_turn");
  }
}
