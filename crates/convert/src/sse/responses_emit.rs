//! State machine that emits a faithful Responses-API SSE lifecycle from a
//! generic stream of `IrDelta`s.
//!
//! Text and reasoning items close when the content kind changes. Function
//! calls remain open until the response finishes because their argument deltas
//! may interleave and the IR has no per-call completion event.

use super::event::SseEvent;
use crate::ir::{IrDelta, Usage};
use alloc::{collections::BTreeMap, string::String, string::ToString, vec::Vec};
use serde_json::{json, Value};
#[cfg(feature = "std")]
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
enum OpenItem {
  Message {
    output_index: usize,
    item_id: String,
    text: String,
    part_open: bool,
  },
  FunctionCall {
    output_index: usize,
    item_id: String,
    call_id: String,
    name: String,
    arguments: String,
  },
  Reasoning {
    output_index: usize,
    item_id: String,
    summary: String,
    part_open: bool,
  },
}

impl OpenItem {
  fn output_index(&self) -> usize {
    match self {
      OpenItem::Message { output_index, .. }
      | OpenItem::FunctionCall { output_index, .. }
      | OpenItem::Reasoning { output_index, .. } => *output_index,
    }
  }

  fn item_id(&self) -> &str {
    match self {
      OpenItem::Message { item_id, .. }
      | OpenItem::FunctionCall { item_id, .. }
      | OpenItem::Reasoning { item_id, .. } => item_id,
    }
  }
}

pub struct ResponsesEmitter {
  id: String,
  model: String,
  created_at: u64,
  sequence: u64,
  next_output_index: usize,
  next_item_counter: usize,
  current: Option<OpenItem>,
  tool_calls: BTreeMap<usize, OpenItem>,
  closed: BTreeMap<usize, Value>,
  created_emitted: bool,
  in_progress_emitted: bool,
  finished: bool,
  usage: Option<Usage>,
  finish_reason: Option<String>,
}

impl ResponsesEmitter {
  pub fn new(id: String, model: String) -> Self {
    #[cfg(feature = "std")]
    let created_at = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map(|d| d.as_secs())
      .unwrap_or(0);
    #[cfg(not(feature = "std"))]
    let created_at = 0;
    Self::with_created_at(id, model, created_at)
  }

  /// Construct an emitter using an application-supplied Unix timestamp.
  pub fn with_created_at(id: String, model: String, created_at: u64) -> Self {
    Self {
      id,
      model,
      created_at,
      sequence: 0,
      next_output_index: 0,
      next_item_counter: 0,
      current: None,
      tool_calls: BTreeMap::new(),
      closed: BTreeMap::new(),
      created_emitted: false,
      in_progress_emitted: false,
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

  fn next_seq(&mut self) -> u64 {
    let n = self.sequence;
    self.sequence += 1;
    n
  }

  fn synth_item_id(&mut self, prefix: &str) -> String {
    self.next_item_counter += 1;
    format!("{prefix}_{}", self.next_item_counter)
  }

  fn ensure_started(&mut self, out: &mut Vec<SseEvent>) {
    if !self.created_emitted {
      let seq = self.next_seq();
      out.push(SseEvent::json(
        Some("response.created"),
        json!({
          "type": "response.created",
          "sequence_number": seq,
          "response": self.response_snapshot("in_progress", false),
        }),
      ));
      self.created_emitted = true;
    }
    if !self.in_progress_emitted {
      let seq = self.next_seq();
      out.push(SseEvent::json(
        Some("response.in_progress"),
        json!({
          "type": "response.in_progress",
          "sequence_number": seq,
          "response": self.response_snapshot("in_progress", false),
        }),
      ));
      self.in_progress_emitted = true;
    }
  }

  fn response_snapshot(&self, status: &str, completed: bool) -> Value {
    let mut response = json!({
      "id": self.id,
      "object": "response",
      "created_at": self.created_at,
      "status": status,
      "model": self.model,
      "output": if completed { Value::Array(self.closed.values().cloned().collect()) } else { Value::Array(Vec::new()) },
      "usage": self.usage.as_ref().map(crate::ir::usage_to_io).unwrap_or(Value::Null),
    });
    if completed {
      if let Some(obj) = response.as_object_mut() {
        obj.insert("completed_at".into(), json!(self.created_at));
        let (_, incomplete_reason) = crate::ir::responses_completion_status(self.finish_reason.as_deref());
        if let Some(reason) = incomplete_reason {
          obj.insert("incomplete_details".into(), json!({"reason": reason}));
        }
      }
    }
    response
  }

  pub fn emit(&mut self, deltas: &[IrDelta]) -> Vec<SseEvent> {
    if self.finished || deltas.is_empty() {
      return Vec::new();
    }
    let mut out = Vec::new();
    self.ensure_started(&mut out);
    for delta in deltas {
      match delta {
        IrDelta::Text(text) => self.handle_text(text, &mut out),
        IrDelta::Reasoning(text) => self.handle_reasoning(text, &mut out),
        IrDelta::ToolCall {
          index,
          id,
          name,
          arguments_delta,
        } => {
          self.handle_tool_call(*index, id.clone(), name.clone(), arguments_delta, &mut out);
        }
        IrDelta::Usage(usage) => {
          if let Some(current) = &mut self.usage {
            current.merge(usage.clone());
          } else {
            self.usage = Some(usage.clone());
          }
        }
        IrDelta::Finish(reason) => {
          self.finish_reason = reason.clone();
        }
      }
    }
    out
  }

  fn close_current(&mut self, out: &mut Vec<SseEvent>) {
    if let Some(item) = self.current.take() {
      self.close_item(item, out);
    }
  }

  fn handle_text(&mut self, text: &str, out: &mut Vec<SseEvent>) {
    // Suppress no-op empty deltas. If no message item is open we don't open
    // one for an empty string; if one is already open we drop the empty
    // delta event silently.
    if text.is_empty() {
      return;
    }
    if !matches!(self.current, Some(OpenItem::Message { .. })) {
      self.close_current(out);
      let output_index = self.next_output_index;
      self.next_output_index += 1;
      let item_id = self.synth_item_id("msg");
      let seq = self.next_seq();
      out.push(SseEvent::json(
        Some("response.output_item.added"),
        json!({
          "type": "response.output_item.added",
          "sequence_number": seq,
          "output_index": output_index,
          "item": {
            "id": item_id,
            "type": "message",
            "status": "in_progress",
            "role": "assistant",
            "content": [],
          }
        }),
      ));
      let seq = self.next_seq();
      out.push(SseEvent::json(
        Some("response.content_part.added"),
        json!({
          "type": "response.content_part.added",
          "sequence_number": seq,
          "item_id": item_id,
          "output_index": output_index,
          "content_index": 0,
          "part": {"type": "output_text", "text": "", "annotations": []},
        }),
      ));
      self.current = Some(OpenItem::Message {
        output_index,
        item_id,
        text: String::new(),
        part_open: true,
      });
    }
    let (output_index, item_id) = if let Some(OpenItem::Message {
      output_index,
      item_id,
      text: buf,
      ..
    }) = &mut self.current
    {
      buf.push_str(text);
      (*output_index, item_id.clone())
    } else {
      unreachable!()
    };
    let seq = self.next_seq();
    out.push(SseEvent::json(
      Some("response.output_text.delta"),
      json!({
        "type": "response.output_text.delta",
        "sequence_number": seq,
        "item_id": item_id,
        "output_index": output_index,
        "content_index": 0,
        "delta": text,
      }),
    ));
  }

  fn handle_reasoning(&mut self, text: &str, out: &mut Vec<SseEvent>) {
    if text.is_empty() {
      return;
    }
    if !matches!(self.current, Some(OpenItem::Reasoning { .. })) {
      self.close_current(out);
      let output_index = self.next_output_index;
      self.next_output_index += 1;
      let item_id = self.synth_item_id("rs");
      let seq = self.next_seq();
      out.push(SseEvent::json(
        Some("response.output_item.added"),
        json!({
          "type": "response.output_item.added",
          "sequence_number": seq,
          "output_index": output_index,
          "item": {
            "id": item_id,
            "type": "reasoning",
            "status": "in_progress",
            "content": [],
            "summary": [],
          }
        }),
      ));
      self.current = Some(OpenItem::Reasoning {
        output_index,
        item_id,
        summary: String::new(),
        part_open: true,
      });
    }
    let (output_index, item_id) = if let Some(OpenItem::Reasoning {
      output_index,
      item_id,
      summary,
      ..
    }) = &mut self.current
    {
      summary.push_str(text);
      (*output_index, item_id.clone())
    } else {
      unreachable!()
    };
    let seq = self.next_seq();
    out.push(SseEvent::json(
      Some("response.reasoning_text.delta"),
      json!({
        "type": "response.reasoning_text.delta",
        "sequence_number": seq,
        "item_id": item_id,
        "output_index": output_index,
        "content_index": 0,
        "delta": text,
      }),
    ));
  }

  fn handle_tool_call(
    &mut self,
    source_index: usize,
    id_hint: Option<String>,
    name_hint: Option<String>,
    args_delta: &str,
    out: &mut Vec<SseEvent>,
  ) {
    self.close_current(out);
    if !self.tool_calls.contains_key(&source_index) {
      let output_index = self.next_output_index;
      self.next_output_index += 1;
      let item_id = self.synth_item_id("fc");
      let call_id = id_hint.clone().unwrap_or_else(|| item_id.clone());
      let name = name_hint.clone().unwrap_or_default();
      let seq = self.next_seq();
      out.push(SseEvent::json(
        Some("response.output_item.added"),
        json!({
          "type": "response.output_item.added",
          "sequence_number": seq,
          "output_index": output_index,
          "item": {
            "id": item_id,
            "type": "function_call",
            "status": "in_progress",
            "call_id": call_id,
            "name": name,
            "arguments": "",
          }
        }),
      ));
      self.tool_calls.insert(
        source_index,
        OpenItem::FunctionCall {
          output_index,
          item_id,
          call_id,
          name,
          arguments: String::new(),
        },
      );
    }
    let (output_index, item_id) = if let Some(OpenItem::FunctionCall {
      output_index,
      item_id,
      name,
      call_id,
      arguments,
      ..
    }) = self.tool_calls.get_mut(&source_index)
    {
      arguments.push_str(args_delta);
      if let Some(n) = name_hint {
        if name.is_empty() {
          *name = n;
        }
      }
      if let Some(id) = id_hint {
        if call_id == item_id.as_str() || call_id.is_empty() {
          *call_id = id;
        }
      }
      (*output_index, item_id.clone())
    } else {
      unreachable!()
    };
    if !args_delta.is_empty() {
      let seq = self.next_seq();
      out.push(SseEvent::json(
        Some("response.function_call_arguments.delta"),
        json!({
          "type": "response.function_call_arguments.delta",
          "sequence_number": seq,
          "item_id": item_id,
          "output_index": output_index,
          "delta": args_delta,
        }),
      ));
    }
  }

  pub fn finish(&mut self) -> Vec<SseEvent> {
    if self.finished {
      return Vec::new();
    }
    self.finished = true;
    let mut out = Vec::new();
    self.ensure_started(&mut out);
    self.close_current(&mut out);
    let mut tool_calls: Vec<_> = core::mem::take(&mut self.tool_calls).into_values().collect();
    tool_calls.sort_by_key(OpenItem::output_index);
    for call in tool_calls {
      self.close_item(call, &mut out);
    }
    let seq = self.next_seq();
    let (status, _) = crate::ir::responses_completion_status(self.finish_reason.as_deref());
    let event_type = match status {
      "incomplete" => "response.incomplete",
      "failed" => "response.failed",
      "cancelled" => "response.cancelled",
      _ => "response.completed",
    };
    out.push(SseEvent::json(
      Some(event_type),
      json!({
        "type": event_type,
        "sequence_number": seq,
        "response": self.response_snapshot(status, true),
      }),
    ));
    out
  }

  fn close_item(&mut self, item: OpenItem, out: &mut Vec<SseEvent>) {
    let output_index = item.output_index();
    let item_id = item.item_id().to_string();
    match item {
      OpenItem::Message { text, part_open, .. } => {
        if part_open {
          let seq = self.next_seq();
          out.push(SseEvent::json(
            Some("response.output_text.done"),
            json!({
              "type": "response.output_text.done",
              "sequence_number": seq,
              "item_id": item_id,
              "output_index": output_index,
              "content_index": 0,
              "text": text,
            }),
          ));
          let seq = self.next_seq();
          out.push(SseEvent::json(
            Some("response.content_part.done"),
            json!({
              "type": "response.content_part.done",
              "sequence_number": seq,
              "item_id": item_id,
              "output_index": output_index,
              "content_index": 0,
              "part": {"type": "output_text", "text": text, "annotations": []},
            }),
          ));
        }
        let final_item = json!({
          "id": item_id,
          "type": "message",
          "status": "completed",
          "role": "assistant",
          "content": [{"type": "output_text", "text": text, "annotations": []}],
        });
        let seq = self.next_seq();
        out.push(SseEvent::json(
          Some("response.output_item.done"),
          json!({
            "type": "response.output_item.done",
            "sequence_number": seq,
            "output_index": output_index,
            "item": final_item.clone(),
          }),
        ));
        self.closed.insert(output_index, final_item);
      }
      OpenItem::FunctionCall {
        call_id,
        name,
        arguments,
        ..
      } => {
        let arguments = if arguments.trim().is_empty() {
          "{}".to_string()
        } else {
          arguments
        };
        let seq = self.next_seq();
        out.push(SseEvent::json(
          Some("response.function_call_arguments.done"),
          json!({
            "type": "response.function_call_arguments.done",
            "sequence_number": seq,
            "item_id": item_id,
            "output_index": output_index,
            "arguments": arguments,
          }),
        ));
        let final_item = json!({
          "id": item_id,
          "type": "function_call",
          "status": "completed",
          "call_id": call_id,
          "name": name,
          "arguments": arguments,
        });
        let seq = self.next_seq();
        out.push(SseEvent::json(
          Some("response.output_item.done"),
          json!({
            "type": "response.output_item.done",
            "sequence_number": seq,
            "output_index": output_index,
            "item": final_item.clone(),
          }),
        ));
        self.closed.insert(output_index, final_item);
      }
      OpenItem::Reasoning {
        summary: text,
        part_open,
        ..
      } => {
        if part_open {
          let seq = self.next_seq();
          out.push(SseEvent::json(
            Some("response.reasoning_text.done"),
            json!({
              "type": "response.reasoning_text.done",
              "sequence_number": seq,
              "item_id": item_id,
              "output_index": output_index,
              "content_index": 0,
              "text": text,
            }),
          ));
        }
        let final_item = json!({
          "id": item_id,
          "type": "reasoning",
          "status": "completed",
          "content": [{"type": "reasoning_text", "text": text}],
          "summary": [],
        });
        let seq = self.next_seq();
        out.push(SseEvent::json(
          Some("response.output_item.done"),
          json!({
            "type": "response.output_item.done",
            "sequence_number": seq,
            "output_index": output_index,
            "item": final_item.clone(),
          }),
        ));
        self.closed.insert(output_index, final_item);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn tool_delta(index: usize, id: Option<&str>, name: Option<&str>, arguments_delta: &str) -> IrDelta {
    IrDelta::ToolCall {
      index,
      id: id.map(str::to_string),
      name: name.map(str::to_string),
      arguments_delta: arguments_delta.to_string(),
    }
  }

  fn event_values<'a>(events: &'a [SseEvent], event_name: &str) -> Vec<&'a Value> {
    events
      .iter()
      .filter(|event| event.event.as_deref() == Some(event_name))
      .map(|event| event.json.as_ref().expect("JSON event"))
      .collect()
  }

  #[test]
  fn interleaved_tool_calls_keep_one_item_and_complete_arguments_per_call() {
    let mut emitter = ResponsesEmitter::with_created_at("resp_1".into(), "model".into(), 1);
    let mut events = emitter.emit(&[
      IrDelta::Reasoning("plan".into()),
      tool_delta(5, Some("call_weather"), Some("weather"), "{\"city\":"),
      tool_delta(2, Some("call_clock"), Some("clock"), "{\"zone\":"),
    ]);
    events.extend(emitter.emit(&[
      tool_delta(5, Some("call_weather"), Some("weather"), "\"Paris\"}"),
      tool_delta(2, Some("call_clock"), Some("clock"), "\"UTC\"}"),
      tool_delta(5, Some("call_weather"), Some("weather"), ""),
    ]));

    let added = event_values(&events, "response.output_item.added");
    assert_eq!(added.len(), 3, "reasoning and exactly two function-call items");
    let weather_item = added[1]["item"]["id"].clone();
    let clock_item = added[2]["item"]["id"].clone();
    assert_ne!(weather_item, clock_item);
    assert_eq!(added[1]["output_index"], 1);
    assert_eq!(added[2]["output_index"], 2);
    let argument_deltas = event_values(&events, "response.function_call_arguments.delta");
    assert_eq!(argument_deltas.len(), 4);
    for (event, item_id, output_index) in [
      (argument_deltas[0], &weather_item, 1),
      (argument_deltas[1], &clock_item, 2),
      (argument_deltas[2], &weather_item, 1),
      (argument_deltas[3], &clock_item, 2),
    ] {
      assert_eq!(&event["item_id"], item_id);
      assert_eq!(event["output_index"], output_index);
    }
    assert!(event_values(&events, "response.function_call_arguments.done").is_empty());

    events.extend(emitter.finish());

    let arguments_done = event_values(&events, "response.function_call_arguments.done");
    assert_eq!(arguments_done.len(), 2);
    assert_eq!(arguments_done[0]["item_id"], weather_item);
    assert_eq!(arguments_done[0]["arguments"], "{\"city\":\"Paris\"}");
    assert_eq!(arguments_done[1]["item_id"], clock_item);
    assert_eq!(arguments_done[1]["arguments"], "{\"zone\":\"UTC\"}");
    let completed = event_values(&events, "response.completed");
    let output = completed[0]["response"]["output"].as_array().unwrap();
    assert_eq!(output.len(), 3);
    assert_eq!(output[0]["type"], "reasoning");
    assert_eq!(output[1]["id"], weather_item);
    assert_eq!(output[1]["call_id"], "call_weather");
    assert_eq!(output[1]["name"], "weather");
    assert_eq!(output[1]["arguments"], "{\"city\":\"Paris\"}");
    assert_eq!(output[2]["id"], clock_item);
    assert_eq!(output[2]["call_id"], "call_clock");
    assert_eq!(output[2]["name"], "clock");
    assert_eq!(output[2]["arguments"], "{\"zone\":\"UTC\"}");
    for (sequence, event) in events.iter().enumerate() {
      assert_eq!(event.json.as_ref().unwrap()["sequence_number"], sequence);
    }
    assert!(emitter.finish().is_empty());
    assert!(emitter.emit(&[tool_delta(5, None, None, "ignored")]).is_empty());
  }

  #[test]
  fn tool_calls_survive_text_interleaving_and_accept_late_identity_hints() {
    let mut emitter = ResponsesEmitter::with_created_at("resp_1".into(), "model".into(), 1);
    let mut events = emitter.emit(&[
      tool_delta(7, None, None, "{"),
      IrDelta::Text("working".into()),
      tool_delta(2, Some("call_second"), Some("second"), "{}"),
      tool_delta(7, Some("call_first"), Some("first"), "}"),
      tool_delta(7, Some("call_first"), Some("first"), ""),
    ]);
    events.extend(emitter.finish());

    let added = event_values(&events, "response.output_item.added");
    assert_eq!(
      added.len(),
      3,
      "the first call must not be recreated after text or another call"
    );
    let done = event_values(&events, "response.output_item.done");
    assert_eq!(done.len(), 3);
    let completed = event_values(&events, "response.completed");
    let output = completed[0]["response"]["output"].as_array().unwrap();
    assert_eq!(output.len(), 3);
    assert_eq!(output[0]["id"], added[0]["item"]["id"]);
    assert_eq!(output[0]["call_id"], "call_first");
    assert_eq!(output[0]["name"], "first");
    assert_eq!(output[0]["arguments"], "{}");
    assert_eq!(output[1]["type"], "message");
    assert_eq!(output[1]["content"][0]["text"], "working");
    assert_eq!(output[2]["id"], added[2]["item"]["id"]);
    assert_eq!(output[2]["call_id"], "call_second");
    assert_eq!(output[2]["name"], "second");
    for event in done {
      let index = event["output_index"].as_u64().unwrap() as usize;
      assert_eq!(&event["item"], &output[index]);
    }
  }

  #[test]
  fn merges_split_usage_before_terminal_event() {
    let mut emitter = ResponsesEmitter::new("resp_1".into(), "model".into());
    emitter.emit(&[IrDelta::Usage(Usage {
      input_tokens: Some(7),
      ..Usage::default()
    })]);
    emitter.emit(&[IrDelta::Usage(Usage {
      output_tokens: Some(5),
      total_tokens: Some(12),
      ..Usage::default()
    })]);

    let completed = emitter
      .finish()
      .into_iter()
      .find(|event| event.event.as_deref() == Some("response.completed"))
      .and_then(|event| event.json)
      .expect("completed event");

    assert_eq!(completed["response"]["usage"]["input_tokens"], 7);
    assert_eq!(completed["response"]["usage"]["output_tokens"], 5);
    assert_eq!(completed["response"]["usage"]["total_tokens"], 12);
  }

  #[test]
  fn length_finish_emits_incomplete_terminal_event() {
    let mut emitter = ResponsesEmitter::new("resp_1".into(), "model".into());
    emitter.emit(&[IrDelta::Finish(Some("length".into()))]);

    let terminal = emitter.finish().pop().expect("terminal event");

    assert_eq!(terminal.event.as_deref(), Some("response.incomplete"));
    let value = terminal.json.expect("terminal JSON");
    assert_eq!(value["response"]["status"], "incomplete");
    assert_eq!(value["response"]["incomplete_details"]["reason"], "max_output_tokens");
  }

  #[test]
  fn cancelled_finish_emits_cancelled_terminal_event() {
    let mut emitter = ResponsesEmitter::new("resp_1".into(), "model".into());
    emitter.emit(&[IrDelta::Finish(Some("cancelled".into()))]);

    let terminal = emitter.finish().pop().expect("terminal event");

    assert_eq!(terminal.event.as_deref(), Some("response.cancelled"));
    assert_eq!(terminal.json.unwrap()["response"]["status"], "cancelled");
  }

  #[test]
  fn empty_function_arguments_are_closed_as_an_empty_object() {
    let mut emitter = ResponsesEmitter::new("resp_1".into(), "model".into());
    emitter.emit(&[IrDelta::ToolCall {
      index: 0,
      id: Some("call_1".into()),
      name: Some("ping".into()),
      arguments_delta: String::new(),
    }]);

    let events = emitter.finish();
    let arguments_done = events
      .iter()
      .find(|event| event.event.as_deref() == Some("response.function_call_arguments.done"))
      .and_then(|event| event.json.as_ref())
      .expect("arguments done event");
    let item_done = events
      .iter()
      .find(|event| event.event.as_deref() == Some("response.output_item.done"))
      .and_then(|event| event.json.as_ref())
      .expect("output item done event");

    assert_eq!(arguments_done["arguments"], "{}");
    assert_eq!(item_done["item"]["arguments"], "{}");
  }
}
