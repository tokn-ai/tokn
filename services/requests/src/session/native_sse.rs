//! Materialize native responses for buffered and live opted-in continuations.
//! Native blocks (including opaque fields) must survive before SDK projection.

use super::{ContinuationError, Error, ErrorKind};
use alloc::collections::BTreeMap;
use serde_json::Value;
use tokn_convert::sse::{Decoder, SseEvent, TerminalState};
use tokn_core::Endpoint;

mod chat;
mod messages;

pub(super) fn invalid(reason: &'static str) -> Error {
  let source = ContinuationError::InvalidState(reason);
  Error::new("convert_response", source).with_kind(ErrorKind::Continuation(source))
}

enum NativeResponse {
  Chat(chat::Chat),
  Messages(messages::Messages),
  Responses(Responses),
}

#[derive(Default)]
struct Responses {
  snapshot: Option<Value>,
  done_items: BTreeMap<u64, Value>,
  highest_index: Option<u64>,
  saw_output: bool,
}

impl Responses {
  fn push(&mut self, mut frame: Value) -> Result<(), Error> {
    let kind = frame["type"]
      .as_str()
      .ok_or_else(|| invalid("Responses event missing type"))?;
    let index = frame["output_index"].as_u64();
    if let Some(index) = index {
      self.highest_index = Some(self.highest_index.map_or(index, |seen| seen.max(index)));
    }
    self.saw_output |= index.is_some() || kind.starts_with("response.output_");
    match kind {
      "response.output_item.done" => {
        let index = index.ok_or_else(|| invalid("Responses item is missing an output index"))?;
        let item = frame["item"].take();
        if !item.is_object() || self.done_items.insert(index, item).is_some() {
          return Err(invalid("invalid or duplicated Responses output item snapshot"));
        }
      }
      "response.completed" => {
        let mut response = frame["response"].take();
        let snapshot = response
          .as_object_mut()
          .ok_or_else(|| invalid("missing terminal Responses snapshot"))?;
        let output = snapshot.entry("output").or_insert_with(|| Value::Array(alloc::vec![]));
        let items = output
          .as_array_mut()
          .ok_or_else(|| invalid("invalid terminal Responses output"))?;
        for (index, item) in core::mem::take(&mut self.done_items) {
          let index = usize::try_from(index).map_err(|_| invalid("invalid Responses output index"))?;
          if let Some(final_item) = items.get(index) {
            if item != *final_item {
              return Err(invalid("terminal Responses output disagrees with completed item"));
            }
          } else if index == items.len() {
            items.push(item);
          } else {
            return Err(invalid("missing Responses output item"));
          }
        }
        if (self.saw_output && items.is_empty()) || self.highest_index.is_some_and(|index| index >= items.len() as u64)
        {
          return Err(invalid("terminal snapshot omitted streamed output"));
        }
        self.snapshot = Some(response);
      }
      _ => {}
    }
    Ok(())
  }
}

impl NativeResponse {
  fn push(&mut self, frame: Value) -> Result<(), Error> {
    match self {
      Self::Chat(chat) => chat.push(&frame),
      Self::Messages(messages) => messages.push(frame),
      Self::Responses(response) => response.push(frame),
    }
  }

  fn finish(self) -> Result<Value, Error> {
    match self {
      Self::Chat(chat) => chat.finish(),
      Self::Messages(messages) => messages.finish(),
      Self::Responses(response) => response
        .snapshot
        .ok_or_else(|| invalid("missing terminal Responses snapshot")),
    }
  }
}

/// One native assembler shared by buffered decoding and demand-driven streams.
pub(super) struct Assembler {
  native: NativeResponse,
  terminal: TerminalState,
  complete: bool,
  consumed_bytes: usize,
  max_bytes: usize,
}

impl Assembler {
  pub(super) fn new(endpoint: Endpoint, max_bytes: usize) -> Self {
    let native = match endpoint {
      Endpoint::ChatCompletions => NativeResponse::Chat(chat::Chat::default()),
      Endpoint::Messages => NativeResponse::Messages(messages::Messages::default()),
      Endpoint::Responses => NativeResponse::Responses(Responses::default()),
    };
    Self {
      native,
      terminal: TerminalState::new(endpoint),
      complete: false,
      consumed_bytes: 0,
      max_bytes,
    }
  }

  pub(super) fn push(&mut self, event: &SseEvent) -> Result<bool, Error> {
    if self.complete {
      return Ok(true);
    }
    if event.data.trim().is_empty() {
      return Ok(false);
    }
    if event.data.len() > self.max_bytes.saturating_sub(self.consumed_bytes) {
      return Err(invalid("native stream checkpoint exceeds byte limit"));
    }
    self.consumed_bytes += event.data.len();
    let done = self
      .terminal
      .observe(event)
      .map_err(|err| Error::new("convert_response", err))?;
    if !event.is_done() {
      self.native.push(
        event
          .json
          .clone()
          .ok_or_else(|| invalid("malformed native SSE frame"))?,
      )?;
    }
    self.complete = done;
    Ok(done)
  }

  pub(super) fn finish(self) -> Result<Value, Error> {
    self
      .terminal
      .finish()
      .map_err(|err| Error::new("convert_response", err))?;
    self.native.finish()
  }
}

pub(super) fn decode(endpoint: Endpoint, bytes: &[u8], max_event_bytes: usize) -> Result<Value, Error> {
  let mut native = Assembler::new(endpoint, bytes.len());
  let mut decoder = Decoder::new(max_event_bytes);
  for byte in bytes {
    let Some(record) = decoder.push(*byte).map_err(|err| Error::new("convert_response", err))? else {
      continue;
    };
    let event = SseEvent::from(record);
    if native.push(&event)? {
      break;
    }
  }
  native.finish()
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::vec::Vec;
  use serde_json::json;

  fn frames(events: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for event in events {
      bytes.extend_from_slice(b"data: ");
      bytes.extend(serde_json::to_vec(event).unwrap());
      bytes.extend_from_slice(b"\n\n");
    }
    bytes
  }

  #[test]
  fn messages_preserve_native_blocks_signature_and_tool_input() {
    let stream = frames(&[
      json!({"type":"message_start","message":{"type":"message","role":"assistant","content":[],"usage":{"input_tokens":9}}}),
      json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","extra":"keep"}}),
      json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Checking"}}),
      json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed"}}),
      json!({"type":"content_block_stop","index":0}),
      json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","name":"weather","id":"c","input":{}}}),
      json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"x\":"}}),
      json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"1}"}}),
      json!({"type":"content_block_stop","index":1}),
      json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":6}}),
      json!({"type":"message_stop"}),
    ]);
    let response = decode(Endpoint::Messages, &stream, 4096).unwrap();
    assert_eq!(
      response["content"][0],
      json!({"type":"thinking","thinking":"Checking","signature":"signed","extra":"keep"})
    );
    assert_eq!(response["content"][1]["input"], json!({"x":1}));
    assert_eq!(response["usage"], json!({"input_tokens":9,"output_tokens":6}));
    assert!(decode(Endpoint::Messages, &stream[..stream.len() - 2], 4096).is_err());
  }

  #[test]
  fn responses_use_the_terminal_native_snapshot() {
    let output =
      json!({"id":"resp_1","status":"completed","output":[{"type":"reasoning","encrypted_content":"cipher"}]});
    let stream = frames(&[json!({"type":"response.completed","response":output})]);
    assert_eq!(decode(Endpoint::Responses, &stream, 4096).unwrap(), output);
    let incomplete = frames(&[
      json!({"type":"response.output_text.delta","delta":"visible"}),
      json!({"type":"response.completed","response":{"output":[]}}),
    ]);
    assert!(decode(Endpoint::Responses, &incomplete, 4096).is_err());
    let compact_terminal = frames(&[
      json!({"type":"response.output_item.done","output_index":0,"item":output["output"][0]}),
      json!({"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[]}}),
    ]);
    assert_eq!(decode(Endpoint::Responses, &compact_terminal, 4096).unwrap(), output);
  }

  #[test]
  fn live_assembler_requires_terminal_and_bounds_all_native_fragments() {
    let delta = SseEvent::json(None, json!({"choices":[{"delta":{"content":"hello"}}]}));
    let mut incomplete = Assembler::new(Endpoint::ChatCompletions, usize::MAX);
    assert!(!incomplete.push(&delta).unwrap());
    assert!(incomplete.finish().is_err());

    let mut limited = Assembler::new(Endpoint::ChatCompletions, delta.data.len() * 2 - 1);
    assert!(!limited.push(&delta).unwrap());
    assert!(limited.push(&delta).is_err());

    let mut complete = Assembler::new(Endpoint::ChatCompletions, usize::MAX);
    assert!(!complete.push(&delta).unwrap());
    let finish = SseEvent::json(None, json!({"choices":[{"delta":{},"finish_reason":"stop"}]}));
    assert!(!complete.push(&finish).unwrap());
    assert!(complete.push(&SseEvent::done()).unwrap());
    assert_eq!(complete.finish().unwrap()["choices"][0]["message"]["content"], "hello");
  }
}
