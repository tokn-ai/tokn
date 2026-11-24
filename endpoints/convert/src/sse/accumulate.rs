use super::super::error::{ConvertError, Result};
use super::super::ir::{IrDelta, IrResponse};
use super::event::SseEvent;
use crate::provider::Endpoint;
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::{boxed::Box, string::String, string::ToString, vec::Vec};
#[cfg(feature = "std")]
use bytes::Bytes;
#[cfg(feature = "std")]
use futures_util::StreamExt;
use serde_json::Value;

#[derive(Default)]
struct ResponsesState {
  response_id: Option<String>,
  model: Option<String>,
  output_items: BTreeMap<usize, ResponseOutputItem>,
}

#[derive(Default)]
struct MessagesState {
  tool_arguments_seen: BTreeMap<usize, bool>,
}

#[derive(Default)]
struct ResponseOutputItem {
  item_type: Option<String>,
  id: Option<String>,
  call_id: Option<String>,
  name: Option<String>,
  status: Option<String>,
  text: String,
  reasoning_summary: BTreeMap<usize, String>,
  reasoning_content: BTreeMap<usize, String>,
  arguments: String,
}

#[derive(Clone, Debug, Default)]
pub struct SseMetadata {
  pub response_id: Option<String>,
  pub model: Option<String>,
}

pub struct SseAccumulator {
  endpoint: Endpoint,
  response: IrResponse,
  tool_indices: BTreeMap<usize, usize>,
  responses: ResponsesState,
  messages: MessagesState,
  media_items: BTreeMap<usize, tokn_core::content::Content>,
  image_previews: BTreeSet<usize>,
  terminal_media_content: Option<Vec<crate::ir::ContentPart>>,
  audio_bytes: Vec<u8>,
  audio: Option<Value>,
}

impl SseAccumulator {
  pub fn new(endpoint: Endpoint) -> Self {
    Self {
      endpoint,
      response: IrResponse::default(),
      tool_indices: BTreeMap::new(),
      responses: ResponsesState::default(),
      messages: MessagesState::default(),
      media_items: BTreeMap::new(),
      image_previews: BTreeSet::new(),
      terminal_media_content: None,
      audio_bytes: Vec::new(),
      audio: None,
    }
  }

  pub fn push_value(&mut self, value: &Value) -> Result<Vec<IrDelta>> {
    let mut deltas = self.media_deltas(value)?;
    let mut text_deltas = match self.endpoint {
      Endpoint::ChatCompletions => {
        self.observe_chat_chunk(value);
        crate::value::chat::delta_from_chat_chunk(value)
      }
      Endpoint::Responses => self.delta_from_responses_event(value),
      Endpoint::Messages => {
        self.observe_messages_event(value);
        self.delta_from_messages_event(value)
      }
    };
    deltas.append(&mut text_deltas);
    for delta in deltas.iter().cloned() {
      self.push_response_delta(delta);
    }
    Ok(deltas)
  }

  fn media_deltas(&mut self, value: &Value) -> Result<Vec<IrDelta>> {
    let mut deltas = Vec::new();
    if self.endpoint == Endpoint::ChatCompletions {
      if let Some(audio) = value.pointer("/choices/0/delta/audio").filter(|value| !value.is_null()) {
        let object = audio
          .as_object()
          .ok_or_else(|| ConvertError::sse("invalid Chat audio delta"))?;
        if object.get("id").is_some_and(|id| id.as_str().is_none_or(str::is_empty))
          || object.get("expires_at").is_some_and(|expiry| expiry.as_u64().is_none())
        {
          return Err(ConvertError::sse("invalid Chat audio identity or expiry"));
        }
        let target = self.audio.get_or_insert_with(|| serde_json::json!({}));
        for field in ["id", "expires_at", "format"] {
          if let Some(value) = object.get(field) {
            if target.get(field).is_some_and(|old| old != value) {
              return Err(ConvertError::sse(
                "Chat audio identity or encoding changed during streaming",
              ));
            }
            target[field] = value.clone();
          }
        }
        let data = match object.get("data") {
          Some(Value::String(data)) => data.as_str(),
          None => "",
          _ => return Err(ConvertError::sse("invalid Chat audio data")),
        };
        if !data.is_empty() {
          self.audio_bytes.extend_from_slice(
            &tokn_core::content::decode_base64(data).map_err(|error| ConvertError::sse(error.to_string()))?,
          );
        }
        let transcript = match object.get("transcript") {
          Some(Value::String(text)) => Some(text.clone()),
          None => None,
          _ => return Err(ConvertError::sse("invalid Chat audio transcript")),
        };
        if let Some(text) = &transcript {
          let current = target
            .as_object_mut()
            .unwrap()
            .entry("transcript")
            .or_insert_with(|| Value::String(String::new()));
          current
            .as_str()
            .ok_or_else(|| ConvertError::sse("invalid Chat audio transcript"))?;
          if let Value::String(current) = current {
            current.push_str(text);
          }
        }
        deltas.push(IrDelta::Audio {
          index: 0,
          id: object.get("id").and_then(Value::as_str).map(str::to_string),
          data: data.into(),
          transcript,
          expires_at: object.get("expires_at").and_then(Value::as_u64),
        });
      }
    }
    if self.endpoint == Endpoint::Responses {
      let kind = value["type"].as_str().unwrap_or_default();
      if kind == "response.image_generation_call.partial_image" {
        let index = value["output_index"]
          .as_u64()
          .ok_or_else(|| ConvertError::sse("image preview missing output index"))? as usize;
        let preview_index = value["partial_image_index"]
          .as_u64()
          .ok_or_else(|| ConvertError::sse("image preview missing preview index"))?;
        let preview = serde_json::json!({"result":value["partial_image_b64"], "output_format":value["output_format"]});
        let image = crate::media::image_from_responses(&preview)?;
        self.image_previews.insert(index);
        deltas.push(IrDelta::ImagePreview {
          index,
          preview_index,
          image: Box::new(image),
        });
      }
      if kind == "response.output_item.done" && value["item"]["type"] == "image_generation_call" {
        let index = value["output_index"]
          .as_u64()
          .ok_or_else(|| ConvertError::sse("image item missing output index"))? as usize;
        self.complete_image(index, &value["item"], &mut deltas)?;
      }
      if kind == "response.completed" {
        if let Some(items) = value.pointer("/response/output").and_then(Value::as_array) {
          if items.iter().any(|item| item["type"] == "image_generation_call") {
            self.terminal_media_content =
              Some(crate::value::responses::response_from_value(&value["response"])?.content);
          }
          for (index, item) in items.iter().enumerate() {
            if item["type"] == "image_generation_call" {
              self.complete_image(index, item, &mut deltas)?;
            } else if item["type"] == "message" {
              let text = item["content"]
                .as_array()
                .map(|parts| {
                  parts
                    .iter()
                    .filter(|part| matches!(part["type"].as_str(), Some("output_text" | "text")))
                    .filter_map(|part| part["text"].as_str())
                    .collect::<String>()
                })
                .unwrap_or_default();
              let seen = self
                .responses
                .output_items
                .get(&index)
                .map(|item| item.text.as_str())
                .unwrap_or_default();
              if let Some(suffix) = text.strip_prefix(seen).filter(|suffix| !suffix.is_empty()) {
                deltas.push(IrDelta::Text(suffix.into()));
              }
            }
          }
        }
      }
    }
    Ok(deltas)
  }

  fn complete_image(&mut self, index: usize, item: &Value, deltas: &mut Vec<IrDelta>) -> Result<()> {
    let content = crate::media::image_from_responses(item)?;
    if let Some(previous) = self.media_items.get(&index) {
      let compatible = match (previous, &content) {
        (tokn_core::content::Content::Image(previous), tokn_core::content::Content::Image(current)) => {
          previous.resource.bytes() == current.resource.bytes()
            && (previous.resource.media_type == current.resource.media_type
              || previous.resource.media_type.is_none()
              || current.resource.media_type.is_none())
        }
        _ => false,
      };
      if !compatible {
        return Err(ConvertError::sse("completed image changed in terminal snapshot"));
      }
    } else {
      self.media_items.insert(index, content.clone());
      deltas.push(IrDelta::Media {
        index,
        content: Box::new(content),
      });
    }
    Ok(())
  }

  fn push_response_delta(&mut self, mut delta: IrDelta) {
    if let IrDelta::ToolCall { index, .. } = &mut delta {
      // Messages block indices and Responses output indices also count
      // non-tool content. Only buffered tool storage needs dense indices;
      // returned deltas retain their source identity for stream adapters.
      let next_index = self.tool_indices.len();
      *index = *self.tool_indices.entry(*index).or_insert(next_index);
    }
    self.response.push_delta(delta);
  }

  pub fn finish(self) -> IrResponse {
    let mut response = self.response;
    if let Some(content) = self.terminal_media_content {
      response.content = content;
    }
    if let Some(mut audio) = self.audio {
      audio["data"] = Value::String(tokn_core::content::encode_base64(&self.audio_bytes));
      if !self.audio_bytes.is_empty() {
        let mut content =
          tokn_core::content::AudioContent::new(tokn_core::content::MediaResource::from_bytes(self.audio_bytes));
        if let Some(transcript) = audio["transcript"].as_str() {
          content.transcript = Some(tokn_core::content::TranscriptContent::new(transcript));
        }
        response.content.push(crate::ir::ContentPart::Media {
          content: Box::new(tokn_core::content::Content::Audio(content)),
        });
      }
      response.extras.insert("__chat_audio".into(), audio);
    }
    if matches!(self.endpoint, Endpoint::Responses) {
      for (source_index, item) in &self.responses.output_items {
        let Some(index) = self.tool_indices.get(source_index) else {
          continue;
        };
        let call = &mut response.tool_calls[*index];
        if call
          .arguments
          .as_str()
          .is_some_and(|arguments| arguments.trim().is_empty())
        {
          // A zero-argument call may have no argument deltas at all. A final
          // item/arguments snapshot can still supply its complete input.
          call.arguments = Value::String(item.arguments.clone());
        }
      }
    }
    for call in &mut response.tool_calls {
      if let Value::String(arguments) = &call.arguments {
        if arguments.trim().is_empty() {
          call.arguments = Value::Object(serde_json::Map::new());
        } else if let Ok(Value::Object(arguments)) = serde_json::from_str(arguments) {
          call.arguments = Value::Object(arguments);
        }
      }
    }
    if response.id.is_none() {
      response.id = self.responses.response_id.clone();
    }
    if response.model.is_none() {
      response.model = self.responses.model.clone();
    }
    if matches!(self.endpoint, Endpoint::Responses)
      && response.finish_reason.is_none()
      && self.responses.has_tool_call()
    {
      response.finish_reason = Some("tool_calls".into());
    }
    response
  }

  pub fn metadata(&self) -> SseMetadata {
    SseMetadata {
      response_id: self.responses.response_id.clone(),
      model: self.responses.model.clone(),
    }
  }

  pub fn finish_reason(&self) -> Option<&str> {
    self.response.finish_reason.as_deref()
  }

  pub fn validate_complete(&self) -> Result<()> {
    if self.audio.is_some() && self.audio_bytes.is_empty() {
      return Err(ConvertError::sse("completed audio stream contains no audio bytes"));
    }
    if self
      .image_previews
      .iter()
      .any(|index| !self.media_items.contains_key(index))
    {
      return Err(ConvertError::sse(
        "image previews were not followed by a completed image",
      ));
    }
    Ok(())
  }

  pub fn responses_completed_finish_reason(&self, value: Option<&Value>) -> &'static str {
    if value
      .and_then(|event| event.get("response"))
      .is_some_and(crate::value::responses::response_has_tool_call_output)
      || self.responses.has_tool_call()
    {
      "tool_calls"
    } else {
      "stop"
    }
  }

  pub fn responses_has_tool_call(&self) -> bool {
    self.responses.has_tool_call()
  }

  fn delta_from_responses_event(&mut self, value: &Value) -> Vec<IrDelta> {
    self.observe_responses_response(value);
    self.observe_responses_output_item(value);
    self.observe_responses_part(value);
    let mut deltas = crate::value::responses::delta_from_responses_event(value);
    self.observe_responses_deltas(value, &deltas);
    let mut item_deltas = self.responses_tool_item_deltas(value);
    item_deltas.append(&mut deltas);
    let mut deltas = item_deltas;
    if matches!(value.get("type").and_then(Value::as_str), Some("response.completed")) {
      let reason = self.responses_completed_finish_reason(Some(value));
      for delta in &mut deltas {
        if let IrDelta::Finish(current) = delta {
          *current = Some(reason.into());
        }
      }
    }
    for delta in &mut deltas {
      if let IrDelta::ToolCall { index, id, name, .. } = delta {
        if let Some(item) = self.responses.output_items.get(index) {
          if item.call_id.is_some() {
            *id = item.call_id.clone();
          } else if id.is_none() {
            *id = item.id.clone();
          }
          if name.is_none() {
            *name = item.name.clone();
          }
        }
      }
    }
    deltas
  }

  fn responses_tool_item_deltas(&self, value: &Value) -> Vec<IrDelta> {
    let snapshot = match value.get("type").and_then(Value::as_str) {
      Some("response.output_item.added" | "response.output_item.done") => value.get("item"),
      Some("response.function_call_arguments.done" | "response.custom_tool_call_input.done") => Some(value),
      _ => return Vec::new(),
    };
    let Some(index) = value
      .get("output_index")
      .and_then(Value::as_u64)
      .map(|index| index as usize)
    else {
      return Vec::new();
    };
    let Some(item) = self.responses.output_items.get(&index) else {
      return Vec::new();
    };
    if !matches!(item.item_type.as_deref(), Some("function_call" | "custom_tool_call")) {
      return Vec::new();
    }
    let mut deltas = Vec::new();
    let current = self
      .tool_indices
      .get(&index)
      .and_then(|index| self.response.tool_calls.get(*index));
    let id = item.call_id.as_ref().or(item.id.as_ref());
    let identity_changed = match current {
      None => true,
      Some(current) => {
        id.is_some_and(|id| current.id.as_ref() != Some(id))
          || item.name.as_ref().is_some_and(|name| current.name != *name)
      }
    };
    if identity_changed {
      deltas.push(IrDelta::ToolCall {
        index,
        id: id.cloned(),
        name: item.name.clone(),
        arguments_delta: String::new(),
      });
    }
    let arguments = current.and_then(|call| call.arguments.as_str()).unwrap_or_default();
    if let Some(suffix) = snapshot
      .and_then(|snapshot| snapshot.get("arguments").or_else(|| snapshot.get("input")))
      .and_then(Value::as_str)
      .and_then(|snapshot| snapshot.strip_prefix(arguments))
      .filter(|suffix| !suffix.is_empty())
    {
      deltas.push(IrDelta::ToolCall {
        index,
        id: None,
        name: None,
        arguments_delta: suffix.to_string(),
      });
    }
    deltas
  }

  fn observe_chat_chunk(&mut self, value: &Value) {
    if self.responses.response_id.is_none() {
      self.responses.response_id = value.get("id").and_then(Value::as_str).map(str::to_string);
    }
    if self.responses.model.is_none() {
      self.responses.model = value.get("model").and_then(Value::as_str).map(str::to_string);
    }
  }

  fn observe_messages_event(&mut self, value: &Value) {
    if !matches!(value.get("type").and_then(Value::as_str), Some("message_start")) {
      return;
    }
    let Some(message) = value.get("message") else {
      return;
    };
    if self.responses.response_id.is_none() {
      self.responses.response_id = message.get("id").and_then(Value::as_str).map(str::to_string);
    }
    if self.responses.model.is_none() {
      self.responses.model = message.get("model").and_then(Value::as_str).map(str::to_string);
    }
  }

  fn delta_from_messages_event(&mut self, value: &Value) -> Vec<IrDelta> {
    let mut deltas = crate::value::messages::delta_from_messages_event(value);
    let index = value.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;

    match value.get("type").and_then(Value::as_str) {
      Some("content_block_start")
        if value
          .get("content_block")
          .and_then(|block| block.get("type"))
          .and_then(Value::as_str)
          == Some("tool_use") =>
      {
        let arguments_seen = deltas
          .iter()
          .any(|delta| matches!(delta, IrDelta::ToolCall { arguments_delta, .. } if !arguments_delta.is_empty()));
        self.messages.tool_arguments_seen.insert(index, arguments_seen);
      }
      Some("content_block_delta") => {
        let arguments_seen = deltas
          .iter()
          .any(|delta| matches!(delta, IrDelta::ToolCall { arguments_delta, .. } if !arguments_delta.is_empty()));
        if arguments_seen {
          self.messages.tool_arguments_seen.insert(index, true);
        }
      }
      Some("content_block_stop") if self.messages.tool_arguments_seen.remove(&index) == Some(false) => {
        deltas.push(IrDelta::ToolCall {
          index,
          id: None,
          name: None,
          arguments_delta: "{}".into(),
        });
      }
      _ => {}
    }

    deltas
  }

  fn observe_responses_response(&mut self, value: &Value) {
    let Some(response) = value.get("response") else {
      return;
    };
    if self.responses.response_id.is_none() {
      self.responses.response_id = response.get("id").and_then(Value::as_str).map(str::to_string);
    }
    if self.responses.model.is_none() {
      self.responses.model = response.get("model").and_then(Value::as_str).map(str::to_string);
    }
    if let Some(usage) = crate::ir::usage_from_openai(response) {
      if let Some(current) = &mut self.response.usage {
        current.merge(usage);
      } else {
        self.response.usage = Some(usage);
      }
    }
  }

  fn observe_responses_output_item(&mut self, value: &Value) {
    match value.get("type").and_then(Value::as_str) {
      Some("response.output_item.added") | Some("response.output_item.done") => {}
      _ => return,
    }
    let Some(index) = value.get("output_index").and_then(Value::as_u64).map(|v| v as usize) else {
      return;
    };
    let Some(item) = value.get("item") else {
      return;
    };
    let entry = self.responses.output_items.entry(index).or_default();
    if entry.item_type.is_none() {
      entry.item_type = item.get("type").and_then(Value::as_str).map(str::to_string);
    }
    if entry.status.is_none() {
      entry.status = item.get("status").and_then(Value::as_str).map(str::to_string);
    }
    if entry.id.is_none() {
      entry.id = item
        .get("id")
        .or_else(|| value.get("item_id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    }
    if entry.call_id.is_none() {
      entry.call_id = item.get("call_id").and_then(Value::as_str).map(str::to_string);
    }
    if entry.name.is_none() {
      entry.name = item.get("name").and_then(Value::as_str).map(str::to_string);
    }
    if let Some(arguments) = item
      .get("arguments")
      .or_else(|| item.get("input"))
      .and_then(Value::as_str)
    {
      entry.arguments = arguments.to_string();
    }
  }

  fn observe_responses_part(&mut self, value: &Value) {
    let Some(index) = value.get("output_index").and_then(Value::as_u64).map(|v| v as usize) else {
      return;
    };
    let Some(entry) = self.responses.output_items.get_mut(&index) else {
      return;
    };
    match value.get("type").and_then(Value::as_str) {
      Some("response.output_text.done") => {
        if let Some(text) = value.get("text").and_then(Value::as_str) {
          entry.text = text.to_string();
        }
      }
      Some("response.reasoning_summary_text.done") => {
        if let (Some(summary_index), Some(text)) = (
          value.get("summary_index").and_then(Value::as_u64).map(|v| v as usize),
          value.get("text").and_then(Value::as_str),
        ) {
          entry.reasoning_summary.insert(summary_index, text.to_string());
        }
      }
      Some("response.function_call_arguments.done") | Some("response.custom_tool_call_input.done") => {
        if let Some(arguments) = value
          .get("arguments")
          .or_else(|| value.get("input"))
          .and_then(Value::as_str)
        {
          entry.arguments = arguments.to_string();
        }
      }
      _ => {}
    }
  }

  fn observe_responses_deltas(&mut self, value: &Value, deltas: &[IrDelta]) {
    let Some(index) = value.get("output_index").and_then(Value::as_u64).map(|v| v as usize) else {
      return;
    };
    let entry = self.responses.output_items.entry(index).or_default();
    for delta in deltas {
      match delta {
        IrDelta::Text(text) => entry.text.push_str(text),
        IrDelta::Reasoning(text) => {
          let target = match value.get("type").and_then(Value::as_str) {
            Some("response.reasoning_summary_text.delta") => value
              .get("summary_index")
              .and_then(Value::as_u64)
              .map(|v| v as usize)
              .map(|i| entry.reasoning_summary.entry(i).or_default()),
            Some("response.reasoning_text.delta") => value
              .get("content_index")
              .and_then(Value::as_u64)
              .map(|v| v as usize)
              .map(|i| entry.reasoning_content.entry(i).or_default()),
            _ => None,
          };
          if let Some(buf) = target {
            buf.push_str(text);
          }
        }
        IrDelta::ToolCall { arguments_delta, .. } => entry.arguments.push_str(arguments_delta),
        _ => {}
      }
    }
  }
}

impl ResponsesState {
  fn has_tool_call(&self) -> bool {
    self
      .output_items
      .values()
      .any(|item| matches!(item.item_type.as_deref(), Some("function_call" | "custom_tool_call")))
  }
}

/// Accumulate a complete, bounded SSE body using the shared protocol framer.
pub fn accumulate_slice(endpoint: Endpoint, body: &[u8], max_event_bytes: usize) -> Result<IrResponse> {
  let mut accumulator = ResponseAccumulator::new(endpoint, max_event_bytes);
  accumulator.push(body)?;
  accumulator.finish()
}

struct ResponseAccumulator {
  decoder: super::Decoder,
  terminal: super::TerminalState,
  accumulator: SseAccumulator,
  done: bool,
}

impl ResponseAccumulator {
  fn new(endpoint: Endpoint, max_event_bytes: usize) -> Self {
    Self {
      decoder: super::Decoder::new(max_event_bytes),
      terminal: super::TerminalState::new(endpoint),
      accumulator: SseAccumulator::new(endpoint),
      done: false,
    }
  }

  fn push(&mut self, body: &[u8]) -> Result<()> {
    for &byte in body {
      if self.done {
        break;
      }
      let Some(record) = self
        .decoder
        .push(byte)
        .map_err(|error| ConvertError::sse(error.to_string()))?
      else {
        continue;
      };
      let event = SseEvent::from(record);
      let done = self.terminal.observe(&event)?;
      if !event.is_done() {
        let value = event
          .json
          .as_ref()
          .ok_or_else(|| ConvertError::sse("expected JSON SSE payload"))?;
        self.accumulator.push_value(value)?;
      }
      self.done = done;
    }
    Ok(())
  }

  fn finish(self) -> Result<IrResponse> {
    self.terminal.finish()?;
    self.accumulator.validate_complete()?;
    Ok(self.accumulator.finish())
  }
}

#[cfg(feature = "std")]
pub async fn accumulate(endpoint: Endpoint, resp: reqwest::Response) -> Result<IrResponse> {
  let mut accumulator = ResponseAccumulator::new(endpoint, usize::MAX);
  let mut stream = resp.bytes_stream();
  while let Some(chunk) = stream.next().await {
    accumulator.push(&chunk.map_err(|error| ConvertError::sse(error.to_string()))?)?;
    if accumulator.done {
      break;
    }
  }
  accumulator.finish()
}

/// Accumulate a complete SSE body into the endpoint-neutral response IR.
#[cfg(feature = "std")]
pub async fn accumulate_bytes(endpoint: Endpoint, body: Bytes) -> Result<IrResponse> {
  accumulate_slice(endpoint, &body, usize::MAX)
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  fn messages_tool_start(input: Value) -> Value {
    json!({
      "type": "content_block_start",
      "index": 0,
      "content_block": {
        "type": "tool_use",
        "id": "toolu_1",
        "name": "lookup",
        "input": input
      }
    })
  }

  #[test]
  fn messages_empty_tool_input_is_synthesized_when_the_block_closes() {
    let mut accumulator = SseAccumulator::new(Endpoint::Messages);

    let start = accumulator.push_value(&messages_tool_start(json!({}))).unwrap();
    assert!(matches!(
      start.as_slice(),
      [IrDelta::ToolCall { arguments_delta, .. }] if arguments_delta.is_empty()
    ));

    let stop = accumulator
      .push_value(&json!({
        "type": "content_block_stop",
        "index": 0
      }))
      .unwrap();
    assert!(matches!(
      stop.as_slice(),
      [IrDelta::ToolCall { arguments_delta, .. }] if arguments_delta == "{}"
    ));

    let response = accumulator.finish();
    assert_eq!(response.tool_calls[0].id.as_deref(), Some("toolu_1"));
    assert_eq!(response.tool_calls[0].name, "lookup");
    assert_eq!(response.tool_calls[0].arguments, json!({}));
  }

  #[test]
  fn messages_empty_tool_start_does_not_prefix_streamed_arguments() {
    let mut accumulator = SseAccumulator::new(Endpoint::Messages);

    accumulator.push_value(&messages_tool_start(json!({}))).unwrap();
    for partial_json in [r#"{"query":"#, r#""rust"}"#] {
      accumulator
        .push_value(&json!({
          "type": "content_block_delta",
          "index": 0,
          "delta": {
            "type": "input_json_delta",
            "partial_json": partial_json
          }
        }))
        .unwrap();
    }
    let stop = accumulator
      .push_value(&json!({
        "type": "content_block_stop",
        "index": 0
      }))
      .unwrap();

    assert!(stop.is_empty());
    let response = accumulator.finish();
    assert_eq!(response.tool_calls[0].arguments, json!({"query": "rust"}));
  }

  #[test]
  fn responses_tool_snapshots_emit_identity_and_only_missing_argument_suffixes() {
    let mut accumulator = SseAccumulator::new(Endpoint::Responses);
    let start = accumulator
      .push_value(&json!({
        "type":"response.output_item.added",
        "output_index":4,
        "item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":""}
      }))
      .unwrap();
    assert!(matches!(
      start.as_slice(),
      [IrDelta::ToolCall { index: 4, id: Some(id), name: Some(name), arguments_delta }]
        if id == "call-1" && name == "lookup" && arguments_delta.is_empty()
    ));
    accumulator
      .push_value(&json!({
        "type":"response.function_call_arguments.delta",
        "output_index":4,
        "delta":"{\"query\":"
      }))
      .unwrap();
    let snapshot = json!({
      "type":"response.function_call_arguments.done",
      "output_index":4,
      "arguments":"{\"query\":\"rust\"}"
    });
    let suffix = accumulator.push_value(&snapshot).unwrap();
    assert!(matches!(
      suffix.as_slice(),
      [IrDelta::ToolCall { index: 4, arguments_delta, .. }] if arguments_delta == "\"rust\"}"
    ));
    assert!(accumulator.push_value(&snapshot).unwrap().is_empty());
    assert!(accumulator.push_value(&json!({
      "type":"response.output_item.done",
      "output_index":4,
      "item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"rust\"}"}
    })).unwrap().is_empty());
    let response = accumulator.finish();
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].arguments, json!({"query": "rust"}));
  }

  #[test]
  fn responses_tool_metadata_corrects_identity_after_argument_deltas() {
    let mut accumulator = SseAccumulator::new(Endpoint::Responses);
    accumulator
      .push_value(&json!({
        "type":"response.function_call_arguments.delta",
        "output_index":4,
        "item_id":"fc-1",
        "delta":"{}"
      }))
      .unwrap();
    let snapshot = json!({
      "type":"response.output_item.done",
      "output_index":4,
      "item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{}"}
    });
    let metadata = accumulator.push_value(&snapshot).unwrap();
    assert!(matches!(
      metadata.as_slice(),
      [IrDelta::ToolCall { index: 4, id: Some(id), name: Some(name), arguments_delta }]
        if id == "call-1" && name == "lookup" && arguments_delta.is_empty()
    ));
    assert!(accumulator.push_value(&snapshot).unwrap().is_empty());
    let response = accumulator.finish();
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].id.as_deref(), Some("call-1"));
    assert_eq!(response.tool_calls[0].name, "lookup");
    assert_eq!(response.tool_calls[0].arguments, json!({}));
  }

  #[test]
  fn responses_tool_done_can_supply_a_call_without_prior_deltas() {
    let mut accumulator = SseAccumulator::new(Endpoint::Responses);
    let deltas = accumulator
      .push_value(&json!({
        "type":"response.output_item.done",
        "output_index":4,
        "item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{}"}
      }))
      .unwrap();
    assert!(matches!(
      deltas.as_slice(),
      [IrDelta::ToolCall { index: 4, arguments_delta: identity_arguments, .. },
        IrDelta::ToolCall { index: 4, arguments_delta: snapshot_arguments, .. }]
        if identity_arguments.is_empty() && snapshot_arguments == "{}"
    ));
    let response = accumulator.finish();
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].id.as_deref(), Some("call-1"));
    assert_eq!(response.tool_calls[0].name, "lookup");
    assert_eq!(response.tool_calls[0].arguments, json!({}));
  }

  #[test]
  fn sparse_tool_indices_are_dense_only_in_the_buffered_response() {
    for (endpoint, frames, source_index) in [
      (
        Endpoint::ChatCompletions,
        vec![
          json!({"choices":[{"delta":{"tool_calls":[{"index":9,"id":"call-1","function":{"name":"lookup","arguments":"{}"}}]}}]}),
        ],
        9,
      ),
      (
        Endpoint::Responses,
        vec![
          json!({"type":"response.output_item.added","output_index":3,"item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup"}}),
          json!({"type":"response.function_call_arguments.delta","output_index":3,"delta":"{}"}),
        ],
        3,
      ),
      (
        Endpoint::Messages,
        vec![
          json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"call-1","name":"lookup","input":{}}}),
          json!({"type":"content_block_stop","index":2}),
        ],
        2,
      ),
    ] {
      let mut accumulator = SseAccumulator::new(endpoint);
      let mut tool_deltas = 0;
      for frame in frames {
        for delta in accumulator.push_value(&frame).unwrap() {
          if let IrDelta::ToolCall { index, .. } = delta {
            assert_eq!(index, source_index);
            tool_deltas += 1;
          }
        }
      }
      assert!(tool_deltas > 0);
      let response = accumulator.finish();
      assert_eq!(response.tool_calls.len(), 1);
      assert_eq!(response.tool_calls[0].id.as_deref(), Some("call-1"));
      assert_eq!(response.tool_calls[0].name, "lookup");
      assert_eq!(response.tool_calls[0].arguments, json!({}));
    }
  }
}
