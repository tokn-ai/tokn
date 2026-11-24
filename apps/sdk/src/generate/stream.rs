use eventsource_stream::{EventStream, Eventsource};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use tokn_convert::ir::IrDelta;
use tokn_convert::sse::SseAccumulator;
use tokn_core::content::Content;
use tokn_core::provider::Endpoint;

use super::response::finish_reason;
use super::Usage;
use crate::{Error, Result};

/// Provider-neutral semantic events produced by a streaming generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GenerateEvent {
  TextDelta {
    text: String,
  },
  ReasoningDelta {
    text: String,
  },
  ToolCallDelta {
    index: usize,
    id: Option<String>,
    name: Option<String>,
    arguments_delta: String,
  },
  /// A complete generated media part. Partial previews use `ImagePreview`.
  ContentCompleted {
    index: usize,
    content: Box<Content>,
  },
  /// One independently encoded audio chunk and optional transcript delta.
  AudioDelta {
    index: usize,
    id: Option<String>,
    data: String,
    transcript: Option<String>,
    expires_at: Option<u64>,
  },
  ImagePreview {
    index: usize,
    preview_index: u64,
    content: Box<Content>,
  },
  Usage {
    usage: Usage,
  },
  Completed {
    finish_reason: Option<String>,
  },
  Other {
    kind: String,
    data: Value,
  },
}

/// A stream of provider-neutral generation events.
pub type GenerateStream = BoxStream<'static, Result<GenerateEvent>>;

/// A convenience stream containing only generated text deltas.
pub type TextStream = BoxStream<'static, Result<String>>;

struct EventParseState {
  events: EventStream<crate::ByteStream>,
  accumulator: SseAccumulator,
  pending: VecDeque<Result<GenerateEvent>>,
  stop_after_pending: bool,
  endpoint: Endpoint,
  finish_reason: Option<String>,
  max_bytes: usize,
  consumed_bytes: usize,
}

impl EventParseState {
  fn new(bytes: crate::ByteStream, endpoint: Endpoint, max_bytes: usize) -> Self {
    Self {
      events: bytes.eventsource(),
      accumulator: SseAccumulator::new(endpoint),
      pending: VecDeque::new(),
      stop_after_pending: false,
      endpoint,
      finish_reason: None,
      max_bytes,
      consumed_bytes: 0,
    }
  }

  fn account_event(&mut self, data_bytes: usize, name_bytes: usize) -> bool {
    let size = data_bytes.saturating_add(name_bytes);
    if size > self.max_bytes.saturating_sub(self.consumed_bytes) {
      return false;
    }
    self.consumed_bytes = self.consumed_bytes.saturating_add(size);
    true
  }

  fn enqueue(&mut self, results: Vec<Result<GenerateEvent>>, terminal: bool) {
    self.stop_after_pending |= terminal;
    for result in results {
      let failed = result.is_err();
      self.pending.push_back(result);
      if failed {
        self.stop_after_pending = true;
        break;
      }
    }
  }

  fn fail(&mut self, error: Error) {
    self.enqueue(vec![Err(error)], true);
  }
}

#[cfg(test)]
pub(super) fn parse_events(bytes: crate::ByteStream) -> GenerateStream {
  parse_endpoint_events(bytes, Endpoint::Responses, usize::MAX)
}

pub(super) fn parse_endpoint_events(bytes: crate::ByteStream, endpoint: Endpoint, max_bytes: usize) -> GenerateStream {
  stream::unfold(
    EventParseState::new(bytes, endpoint, max_bytes),
    |mut state| async move {
      loop {
        if let Some(result) = state.pending.pop_front() {
          return Some((result, state));
        }
        if state.stop_after_pending {
          return None;
        }

        match state.events.next().await {
          Some(Ok(event)) if !state.account_event(event.data.len(), event.event.len()) => {
            state.fail(Error::GenerateStream {
              message: "generation stream exceeds response byte limit".into(),
            });
          }
          Some(Ok(event)) if event.data.trim().is_empty() => {}
          Some(Ok(event)) if event.data.trim() == "[DONE]" => {
            if state.endpoint == Endpoint::ChatCompletions {
              if let Err(error) = state.accumulator.validate_complete() {
                state.fail(Error::GenerateStream {
                  message: error.to_string(),
                });
                continue;
              }
              state.enqueue(
                vec![Ok(GenerateEvent::Completed {
                  finish_reason: state.finish_reason.clone(),
                })],
                true,
              );
            } else {
              state.fail(Error::GenerateStream {
                message: "stream ended before its terminal response event".into(),
              });
            }
          }
          Some(Ok(event)) => match serde_json::from_str::<Value>(&event.data) {
            Ok(value) => {
              let terminal = match state.endpoint {
                Endpoint::Responses => is_terminal_value(&value),
                Endpoint::Messages => value["type"] == "message_stop",
                Endpoint::ChatCompletions => false,
              };
              if state.endpoint == Endpoint::ChatCompletions {
                if let Some(reason) = value
                  .get("choices")
                  .and_then(Value::as_array)
                  .into_iter()
                  .flatten()
                  .find(|choice| choice.get("index").and_then(Value::as_u64).unwrap_or(0) == 0)
                  .and_then(|choice| choice.get("finish_reason"))
                  .and_then(Value::as_str)
                {
                  state.finish_reason = Some(reason.into());
                }
              }
              let endpoint = state.endpoint;
              let results = results_for_endpoint(&mut state.accumulator, endpoint, value);
              state.enqueue(results, terminal);
            }
            Err(source) => state.fail(Error::DeserializeStreamEvent { source }),
          },
          Some(Err(error)) => state.fail(Error::GenerateStream {
            message: error.to_string(),
          }),
          None => state.fail(Error::GenerateStream {
            message: "generation stream ended before a terminal event".into(),
          }),
        }
      }
    },
  )
  .boxed()
}

pub(super) fn results_for_endpoint(
  accumulator: &mut SseAccumulator,
  endpoint: Endpoint,
  value: Value,
) -> Vec<Result<GenerateEvent>> {
  if endpoint == Endpoint::Responses {
    return results_from_value(accumulator, value);
  }
  if value["type"] == "error" || value.get("error").is_some_and(|error| !error.is_null()) {
    return vec![Err(Error::GenerateStream {
      message: stream_error_message(&value),
    })];
  }
  let deltas = match accumulator.push_value(&value) {
    Ok(deltas) => deltas,
    Err(error) => {
      return vec![Err(Error::GenerateStream {
        message: error.to_string(),
      })]
    }
  };
  if endpoint == Endpoint::Messages && value["type"] == "message_stop" {
    if let Err(error) = accumulator.validate_complete() {
      return vec![Err(Error::GenerateStream {
        message: error.to_string(),
      })];
    }
    return vec![Ok(GenerateEvent::Completed {
      finish_reason: accumulator.finish_reason().map(str::to_string),
    })];
  }
  deltas
    .into_iter()
    .filter_map(|delta| {
      if matches!(endpoint, Endpoint::ChatCompletions | Endpoint::Messages) && matches!(delta, IrDelta::Finish(_)) {
        None
      } else {
        Some(Ok(event_from_delta(delta)))
      }
    })
    .collect()
}

fn is_terminal_value(value: &Value) -> bool {
  matches!(
    value.get("type").and_then(Value::as_str),
    Some("response.completed" | "response.incomplete" | "response.failed" | "response.cancelled" | "error")
  )
}

pub(super) fn text_only(events: GenerateStream) -> TextStream {
  events
    .try_filter_map(|event| async move {
      Ok(match event {
        GenerateEvent::TextDelta { text } => Some(text),
        _ => None,
      })
    })
    .boxed()
}

pub(super) fn results_from_value(accumulator: &mut SseAccumulator, value: Value) -> Vec<Result<GenerateEvent>> {
  let kind = value.get("type").and_then(Value::as_str).unwrap_or_default();
  let completion_reason = matches!(
    kind,
    "response.completed" | "response.incomplete" | "response.cancelled"
  )
  .then(|| value.get("response"))
  .flatten()
  .and_then(|response| finish_reason(response, response_has_tool_calls(response)));
  let deltas = match accumulator.push_value(&value) {
    Ok(deltas) => deltas,
    Err(error) => {
      return vec![Err(Error::GenerateStream {
        message: error.to_string(),
      })]
    }
  };

  if kind == "response.completed" {
    if let Err(error) = accumulator.validate_complete() {
      return vec![Err(Error::GenerateStream {
        message: error.to_string(),
      })];
    }
  }
  if matches!(kind, "response.failed" | "error") {
    return vec![Err(Error::GenerateStream {
      message: stream_error_message(&value),
    })];
  }
  if matches!(kind, "response.incomplete" | "response.cancelled") {
    let mut events = value
      .get("response")
      .and_then(tokn_convert::ir::usage_from_openai)
      .map(|usage| {
        vec![Ok(GenerateEvent::Usage {
          usage: Usage::from_ir(usage),
        })]
      })
      .unwrap_or_default();
    events.push(Ok(GenerateEvent::Completed {
      finish_reason: completion_reason.or_else(|| Some(kind.trim_start_matches("response.").into())),
    }));
    return events;
  }
  if let Some(event) = tool_call_started(&value) {
    return vec![Ok(event)];
  }
  if deltas.is_empty() {
    return vec![Ok(GenerateEvent::Other {
      kind: value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string(),
      data: value,
    })];
  }
  deltas
    .into_iter()
    .map(|delta| match delta {
      IrDelta::Finish(_) if completion_reason.is_some() => Ok(GenerateEvent::Completed {
        finish_reason: completion_reason.clone(),
      }),
      other => Ok(event_from_delta(other)),
    })
    .collect()
}

fn tool_call_started(value: &Value) -> Option<GenerateEvent> {
  if value.get("type").and_then(Value::as_str) != Some("response.output_item.added") {
    return None;
  }
  let item = value.get("item")?;
  if !matches!(
    item.get("type").and_then(Value::as_str),
    Some("function_call" | "custom_tool_call")
  ) {
    return None;
  }
  Some(GenerateEvent::ToolCallDelta {
    index: value.get("output_index").and_then(Value::as_u64).unwrap_or(0) as usize,
    id: item
      .get("call_id")
      .or_else(|| item.get("id"))
      .and_then(Value::as_str)
      .map(str::to_string),
    name: item.get("name").and_then(Value::as_str).map(str::to_string),
    arguments_delta: item
      .get("arguments")
      .or_else(|| item.get("input"))
      .and_then(Value::as_str)
      .unwrap_or_default()
      .to_string(),
  })
}

fn stream_error_message(value: &Value) -> String {
  value
    .pointer("/error/message")
    .or_else(|| value.pointer("/response/error/message"))
    .and_then(Value::as_str)
    .map(str::to_string)
    .unwrap_or_else(|| value.to_string())
}

fn response_has_tool_calls(response: &Value) -> bool {
  response.get("output").and_then(Value::as_array).is_some_and(|output| {
    output.iter().any(|item| {
      matches!(
        item.get("type").and_then(Value::as_str),
        Some("function_call" | "custom_tool_call")
      )
    })
  })
}

fn event_from_delta(delta: IrDelta) -> GenerateEvent {
  match delta {
    IrDelta::Text(text) => GenerateEvent::TextDelta { text },
    IrDelta::Reasoning(text) => GenerateEvent::ReasoningDelta { text },
    IrDelta::ToolCall {
      index,
      id,
      name,
      arguments_delta,
    } => GenerateEvent::ToolCallDelta {
      index,
      id,
      name,
      arguments_delta,
    },
    IrDelta::Usage(usage) => GenerateEvent::Usage {
      usage: Usage::from_ir(usage),
    },
    IrDelta::Finish(finish_reason) => GenerateEvent::Completed { finish_reason },
    IrDelta::Media { index, content } => GenerateEvent::ContentCompleted { index, content },
    IrDelta::Audio {
      index,
      id,
      data,
      transcript,
      expires_at,
    } => GenerateEvent::AudioDelta {
      index,
      id,
      data,
      transcript,
      expires_at,
    },
    IrDelta::ImagePreview {
      index,
      preview_index,
      image,
    } => GenerateEvent::ImagePreview {
      index,
      preview_index,
      content: image,
    },
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Bytes;
  use futures_util::stream;

  #[tokio::test]
  async fn messages_complete_once_at_message_stop_and_ignore_later_frames() {
    let body = sse([
      serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}),
      serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}),
      serde_json::json!({"type":"message_stop"}),
      serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ignored"}}),
    ]);
    let events = parse_endpoint_events(byte_stream(body), Endpoint::Messages, usize::MAX)
      .try_collect::<Vec<_>>()
      .await
      .unwrap();
    assert_eq!(
      events
        .iter()
        .filter(|event| matches!(event, GenerateEvent::Completed { .. }))
        .count(),
      1
    );
    assert!(matches!(events.last(), Some(GenerateEvent::Completed{finish_reason:Some(reason)}) if reason == "stop"));
    assert!(!events
      .iter()
      .any(|event| matches!(event, GenerateEvent::TextDelta{text} if text == "ignored")));
  }

  #[tokio::test]
  async fn messages_finish_reason_without_message_stop_is_incomplete() {
    let body = sse([serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}})]);
    let mut events = parse_endpoint_events(byte_stream(body), Endpoint::Messages, usize::MAX);
    assert!(matches!(events.next().await, Some(Err(Error::GenerateStream { .. }))));
    assert!(events.next().await.is_none());
  }

  #[tokio::test]
  async fn image_preview_without_completed_image_cannot_complete() {
    let body = sse([
      serde_json::json!({"type":"response.image_generation_call.partial_image","output_index":0,"partial_image_index":0,"partial_image_b64":"AQID","output_format":"png"}),
      serde_json::json!({"type":"response.completed","response":{"status":"completed","output":[]}}),
    ]);
    let mut events = parse_events(byte_stream(body));
    assert!(matches!(
      events.next().await,
      Some(Ok(GenerateEvent::ImagePreview { .. }))
    ));
    assert!(matches!(events.next().await, Some(Err(Error::GenerateStream { .. }))));
    assert!(events.next().await.is_none());
  }

  #[tokio::test]
  async fn cumulative_audio_stream_limit_prevents_unbounded_accumulation() {
    let chunk = serde_json::json!({"choices":[{"index":0,"delta":{"audio":{"id":"audio-1","data":"AQID"}}}]});
    let limit = (chunk.to_string().len() + "message".len()) * 2;
    let body = format!("{}data: [DONE]\n\n", sse([chunk.clone(), chunk.clone(), chunk]));
    let mut events = parse_endpoint_events(byte_stream(body), Endpoint::ChatCompletions, limit);
    for _ in 0..2 {
      assert!(matches!(
        events.next().await,
        Some(Ok(GenerateEvent::AudioDelta { .. }))
      ));
    }
    assert!(
      matches!(events.next().await, Some(Err(Error::GenerateStream { message })) if message.contains("byte limit"))
    );
    assert!(events.next().await.is_none());
  }

  fn byte_stream(body: String) -> crate::ByteStream {
    Box::pin(stream::once(async move { Ok(Bytes::from(body)) }))
  }

  fn sse(values: impl IntoIterator<Item = Value>) -> String {
    values.into_iter().map(|value| format!("data: {value}\n\n")).collect()
  }

  #[tokio::test]
  async fn failed_streams_surface_as_errors() {
    let body = sse([serde_json::json!({
      "type": "response.failed",
      "response": {
        "status": "failed",
        "error": {"message": "provider failed"}
      }
    })]);

    let error = parse_events(byte_stream(body))
      .try_collect::<Vec<_>>()
      .await
      .expect_err("failed terminal event should fail the stream");

    assert!(matches!(error, Error::GenerateStream { message } if message == "provider failed"));
  }

  #[tokio::test]
  async fn semantic_errors_are_terminal() {
    let body = sse([
      serde_json::json!({
        "type": "response.failed",
        "response": {
          "status": "failed",
          "error": {"message": "provider failed"}
        }
      }),
      serde_json::json!({
        "type": "response.output_text.delta",
        "delta": "must not be emitted"
      }),
    ]);
    let mut events = parse_events(byte_stream(body));

    assert!(matches!(
      events.next().await,
      Some(Err(Error::GenerateStream { message })) if message == "provider failed"
    ));
    assert!(events.next().await.is_none());
  }

  #[tokio::test]
  async fn decode_errors_are_terminal() {
    let body = format!(
      "data: not-json\n\n{}",
      sse([serde_json::json!({
        "type": "response.output_text.delta",
        "delta": "must not be emitted"
      })])
    );
    let mut events = parse_events(byte_stream(body));

    assert!(matches!(
      events.next().await,
      Some(Err(Error::DeserializeStreamEvent { .. }))
    ));
    assert!(events.next().await.is_none());
  }

  #[tokio::test]
  async fn transport_errors_are_terminal() {
    let trailing = sse([serde_json::json!({
      "type": "response.output_text.delta",
      "delta": "must not be emitted"
    })]);
    let bytes: crate::ByteStream = Box::pin(stream::iter([
      Err(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "connection reset",
      )),
      Ok(Bytes::from(trailing)),
    ]));
    let mut events = parse_events(bytes);

    assert!(matches!(
      events.next().await,
      Some(Err(Error::GenerateStream { message })) if message.contains("connection reset")
    ));
    assert!(events.next().await.is_none());
  }

  #[tokio::test]
  async fn incomplete_streams_keep_usage_and_finish_reason() {
    let body = sse([serde_json::json!({
      "type": "response.incomplete",
      "response": {
        "status": "incomplete",
        "incomplete_details": {"reason": "max_output_tokens"},
        "usage": {"input_tokens": 4, "output_tokens": 8, "total_tokens": 12}
      }
    })]);

    let events = parse_events(byte_stream(body))
      .try_collect::<Vec<_>>()
      .await
      .expect("incomplete terminal event remains a valid partial result");

    assert!(events.iter().any(|event| {
      matches!(
        event,
        GenerateEvent::Usage {
          usage: Usage {
            total_tokens: Some(12),
            ..
          }
        }
      )
    }));
    assert!(events.iter().any(|event| {
      matches!(
        event,
        GenerateEvent::Completed {
          finish_reason: Some(reason)
        } if reason == "length"
      )
    }));
  }

  #[tokio::test]
  async fn cancelled_streams_complete_with_a_clear_reason() {
    let body = sse([serde_json::json!({
      "type": "response.cancelled",
      "response": {
        "status": "cancelled",
        "usage": {"input_tokens": 3, "output_tokens": 1, "total_tokens": 4}
      }
    })]);

    let events = parse_events(byte_stream(body))
      .try_collect::<Vec<_>>()
      .await
      .expect("cancelled is a valid terminal result");

    assert!(events.iter().any(|event| {
      matches!(
        event,
        GenerateEvent::Usage {
          usage: Usage {
            total_tokens: Some(4),
            ..
          }
        }
      )
    }));
    assert!(events.iter().any(|event| {
      matches!(
        event,
        GenerateEvent::Completed {
          finish_reason: Some(reason)
        } if reason == "cancelled"
      )
    }));
  }

  #[tokio::test]
  async fn successful_terminal_events_finish_without_polling_later_events() {
    let body = sse([
      serde_json::json!({
        "type": "response.completed",
        "response": {
          "status": "completed",
          "output": []
        }
      }),
      serde_json::json!({
        "type": "response.output_text.delta",
        "delta": "must not be emitted"
      }),
    ]);
    let events = parse_events(byte_stream(body))
      .try_collect::<Vec<_>>()
      .await
      .expect("completed terminal event should end the stream");

    assert_eq!(
      events,
      vec![GenerateEvent::Completed {
        finish_reason: Some("stop".into())
      }]
    );
  }

  #[tokio::test]
  async fn clean_eof_without_terminal_event_is_an_error() {
    let body = sse([serde_json::json!({
      "type": "response.output_text.delta",
      "delta": "partial"
    })]);
    let mut events = parse_events(byte_stream(body));

    assert!(matches!(
      events.next().await,
      Some(Ok(GenerateEvent::TextDelta { text })) if text == "partial"
    ));
    assert!(matches!(
      events.next().await,
      Some(Err(Error::GenerateStream { message })) if message.contains("before a terminal event")
    ));
  }
}
