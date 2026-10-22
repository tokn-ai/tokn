use alloc::{string::String, string::ToString};
#[cfg(feature = "std")]
use eventsource_stream::Event;
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct SseEvent {
  pub event: Option<String>,
  pub data: String,
  pub json: Option<Value>,
}

impl SseEvent {
  pub fn raw(event: Option<String>, data: String) -> Self {
    let payload = data.trim();
    let json = if payload.is_empty() || payload == "[DONE]" {
      None
    } else {
      serde_json::from_str(payload).ok()
    };
    Self { event, data, json }
  }

  pub fn json(event: Option<&str>, value: Value) -> Self {
    let data = serde_json::to_string(&value).unwrap_or_else(|_| "{}".into());
    Self {
      event: event.map(str::to_string),
      data,
      json: Some(value),
    }
  }

  pub fn done() -> Self {
    Self {
      event: None,
      data: "[DONE]".into(),
      json: None,
    }
  }

  pub fn is_done(&self) -> bool {
    self.data.trim() == "[DONE]"
  }
}

impl From<super::decoder::Record> for SseEvent {
  fn from(record: super::decoder::Record) -> Self {
    // SSE's default event type is "message". Preserve the standard parser's
    // established observable event name for both platform adapters.
    let event = record
      .event
      .filter(|name| !name.is_empty())
      .unwrap_or_else(|| "message".into());
    Self::raw(Some(event), record.data)
  }
}

#[cfg(feature = "std")]
impl From<Event> for SseEvent {
  fn from(value: Event) -> Self {
    let event = (!value.event.is_empty()).then_some(value.event);
    Self::raw(event, value.data)
  }
}
