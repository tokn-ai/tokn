//! Endpoint completion tracking independent of transport and buffering.

use super::SseEvent;
use crate::{error::ConvertError, provider::Endpoint, Result};

/// Tracks the terminal convention of one upstream protocol.
///
/// Failed lifecycle events and error envelopes are errors even after HTTP 200.
/// An EOF does not imply successful completion.
pub struct TerminalState {
  endpoint: Endpoint,
  completed: bool,
}

impl TerminalState {
  pub fn new(endpoint: Endpoint) -> Self {
    Self {
      endpoint,
      completed: false,
    }
  }

  pub fn observe(&mut self, event: &SseEvent) -> Result<bool> {
    if self.completed {
      return Ok(true);
    }
    if event.is_done() && self.endpoint != Endpoint::ChatCompletions {
      return Err(ConvertError::sse(format!(
        "upstream {} stream received [DONE] before its terminal event",
        self.endpoint.as_str(),
      )));
    }
    let json_kind = event
      .json
      .as_ref()
      .and_then(|value| value.get("type"))
      .and_then(serde_json::Value::as_str);
    if matches!(self.endpoint, Endpoint::Responses | Endpoint::Messages) {
      if let (Some(event_name), Some(kind)) = (event.event.as_deref(), json_kind) {
        if event_name != "message" && event_name != kind {
          return Err(ConvertError::sse("SSE event name disagrees with the JSON type"));
        }
      }
    }
    let kind = json_kind.or(event.event.as_deref());
    let error_envelope = event
      .json
      .as_ref()
      .and_then(|value| value.get("error"))
      .is_some_and(|error| !error.is_null());
    let failure = event.event.as_deref() == Some("error")
      || kind == Some("error")
      || error_envelope
      || matches!(
        (self.endpoint, kind),
        (
          Endpoint::Responses,
          Some("response.failed" | "response.incomplete" | "response.cancelled")
        )
      );
    if failure {
      return Err(ConvertError::sse(format!(
        "upstream {} stream terminated with {}",
        self.endpoint.as_str(),
        kind.unwrap_or("error")
      )));
    }
    self.completed = match self.endpoint {
      Endpoint::ChatCompletions => event.is_done(),
      Endpoint::Responses => kind == Some("response.completed"),
      Endpoint::Messages => kind == Some("message_stop"),
    };
    Ok(self.completed)
  }

  pub fn finish(&self) -> Result<()> {
    if self.completed {
      return Ok(());
    }
    Err(ConvertError::sse(match self.endpoint {
      Endpoint::ChatCompletions => "upstream chat completions stream ended without [DONE]",
      Endpoint::Responses => "upstream responses stream ended without a terminal response event",
      Endpoint::Messages => "upstream messages stream ended without message_stop",
    }))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn requires_each_protocols_success_terminal() {
    for (endpoint, event) in [
      (Endpoint::ChatCompletions, SseEvent::done()),
      (
        Endpoint::Responses,
        SseEvent::json(Some("response.completed"), json!({"type":"response.completed"})),
      ),
      (
        Endpoint::Messages,
        SseEvent::json(Some("message_stop"), json!({"type":"message_stop"})),
      ),
    ] {
      let mut state = TerminalState::new(endpoint);
      assert!(state.finish().is_err());
      assert!(state.observe(&event).unwrap());
      assert!(state.finish().is_ok());
    }
  }

  #[test]
  fn named_errors_and_conflicting_lifecycle_names_cannot_complete() {
    for endpoint in [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages] {
      let mut state = TerminalState::new(endpoint);
      let event = SseEvent::json(Some("error"), json!({"type":"response.completed"}));
      assert!(state.observe(&event).is_err());
      assert!(state.finish().is_err());
    }
    for (endpoint, event_name, kind) in [
      (Endpoint::Responses, "response.failed", "response.completed"),
      (Endpoint::Messages, "content_block_delta", "message_stop"),
    ] {
      let mut state = TerminalState::new(endpoint);
      assert!(state
        .observe(&SseEvent::json(Some(event_name), json!({"type":kind})))
        .is_err());
      assert!(state.finish().is_err());
    }
  }

  #[test]
  fn provider_errors_and_unsuccessful_responses_do_not_complete() {
    for endpoint in [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages] {
      let mut state = TerminalState::new(endpoint);
      assert!(state
        .observe(&SseEvent::json(None, json!({"error":{"message":"failed"}})))
        .is_err());
      assert!(state.finish().is_err());
    }
    for kind in ["response.failed", "response.incomplete", "response.cancelled", "error"] {
      let mut state = TerminalState::new(Endpoint::Responses);
      assert!(state
        .observe(&SseEvent::json(Some(kind), json!({"type":kind})))
        .is_err());
      assert!(state.finish().is_err());
    }
  }
}
