//! Transport-independent processing and completion of an SSE response.

use super::{EventTransformer, SseEvent, TerminalState};
use crate::{provider::Endpoint, Result};
use alloc::collections::VecDeque;

/// The final outcome after processing and delivering a stream's events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamOutcome {
  Complete,
  Failed,
  Cancelled,
}

/// Shared event processing for local and concurrent transport adapters.
///
/// A known upstream endpoint requires its successful protocol terminal, then
/// closes input without waiting for transport EOF. Generic SSE (`None`) closes
/// on EOF. In both cases completion is reported only after all queued output
/// has been taken. Transport adapters report read/framing errors with [`fail`](Self::fail)
/// and abandonment with [`cancel`](Self::cancel).
pub struct StreamProcessor<T> {
  terminal: Option<TerminalState>,
  transformer: T,
  pending: VecDeque<SseEvent>,
  input_closed: bool,
  outcome: Option<StreamOutcome>,
}

impl<T: EventTransformer> StreamProcessor<T> {
  pub fn new(endpoint: Option<Endpoint>, transformer: T) -> Self {
    Self {
      terminal: endpoint.map(TerminalState::new),
      transformer,
      pending: VecDeque::new(),
      input_closed: false,
      outcome: None,
    }
  }

  /// Process one event, validating upstream lifecycle before transforming it.
  /// Further input after termination is ignored.
  pub fn push(&mut self, event: SseEvent) -> Result<()> {
    if self.input_closed {
      return Ok(());
    }
    // Managed protocols commonly send empty keepalive records. Generic SSE
    // retains these events, including their original event names.
    if self.terminal.is_some() && event.data.trim().is_empty() {
      return Ok(());
    }
    let result = self.process(event);
    if result.is_err() {
      self.fail();
    }
    result
  }

  fn process(&mut self, event: SseEvent) -> Result<()> {
    let complete = match self.terminal.as_mut() {
      Some(terminal) => terminal.observe(&event)?,
      None => false,
    };
    self.pending.extend(self.transformer.transform(event)?);
    if complete {
      self.finish_input()?;
    }
    Ok(())
  }

  /// Finish at transport EOF. EOF without a managed protocol terminal fails.
  pub fn finish(&mut self) -> Result<()> {
    if self.input_closed {
      return Ok(());
    }
    let result = (|| {
      if let Some(terminal) = &self.terminal {
        terminal.finish()?;
      }
      self.finish_input()
    })();
    if result.is_err() {
      self.fail();
    }
    result
  }

  fn finish_input(&mut self) -> Result<()> {
    self.pending.extend(self.transformer.finish()?);
    self.input_closed = true;
    self.complete_if_drained();
    Ok(())
  }

  /// Take the next event. Taking the last event completes a closed input.
  pub fn next_event(&mut self) -> Option<SseEvent> {
    let event = self.pending.pop_front();
    self.complete_if_drained();
    event
  }

  /// Whether the adapter should stop reading upstream bytes.
  pub fn input_closed(&self) -> bool {
    self.input_closed
  }

  pub fn outcome(&self) -> Option<StreamOutcome> {
    self.outcome
  }

  /// Discard queued output after a transport or framing failure.
  /// Returns whether this call established the terminal outcome.
  pub fn fail(&mut self) -> bool {
    self.terminate(StreamOutcome::Failed)
  }

  /// Discard queued output when the caller abandons the response.
  /// Returns whether this call established the terminal outcome.
  pub fn cancel(&mut self) -> bool {
    self.terminate(StreamOutcome::Cancelled)
  }

  fn terminate(&mut self, outcome: StreamOutcome) -> bool {
    if self.outcome.is_some() {
      return false;
    }
    self.pending.clear();
    self.input_closed = true;
    self.outcome = Some(outcome);
    true
  }

  fn complete_if_drained(&mut self) {
    if self.input_closed && self.pending.is_empty() && self.outcome.is_none() {
      self.outcome = Some(StreamOutcome::Complete);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{error::ConvertError, sse::EndpointTranslator};
  use alloc::{string::String, vec::Vec};
  use serde_json::json;

  struct Expand {
    finishes: usize,
  }

  impl EventTransformer for Expand {
    fn transform(&mut self, event: SseEvent) -> Result<Vec<SseEvent>> {
      Ok(vec![event.clone(), event])
    }

    fn finish(&mut self) -> Result<Vec<SseEvent>> {
      self.finishes += 1;
      Ok(vec![SseEvent::raw(None, "flushed".into())])
    }
  }

  fn identity(endpoint: Option<Endpoint>) -> StreamProcessor<Option<EndpointTranslator>> {
    StreamProcessor::new(endpoint, None)
  }

  #[test]
  fn completes_only_after_terminal_output_is_delivered() {
    let mut processor = StreamProcessor::new(Some(Endpoint::ChatCompletions), Expand { finishes: 0 });
    processor.push(SseEvent::done()).unwrap();
    assert!(processor.input_closed());
    assert_eq!(processor.outcome(), None);
    processor.push(SseEvent::raw(None, "ignored".into())).unwrap();
    processor.finish().unwrap();
    assert!(processor.next_event().unwrap().is_done());
    assert!(processor.next_event().unwrap().is_done());
    assert_eq!(processor.outcome(), None);
    assert_eq!(processor.next_event().unwrap().data, "flushed");
    assert_eq!(processor.outcome(), Some(StreamOutcome::Complete));
    assert!(processor.next_event().is_none());
    assert!(!processor.cancel());
    assert!(!processor.fail());
    assert_eq!(processor.transformer.finishes, 1);
  }

  #[test]
  fn cancellation_discards_terminal_output_and_is_reported_once() {
    let mut processor = StreamProcessor::new(Some(Endpoint::ChatCompletions), Expand { finishes: 0 });
    processor.push(SseEvent::done()).unwrap();
    processor.next_event().unwrap();
    assert!(processor.cancel());
    assert!(!processor.cancel());
    assert!(!processor.fail());
    assert_eq!(processor.outcome(), Some(StreamOutcome::Cancelled));
    assert!(processor.next_event().is_none());
    processor.finish().unwrap();
    assert_eq!(processor.outcome(), Some(StreamOutcome::Cancelled));
  }

  #[test]
  fn requires_protocol_terminal_even_after_semantic_finish() {
    for (endpoint, event) in [
      (
        Endpoint::ChatCompletions,
        SseEvent::json(None, json!({"choices":[{"delta":{}, "finish_reason":"stop"}]})),
      ),
      (
        Endpoint::Messages,
        SseEvent::json(
          Some("message_delta"),
          json!({"type":"message_delta", "delta":{"stop_reason":"end_turn"}}),
        ),
      ),
      (
        Endpoint::Responses,
        SseEvent::json(Some("response.created"), json!({"type":"response.created"})),
      ),
    ] {
      let mut processor = StreamProcessor::new(
        Some(endpoint),
        Some(EndpointTranslator::with_created_at(endpoint, Endpoint::Responses, 0)),
      );
      processor.push(event).unwrap();
      while let Some(output) = processor.next_event() {
        assert_ne!(output.event.as_deref(), Some("response.completed"));
      }
      assert!(processor.finish().is_err());
      assert!(processor.input_closed());
      assert_eq!(processor.outcome(), Some(StreamOutcome::Failed));
      assert!(processor.next_event().is_none());
    }
  }

  #[test]
  fn another_protocols_done_sentinel_cannot_synthesize_success() {
    for from in [Endpoint::Responses, Endpoint::Messages] {
      for to in [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages] {
        let mut processor = StreamProcessor::new(
          Some(from),
          (from != to).then(|| EndpointTranslator::with_created_at(from, to, 0)),
        );
        assert!(processor.push(SseEvent::done()).is_err());
        assert_eq!(processor.outcome(), Some(StreamOutcome::Failed));
        assert!(processor.next_event().is_none());
      }
    }
  }

  #[test]
  fn each_native_terminal_closes_before_transport_eof() {
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
      let mut processor = identity(Some(endpoint));
      processor.push(event).unwrap();
      assert!(processor.input_closed());
      assert_eq!(processor.outcome(), None);
      assert!(processor.next_event().is_some());
      assert_eq!(processor.outcome(), Some(StreamOutcome::Complete));
    }
  }

  #[test]
  fn rejects_provider_failures_before_transforming() {
    for event in [
      SseEvent::json(Some("response.failed"), json!({"type":"response.failed"})),
      SseEvent::json(Some("response.incomplete"), json!({"type":"response.incomplete"})),
      SseEvent::json(Some("response.cancelled"), json!({"type":"response.cancelled"})),
      SseEvent::json(Some("error"), json!({"error":{"message":"failed"}})),
      SseEvent::json(Some("response.failed"), json!({"type":"response.completed"})),
    ] {
      let mut processor = StreamProcessor::new(Some(Endpoint::Responses), Expand { finishes: 0 });
      assert!(processor.push(event).is_err());
      assert_eq!(processor.outcome(), Some(StreamOutcome::Failed));
      assert_eq!(processor.transformer.finishes, 0);
      assert!(processor.next_event().is_none());
    }
  }

  struct Fail {
    on_finish: bool,
  }

  impl EventTransformer for Fail {
    fn transform(&mut self, event: SseEvent) -> Result<Vec<SseEvent>> {
      if self.on_finish {
        Ok(vec![event])
      } else {
        Err(ConvertError::sse("transform failed"))
      }
    }

    fn finish(&mut self) -> Result<Vec<SseEvent>> {
      Err(ConvertError::sse("finish failed"))
    }
  }

  #[test]
  fn transform_finish_and_transport_failures_are_terminal() {
    for on_finish in [false, true] {
      let mut processor = StreamProcessor::new(Some(Endpoint::ChatCompletions), Fail { on_finish });
      assert!(processor.push(SseEvent::done()).is_err());
      assert_eq!(processor.outcome(), Some(StreamOutcome::Failed));
      assert!(processor.next_event().is_none());
      assert!(!processor.fail());
    }
    let mut processor = identity(Some(Endpoint::ChatCompletions));
    processor.push(SseEvent::json(None, json!({"delta":"hello"}))).unwrap();
    assert!(processor.fail());
    assert!(processor.next_event().is_none());
    assert_eq!(processor.outcome(), Some(StreamOutcome::Failed));
  }

  #[test]
  fn generic_sse_retains_empty_events_and_finishes_on_eof() {
    let mut processor = StreamProcessor::new(None, Expand { finishes: 0 });
    processor
      .push(SseEvent::raw(Some("empty".into()), String::new()))
      .unwrap();
    assert!(!processor.input_closed());
    processor.finish().unwrap();
    assert!(processor.next_event().unwrap().data.is_empty());
    assert!(processor.next_event().unwrap().data.is_empty());
    assert_eq!(processor.next_event().unwrap().data, "flushed");
    assert_eq!(processor.outcome(), Some(StreamOutcome::Complete));

    let mut managed = identity(Some(Endpoint::ChatCompletions));
    managed.push(SseEvent::raw(None, " \t".into())).unwrap();
    assert!(managed.next_event().is_none());
    assert!(managed.finish().is_err());
  }
}
