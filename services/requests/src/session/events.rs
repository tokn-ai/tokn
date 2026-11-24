//! Shared typed event construction, request records, and lifecycle delivery.

use super::*;
use tokn_core::request_event::{
  BuiltHeadersSummary, ConvertedRequestSummary, ExtractedSummary, RequestEvent, ResolvedSummary,
};

/// Streaming telemetry independent of a delivery mechanism.
pub struct StreamProgress<'a> {
  pub request_id: &'a str,
  pub model: &'a str,
  pub endpoint: Endpoint,
  pub usage: &'a tokn_core::usage::Usage,
  pub bytes_streamed: u64,
  pub chunks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Lifecycle<'a> {
  pub request_id: &'a str,
  pub attempt: u32,
  pub stage: &'static str,
  pub success: Option<bool>,
}

pub trait Events {
  fn progress(&self, _progress: StreamProgress<'_>) {}
  fn enabled(&self) -> bool {
    true
  }
  fn emit(&self, event: RequestEvent);
  fn observe(&self, _event: Lifecycle<'_>) {}
}

pub(super) struct Observer<C, E> {
  pub(super) clock: C,
  pub(super) events: E,
}
impl<C: Clock, E: Events> Observer<C, E> {
  pub(super) fn emit_event(
    &self,
    request_id: &str,
    attempt: u32,
    legacy: Option<(&'static str, Option<bool>)>,
    payload: impl FnOnce() -> Option<RequestEventPayload>,
  ) {
    if self.events.enabled() {
      if let Some(payload) = payload() {
        self.events.emit(RequestEvent {
          request_id: SmolStr::new(request_id),
          attempt,
          ts: self.clock.unix_millis(),
          payload,
        });
      }
    }
    if let Some((stage, success)) = legacy {
      self.events.observe(Lifecycle {
        request_id,
        attempt,
        stage,
        success,
      });
    }
  }
  pub(super) fn emit_usage(&self, request_id: &str, attempt: u32, value: &Value) {
    self.emit_event(request_id, attempt, None, || {
      tokn_convert::usage::observed_usage(value).map(|usage| RequestEventPayload::Record(RecordEvent::Usage(usage)))
    });
  }
  pub(super) fn emit_complete(&self, request_id: &str, attempt: u32, success: bool) {
    self.emit_event(request_id, attempt, Some(("completed", Some(success))), || {
      Some(RequestEventPayload::Stage(StageEvent::Completed {
        success,
        attempts: attempt.saturating_add(1),
      }))
    });
  }
  pub(super) fn emit_error(&self, request_id: &str, attempt: u32, stage: Stage, error: &Error, legacy: bool) {
    self.emit_event(
      request_id,
      attempt,
      legacy.then_some((error.stage, Some(false))),
      || {
        Some(RequestEventPayload::Stage(StageEvent::Error {
          stage,
          message: error.message.clone(),
          recoverable: error.recoverable,
          stop: error.stop,
        }))
      },
    );
  }
}

impl<T: Transport, C: Clock, E: Events> Session<T, C, E> {
  pub(crate) fn emit_stage(&self, stage: Stage, payload: impl FnOnce() -> StageEvent) {
    self.observer().emit_event(
      &self.input.request_id,
      self.attempt,
      Some((stage.as_str(), None)),
      || Some(RequestEventPayload::Stage(payload())),
    );
  }

  pub(crate) fn emit_extracted(&self, extracted: &RequestInterpretation) {
    self.emit_stage(Stage::Extract, || {
      let body = self.input.raw_body.clone();
      StageEvent::Extract(ExtractedSummary {
        agent_id: None,
        request_classification: extracted.request_classification,
        model: extracted.model.clone(),
        stream: extracted.stream,
        session_id: extracted.session_id.clone(),
        project_id: extracted.project_id.clone(),
        initiator: extracted.initiator.clone(),
        header_initiator: extracted.header_initiator.clone(),
        route_mode_hint: extracted.route_mode_hint.clone(),
        headers: self.input.headers.clone(),
        raw_body: body.clone(),
        decoded_body: self.input.decoded_body.clone(),
        body_json: Shared::new(self.input.body.clone()),
      })
    });
  }

  pub(crate) fn emit_resolved(&self, extracted: &RequestInterpretation, resolved: &Resolved) {
    self.emit_stage(Stage::Resolve, || {
      StageEvent::Resolve(ResolvedSummary {
        agent_id: self.wire_identity(resolved),
        model: extracted.model.clone(),
        resolved_endpoint: Some(self.input.endpoint),
        upstream_model: resolved.model.clone(),
        upstream_endpoint: Some(resolved.endpoint),
        account_id: match resolved.target {
          Target::Account { binding, .. } => self.state.bindings[binding].account.id.clone().into(),
          Target::Client => SmolStr::new("proxy"),
        },
        provider_id: match resolved.target {
          Target::Account { binding, .. } => self.state.bindings[binding].driver_identity.clone(),
          Target::Client => SmolStr::new(resolved.provider.as_str()),
        },
      })
    });
  }

  pub(crate) fn emit_headers(&self, headers: &HeaderMap, resolved: &Resolved) {
    self.emit_stage(Stage::BuildHeaders, || {
      StageEvent::BuildHeaders(BuiltHeadersSummary {
        headers: headers.clone(),
        vars: self.template_vars(),
        agent_id: self.header_identity(resolved),
      })
    });
  }

  pub(crate) fn emit_converted_request(&self, prepared: &PreparedRequest, shaped: &Value, decoded: &Bytes) {
    self.emit_stage(Stage::ConvertRequest, || {
      StageEvent::ConvertRequest(ConvertedRequestSummary {
        upstream_body: Shared::new(shaped.clone()),
        upstream_wire_body: prepared.body.clone(),
        debug_outbound_body: decoded.clone(),
        content_encoding: self.input.content_encoding.as_deref().map(SmolStr::new),
      })
    });
  }

  pub(crate) fn report_failure(&mut self, error: &Error, legacy: bool) {
    self.terminal = true;
    self
      .observer()
      .emit_error(&self.input.request_id, self.attempt, self.stage, error, legacy);
    self
      .observer()
      .emit_complete(&self.input.request_id, self.attempt, false);
  }
}

impl<T: Transport, C: Clock, E: Events> Session<T, C, E> {
  pub(super) fn emit_record(&self, record: impl FnOnce() -> RecordEvent) {
    self
      .observer()
      .emit_event(&self.input.request_id, self.attempt, None, || {
        Some(RequestEventPayload::Record(record()))
      });
  }
}

#[cfg(test)]
mod observer_tests {
  use super::*;
  use core::cell::Cell;

  struct NoClock;
  impl Clock for NoClock {
    fn now(&self) -> Duration {
      Duration::ZERO
    }
    fn unix_seconds(&self) -> u64 {
      panic!("disabled events must not read wall time")
    }
    async fn sleep(&mut self, _: Duration) {
      panic!("observer must not sleep")
    }
  }
  struct NoSink {
    observations: Cell<usize>,
  }
  impl Events for NoSink {
    fn enabled(&self) -> bool {
      false
    }
    fn emit(&self, _: RequestEvent) {
      panic!("disabled sink must not receive events")
    }
    fn observe(&self, _: Lifecycle<'_>) {
      self.observations.set(self.observations.get() + 1);
    }
  }

  #[test]
  fn disabled_typed_sink_skips_payload_and_clock_but_preserves_lifecycle() {
    let observer = Observer {
      clock: NoClock,
      events: NoSink {
        observations: Cell::new(0),
      },
    };
    observer.emit_event("request", 0, Some(("extract", None)), || {
      panic!("unused payload must not be built")
    });
    observer.emit_event("request", 0, None, || panic!("unused usage must not be parsed"));
    assert_eq!(observer.events.observations.get(), 1);
  }
}
