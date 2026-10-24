//! Local delivery of the same request event values used by the standard bus.

use super::{Error, Injected, Lifecycle, LocalSession, Resolved, Runtime, Target};
use crate::interpretation::RequestInterpretation;
use core::time::Duration;
use serde_json::Value;
use tokn_core::{
  headers::HeaderMap,
  provider::PreparedRequest,
  request_event::{
    BuiltHeadersSummary, ConvertedRequestSummary, ExtractedSummary, RecordEvent, RequestEvent, RequestEventPayload,
    RequestEventSink, ResolvedSummary, Stage, StageEvent,
  },
  types::{Bytes, Shared, SmolStr},
};
use tokn_service::transport::Transport;

/// Adds a typed event sink to application-supplied runtime services.
/// Neither the runtime nor the sink needs to be `Send` or `'static`.
pub struct EventRuntime<R, S> {
  runtime: R,
  sink: S,
}

impl<R, S> EventRuntime<R, S> {
  pub fn new(runtime: R, sink: S) -> Self {
    Self { runtime, sink }
  }

  pub fn into_parts(self) -> (R, S) {
    (self.runtime, self.sink)
  }
}

impl<R: Runtime, S: RequestEventSink> Runtime for EventRuntime<R, S> {
  fn now(&self) -> Duration {
    self.runtime.now()
  }

  fn unix_seconds(&self) -> u64 {
    self.runtime.unix_seconds()
  }

  fn unix_millis(&self) -> i64 {
    self.runtime.unix_millis()
  }

  async fn sleep(&mut self, duration: Duration) {
    self.runtime.sleep(duration).await;
  }

  fn event_sink(&mut self) -> Option<&mut dyn RequestEventSink> {
    Some(&mut self.sink)
  }

  fn observe(&mut self, event: Lifecycle<'_>) {
    self.runtime.observe(event);
  }
}

impl<T: Transport, R: Runtime> Injected<T, R> {
  pub(super) fn emit_event(
    &self,
    request_id: &str,
    attempt: u32,
    legacy: Option<(&'static str, Option<bool>)>,
    payload: impl FnOnce() -> Option<RequestEventPayload>,
  ) {
    // Keep the runtime borrowed through both callbacks. A sink can observe a
    // released stream, but synchronous request reentry still returns busy.
    let mut runtime = self.runtime.borrow_mut();
    let ts = runtime.unix_millis();
    if let Some(sink) = runtime.event_sink() {
      if let Some(payload) = payload() {
        sink.emit_request(RequestEvent {
          request_id: SmolStr::new(request_id),
          attempt,
          ts,
          payload,
        });
      }
    }
    if let Some((stage, success)) = legacy {
      runtime.observe(Lifecycle {
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
          stop: false,
        }))
      },
    );
  }
}

impl<T: Transport, R: Runtime> LocalSession<'_, T, R> {
  pub(super) fn emit_stage(&self, stage: Stage, payload: impl FnOnce() -> StageEvent) {
    self
      .platform
      .emit_event(&self.request_id, self.attempt, Some((stage.as_str(), None)), || {
        Some(RequestEventPayload::Stage(payload()))
      });
  }

  pub(super) fn emit_extracted(&self, extracted: &RequestInterpretation) {
    self.emit_stage(Stage::Extract, || {
      // Injected callers supply a JSON value, so there are no distinct ingress
      // wire bytes or transport encodings to retain in this summary.
      let body = Bytes::from(serde_json::to_vec(&self.body).expect("JSON value serializes"));
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
        headers: self.headers.clone(),
        raw_body: body.clone(),
        decoded_body: body,
        body_json: Shared::new(self.body.clone()),
      })
    });
  }

  pub(super) fn emit_resolved(&self, extracted: &RequestInterpretation, resolved: &Resolved) {
    self.emit_stage(Stage::Resolve, || {
      StageEvent::Resolve(ResolvedSummary {
        agent_id: None,
        model: extracted.model.clone(),
        resolved_endpoint: Some(self.endpoint),
        upstream_model: resolved.model.clone(),
        upstream_endpoint: Some(resolved.endpoint),
        account_id: match resolved.target {
          Target::Account { binding, .. } => self.platform.bindings[binding].account.id.clone().into(),
          Target::Client => SmolStr::new("proxy"),
        },
        provider_id: match resolved.target {
          Target::Account { binding, .. } => self.platform.bindings[binding].driver_identity.clone(),
          Target::Client => SmolStr::new(resolved.provider.as_str()),
        },
      })
    });
  }

  pub(super) fn emit_headers(&self, headers: &HeaderMap, resolved: &Resolved) {
    self.emit_stage(Stage::BuildHeaders, || {
      StageEvent::BuildHeaders(BuiltHeadersSummary {
        headers: headers.clone(),
        vars: self.template_vars(),
        agent_id: self.wire_identity(resolved).unwrap_or_default(),
      })
    });
  }

  pub(super) fn emit_converted_request(&self, prepared: &PreparedRequest) {
    self.emit_stage(Stage::ConvertRequest, || {
      StageEvent::ConvertRequest(ConvertedRequestSummary {
        upstream_body: Shared::new(serde_json::from_slice(&prepared.body).expect("prepared request is JSON")),
        upstream_wire_body: prepared.body.clone(),
        debug_outbound_body: prepared.body.clone(),
        content_encoding: None,
      })
    });
  }

  pub(super) fn report_failure(&mut self, error: &Error, legacy: bool) {
    self.terminal = true;
    self
      .platform
      .emit_error(&self.request_id, self.attempt, self.stage, error, legacy);
    self.platform.emit_complete(&self.request_id, self.attempt, false);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use core::{cell::Cell, convert::Infallible};
  use tokn_service::transport::{Request, ResponseHead};

  #[derive(Default)]
  struct Clock {
    observations: usize,
  }

  impl Runtime for Clock {
    fn now(&self) -> Duration {
      Duration::ZERO
    }
    fn unix_seconds(&self) -> u64 {
      u64::MAX
    }
    async fn sleep(&mut self, _: Duration) {}
    fn observe(&mut self, _: Lifecycle<'_>) {
      self.observations += 1;
    }
  }

  #[test]
  fn default_millisecond_clock_saturates() {
    assert_eq!(Clock::default().unix_millis(), i64::MAX);
  }

  struct NoIo;
  impl Transport for NoIo {
    type Error = Infallible;
    async fn send(&mut self, _: Request) -> Result<ResponseHead, Self::Error> {
      panic!("event delivery must not perform I/O")
    }
    async fn read(&mut self, _: &mut [u8]) -> Result<usize, Self::Error> {
      panic!("event delivery must not perform I/O")
    }
  }

  #[test]
  fn no_typed_sink_skips_payload_construction_and_preserves_legacy_observer() {
    let platform = Injected {
      plan: Default::default(),
      destinations: Default::default(),
      bindings: Default::default(),
      pools: Default::default(),
      transport: core::cell::RefCell::new(NoIo),
      runtime: core::cell::RefCell::new(Clock::default()),
      limits: Default::default(),
      request_sequence: Cell::new(0),
    };
    platform.emit_event("request", 0, Some(("extract", None)), || {
      panic!("unused event payload must not be constructed")
    });
    platform.emit_event("request", 0, None, || panic!("unused usage must not be parsed"));
    assert_eq!(platform.runtime.borrow().observations, 1);
  }
}
