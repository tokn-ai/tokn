//! Event payload types for the `tokn-requests` pipeline.
//!
//! These types live in `tokn-core` so standard and injected execution publish
//! the same values without depending on a platform event bus.
//!
//! Three payload shapes are supported as peers under
//! [`RequestEventPayload`]:
//!
//! * [`StageEvent`] — a closed enum of lifecycle/observation variants
//!   the runner emits at well-defined points (Started, per-stage
//!   summaries, Error, Completed). Subscribers `match` on them.
//! * [`RecordEvent`] — transport-adjacent captures that sit alongside the
//!   stage lifecycle (inbound connection facts, outbound wire-truth,
//!   parsed usage). Split from `StageEvent` so subscribers that only care
//!   about one axis don't pay a match-arm tax for the other.
//! * [`CustomEvent`] — an `Any`-typed escape hatch for middleware /
//!   decorator stages (e.g. retry, cache) to publish their own
//!   structured records without modifying either of the closed enums.
//!
//! [`RequestEventSink`] accepts these values synchronously. Standard builds
//! provide `tokn_core::event::EventBus` as a Tokio broadcast adapter; embedded
//! applications supply a callback or another local sink.

pub mod record;
pub mod stage;

pub use record::{IngressRejection, RecordEvent};
pub use stage::{
  BuiltHeadersSummary, ConvertedRequestSummary, ConvertedResponseSummary, ExtractedSummary, RequestEndpoint,
  ResolvedSummary, SentSummary, Stage, StageEvent,
};

use crate::types::{Shared, SmolStr};
use core::any::Any;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

/// A single requests pipeline event. Carries the per-request bookkeeping
/// (request_id, attempt, ts) plus a typed or `Any`-typed payload.
/// `ts` is a millisecond-precision unix timestamp captured at emission time.
#[derive(Clone, Debug, Serialize)]
pub struct RequestEvent {
  pub request_id: SmolStr,
  pub attempt: u32,
  pub ts: i64,
  pub payload: RequestEventPayload,
}

/// Application-owned delivery for the shared request event values.
///
/// Delivery is synchronous and infallible from the pipeline's perspective.
/// Implementations decide whether to retain, forward, or discard an event;
/// they should return promptly and avoid re-entering the emitting runtime.
pub trait RequestEventSink {
  fn emit_request(&mut self, event: RequestEvent);
}

impl<F: FnMut(RequestEvent)> RequestEventSink for F {
  fn emit_request(&mut self, event: RequestEvent) {
    self(event);
  }
}

/// One of three payload shapes carried on a [`RequestEvent`].
///
/// - [`Stage`](RequestEventPayload::Stage) — closed-set lifecycle /
///   per-stage observation events.
/// - [`Record`](RequestEventPayload::Record) — transport-adjacent captures
///   such as inbound connection facts, outbound wire-truth, and usage.
/// - [`Custom`](RequestEventPayload::Custom) — `Any`-typed escape hatch
///   for middleware / decorator stages.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "category", content = "event", rename_all = "snake_case")]
pub enum RequestEventPayload {
  Stage(StageEvent),
  Record(RecordEvent),
  Custom(CustomEvent),
}

/// `Any`-typed payload published by stages or decorators that need to share
/// structured data outside the closed [`StageEvent`] set.
#[derive(Clone)]
pub struct CustomEvent {
  /// Stable namespaced identifier (e.g. `"retry.attempt"`). Subscribers match
  /// on this before downcasting.
  pub kind: &'static str,
  /// Reference-counted so subscribers can cheaply clone the event and
  /// downcast independently.
  #[cfg(feature = "std")]
  pub payload: Shared<dyn Any + Send + Sync>,
  /// Locally reference-counted so subscribers can clone and downcast the event
  /// without requiring thread-safe payloads.
  #[cfg(not(feature = "std"))]
  pub payload: Shared<dyn Any>,
}

impl core::fmt::Debug for CustomEvent {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("CustomEvent")
      .field("kind", &self.kind)
      .field("payload", &"<Any>")
      .finish()
  }
}

impl Serialize for CustomEvent {
  /// Serialize the stable custom-event kind. The type-erased Rust payload is
  /// intentionally unavailable to cross-language event consumers.
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut state = serializer.serialize_struct("CustomEvent", 1)?;
    state.serialize_field("kind", self.kind)?;
    state.end()
  }
}

impl CustomEvent {
  #[cfg(feature = "std")]
  pub fn new<T: Any + Send + Sync>(kind: &'static str, value: T) -> Self {
    Self {
      kind,
      payload: Shared::new(value),
    }
  }

  #[cfg(not(feature = "std"))]
  pub fn new<T: Any>(kind: &'static str, value: T) -> Self {
    Self {
      kind,
      payload: Shared::new(value),
    }
  }

  pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
    self.payload.downcast_ref::<T>()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::vec::Vec;
  use serde_json::json;

  fn event(payload: RequestEventPayload) -> RequestEvent {
    RequestEvent {
      request_id: SmolStr::new("request-1"),
      attempt: 2,
      ts: 1_700_000_000_123,
      payload,
    }
  }

  #[test]
  fn event_serialization_keeps_existing_tags_and_fields() {
    let stage = event(RequestEventPayload::Stage(StageEvent::ConvertResponse(
      ConvertedResponseSummary {
        status: 200,
        headers: crate::headers::HeaderMap::new(),
        body: Some(Shared::new(json!({"id": "response-1"}))),
      },
    )));
    assert_eq!(
      serde_json::to_value(stage).unwrap(),
      json!({
        "request_id": "request-1",
        "attempt": 2,
        "ts": 1_700_000_000_123_i64,
        "payload": {
          "category": "stage",
          "event": {
            "type": "convert_response",
            "data": {"status": 200, "headers": {}, "body": {"id": "response-1"}}
          }
        }
      }),
    );

    let usage = event(RequestEventPayload::Record(RecordEvent::Usage(crate::usage::Usage {
      input_tokens: Some(12),
      output_tokens: Some(5),
      total_tokens: Some(17),
      usage_type: Some(crate::usage::UsageType::Chat),
      details: crate::usage::UsageDetails {
        cache_read: Some(4),
        cache_write: None,
        reasoning: Some(2),
      },
    })));
    assert_eq!(
      serde_json::to_value(usage).unwrap()["payload"],
      json!({
        "category": "record",
        "event": {
          "type": "usage",
          "data": {
            "input_tokens": 12, "output_tokens": 5, "total_tokens": 17,
            "usage_type": "chat",
            "details": {"cache_read": 4, "cache_write": null, "reasoning": 2}
          }
        }
      }),
    );

    let body = event(RequestEventPayload::Record(RecordEvent::UpstreamBody {
      body: crate::types::Bytes::from_static(b"ok"),
      error: None,
    }));
    assert_eq!(
      serde_json::to_value(body).unwrap()["payload"],
      json!({"category": "record", "event": {"type": "upstream_body", "data": {"body": [111, 107], "error": null}}}),
    );

    let custom = event(RequestEventPayload::Custom(CustomEvent::new("test.value", 42_u32)));
    assert_eq!(
      serde_json::to_value(custom).unwrap()["payload"],
      json!({"category": "custom", "event": {"kind": "test.value"}}),
    );
  }

  #[test]
  fn shared_summary_fields_serialize_as_their_values() {
    let body = Shared::new(json!({"messages": [{"role": "user", "content": "hello"}]}));
    let extracted = ExtractedSummary {
      agent_id: None,
      request_classification: None,
      model: SmolStr::new("test-model"),
      stream: false,
      session_id: None,
      project_id: None,
      initiator: None,
      header_initiator: None,
      route_mode_hint: None,
      headers: crate::headers::HeaderMap::new(),
      raw_body: crate::types::Bytes::new(),
      decoded_body: crate::types::Bytes::new(),
      body_json: body.clone(),
    };
    assert_eq!(serde_json::to_value(extracted).unwrap()["body_json"], *body);
    let converted = ConvertedRequestSummary {
      upstream_body: body.clone(),
      upstream_wire_body: crate::types::Bytes::new(),
      debug_outbound_body: crate::types::Bytes::new(),
      content_encoding: None,
    };
    assert_eq!(serde_json::to_value(converted).unwrap()["upstream_body"], *body);
    for optional_body in [None, Some(body)] {
      let expected = optional_body.as_deref().cloned().unwrap_or(serde_json::Value::Null);
      let converted = ConvertedResponseSummary {
        status: 200,
        headers: crate::headers::HeaderMap::new(),
        body: optional_body,
      };
      assert_eq!(serde_json::to_value(converted).unwrap()["body"], expected);
    }
  }

  #[test]
  fn closure_sink_can_retain_shared_event_values() {
    let mut events = Vec::new();
    let mut sink = |event| events.push(event);
    sink.emit_request(event(RequestEventPayload::Stage(StageEvent::Completed {
      success: true,
      attempts: 3,
    })));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].attempt, 2);
    assert!(matches!(
      events[0].payload,
      RequestEventPayload::Stage(StageEvent::Completed {
        success: true,
        attempts: 3
      })
    ));
  }

  #[cfg(feature = "std")]
  #[test]
  fn standard_events_remain_send_sync_and_broadcast_through_sink() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RequestEvent>();
    assert_send_sync::<CustomEvent>();

    let mut bus = crate::event::EventBus::new(2);
    let mut receiver = bus.subscribe();
    RequestEventSink::emit_request(
      &mut bus,
      event(RequestEventPayload::Custom(CustomEvent::new("test.bus", 1))),
    );
    let mut shared_bus = Shared::new(bus);
    RequestEventSink::emit_request(
      &mut shared_bus,
      event(RequestEventPayload::Custom(CustomEvent::new("test.shared_bus", 2))),
    );
    for kind in ["test.bus", "test.shared_bus"] {
      let received = receiver.try_recv().unwrap();
      let crate::event::Event::Requests(received) = received.as_ref() else {
        panic!("sink should deliver the request event");
      };
      assert!(matches!(&received.payload, RequestEventPayload::Custom(custom) if custom.kind == kind));
    }
  }

  #[cfg(not(feature = "std"))]
  #[test]
  fn local_custom_events_accept_non_send_values_and_share_ownership() {
    let value = Shared::new(core::cell::Cell::new(1));
    let event = CustomEvent::new("test.local", value.clone());
    let retained = event.clone();
    drop(event);
    value.set(2);
    assert_eq!(
      retained.downcast_ref::<Shared<core::cell::Cell<i32>>>().unwrap().get(),
      2
    );
    assert_eq!(Shared::strong_count(&value), 2);
    drop(retained);
    assert_eq!(Shared::strong_count(&value), 1);
  }

  #[test]
  fn custom_event_roundtrips_via_downcast() {
    #[derive(Debug, PartialEq)]
    struct RetryAttempt {
      n: u32,
      reason: SmolStr,
    }

    let ev = CustomEvent::new(
      "retry.attempt",
      RetryAttempt {
        n: 2,
        reason: SmolStr::new("timeout"),
      },
    );
    assert_eq!(ev.kind, "retry.attempt");
    let inner = ev
      .downcast_ref::<RetryAttempt>()
      .expect("payload should downcast back to its declared type");
    assert_eq!(
      inner,
      &RetryAttempt {
        n: 2,
        reason: SmolStr::new("timeout"),
      }
    );
    assert!(
      ev.downcast_ref::<u32>().is_none(),
      "downcast to the wrong type must return None"
    );
  }
}
