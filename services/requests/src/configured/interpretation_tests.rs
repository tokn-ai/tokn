use super::*;
use crate::interpretation::RequestInterpretation;
use crate::session::{ExecutionState, RequestInput};
use crate::{driver::Session, stage_traits::ExtractStage, stages::DefaultExtract, EventBus, PipelineCtx, RawInbound};
use core::convert::Infallible;
use serde_json::{json, Value};
use std::sync::Arc;
use tokn_core::request_classification::{RequestClassificationSource, RequestPurpose};
use tokn_core::{
  types::{Bytes, Shared},
  Endpoint,
};

struct NoIo;

impl Transport for NoIo {
  type Error = Infallible;

  async fn send(&mut self, _: Request) -> Result<ResponseHead, Self::Error> {
    panic!("interpretation must not perform I/O")
  }

  async fn read(&mut self, _: &mut [u8]) -> Result<usize, Self::Error> {
    panic!("interpretation must not perform I/O")
  }
}

impl Runtime for NoIo {
  fn now(&self) -> Duration {
    Duration::ZERO
  }

  fn unix_seconds(&self) -> u64 {
    0
  }

  async fn sleep(&mut self, _: Duration) {
    panic!("interpretation must not sleep")
  }
}

// Exercise the two actual extraction stages, including metadata that does not
// have a provider-visible effect.
async fn extract_both(endpoint: Endpoint, body: Value, options: RequestOptions) -> RequestInterpretation {
  let headers = options.inbound_headers();
  let bytes = Bytes::from(serde_json::to_vec(&body).unwrap());
  let standard = DefaultExtract
    .extract(
      &PipelineCtx::new("parity", endpoint.into(), Arc::new(EventBus::new(16))),
      RawInbound {
        request_endpoint: endpoint.into(),
        headers: headers.clone(),
        raw_body: bytes.clone(),
        decoded_body: bytes,
        body_json: body.clone(),
        request_id: None,
      },
    )
    .await
    .unwrap();
  let raw = serde_json::from_value(json!({
    "schema_version": 2,
    "defaults": {"provider": {"kind": "fixed", "provider": "openai"}},
    "providers": {"openai": {"driver": "openai"}}
  }))
  .unwrap();
  let plan = tokn_config::v2::compile_execution(&raw).unwrap();
  let state = Shared::new(ExecutionState::new(plan, Vec::new()).unwrap());
  let platform = Adapter::new(NoIo, NoIo);
  let (transport, clock, events) = platform.acquire().unwrap();
  let mut input = RequestInput::new(endpoint, body, headers, "parity".into());
  input.profile = options.profile;
  let mut local = crate::session::Session::new(state, input, transport, clock, events);
  let injected = local.extract().await.unwrap();
  assert_eq!(
    injected,
    RequestInterpretation {
      model: standard.model,
      stream: standard.stream,
      session_id: standard.session_id,
      project_id: standard.project_id,
      initiator: standard.initiator,
      header_initiator: standard.header_initiator,
      request_classification: standard.request_classification,
      route_mode_hint: standard.route_mode_hint,
    },
    "standard and injected extraction disagree"
  );
  injected
}

#[tokio::test]
async fn request_purpose_is_interpreted_before_provider_conversion() {
  for (endpoint, body, source) in [
    (
      Endpoint::ChatCompletions,
      json!({"model":"m", "messages":[{"role":"user", "content":
        "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary for another LLM that will resume the task."
      }]}),
      RequestClassificationSource::CodexPrompt,
    ),
    (
      Endpoint::Responses,
      json!({"model":"m", "input":[{"type":"compaction_trigger"}]}),
      RequestClassificationSource::RequestField,
    ),
    (
      Endpoint::Messages,
      json!({"model":"m", "messages":[], "compaction":{"type":"summarize"}}),
      RequestClassificationSource::RequestField,
    ),
  ] {
    let interpreted = extract_both(endpoint, body, RequestOptions::default()).await;
    let classification = interpreted.request_classification.expect("compaction classification");
    assert_eq!(classification.purpose, RequestPurpose::Compaction);
    assert_eq!(classification.source, source);
  }
  let normal = extract_both(
    Endpoint::Responses,
    json!({"model":"m", "input":[{"type":"compaction_trigger"}, {"role":"user", "content":"hello"}]}),
    RequestOptions::default(),
  )
  .await;
  assert_eq!(normal.request_classification, None, "history is not the active request");
}

#[tokio::test]
async fn header_precedence_and_normalization_match_standard_metadata() {
  let interpreted = extract_both(
    Endpoint::ChatCompletions,
    json!({"model":"m", "stream":false, "tools":[{"type":"function"}]}),
    RequestOptions::default()
      .with_session_id("option-session")
      .with_project_id("option-project")
      .with_initiator("user")
      .with_header("X-Session-Id", " ")
      .with_header("session_id", " alias-session ")
      .with_header("x-opencode-project", " /project ")
      .with_header("X-Initiator", "invalid")
      .with_header("x-route-mode", " exact ")
      .with_header("Accept", "text/event-stream"),
  )
  .await;
  assert_eq!(interpreted.session_id.as_deref(), Some("alias-session"));
  assert_eq!(interpreted.project_id.as_deref(), Some("/project"));
  assert_eq!(interpreted.header_initiator, None);
  assert_eq!(interpreted.initiator.as_deref(), Some("agent"));
  assert_eq!(interpreted.route_mode_hint.as_deref(), Some("exact"));
  assert!(!interpreted.stream, "body boolean takes precedence over Accept");
}

#[tokio::test]
async fn missing_or_invalid_model_preserves_standard_default_route_input() {
  for body in [
    Value::Null,
    json!([]),
    json!({}),
    json!({"model":""}),
    json!({"model":42}),
  ] {
    let interpreted = extract_both(Endpoint::ChatCompletions, body, RequestOptions::default()).await;
    assert_eq!(interpreted.model, "unknown");
  }
}
