//! Default Extract stage — turns a [`RawInbound`] into normalized
//! [`Extracted`] state.
//!
//! Behavior is a clean reimplementation of the legacy
//! `crates/router/src/pipeline/parse.rs::{request_header_extract,
//! request_body_extract, RequestParser::parse}`, with all small strings stored
//! as [`SmolStr`].
//!
//! Header name lists are duplicated here intentionally to keep requests free
//! of any dependency on the legacy `crates/router` crate. PR2 will move the
//! canonical constants to a shared location.

pub mod passthrough;
pub use passthrough::PassthroughExtract;

use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::error::PipelineError;
use crate::pipeline::stages::{ExtractStage, Extracted, RawInbound};
use crate::utils::codec::request_content_encoding;
use async_trait::async_trait;
use serde_json::Value;
use smol_str::SmolStr;
use std::sync::Arc;
use tokn_core::request_classification::classify_request;
use tokn_core::util::initiator::{classify_initiator as classify_chat_initiator, classify_initiator_responses};
use tokn_headers::inbound::{first_present_smol, inbound_correlation, PROJECT_ID_HEADERS};
use tokn_headers::HeaderMap;

pub struct DefaultExtract;

#[async_trait]
impl ExtractStage for DefaultExtract {
  async fn extract(&self, ctx: &PipelineCtx, raw: RawInbound) -> Result<Extracted, PipelineError> {
    let RawInbound {
      request_endpoint: _,
      headers,
      raw_body,
      decoded_body,
      body_json,
      request_id: _,
    } = raw;

    let model = body_json
      .get("model")
      .and_then(Value::as_str)
      .filter(|s| !s.is_empty())
      .map(SmolStr::new)
      .unwrap_or_else(|| SmolStr::new("unknown"));

    let stream = infer_stream(&headers, &body_json);

    let header_initiator = header_str(&headers, "x-initiator")
      .map(|s| s.trim().to_ascii_lowercase())
      .filter(|s| s == "user" || s == "agent")
      .map(SmolStr::new);

    let initiator = header_initiator
      .clone()
      .or_else(|| classify_initiator(&body_json).map(SmolStr::new));
    let request_classification = classify_request(&ctx.request_endpoint, &body_json);

    let session_id = inbound_correlation(&headers).session_id;
    let project_id = first_present_smol(&headers, PROJECT_ID_HEADERS);

    let route_mode_hint = header_str(&headers, "x-route-mode")
      .map(str::trim)
      .filter(|s| !s.is_empty())
      .map(SmolStr::new);

    // Parsing failures here are recoverable for the codec layer
    // (which would have failed loudly at the transport boundary
    // before we got here) but not for ConvertRequest. We treat a
    // parse error as `None` so downstream stages just emit an
    // uncompressed body; the legacy router behaviour was identical.
    let content_encoding = request_content_encoding(&headers).ok().flatten();

    let agent_id = ctx.config.agent_id().cloned();

    Ok(Extracted {
      agent_id,
      model,
      stream,
      session_id,
      project_id,
      initiator,
      header_initiator,
      request_classification,
      route_mode_hint,
      headers,
      raw_body,
      decoded_body,
      body_json: Arc::new(body_json),
      content_encoding,
    })
  }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
  headers.get(name).map(|v| v.as_str())
}

fn infer_stream(headers: &HeaderMap, body: &Value) -> bool {
  if let Some(stream) = body.get("stream").and_then(Value::as_bool) {
    return stream;
  }
  header_str(headers, "accept")
    .map(|v| {
      v.split(',')
        .any(|part| part.split(';').next().map(str::trim) == Some("text/event-stream"))
    })
    .unwrap_or(false)
}

fn classify_initiator(body: &Value) -> Option<&'static str> {
  if body.get("input").is_some() {
    classify_initiator_responses(body)
  } else {
    classify_chat_initiator(body).or_else(|| {
      let has_tools = body
        .get("tools")
        .and_then(Value::as_array)
        .map(|a| !a.is_empty())
        .unwrap_or(false);
      has_tools.then_some("agent")
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::event::EventBus;
  use bytes::Bytes;
  use std::sync::Arc;
  use tokn_core::provider::Endpoint;
  use tokn_core::request_classification::{RequestClassification, RequestClassificationSource, RequestPurpose};
  use tokn_core::request_event::ExtractedSummary;
  use tokn_core::AgentId;
  use tokn_headers::inbound::{first_present, SESSION_ID_HEADERS};

  fn ctx() -> PipelineCtx {
    PipelineCtx::new(
      "req-test",
      Endpoint::ChatCompletions.into(),
      Arc::new(EventBus::new(64)),
    )
  }

  fn raw(headers: HeaderMap, body: Value) -> RawInbound {
    let decoded = Bytes::from(serde_json::to_vec(&body).unwrap());
    RawInbound {
      request_endpoint: Endpoint::ChatCompletions.into(),
      headers,
      raw_body: decoded.clone(),
      decoded_body: decoded,
      body_json: body,
      request_id: None,
    }
  }

  fn header_map(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
      h.insert(
        tokn_headers::HeaderName::new(*k),
        tokn_headers::HeaderValue::from_string((*v).to_string()),
      );
    }
    h
  }

  #[tokio::test]
  async fn extracts_model_and_unknown_initiator_without_signal() {
    let body = serde_json::json!({"model": "gpt-x", "messages": []});
    let ex = DefaultExtract
      .extract(&ctx(), raw(HeaderMap::new(), body))
      .await
      .expect("extract should succeed");
    assert_eq!(ex.model, "gpt-x");
    assert_eq!(ex.initiator, None);
    assert!(!ex.stream);
    assert!(ex.agent_id.is_none());
  }

  #[tokio::test]
  async fn codex_compaction_classification_reaches_extract_event_summary() {
    let body = serde_json::json!({
      "model": "gpt-test",
      "input": [{
        "role": "user",
        "content": "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary for another LLM that will resume the task."
      }]
    });
    let ctx = PipelineCtx::new("req-compact", Endpoint::Responses.into(), Arc::new(EventBus::new(64)));
    let ex = DefaultExtract
      .extract(
        &ctx,
        RawInbound {
          request_endpoint: Endpoint::Responses.into(),
          ..raw(HeaderMap::new(), body)
        },
      )
      .await
      .unwrap();
    let expected = Some(RequestClassification {
      purpose: RequestPurpose::Compaction,
      source: RequestClassificationSource::CodexPrompt,
    });
    assert_eq!(ex.request_classification, expected);
    let summary = ExtractedSummary::from(&ex);
    assert_eq!(summary.request_classification, expected);
  }

  #[tokio::test]
  async fn x_behave_as_is_ignored() {
    let body = serde_json::json!({"model": "m"});
    let headers = header_map(&[("x-behave-as", "  codex  ")]);
    let ex = DefaultExtract.extract(&ctx(), raw(headers, body)).await.unwrap();
    assert!(ex.agent_id.is_none());
  }

  #[tokio::test]
  async fn run_config_agent_id_is_extracted() {
    let body = serde_json::json!({"model": "m"});
    let ctx = PipelineCtx::new_with_config(
      "req-test",
      Endpoint::ChatCompletions.into(),
      Arc::new(EventBus::new(64)),
      Arc::new(crate::RunConfig::builder().with_agent_id(AgentId::CodexCli).build()),
    );
    let ex = DefaultExtract.extract(&ctx, raw(HeaderMap::new(), body)).await.unwrap();
    assert_eq!(ex.agent_id, Some(AgentId::CodexCli));
  }

  #[tokio::test]
  async fn stream_from_body_takes_precedence() {
    let body = serde_json::json!({"model": "m", "stream": true});
    let headers = header_map(&[("accept", "application/json")]);
    let ex = DefaultExtract.extract(&ctx(), raw(headers, body)).await.unwrap();
    assert!(ex.stream);
  }

  #[tokio::test]
  async fn stream_inferred_from_accept_sse() {
    let body = serde_json::json!({"model": "m"});
    let headers = header_map(&[("accept", "text/event-stream, application/json")]);
    let ex = DefaultExtract.extract(&ctx(), raw(headers, body)).await.unwrap();
    assert!(ex.stream);
  }

  #[tokio::test]
  async fn agent_initiator_when_tools_present() {
    let body = serde_json::json!({"model": "m", "tools": [{"type":"function"}]});
    let ex = DefaultExtract
      .extract(&ctx(), raw(HeaderMap::new(), body))
      .await
      .unwrap();
    assert_eq!(ex.initiator.as_deref(), Some("agent"));
  }

  #[tokio::test]
  async fn user_initiator_when_messages_show_user_turn() {
    let body = serde_json::json!({
      "model": "m",
      "messages": [{"role":"system","content":"x"},{"role":"user","content":"hi"}]
    });
    let ex = DefaultExtract
      .extract(&ctx(), raw(HeaderMap::new(), body))
      .await
      .unwrap();
    assert_eq!(ex.initiator.as_deref(), Some("user"));
  }

  #[tokio::test]
  async fn user_initiator_for_single_response_input_object() {
    let body = serde_json::json!({
      "model": "m",
      "input": {"role":"user","content":"hi"}
    });
    let ex = DefaultExtract
      .extract(&ctx(), raw(HeaderMap::new(), body))
      .await
      .unwrap();
    assert_eq!(ex.initiator.as_deref(), Some("user"));
  }

  #[tokio::test]
  async fn header_initiator_overrides_body_classification() {
    let body = serde_json::json!({"model": "m", "tools": [{"type":"function"}]});
    let headers = header_map(&[("x-initiator", "user")]);
    let ex = DefaultExtract.extract(&ctx(), raw(headers, body)).await.unwrap();
    assert_eq!(ex.initiator.as_deref(), Some("user"));
    assert_eq!(ex.header_initiator.as_deref(), Some("user"));
  }

  #[tokio::test]
  async fn session_request_project_ids_extracted_with_priority() {
    let body = serde_json::json!({"model": "m"});
    let headers = header_map(&[
      ("x-session-id", "   "),
      ("x-client-session-id", " sess-2 "),
      ("x-opencode-project", "proj-9"),
    ]);
    let ex = DefaultExtract.extract(&ctx(), raw(headers, body)).await.unwrap();
    assert_eq!(ex.session_id.as_deref(), first_present(&ex.headers, SESSION_ID_HEADERS));
    assert_eq!(ex.project_id.as_deref(), Some("proj-9"));
  }

  #[tokio::test]
  async fn session_id_falls_back_to_codex_turn_metadata() {
    let body = serde_json::json!({"model": "m"});
    let headers = header_map(&[(
      "x-codex-turn-metadata",
      r#"{"session_id":"session-meta","thread_id":"thread-meta","turn_id":"turn-meta"}"#,
    )]);

    let ex = DefaultExtract.extract(&ctx(), raw(headers, body)).await.unwrap();

    assert_eq!(ex.session_id.as_deref(), Some("session-meta"));
  }
}
