//! Best-effort inspection for the passthrough pipeline.
//!
//! The contract differs from [`DefaultExtract`](super::DefaultExtract) on
//! one axis: we must **not** treat the inbound JSON body as authoritative
//! and we must **not** keep it around as `Arc<Value>` for downstream
//! stages to re-serialize. Conversion normally forwards the original bytes;
//! detected Codex compactions receive a scoped priority-tier override.
//!
//! Strategy: retain the typed metadata peek for model and stream, then parse
//! the decoded bytes separately for observational classification. Both values
//! are discarded; classification can select the compaction priority policy.
//! The full body bytes remain
//! in `raw_body` / `decoded_body` and
//! `body_json` is set to `Value::Null` to signal "do not consult".
//!
//! Header extraction (session, project, route mode, initiator, etc.)
//! mirrors `DefaultExtract` since BuildHeaders and Send both depend on
//! those values.

use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::error::PipelineError;
use crate::pipeline::stages::{ExtractStage, Extracted, RawInbound};
use crate::utils::codec::request_content_encoding;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use smol_str::SmolStr;
use std::sync::Arc;
use tokn_core::request_classification::classify_request;
use tokn_headers::inbound::{first_present_smol, inbound_correlation, PROJECT_ID_HEADERS};
use tokn_headers::HeaderMap;

/// Preserve the original all-or-nothing metadata peek: an invalid type in
/// either field makes both peeked fields unavailable.
#[derive(Debug, Default, Deserialize)]
struct ModelPeek {
  #[serde(default)]
  model: Option<SmolStr>,
  #[serde(default)]
  stream: Option<bool>,
}

pub struct PassthroughExtract;

#[async_trait]
impl ExtractStage for PassthroughExtract {
  async fn extract(&self, ctx: &PipelineCtx, raw: RawInbound) -> Result<Extracted, PipelineError> {
    let RawInbound {
      request_endpoint: _,
      headers,
      raw_body,
      decoded_body,
      body_json: _,
      request_id: _,
    } = raw;

    // Preserve typed-peek behavior, including rejection of duplicate keys
    // and invalid field types. Classification parses independently so it
    // cannot change routing metadata or the original forwarded bytes.
    let peek = serde_json::from_slice::<ModelPeek>(&decoded_body).unwrap_or_default();
    let inspected_body = serde_json::from_slice::<Value>(&decoded_body).unwrap_or(Value::Null);

    let model = peek
      .model
      .filter(|s| !s.is_empty())
      .unwrap_or_else(|| SmolStr::new("unknown"));

    let stream = peek.stream.unwrap_or_else(|| accept_is_sse(&headers));
    let request_classification = classify_request(&ctx.request_endpoint, &inspected_body);

    let header_initiator = header_str(&headers, "x-initiator")
      .map(|s| s.trim().to_ascii_lowercase())
      .filter(|s| s == "user" || s == "agent")
      .map(SmolStr::new);

    // Passthrough has no body-shape classifier, so missing header
    // initiator stays unknown for persistence purposes.
    let initiator = header_initiator.clone();

    let session_id = inbound_correlation(&headers).session_id;
    let project_id = first_present_smol(&headers, PROJECT_ID_HEADERS);

    let route_mode_hint = header_str(&headers, "x-route-mode")
      .map(str::trim)
      .filter(|s| !s.is_empty())
      .map(SmolStr::new);

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
      // Sentinel: passthrough downstream stages must not read body JSON.
      body_json: Arc::new(Value::Null),
      content_encoding,
    })
  }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
  headers.get(name).map(|v| v.as_str())
}

fn accept_is_sse(headers: &HeaderMap) -> bool {
  header_str(headers, "accept")
    .map(|v| {
      v.split(',')
        .any(|part| part.split(';').next().map(str::trim) == Some("text/event-stream"))
    })
    .unwrap_or(false)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::event::EventBus;
  use bytes::Bytes;
  use std::sync::Arc;
  use tokn_core::provider::Endpoint;
  use tokn_core::request_classification::{RequestClassification, RequestClassificationSource, RequestPurpose};
  use tokn_core::request_event::RequestEndpoint;

  fn ctx() -> PipelineCtx {
    PipelineCtx::new(
      "req-passthrough",
      Endpoint::ChatCompletions.into(),
      Arc::new(EventBus::new(64)),
    )
  }

  fn raw_with_body(body_bytes: Bytes, headers: HeaderMap) -> RawInbound {
    RawInbound {
      request_endpoint: Endpoint::ChatCompletions.into(),
      headers,
      raw_body: body_bytes.clone(),
      decoded_body: body_bytes,
      // Pretend the transport did decode the JSON for the legacy path;
      // PassthroughExtract must NOT consult this.
      body_json: serde_json::json!({"sentinel": "should-not-be-read"}),
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
  async fn peeks_model_from_body_without_keeping_value() {
    let body = Bytes::from(r#"{"model":"gpt-4o","stream":true,"messages":[]}"#);
    let ex = PassthroughExtract
      .extract(&ctx(), raw_with_body(body.clone(), HeaderMap::new()))
      .await
      .expect("extract should succeed");
    assert_eq!(ex.model, "gpt-4o");
    assert!(ex.stream);
    assert_eq!(*ex.body_json, Value::Null, "body_json must be null sentinel");
    assert_eq!(ex.raw_body, body, "raw bytes preserved verbatim");
  }

  #[tokio::test]
  async fn unparseable_body_yields_unknown_model() {
    let body = Bytes::from_static(&[0xff, 0xfe, 0xfd]);
    let ex = PassthroughExtract
      .extract(&ctx(), raw_with_body(body.clone(), HeaderMap::new()))
      .await
      .unwrap();
    assert_eq!(ex.model, "unknown");
    assert!(!ex.stream);
    assert_eq!(ex.raw_body, body);
  }

  #[tokio::test]
  async fn invalid_stream_type_invalidates_the_entire_metadata_peek() {
    let body = Bytes::from_static(br#"{"model":"gpt-test","stream":"true"}"#);
    let headers = header_map(&[("accept", "text/event-stream")]);
    let ex = PassthroughExtract
      .extract(&ctx(), raw_with_body(body.clone(), headers))
      .await
      .unwrap();
    assert_eq!(ex.model, "unknown");
    assert!(ex.stream, "invalid stream falls back to Accept");
    assert_eq!(ex.raw_body, body);
    assert_eq!(*ex.body_json, Value::Null);
  }

  #[tokio::test]
  async fn duplicate_model_key_invalidates_peek_but_not_classification() {
    let body = Bytes::from_static(
      br#"{"model":"first","model":"second","stream":false,"messages":[{"role":"user","content":"You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary for another LLM that will resume the task."}]}"#,
    );
    let headers = header_map(&[("accept", "text/event-stream")]);
    let ex = PassthroughExtract
      .extract(&ctx(), raw_with_body(body.clone(), headers))
      .await
      .unwrap();
    assert_eq!(ex.model, "unknown");
    assert!(ex.stream, "duplicate model invalidates stream peek too");
    assert_eq!(ex.raw_body, body);
    assert_eq!(
      ex.request_classification,
      Some(RequestClassification {
        purpose: RequestPurpose::Compaction,
        source: RequestClassificationSource::CodexPrompt,
      })
    );
  }

  #[tokio::test]
  async fn responses_compaction_trigger_is_classified_without_changing_proxy_bytes() {
    let endpoint = RequestEndpoint::custom("/backend-api/codex/responses");
    let ctx = PipelineCtx::new("req-passthrough-trigger", endpoint.clone(), Arc::new(EventBus::new(64)));
    let body =
      Bytes::from_static(br#"{ "model": "gpt-test", "stream": true, "input": [{"type":"compaction_trigger"}] }"#);
    let ex = PassthroughExtract
      .extract(
        &ctx,
        RawInbound {
          request_endpoint: endpoint,
          headers: HeaderMap::new(),
          raw_body: body.clone(),
          decoded_body: body.clone(),
          body_json: Value::Null,
          request_id: None,
        },
      )
      .await
      .unwrap();
    assert_eq!(ex.model, "gpt-test");
    assert!(ex.stream);
    assert_eq!(
      ex.request_classification,
      Some(RequestClassification {
        purpose: RequestPurpose::Compaction,
        source: RequestClassificationSource::RequestField,
      })
    );
    assert_eq!(ex.raw_body, body);
    assert_eq!(ex.decoded_body, body);
    assert_eq!(*ex.body_json, Value::Null);
  }

  #[tokio::test]
  async fn compact_endpoint_is_classified_without_changing_forwarded_bytes() {
    let endpoint = RequestEndpoint::custom("/v1/responses/compact");
    let ctx = PipelineCtx::new("req-passthrough-compact", endpoint.clone(), Arc::new(EventBus::new(64)));
    let raw_body = Bytes::from_static(b"wire bytes stay untouched");
    let decoded_body = Bytes::from_static(br#"{"model":"gpt-test","stream":true}"#);
    let inbound = RawInbound {
      request_endpoint: endpoint,
      headers: HeaderMap::new(),
      raw_body: raw_body.clone(),
      decoded_body: decoded_body.clone(),
      body_json: serde_json::json!({"model": "do-not-use"}),
      request_id: None,
    };
    let ex = PassthroughExtract.extract(&ctx, inbound).await.unwrap();
    assert_eq!(ex.model, "gpt-test");
    assert!(ex.stream);
    assert_eq!(
      ex.request_classification,
      Some(RequestClassification {
        purpose: RequestPurpose::Compaction,
        source: RequestClassificationSource::Endpoint,
      })
    );
    assert_eq!(ex.raw_body, raw_body);
    assert_eq!(ex.decoded_body, decoded_body);
    assert_eq!(*ex.body_json, Value::Null);
  }

  #[tokio::test]
  async fn compact_endpoint_is_classified_even_when_body_is_malformed() {
    let endpoint = RequestEndpoint::custom("/v1/responses/compact");
    let ctx = PipelineCtx::new("req-passthrough-compact", endpoint.clone(), Arc::new(EventBus::new(64)));
    let malformed = Bytes::from_static(b"{not json");
    let ex = PassthroughExtract
      .extract(
        &ctx,
        RawInbound {
          request_endpoint: endpoint,
          headers: HeaderMap::new(),
          raw_body: malformed.clone(),
          decoded_body: malformed.clone(),
          body_json: Value::Null,
          request_id: None,
        },
      )
      .await
      .unwrap();
    assert_eq!(ex.model, "unknown");
    assert_eq!(ex.raw_body, malformed);
    assert_eq!(
      ex.request_classification,
      Some(RequestClassification {
        purpose: RequestPurpose::Compaction,
        source: RequestClassificationSource::Endpoint,
      })
    );
  }

  #[tokio::test]
  async fn stream_falls_back_to_accept_when_body_silent() {
    let body = Bytes::from(r#"{"model":"m"}"#);
    let headers = header_map(&[("accept", "text/event-stream, application/json")]);
    let ex = PassthroughExtract
      .extract(&ctx(), raw_with_body(body, headers))
      .await
      .unwrap();
    assert!(ex.stream);
  }

  #[tokio::test]
  async fn headers_extracted_like_default() {
    let body = Bytes::from(r#"{"model":"m"}"#);
    let headers = header_map(&[
      ("x-session-id", "sess-1"),
      ("x-opencode-project", "/p"),
      ("x-route-mode", "passthrough"),
      ("x-behave-as", "codex"),
      ("x-initiator", "agent"),
    ]);
    let ex = PassthroughExtract
      .extract(&ctx(), raw_with_body(body, headers))
      .await
      .unwrap();
    assert_eq!(ex.session_id.as_deref(), Some("sess-1"));
    assert_eq!(ex.project_id.as_deref(), Some("/p"));
    assert_eq!(ex.route_mode_hint.as_deref(), Some("passthrough"));
    assert!(ex.agent_id.is_none());
    assert_eq!(ex.initiator.as_deref(), Some("agent"));
    assert_eq!(ex.header_initiator.as_deref(), Some("agent"));
  }

  #[tokio::test]
  async fn session_id_falls_back_to_codex_turn_metadata() {
    let body = Bytes::from(r#"{"model":"m"}"#);
    let headers = header_map(&[(
      "x-codex-turn-metadata",
      r#"{"session_id":"session-meta","thread_id":"thread-meta","turn_id":"turn-meta"}"#,
    )]);

    let ex = PassthroughExtract
      .extract(&ctx(), raw_with_body(body, headers))
      .await
      .unwrap();

    assert_eq!(ex.session_id.as_deref(), Some("session-meta"));
  }
}
