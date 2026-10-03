//! Relay ConvertRequest stage with a scoped Codex compaction override.
//!
//! Forwards the inbound body **verbatim** to the upstream:
//! `upstream_wire_body = extracted.raw_body.clone()` (bytes still in their
//! original on-wire encoding). Detected compactions sent to Codex instead
//! receive `service_tier: "priority"` and are re-encoded with the inbound codec.
//! No model rewrite, cross-endpoint translation, or provider input transformer.
//!
//! Unchanged requests keep `upstream_body` set to `Value::Null`; subscribers
//! that care about request bodies must read the `Bytes` instead.

use super::apply_compaction_priority;
use super::compaction::{compaction_provider_id, requires_compaction_priority};
use crate::event::Stage;
use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::error::{PipelineError, RequestsError};
use crate::pipeline::stages::{require_upstream_endpoint, ConvertRequestStage, ConvertedRequest, Extracted, Resolved};
use crate::utils::codec::{encode_body_bytes, request_content_encoding};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;

pub struct PassthroughConvertRequest;

#[async_trait]
impl ConvertRequestStage for PassthroughConvertRequest {
  async fn convert_request(
    &self,
    ctx: &PipelineCtx,
    extracted: &Extracted,
    resolved: &Resolved,
  ) -> Result<ConvertedRequest, PipelineError> {
    if let Some(options) = ctx.config.generation_options() {
      options.validate().map_err(|source| {
        PipelineError::permanent(
          Stage::ConvertRequest,
          RequestsError::InvalidGenerationOptions { source },
        )
      })?;
      let control = if options.max_output_tokens.is_some() {
        Some("max_output_tokens")
      } else if options.top_k.is_some() {
        Some("top_k")
      } else if options.reasoning.is_some() {
        Some("reasoning")
      } else {
        None
      };
      if let Some(control) = control {
        let endpoint = require_upstream_endpoint(ctx, resolved, Stage::ConvertRequest)?;
        return Err(PipelineError::permanent(
          Stage::ConvertRequest,
          RequestsError::UnsupportedGenerationControl {
            control,
            provider_id: resolved.account_handle.provider.info().id.clone().into(),
            endpoint,
            reason: "verbatim routing cannot lower provider-neutral generation controls",
          },
        ));
      }
    }
    let provider_id = compaction_provider_id(ctx, resolved);
    if requires_compaction_priority(provider_id, extracted.request_classification)
      && request_content_encoding(&extracted.headers).is_ok()
    {
      // Inspection is best-effort: opaque endpoints can be classified even
      // when their bodies are not JSON objects. Keep those bytes untouched.
      if let Ok(mut body) = serde_json::from_slice::<Value>(&extracted.decoded_body) {
        if apply_compaction_priority(provider_id, extracted.request_classification, &mut body) {
          let debug_outbound_body = Bytes::from(serde_json::to_vec(&body).map_err(|source| {
            PipelineError::permanent(Stage::ConvertRequest, RequestsError::SerializeUpstreamBody { source })
          })?);
          let upstream_wire_body =
            encode_body_bytes(&debug_outbound_body, extracted.content_encoding).map_err(|source| {
              PipelineError::permanent(Stage::ConvertRequest, RequestsError::ReencodeOutboundBody { source })
            })?;
          return Ok(ConvertedRequest {
            upstream_body: Arc::new(body),
            upstream_wire_body,
            debug_outbound_body,
            content_encoding: extracted.content_encoding,
          });
        }
      }
    }
    Ok(ConvertedRequest {
      // Sentinel: observers must read the untouched wire/debug bytes.
      upstream_body: Arc::new(Value::Null),
      upstream_wire_body: extracted.raw_body.clone(),
      debug_outbound_body: extracted.decoded_body.clone(),
      content_encoding: extracted.content_encoding,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::event::EventBus;
  use crate::pipeline::config::RunConfig;
  use bytes::Bytes;
  use serde_json::json;
  use smol_str::SmolStr;
  use std::sync::Arc;
  use tokn_core::generation::GenerationOptions;
  use tokn_core::provider::Endpoint;
  use tokn_core::request_event::RequestEndpoint;
  use tokn_headers::HeaderMap;

  async fn convert_body(
    decoded: Bytes,
    encoding: Option<crate::utils::codec::ContentEncodingKind>,
    provider_id: &str,
    path: &str,
  ) -> (Extracted, ConvertedRequest) {
    use crate::pipeline::stages::{ExtractStage, RawInbound, ResolveStage};
    use crate::stages::resolve::proxy::keys;
    use crate::stages::{PassthroughExtract, ProxyResolve};

    let endpoint = RequestEndpoint::custom(path);
    let config = RunConfig::builder()
      .with_str(keys::HOST, "chatgpt.com")
      .with_str(keys::PROVIDER_ID, provider_id)
      .with_str(keys::PATH, path)
      .build();
    let ctx = PipelineCtx::new_with_config("req", endpoint.clone(), Arc::new(EventBus::new(16)), Arc::new(config));
    let raw_body = encode_body_bytes(&decoded, encoding).unwrap();
    let mut headers = HeaderMap::new();
    if let Some(encoding) = encoding {
      headers.insert(
        "content-encoding",
        tokn_headers::HeaderValue::from_static(encoding.as_str()),
      );
    }
    let extracted = PassthroughExtract
      .extract(
        &ctx,
        RawInbound {
          request_endpoint: endpoint,
          headers,
          raw_body,
          decoded_body: decoded,
          body_json: Value::Null,
          request_id: None,
        },
      )
      .await
      .unwrap();
    let resolved = ProxyResolve.resolve(&ctx, &extracted).await.unwrap();
    let converted = PassthroughConvertRequest
      .convert_request(&ctx, &extracted, &resolved)
      .await
      .unwrap();
    (extracted, converted)
  }

  #[tokio::test]
  async fn prioritizes_codex_compaction_with_original_content_encoding() {
    use crate::utils::codec::{decode_body_bytes, ContentEncodingKind};

    for encoding in [None, Some(ContentEncodingKind::Gzip), Some(ContentEncodingKind::Zstd)] {
      for tier in [None, Some("auto"), Some("default"), Some("flex")] {
        let mut body = json!({
          "model": "gpt-6.1-sol", "stream": false,
          "input": [{"type": "compaction_trigger"}],
          "reasoning": {"effort": "xhigh"},
          "unknown_control": {"keep": true}
        });
        if let Some(tier) = tier {
          body["service_tier"] = json!(tier);
        }
        let inbound = body.clone();
        let (extracted, out) = convert_body(
          Bytes::from(serde_json::to_vec(&body).unwrap()),
          encoding,
          "codex",
          "/backend-api/codex/responses",
        )
        .await;
        body["service_tier"] = json!("priority");

        assert_eq!(*out.upstream_body, body);
        assert_eq!(serde_json::from_slice::<Value>(&out.debug_outbound_body).unwrap(), body);
        assert_eq!(
          decode_body_bytes(out.upstream_wire_body, encoding).unwrap(),
          out.debug_outbound_body
        );
        assert_eq!(out.content_encoding, encoding);
        assert_eq!(
          serde_json::from_slice::<Value>(&extracted.decoded_body).unwrap(),
          inbound
        );
      }
    }
  }

  #[tokio::test]
  async fn unchanged_relay_requests_preserve_original_wire_bytes() {
    use crate::utils::codec::ContentEncodingKind;

    let cases: &[(&str, &[u8])] = &[
      (
        "codex",
        br#"{ "service_tier": "priority", "input": [{"type":"compaction_trigger"}] }"#,
      ),
      ("openai", br#"{ "input": [{"type":"compaction_trigger"}] }"#),
      (
        "codex",
        br#"{ "input": [{"type":"compaction_trigger"}, {"role":"user","content":"continue"}] }"#,
      ),
      (
        "codex",
        br#"{ "input": [{"type":"compaction","encrypted_content":"old summary"}] }"#,
      ),
      ("codex", br#"{ "input": "compaction_trigger" }"#),
    ];
    for encoding in [None, Some(ContentEncodingKind::Gzip), Some(ContentEncodingKind::Zstd)] {
      for &(provider_id, raw) in cases {
        let (extracted, out) = convert_body(Bytes::copy_from_slice(raw), encoding, provider_id, "/responses").await;
        assert_eq!(out.upstream_wire_body, extracted.raw_body);
        assert_eq!(out.debug_outbound_body, extracted.decoded_body);
        assert_eq!(*out.upstream_body, Value::Null);
      }
    }
  }

  #[tokio::test]
  async fn prioritizes_codex_compact_endpoint_and_legacy_prompt() {
    for (path, body) in [
      ("/backend-api/codex/responses/compact", json!({"input": "history"})),
      (
        "/responses",
        json!({"input": [{"role": "user", "content":
          "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary for another LLM that will resume the task."
        }]}),
      ),
    ] {
      let (_, out) = convert_body(Bytes::from(serde_json::to_vec(&body).unwrap()), None, "codex", path).await;
      assert_eq!(out.upstream_body["service_tier"], "priority");
    }
  }

  #[tokio::test]
  async fn opaque_compact_bodies_stay_verbatim_when_they_cannot_be_modified() {
    for body in [b"not-json".as_slice(), b"[]", b"null"] {
      let (extracted, out) = convert_body(Bytes::copy_from_slice(body), None, "codex", "/responses/compact").await;
      assert_eq!(out.upstream_wire_body, extracted.raw_body);
      assert_eq!(out.debug_outbound_body, extracted.decoded_body);
      assert_eq!(*out.upstream_body, Value::Null);
    }
  }

  #[tokio::test]
  async fn unsupported_encoding_does_not_mutate_relay_body() {
    use tokn_core::request_classification::{RequestClassification, RequestClassificationSource, RequestPurpose};

    let body = Bytes::from_static(br#"{"input":[{"type":"compaction_trigger"}]}"#);
    let mut extracted = extracted(body.clone(), body.clone());
    extracted.request_classification = Some(RequestClassification {
      purpose: RequestPurpose::Compaction,
      source: RequestClassificationSource::RequestField,
    });
    extracted
      .headers
      .insert("content-encoding", tokn_headers::HeaderValue::from_static("br"));
    let mut resolved = resolved();
    resolved.account_handle = crate::test_support::mock_handle("a", "codex");

    let out = PassthroughConvertRequest
      .convert_request(&ctx(), &extracted, &resolved)
      .await
      .unwrap();
    assert_eq!(out.upstream_wire_body, body);
  }

  fn ctx() -> PipelineCtx {
    PipelineCtx::new("req", Endpoint::ChatCompletions.into(), Arc::new(EventBus::new(16)))
  }

  fn extracted(raw: Bytes, decoded: Bytes) -> Extracted {
    Extracted {
      agent_id: None,
      model: SmolStr::new("m"),
      stream: false,
      session_id: None,
      project_id: None,
      initiator: None,
      header_initiator: None,
      request_classification: None,
      route_mode_hint: None,
      headers: HeaderMap::new(),
      raw_body: raw,
      decoded_body: decoded,
      body_json: Arc::new(json!(null)),
      content_encoding: None,
    }
  }

  fn resolved() -> Resolved {
    Resolved {
      agent_id: None,
      model: SmolStr::new("m"),
      upstream_model: SmolStr::new("m"),
      route: crate::pipeline::stages::ResolvedRoute::operation(Endpoint::ChatCompletions, Endpoint::ChatCompletions),
      account_id: SmolStr::new("a"),
      provider_id: SmolStr::new("openai"),
      account_handle: crate::test_support::mock_handle("a", "openai"),
    }
  }

  #[tokio::test]
  async fn forwards_bytes_verbatim() {
    let raw = Bytes::from_static(b"\x1f\x8b\x08\x00not-json-just-bytes");
    let decoded = Bytes::from_static(b"{\"model\":\"m\"}");
    let out = PassthroughConvertRequest
      .convert_request(&ctx(), &extracted(raw.clone(), decoded.clone()), &resolved())
      .await
      .unwrap();
    assert_eq!(out.upstream_wire_body, raw);
    assert_eq!(out.debug_outbound_body, decoded);
    assert_eq!(*out.upstream_body, Value::Null, "upstream_body must be null sentinel");
  }

  #[tokio::test]
  async fn rejects_generation_controls_that_require_lowering() {
    let raw = Bytes::from_static(b"{\"model\":\"m\"}");
    let config = RunConfig::builder()
      .with_generation_options(GenerationOptions::new().with_top_k(40))
      .build();
    let ctx = PipelineCtx::new_with_config(
      "req",
      Endpoint::ChatCompletions.into(),
      Arc::new(EventBus::new(16)),
      Arc::new(config),
    );

    let error = PassthroughConvertRequest
      .convert_request(&ctx, &extracted(raw.clone(), raw), &resolved())
      .await
      .unwrap_err();

    assert!(matches!(
      error.inner(),
      RequestsError::UnsupportedGenerationControl {
        control: "top_k",
        endpoint: Endpoint::ChatCompletions,
        ..
      }
    ));
  }

  #[tokio::test]
  async fn rejects_out_of_band_max_output_tokens_even_when_the_wire_contains_a_limit() {
    let raw = Bytes::from_static(b"{\"model\":\"m\",\"max_output_tokens\":64}");
    let config = RunConfig::builder()
      .with_generation_options(GenerationOptions::new().with_max_output_tokens(64))
      .build();
    let ctx = PipelineCtx::new_with_config(
      "req",
      Endpoint::ChatCompletions.into(),
      Arc::new(EventBus::new(16)),
      Arc::new(config),
    );

    let error = PassthroughConvertRequest
      .convert_request(&ctx, &extracted(raw.clone(), raw.clone()), &resolved())
      .await
      .unwrap_err();

    assert!(matches!(
      error.inner(),
      RequestsError::UnsupportedGenerationControl {
        control: "max_output_tokens",
        ..
      }
    ));
  }
}
