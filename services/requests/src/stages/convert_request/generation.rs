//! Standard-stage regression coverage for shared generation preparation.

#[cfg(test)]
use crate::pipeline::error::RequestsError;
#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use tokn_core::generation::{GenerationOptions, ReasoningEffort, ReasoningMode, ReasoningOptions};
#[cfg(test)]
use tokn_core::provider::{Endpoint, ID_CODEX, ID_DEEPSEEK, ID_GITHUB_COPILOT, ID_LLAMA_CPP};

#[cfg(test)]
fn lower_generation_options(
  body: &mut serde_json::Value,
  endpoint: Endpoint,
  provider_id: &str,
  model: &str,
  options: &GenerationOptions,
) -> Result<(), RequestsError> {
  crate::preparation::lower_generation_options(body, endpoint, provider_id, model, options).map_err(Into::into)
}

#[cfg(test)]
fn ensure_model_supports_reasoning(
  endpoint: Endpoint,
  provider_id: &str,
  reasoning_supported: Option<bool>,
  options: &GenerationOptions,
) -> Result<(), RequestsError> {
  crate::preparation::ensure_model_supports_reasoning(endpoint, provider_id, reasoning_supported, options)
    .map_err(Into::into)
}

#[cfg(test)]
fn ensure_model_supports_effort(
  endpoint: Endpoint,
  provider_id: &str,
  model: &str,
  supported: Option<&[ReasoningEffort]>,
  options: &GenerationOptions,
) -> Result<(), RequestsError> {
  crate::preparation::ensure_model_supports_effort(endpoint, provider_id, model, supported, options).map_err(Into::into)
}

#[cfg(test)]
mod tests {
  use super::super::default::DefaultConvertRequest;
  use super::*;
  use crate::event::{EventBus, Stage};
  use crate::pipeline::config::RunConfig;
  use crate::pipeline::ctx::PipelineCtx;
  use crate::pipeline::stages::{ConvertRequestStage, Extracted, Resolved, ResolvedRoute};
  use crate::test_support::mock_handle;
  use crate::utils::codec::ContentEncodingKind;
  use bytes::Bytes;
  use std::sync::Arc;
  use tokn_core::generation::ReasoningSummary;
  use tokn_core::provider::{Endpoint, ID_OPENAI, ID_ZAI};
  use tokn_headers::HeaderMap;

  #[test]
  fn effort_validation_distinguishes_unknown_and_known_support() {
    for (supported, effort, rejected) in [
      (None, "future", false),
      (Some(vec![]), "high", true),
      (Some(vec![ReasoningEffort::Low, ReasoningEffort::High]), "low", false),
      (Some(vec![ReasoningEffort::High]), "low", true),
      (Some(vec![ReasoningEffort::High]), "future", true),
      (Some(vec![ReasoningEffort::from("future")]), "future", false),
    ] {
      let options = GenerationOptions::new().with_reasoning(ReasoningOptions::new().with_effort(effort));
      let result =
        ensure_model_supports_effort(Endpoint::Responses, ID_OPENAI, "model", supported.as_deref(), &options);
      assert_eq!(result.is_err(), rejected, "{supported:?}: {effort}");
    }
  }

  #[test]
  fn disabled_mode_validates_none_only_when_lowered_to_effort() {
    let options = GenerationOptions::new().with_reasoning(ReasoningOptions::new().with_mode(ReasoningMode::Disabled));
    for (endpoint, provider, model, rejected) in [
      (Endpoint::Responses, ID_OPENAI, "gpt-5", true),
      (Endpoint::ChatCompletions, ID_GITHUB_COPILOT, "gpt-5", true),
      (Endpoint::ChatCompletions, ID_GITHUB_COPILOT, "claude-sonnet-4.6", false),
      (Endpoint::ChatCompletions, ID_DEEPSEEK, "deepseek-v4-flash", false),
      (Endpoint::Messages, ID_DEEPSEEK, "deepseek-v4-flash", false),
    ] {
      assert_eq!(
        ensure_model_supports_effort(endpoint, provider, model, Some(&[ReasoningEffort::High]), &options).is_err(),
        rejected
      );
    }
    ensure_model_supports_effort(
      Endpoint::Responses,
      ID_OPENAI,
      "model",
      Some(&[ReasoningEffort::from("none")]),
      &options,
    )
    .unwrap();
  }

  #[tokio::test]
  async fn deepseek_metadata_controls_validation_and_final_payload() {
    use tokn_core::provider::Provider;
    use tokn_core::util::secret::Secret;
    use tokn_provider_deepseek::DeepSeekProvider;

    for (model, effort, live_efforts, rejected) in [
      ("deepseek-v4-flash", "low", None, false),
      ("deepseek-v4-pro", "low", None, true),
      ("deepseek-v4-pro", "max", None, false),
      ("deepseek-v4-flash", "medium", None, true),
      ("unknown-deepseek", "future", None, false),
      ("deepseek-v4-pro", "future", Some(serde_json::json!(["future"])), false),
      ("deepseek-v4-flash", "low", Some(serde_json::json!([])), true),
    ] {
      for endpoint in [Endpoint::ChatCompletions, Endpoint::Messages] {
        let mut account = (*mock_handle("acct", ID_DEEPSEEK).config.load_full()).clone();
        account.api_key = Some(Secret::new("test-key".into()));
        let account = Arc::new(account);
        let provider = Arc::new(DeepSeekProvider::from_account(account.clone()).unwrap());
        if let Some(efforts) = &live_efforts {
          provider.info().model_cache.set_models(&[serde_json::json!({
            "id": model, "capabilities": {"supports": {"reasoning_effort": efforts}}
          })]);
        }
        let handle = Arc::new(tokn_accounts::AccountHandle::new(account, provider));
        let resolved = resolved_with(handle, Endpoint::Responses, endpoint, model);
        let body = serde_json::json!({"model": model, "input": "hi"});
        let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
        let extracted = extracted_with(body, None, raw, None);
        let options = GenerationOptions::new().with_reasoning(ReasoningOptions::new().with_effort(effort));
        let ctx = ctx_with_generation_options(Endpoint::Responses, options);
        let result = DefaultConvertRequest.convert_request(&ctx, &extracted, &resolved).await;
        assert_eq!(result.is_err(), rejected, "{model}: {effort} on {endpoint:?}");
        if let Ok(result) = result {
          let body: Value = serde_json::from_slice(&result.upstream_wire_body).unwrap();
          assert_eq!(body["thinking"]["type"], "enabled");
          let actual = if endpoint == Endpoint::ChatCompletions {
            &body["reasoning_effort"]
          } else {
            &body["output_config"]["effort"]
          };
          assert_eq!(actual, effort);
        }
      }
    }
  }

  fn ctx_at(endpoint: Endpoint) -> PipelineCtx {
    PipelineCtx::new("req-cr", endpoint.into(), Arc::new(EventBus::new(64)))
  }

  fn ctx() -> PipelineCtx {
    ctx_at(Endpoint::ChatCompletions)
  }

  fn ctx_with_config(endpoint: Endpoint, config: RunConfig) -> PipelineCtx {
    PipelineCtx::new_with_config("req-cr", endpoint.into(), Arc::new(EventBus::new(64)), Arc::new(config))
  }

  fn ctx_with_generation_options(endpoint: Endpoint, options: GenerationOptions) -> PipelineCtx {
    ctx_with_config(endpoint, RunConfig::builder().with_generation_options(options).build())
  }

  fn extracted_with(
    body: Value,
    encoding: Option<ContentEncodingKind>,
    wire: Bytes,
    initiator: Option<&str>,
  ) -> Extracted {
    Extracted {
      agent_id: None,
      model: smol_str::SmolStr::new("input-model"),
      stream: false,
      session_id: None,
      project_id: None,
      initiator: initiator.map(smol_str::SmolStr::new),
      header_initiator: None,
      request_classification: None,
      route_mode_hint: None,
      headers: HeaderMap::new(),
      raw_body: wire.clone(),
      decoded_body: Bytes::from(serde_json::to_vec(&body).unwrap()),
      body_json: Arc::new(body),
      content_encoding: encoding,
    }
  }

  fn resolved_with(
    handle: Arc<tokn_accounts::AccountHandle>,
    resolved_endpoint: Endpoint,
    upstream_endpoint: Endpoint,
    upstream_model: &str,
  ) -> Resolved {
    Resolved {
      agent_id: None,
      model: smol_str::SmolStr::new("input-model"),
      upstream_model: smol_str::SmolStr::new(upstream_model),
      route: ResolvedRoute::operation(resolved_endpoint, upstream_endpoint),
      account_id: smol_str::SmolStr::new("acct-1"),
      provider_id: smol_str::SmolStr::from(handle.provider.id()),
      account_handle: handle,
    }
  }

  #[tokio::test]
  async fn openai_responses_lowers_effort_and_summary() {
    let body = serde_json::json!({
      "model": "input-model",
      "input": [{"role": "user", "content": "hi"}],
      "reasoning": {
        "mode": "enabled",
        "effort": "low",
        "budget_tokens": 512,
        "summary": "detailed",
        "provider_extension": true
      }
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_OPENAI),
      Endpoint::Responses,
      Endpoint::Responses,
      "input-model",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::Responses,
      GenerationOptions {
        reasoning: Some(ReasoningOptions {
          effort: Some(ReasoningEffort::High),
          summary: Some(ReasoningSummary::Concise),
          ..ReasoningOptions::default()
        }),
        ..GenerationOptions::default()
      },
    );

    let out = DefaultConvertRequest.convert_request(&ctx, &ex, &res).await.unwrap();

    assert_eq!(out.upstream_body["reasoning"]["effort"], "high");
    assert_eq!(out.upstream_body["reasoning"]["summary"], "concise");
    assert_eq!(out.upstream_body["reasoning"]["provider_extension"], true);
    assert!(out.upstream_body["reasoning"].get("mode").is_none());
    assert!(out.upstream_body["reasoning"].get("budget_tokens").is_none());
  }

  #[tokio::test]
  async fn openai_chat_uses_max_completion_tokens() {
    let body = serde_json::json!({
      "model": "gpt-4o",
      "input": [{"role": "user", "content": "hi"}],
      "max_output_tokens": 512
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_OPENAI),
      Endpoint::Responses,
      Endpoint::ChatCompletions,
      "gpt-4o",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::Responses,
      GenerationOptions {
        max_output_tokens: Some(512),
        ..GenerationOptions::default()
      },
    );

    let out = DefaultConvertRequest.convert_request(&ctx, &ex, &res).await.unwrap();

    assert_eq!(out.upstream_body["max_completion_tokens"], 512);
    assert!(out.upstream_body.get("max_tokens").is_none());
    assert!(out.upstream_body.get("max_output_tokens").is_none());
  }

  #[tokio::test]
  async fn codex_responses_rejects_explicit_output_token_limit() {
    let body = serde_json::json!({
      "model": "gpt-5-codex",
      "input": [{"role": "user", "content": "hi"}],
      "max_output_tokens": 512
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_CODEX),
      Endpoint::Responses,
      Endpoint::Responses,
      "gpt-5-codex",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::Responses,
      GenerationOptions {
        max_output_tokens: Some(512),
        ..GenerationOptions::default()
      },
    );

    let error = DefaultConvertRequest
      .convert_request(&ctx, &ex, &res)
      .await
      .unwrap_err();

    assert!(matches!(
      error.inner(),
      RequestsError::UnsupportedGenerationControl {
        control: "max_output_tokens",
        provider_id,
        endpoint: Endpoint::Responses,
        ..
      } if provider_id == ID_CODEX
    ));
  }

  #[tokio::test]
  async fn responses_rejects_explicit_top_k() {
    let body = serde_json::json!({
      "model": "input-model",
      "input": [{"role": "user", "content": "hi"}]
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_OPENAI),
      Endpoint::Responses,
      Endpoint::Responses,
      "input-model",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::Responses,
      GenerationOptions {
        top_k: Some(40),
        ..GenerationOptions::default()
      },
    );

    let err = DefaultConvertRequest
      .convert_request(&ctx, &ex, &res)
      .await
      .unwrap_err();

    assert_eq!(err.stage, Stage::ConvertRequest);
    assert!(!err.recoverable);
    match err.inner() {
      RequestsError::UnsupportedGenerationControl {
        control,
        provider_id,
        endpoint,
        ..
      } => {
        assert_eq!(*control, "top_k");
        assert_eq!(provider_id, ID_OPENAI);
        assert_eq!(*endpoint, Endpoint::Responses);
      }
      other => panic!("expected UnsupportedGenerationControl, got {other:?}"),
    }
  }

  #[tokio::test]
  async fn openai_chat_rejects_explicit_top_k() {
    let body = serde_json::json!({
      "model": "input-model",
      "messages": [{"role": "user", "content": "hi"}]
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_OPENAI),
      Endpoint::ChatCompletions,
      Endpoint::ChatCompletions,
      "input-model",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::ChatCompletions,
      GenerationOptions {
        top_k: Some(40),
        ..GenerationOptions::default()
      },
    );

    let err = DefaultConvertRequest
      .convert_request(&ctx, &ex, &res)
      .await
      .unwrap_err();

    assert!(matches!(
      err.inner(),
      RequestsError::UnsupportedGenerationControl {
        control: "top_k",
        provider_id,
        endpoint: Endpoint::ChatCompletions,
        ..
      } if provider_id == ID_OPENAI
    ));
  }

  #[tokio::test]
  async fn llama_chat_lowers_top_k_and_overwrites_legacy_value() {
    let body = serde_json::json!({
      "model": "input-model",
      "messages": [{"role": "user", "content": "hi"}],
      "top_k": 7
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_LLAMA_CPP),
      Endpoint::ChatCompletions,
      Endpoint::ChatCompletions,
      "input-model",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::ChatCompletions,
      GenerationOptions {
        top_k: Some(40),
        ..GenerationOptions::default()
      },
    );

    let out = DefaultConvertRequest.convert_request(&ctx, &ex, &res).await.unwrap();

    assert_eq!(out.upstream_body["top_k"], 40);
  }

  #[test]
  fn llama_chat_rejects_typed_reasoning() {
    let mut body = serde_json::json!({
      "model": "input-model",
      "messages": [{"role": "user", "content": "hi"}]
    });
    let options = GenerationOptions::new().with_reasoning(ReasoningOptions::new().with_effort(ReasoningEffort::High));

    let error = lower_generation_options(
      &mut body,
      Endpoint::ChatCompletions,
      ID_LLAMA_CPP,
      "input-model",
      &options,
    )
    .expect_err("llama.cpp has no portable reasoning control");

    assert!(matches!(
      error,
      RequestsError::UnsupportedGenerationControl {
        control: "reasoning",
        provider_id,
        endpoint: Endpoint::ChatCompletions,
        ..
      } if provider_id == ID_LLAMA_CPP
    ));
  }

  #[tokio::test]
  async fn deepseek_chat_lowers_mode_and_effort_for_provider_transformer() {
    let body = serde_json::json!({
      "model": "input-model",
      "messages": [{"role": "user", "content": "hi"}],
      "reasoning": {"effort": "low"},
      "reasoning_effort": "low"
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_DEEPSEEK),
      Endpoint::ChatCompletions,
      Endpoint::ChatCompletions,
      "input-model",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::ChatCompletions,
      GenerationOptions {
        reasoning: Some(ReasoningOptions {
          mode: Some(ReasoningMode::Enabled),
          effort: Some(ReasoningEffort::High),
          ..ReasoningOptions::default()
        }),
        ..GenerationOptions::default()
      },
    );

    let out = DefaultConvertRequest.convert_request(&ctx, &ex, &res).await.unwrap();

    assert!(out.upstream_body.get("reasoning").is_none());
    assert_eq!(out.upstream_body["thinking"]["type"], "enabled");
    assert_eq!(out.upstream_body["thinking"]["effort"], "high");
    assert_eq!(out.upstream_body["reasoning_effort"], "high");
  }

  #[test]
  fn deepseek_responses_returns_typed_unsupported_error() {
    let mut body = serde_json::json!({
      "model": "deepseek-v4-pro",
      "input": [{"role": "user", "content": "hi"}]
    });
    let original = body.clone();
    let reasoning = ReasoningOptions::new().with_effort(ReasoningEffort::High);

    let error = crate::preparation::generation::lower_deepseek_reasoning(
      body.as_object_mut().expect("object body"),
      Endpoint::Responses,
      ID_DEEPSEEK,
      &reasoning,
    )
    .map_err(RequestsError::from)
    .expect_err("DeepSeek Responses must fail without panicking");

    assert!(matches!(
      error,
      RequestsError::UnsupportedGenerationControl {
        control: "reasoning",
        provider_id,
        endpoint: Endpoint::Responses,
        ..
      } if provider_id == ID_DEEPSEEK
    ));
    assert_eq!(body, original, "unsupported lowering must not mutate the request");
  }

  #[test]
  fn deepseek_rejects_sampling_controls_that_thinking_would_ignore() {
    let mut body = serde_json::json!({
      "model": "deepseek-v4-pro",
      "messages": [{"role": "user", "content": "hi"}],
      "top_p": 0.9
    });
    let options = GenerationOptions::new().with_reasoning(
      ReasoningOptions::new()
        .with_mode(ReasoningMode::Enabled)
        .with_effort(ReasoningEffort::Max),
    );

    let error = lower_generation_options(
      &mut body,
      Endpoint::ChatCompletions,
      ID_DEEPSEEK,
      "deepseek-v4-pro",
      &options,
    )
    .expect_err("ignored sampling controls must fail locally");

    assert!(matches!(
      error,
      RequestsError::UnsupportedGenerationControl { control: "top_p", .. }
    ));
  }

  #[tokio::test]
  async fn zai_chat_lowers_disabled_mode() {
    let body = serde_json::json!({
      "model": "input-model",
      "messages": [{"role": "user", "content": "hi"}],
      "reasoning": {"mode": "enabled"},
      "thinking": {"type": "enabled", "clear_thinking": false}
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_ZAI),
      Endpoint::ChatCompletions,
      Endpoint::ChatCompletions,
      "input-model",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::ChatCompletions,
      GenerationOptions {
        reasoning: Some(ReasoningOptions {
          mode: Some(ReasoningMode::Disabled),
          ..ReasoningOptions::default()
        }),
        ..GenerationOptions::default()
      },
    );

    let out = DefaultConvertRequest.convert_request(&ctx, &ex, &res).await.unwrap();

    assert!(out.upstream_body.get("reasoning").is_none());
    assert_eq!(out.upstream_body["thinking"]["type"], "disabled");
    assert!(out.upstream_body["thinking"].get("clear_thinking").is_none());
  }

  #[tokio::test]
  async fn zai_chat_lowers_enabled_mode_with_clear_thinking_disabled() {
    let body = serde_json::json!({
      "model": "input-model",
      "messages": [{"role": "user", "content": "hi"}]
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_ZAI),
      Endpoint::ChatCompletions,
      Endpoint::ChatCompletions,
      "input-model",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::ChatCompletions,
      GenerationOptions {
        reasoning: Some(ReasoningOptions {
          mode: Some(ReasoningMode::Enabled),
          ..ReasoningOptions::default()
        }),
        ..GenerationOptions::default()
      },
    );

    let out = DefaultConvertRequest.convert_request(&ctx, &ex, &res).await.unwrap();

    assert_eq!(
      out.upstream_body["thinking"],
      serde_json::json!({"type": "enabled", "clear_thinking": false})
    );
  }

  #[tokio::test]
  async fn copilot_messages_lowers_adaptive_mode_and_effort() {
    let body = serde_json::json!({
      "model": "claude-sonnet-4.6",
      "messages": [{"role": "user", "content": "hi"}],
      "max_tokens": 1024,
      "thinking": {"mode": "adaptive", "effort": "low"},
      "output_config": {"effort": "low"}
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_GITHUB_COPILOT),
      Endpoint::Messages,
      Endpoint::Messages,
      "claude-sonnet-4.6",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::Messages,
      GenerationOptions {
        reasoning: Some(ReasoningOptions {
          mode: Some(ReasoningMode::Adaptive),
          effort: Some(ReasoningEffort::Medium),
          ..ReasoningOptions::default()
        }),
        ..GenerationOptions::default()
      },
    );

    let out = DefaultConvertRequest.convert_request(&ctx, &ex, &res).await.unwrap();

    assert_eq!(out.upstream_body["thinking"], serde_json::json!({"type": "adaptive"}));
    assert_eq!(out.upstream_body["output_config"]["effort"], "medium");
  }

  #[tokio::test]
  async fn copilot_claude_chat_fallback_lowers_manual_budget_and_effort() {
    let body = serde_json::json!({
      "model": "claude-sonnet-4.6",
      "input": [{"role": "user", "content": "hi"}],
      "reasoning": {
        "mode": "enabled",
        "effort": "high",
        "budget_tokens": 2048
      },
      "max_output_tokens": 4096
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_GITHUB_COPILOT),
      Endpoint::Responses,
      Endpoint::ChatCompletions,
      "claude-sonnet-4.6",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::Responses,
      GenerationOptions {
        max_output_tokens: Some(4096),
        reasoning: Some(ReasoningOptions {
          mode: Some(ReasoningMode::Enabled),
          effort: Some(ReasoningEffort::High),
          budget_tokens: Some(2048),
          ..ReasoningOptions::default()
        }),
        ..GenerationOptions::default()
      },
    );

    let out = DefaultConvertRequest.convert_request(&ctx, &ex, &res).await.unwrap();

    assert!(out.upstream_body.get("reasoning").is_none());
    assert_eq!(
      out.upstream_body["thinking"],
      serde_json::json!({"type": "enabled", "budget_tokens": 2048})
    );
    assert_eq!(out.upstream_body["max_tokens"], 4096);
    assert_eq!(out.upstream_body["output_config"]["effort"], "high");
  }

  #[test]
  fn copilot_claude_chat_dispatch_normalizes_case_and_separators() {
    for model in ["CLAUDE-SONNET-4.6", "claude_sonnet_4_6", "Claude.Sonnet.4.6"] {
      let mut body = serde_json::json!({
        "model": model,
        "messages": [{"role": "user", "content": "hi"}]
      });
      let options = GenerationOptions::new().with_reasoning(
        ReasoningOptions::new()
          .with_mode(ReasoningMode::Adaptive)
          .with_effort(ReasoningEffort::Medium),
      );

      lower_generation_options(&mut body, Endpoint::ChatCompletions, ID_GITHUB_COPILOT, model, &options)
        .unwrap_or_else(|error| panic!("{model} should use Claude lowering: {error}"));

      assert_eq!(body["thinking"], serde_json::json!({"type": "adaptive"}), "{model}");
      assert_eq!(body["output_config"]["effort"], "medium", "{model}");
    }
  }

  #[test]
  fn claude_reasoning_rejects_invalid_model_mode_and_budget_combinations() {
    let cases = [
      (
        "enabled without budget",
        "claude-sonnet-4.6",
        ReasoningOptions::new().with_mode(ReasoningMode::Enabled),
        "reasoning.budget_tokens",
      ),
      (
        "budget below minimum",
        "claude-sonnet-4.6",
        ReasoningOptions::new()
          .with_mode(ReasoningMode::Enabled)
          .with_budget_tokens(512),
        "reasoning.budget_tokens",
      ),
      (
        "budget does not leave output room",
        "claude-sonnet-4.6",
        ReasoningOptions::new()
          .with_mode(ReasoningMode::Enabled)
          .with_budget_tokens(2048),
        "reasoning.budget_tokens",
      ),
      (
        "adaptive mode on a manual-only model",
        "claude-sonnet-4.5",
        ReasoningOptions::new().with_mode(ReasoningMode::Adaptive),
        "reasoning.mode",
      ),
      (
        "adaptive mode on Copilot's compact Opus 4.1 id",
        "claude-opus-41",
        ReasoningOptions::new().with_mode(ReasoningMode::Adaptive),
        "reasoning.mode",
      ),
      (
        "adaptive mode on Copilot's Sonnet 4 id",
        "claude-sonnet-4",
        ReasoningOptions::new().with_mode(ReasoningMode::Adaptive),
        "reasoning.mode",
      ),
      (
        "manual mode on an adaptive-only model",
        "claude-opus-4.7",
        ReasoningOptions::new()
          .with_mode(ReasoningMode::Enabled)
          .with_budget_tokens(1024),
        "reasoning.mode",
      ),
      (
        "disabled mode on an always-adaptive model",
        "claude-mythos-preview",
        ReasoningOptions::new().with_mode(ReasoningMode::Disabled),
        "reasoning.mode",
      ),
    ];

    for (name, model, reasoning, expected_control) in cases {
      let mut body = serde_json::json!({
        "model": model,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 2048
      });
      let options = GenerationOptions {
        reasoning: Some(reasoning),
        ..GenerationOptions::default()
      };

      let error = lower_generation_options(&mut body, Endpoint::ChatCompletions, ID_GITHUB_COPILOT, model, &options)
        .expect_err(name);

      assert!(
        matches!(
          error,
          RequestsError::UnsupportedGenerationControl { control, .. } if control == expected_control
        ),
        "{name}: {error}"
      );
    }
  }

  #[test]
  fn manual_claude_reasoning_requires_an_explicit_output_limit() {
    let mut body = serde_json::json!({
      "model": "claude-sonnet-4.6",
      "messages": [{"role": "user", "content": "hi"}]
    });
    let options = GenerationOptions::new().with_reasoning(
      ReasoningOptions::new()
        .with_mode(ReasoningMode::Enabled)
        .with_budget_tokens(2048),
    );

    let error = lower_generation_options(
      &mut body,
      Endpoint::ChatCompletions,
      ID_GITHUB_COPILOT,
      "claude-sonnet-4.6",
      &options,
    )
    .expect_err("manual reasoning must have an output limit");

    assert!(matches!(
      error,
      RequestsError::UnsupportedGenerationControl {
        control: "max_output_tokens",
        ..
      }
    ));
  }

  #[test]
  fn claude_reasoning_validates_sampling_compatibility() {
    let mut old_model_body = serde_json::json!({"temperature": 0.2, "top_p": 0.97});
    let error = crate::preparation::generation::validate_claude_sampling(
      old_model_body.as_object().expect("object"),
      Endpoint::Messages,
      ID_GITHUB_COPILOT,
      "claude-sonnet-4.6",
      true,
    )
    .expect_err("non-default temperature is incompatible with thinking");
    assert!(matches!(
      error,
      crate::preparation::PreparationError::UnsupportedGenerationControl {
        control: "temperature",
        ..
      }
    ));

    old_model_body["temperature"] = Value::from(1.0);
    crate::preparation::generation::validate_claude_sampling(
      old_model_body.as_object().expect("object"),
      Endpoint::Messages,
      ID_GITHUB_COPILOT,
      "claude-sonnet-4.6",
      true,
    )
    .expect("supported manual-thinking sampling values");

    let current_model_body = serde_json::json!({"top_p": 1.0});
    let error = crate::preparation::generation::validate_claude_sampling(
      current_model_body.as_object().expect("object"),
      Endpoint::Messages,
      ID_GITHUB_COPILOT,
      "claude-opus-4.7",
      false,
    )
    .expect_err("current Claude models reject explicit non-default sampling");
    assert!(matches!(
      error,
      crate::preparation::PreparationError::UnsupportedGenerationControl { control: "top_p", .. }
    ));
  }

  #[test]
  fn known_non_reasoning_models_reject_typed_reasoning() {
    let options = GenerationOptions::new().with_reasoning(ReasoningOptions::new().with_effort(ReasoningEffort::High));

    let error =
      ensure_model_supports_reasoning(Endpoint::Responses, ID_OPENAI, Some(false), &options).expect_err("reject");
    assert!(matches!(
      error,
      RequestsError::UnsupportedGenerationControl {
        control: "reasoning",
        ..
      }
    ));
    ensure_model_supports_reasoning(Endpoint::Responses, ID_OPENAI, Some(true), &options).expect("reasoning model");
    ensure_model_supports_reasoning(Endpoint::Responses, ID_OPENAI, None, &options).expect("unknown future model");
  }

  #[test]
  fn non_object_bodies_reject_explicit_generation_controls() {
    let mut body = serde_json::json!(["not", "an", "object"]);
    let options = GenerationOptions::new().with_max_output_tokens(128);

    let error =
      lower_generation_options(&mut body, Endpoint::Responses, ID_OPENAI, "gpt-5", &options).expect_err("reject");

    assert!(matches!(
      error,
      RequestsError::UnsupportedGenerationControl {
        control: "max_output_tokens",
        ..
      }
    ));
  }

  #[tokio::test]
  async fn invalid_generation_options_in_run_config_fail_before_lowering() {
    let body = serde_json::json!({
      "model": "input-model",
      "messages": [{"role": "user", "content": "hi"}]
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body, None, raw, None);
    let res = resolved_with(
      mock_handle("acct", ID_OPENAI),
      Endpoint::ChatCompletions,
      Endpoint::ChatCompletions,
      "input-model",
    );
    let ctx = ctx_with_generation_options(
      Endpoint::ChatCompletions,
      GenerationOptions {
        max_output_tokens: Some(0),
        ..GenerationOptions::default()
      },
    );

    let error = DefaultConvertRequest
      .convert_request(&ctx, &ex, &res)
      .await
      .expect_err("invalid generation options");
    assert!(matches!(error.inner(), RequestsError::InvalidGenerationOptions { .. }));
  }

  #[tokio::test]
  async fn raw_request_without_generation_options_keeps_control_fields() {
    let body = serde_json::json!({
      "model": "input-model",
      "messages": [{"role": "user", "content": "hi"}],
      "top_k": 7,
      "reasoning_effort": "custom",
      "thinking": {"type": "provider-specific"}
    });
    let raw = Bytes::from(serde_json::to_vec(&body).unwrap());
    let ex = extracted_with(body.clone(), None, raw.clone(), None);
    let res = resolved_with(
      mock_handle("acct", ID_OPENAI),
      Endpoint::ChatCompletions,
      Endpoint::ChatCompletions,
      "input-model",
    );

    let out = DefaultConvertRequest.convert_request(&ctx(), &ex, &res).await.unwrap();

    assert_eq!(*out.upstream_body, body);
    assert_eq!(out.upstream_wire_body, raw);
  }
}
