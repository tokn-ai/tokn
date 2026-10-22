use crate::util::secret::Secret;
use async_trait::async_trait;
use reqwest::Method;
use serde_json::Value;
use std::sync::Arc;
use tokn_core::account::AccountConfig;
use tokn_core::pipeline::InputTransformer;
use tokn_core::provider::ProviderTarget;
use tokn_core::upstream_url::CleartextHttpPolicy;
use tokn_headers::HeaderMap;
use tracing::{debug, instrument, warn};

use crate::{
  error, AuthKind, Endpoint, HeaderPatchCtx, Provider, ProviderInfo, ProviderRequestKind, RequestCtx, Result,
  TemplateVars, ID_DEEPSEEK,
};

pub use crate::native::DEFAULT_BASE_URL;

pub struct DeepSeekProvider {
  pub id: String,
  api_key: Secret<String>,
  target: ProviderTarget,
  info: ProviderInfo,
}

impl DeepSeekProvider {
  pub fn validate_account(a: &AccountConfig) -> Result<()> {
    if a.provider != ID_DEEPSEEK {
      return error::ProviderMismatchSnafu {
        expected: ID_DEEPSEEK,
        got: a.provider.clone(),
      }
      .fail();
    }
    let _ = a
      .api_key
      .clone()
      .filter(|s| !s.expose().trim().is_empty())
      .ok_or(error::Error::MissingCredential {
        account: a.id.clone(),
        what: "api_key",
      })?;
    Ok(())
  }

  pub fn from_account(a: Arc<AccountConfig>) -> Result<Self> {
    let base_url = a.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL);
    let target = ProviderTarget::parse(base_url, CleartextHttpPolicy::Allow).map_err(|source| {
      error::Error::InvalidUpstreamUrl {
        account: a.id.clone(),
        source,
      }
    })?;
    Self::from_account_at(a, target)
  }

  pub fn from_account_at(a: Arc<AccountConfig>, target: ProviderTarget) -> Result<Self> {
    Self::validate_account(&a)?;
    let key = a.api_key.clone().expect("validated api_key");
    let upstream_url = target.base_url().to_string();
    Ok(Self {
      id: format!("deepseek:{}", a.id),
      api_key: key,
      info: ProviderInfo {
        id: ID_DEEPSEEK.to_string(),
        aliases: &[ID_DEEPSEEK],
        display_name: "DeepSeek",
        upstream_url,
        auth_kind: AuthKind::StaticApiKey,
        default_models: crate::catalogue::default_models_for(ID_DEEPSEEK),
        default_endpoints: crate::DEFAULT_ENDPOINTS,
        model_cache: Arc::clone(target.model_cache()),
      },
      target,
    })
  }

  fn operation_url(&self, segments: &[&str]) -> Result<reqwest::Url> {
    Ok(self.target.base_url().operation_url(segments.iter().copied())?)
  }

  fn messages_url(&self) -> Result<reqwest::Url> {
    crate::operation_url(&self.target, Endpoint::Messages)
  }

  async fn upstream_post(
    &self,
    ctx: RequestCtx<'_>,
    url: reqwest::Url,
    what: &'static str,
  ) -> Result<reqwest::Response> {
    debug!(%url, "POST deepseek upstream");
    let mut headers = ctx.client_headers.clone().unwrap_or_default();
    self.patch_headers(
      &mut headers,
      &HeaderPatchCtx {
        request_kind: ProviderRequestKind::Operation(ctx.endpoint),
        body: ctx.body,
        bearer_token: None,
        content_encoding: ctx.content_encoding,
        stream: ctx.stream,
        initiator: ctx.initiator,
        inbound_headers: ctx.inbound_headers,
        vars: &ctx.vars,
        agent_id: &ctx.agent_id,
      },
    )?;
    let body_bytes = ctx.request_body_bytes();
    let resp = crate::util::http::send(
      ctx.http,
      Method::POST,
      url.as_str(),
      headers,
      Some(body_bytes),
      ctx.outbound.as_ref(),
      what,
    )
    .await?;
    Ok(resp)
  }
}

impl InputTransformer for DeepSeekProvider {
  fn transform_input(&self, endpoint: crate::Endpoint, body: Value) -> Result<Value> {
    Ok(shape_request(endpoint, body))
  }
}

#[async_trait]
impl Provider for DeepSeekProvider {
  fn id(&self) -> &str {
    &self.id
  }

  fn info(&self) -> &ProviderInfo {
    &self.info
  }

  fn input_transformer(&self) -> Option<&dyn InputTransformer> {
    Some(self)
  }

  fn inject_credentials(&self, headers: &mut HeaderMap, ctx: &HeaderPatchCtx<'_>) -> Result<()> {
    crate::native::inject_credentials(
      headers,
      ctx.endpoint().unwrap_or(Endpoint::ChatCompletions),
      self.api_key.expose(),
    );
    Ok(())
  }

  fn normalize_headers(&self, headers: &mut HeaderMap, ctx: &HeaderPatchCtx<'_>) -> Result<Option<HeaderMap>> {
    Ok(crate::native::normalize_headers(headers, ctx))
  }

  async fn list_models(&self, http: &reqwest::Client) -> Result<Value> {
    let mut headers = HeaderMap::new();
    self.patch_headers(
      &mut headers,
      &HeaderPatchCtx {
        request_kind: ProviderRequestKind::Models,
        body: &Value::Null,
        bearer_token: None,
        content_encoding: None,
        stream: false,
        initiator: "user",
        inbound_headers: &HeaderMap::new(),
        vars: &TemplateVars::default(),
        agent_id: &tokn_core::AgentId::Opencode,
      },
    )?;
    let url = self.operation_url(&["models"])?;
    let resp =
      crate::util::http::send(http, Method::GET, url.as_str(), headers, None, None, "deepseek /models").await?;
    crate::util::http::read_json(resp, "deepseek /models").await
  }

  #[instrument(name = "deepseek_chat", skip_all, fields(account = %self.id, stream = ctx.stream))]
  async fn chat(&self, ctx: RequestCtx<'_>) -> Result<reqwest::Response> {
    let url = crate::operation_url(&self.target, ctx.endpoint)?;
    self.upstream_post(ctx, url, "deepseek chat").await
  }

  #[instrument(name = "deepseek_messages", skip_all, fields(account = %self.id, stream = ctx.stream))]
  async fn messages(&self, ctx: RequestCtx<'_>) -> Result<reqwest::Response> {
    let url = self.messages_url()?;
    self.upstream_post(ctx, url, "deepseek messages").await
  }

  fn on_unauthorized(&self) {
    warn!(account = %self.id, key_fp = %self.api_key.fingerprint(), "deepseek upstream returned 401: api_key likely revoked or expired");
  }
}

use crate::native::transform_input as shape_request;

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;
  use tokn_core::account::AccountTier;
  use tokn_core::provider::Endpoint;

  fn acct(key: Option<&str>) -> AccountConfig {
    AccountConfig {
      id: "test".into(),
      provider: ID_DEEPSEEK.into(),
      enabled: true,
      tier: AccountTier::Active,
      tags: Vec::new(),
      label: None,
      base_url: None,
      headers: Default::default(),
      auth_type: None,
      username: None,
      api_key: key.map(|s| Secret::new(s.into())),
      api_key_expires_at: None,
      access_token: None,
      access_token_expires_at: None,
      id_token: None,
      refresh_token: None,
      provider_account_id: None,
      extra: Default::default(),
      refresh_url: None,
      last_refresh: None,
      settings: toml::Table::new(),
    }
  }

  #[test]
  fn rejects_missing_api_key() {
    let err = DeepSeekProvider::from_account(Arc::new(acct(None))).err().unwrap();
    assert!(err.to_string().contains("api_key"));
  }

  #[test]
  fn constructs_with_default_url() {
    let p = DeepSeekProvider::from_account(Arc::new(acct(Some("sk-test")))).unwrap();
    assert_eq!(p.info().id, ID_DEEPSEEK);
    assert_eq!(p.info().upstream_url, format!("{DEFAULT_BASE_URL}/"));
  }

  #[test]
  fn explicit_target_owns_destination_and_model_cache() {
    let mut account = acct(Some("sk-test"));
    account.base_url = Some("https://ignored.example".into());
    let target = ProviderTarget::parse("https://proxy.example/deepseek", CleartextHttpPolicy::Allow).unwrap();
    let model_cache = Arc::clone(target.model_cache());

    let p = DeepSeekProvider::from_account_at(Arc::new(account), target).unwrap();

    assert_eq!(p.info().upstream_url, "https://proxy.example/deepseek/");
    assert_eq!(
      p.operation_url(&["models"]).unwrap().as_str(),
      "https://proxy.example/deepseek/models"
    );
    assert!(Arc::ptr_eq(&p.info().model_cache, &model_cache));
  }

  #[test]
  fn supports_messages_endpoint() {
    let p = DeepSeekProvider::from_account(Arc::new(acct(Some("sk-test")))).unwrap();
    assert!(p.supports("", crate::Endpoint::ChatCompletions));
    assert!(p.supports("", crate::Endpoint::Messages));
  }

  #[test]
  fn chat_shape_uses_thinking_and_reasoning_effort() {
    let body = json!({
      "model": "deepseek-v4-flash",
      "messages": [{"role": "user", "content": "hi"}],
      "reasoning": {"effort": "high"}
    });

    let out = shape_request(Endpoint::ChatCompletions, body);

    assert_eq!(out.get("thinking"), Some(&json!({"type": "enabled"})));
    assert_eq!(out.get("reasoning_effort"), Some(&json!("high")));
    assert!(out.get("reasoning").is_none());
    assert!(out.get("output_config").is_none());
  }

  #[test]
  fn messages_shape_uses_thinking_and_output_config_effort() {
    let body = json!({
      "model": "deepseek-v4-flash",
      "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
      "thinking": {"effort": "max"}
    });

    let out = shape_request(Endpoint::Messages, body);

    assert_eq!(out.get("thinking"), Some(&json!({"type": "enabled"})));
    assert_eq!(out.get("output_config"), Some(&json!({"effort": "max"})));
    assert!(out.get("reasoning").is_none());
    assert!(out.get("reasoning_effort").is_none());
  }

  #[test]
  fn native_effort_survives_both_endpoint_transforms() {
    let provider = DeepSeekProvider::from_account(Arc::new(acct(Some("sk-test")))).unwrap();
    for effort in ["low", "high", "max"] {
      for control in [
        json!({"reasoning_effort": effort}),
        json!({"output_config": {"effort": effort}}),
      ] {
        for endpoint in [Endpoint::ChatCompletions, Endpoint::Messages] {
          let mut body = json!({
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": "hi"}]
          });
          body
            .as_object_mut()
            .unwrap()
            .extend(control.as_object().unwrap().clone());

          let out = provider.transform_input(endpoint, body).unwrap();

          assert_eq!(out["thinking"], json!({"type": "enabled"}));
          match endpoint {
            Endpoint::ChatCompletions => {
              assert_eq!(out["reasoning_effort"], effort);
              assert!(out.get("output_config").is_none());
            }
            Endpoint::Messages => {
              assert_eq!(out["output_config"]["effort"], effort);
              assert!(out.get("reasoning_effort").is_none());
            }
            Endpoint::Responses => unreachable!(),
          }
          assert_eq!(provider.transform_input(endpoint, out.clone()).unwrap(), out);
        }
      }
    }
  }

  #[test]
  fn native_effort_respects_explicit_disabled_thinking() {
    for endpoint in [Endpoint::ChatCompletions, Endpoint::Messages] {
      let out = shape_request(
        endpoint,
        json!({
          "thinking": {"type": "disabled"},
          "reasoning_effort": "high"
        }),
      );

      assert_eq!(out["thinking"], json!({"type": "disabled"}));
      let effort = match endpoint {
        Endpoint::ChatCompletions => &out["reasoning_effort"],
        Endpoint::Messages => &out["output_config"]["effort"],
        Endpoint::Responses => unreachable!(),
      };
      assert_eq!(effort, "high");
    }
  }

  #[test]
  fn nested_effort_keeps_precedence_over_native_fields() {
    for control in ["thinking", "reasoning"] {
      let mut body = json!({
        "reasoning_effort": "high",
        "output_config": {"effort": "high"}
      });
      body[control] = json!({"effort": "max"});

      assert_eq!(
        shape_request(Endpoint::ChatCompletions, body.clone())["reasoning_effort"],
        "max"
      );
      assert_eq!(
        shape_request(Endpoint::Messages, body)["output_config"]["effort"],
        "max"
      );
    }
  }

  #[test]
  fn live_efforts_override_catalogue_and_missing_metadata_falls_back() {
    let provider = DeepSeekProvider::from_account(Arc::new(acct(Some("sk-test")))).unwrap();
    assert_eq!(
      serde_json::to_value(provider.reasoning_efforts("deepseek-v4-flash")).unwrap(),
      json!(["low", "high", "max"])
    );
    assert_eq!(
      serde_json::to_value(provider.reasoning_efforts("deepseek-v4-pro")).unwrap(),
      json!(["high", "max"])
    );
    provider.info().model_cache.set_models(&[json!({
      "id": "deepseek-v4-flash", "capabilities": {"supports": {"reasoning_effort": []}}
    })]);
    assert_eq!(provider.reasoning_efforts("deepseek-v4-flash"), Some(vec![]));
    provider
      .info()
      .model_cache
      .set_models(&[json!({"id": "deepseek-v4-flash"})]);
    assert_eq!(
      serde_json::to_value(provider.reasoning_efforts("deepseek-v4-flash")).unwrap(),
      json!(["low", "high", "max"])
    );
    assert!(provider.reasoning_efforts("future-model").is_none());
  }

  #[test]
  fn defaults_thinking_to_disabled_without_reasoning() {
    let body = json!({
      "model": "deepseek-v4-flash",
      "messages": [{"role": "user", "content": "hi"}]
    });

    let out = shape_request(Endpoint::ChatCompletions, body);

    assert_eq!(out.get("thinking"), Some(&json!({"type": "disabled"})));
    assert!(out.get("reasoning_effort").is_none());
  }

  #[test]
  fn messages_url_adds_the_anthropic_prefix() {
    let p = DeepSeekProvider::from_account(Arc::new(acct(Some("sk-test")))).unwrap();
    assert_eq!(
      p.messages_url().unwrap().as_str(),
      "https://api.deepseek.com/anthropic/v1/messages"
    );
  }

  #[test]
  fn messages_url_does_not_duplicate_an_anthropic_prefix() {
    let mut account = acct(Some("sk-test"));
    account.base_url = Some("https://api.deepseek.com/anthropic".into());
    let p = DeepSeekProvider::from_account(Arc::new(account)).unwrap();
    assert_eq!(
      p.messages_url().unwrap().as_str(),
      "https://api.deepseek.com/anthropic/v1/messages"
    );
  }

  fn patch_ctx(endpoint: Endpoint, stream: bool, content_encoding: Option<&'static str>) -> HeaderPatchCtx<'static> {
    HeaderPatchCtx {
      request_kind: ProviderRequestKind::Operation(endpoint),
      body: &Value::Null,
      bearer_token: None,
      content_encoding,
      stream,
      initiator: "user",
      inbound_headers: Box::leak(Box::new(HeaderMap::new())),
      vars: Box::leak(Box::new(TemplateVars::default())),
      agent_id: Box::leak(Box::new(tokn_core::AgentId::Opencode)),
    }
  }

  fn provider() -> DeepSeekProvider {
    DeepSeekProvider::from_account(Arc::new(acct(Some("sk-test-fixture")))).unwrap()
  }

  #[test]
  fn deepseek_patch_headers_chat_streaming() {
    let p = provider();
    let mut h = HeaderMap::new();
    p.patch_headers(&mut h, &patch_ctx(Endpoint::ChatCompletions, true, None))
      .unwrap();
    assert_eq!(h.get("authorization").unwrap().as_str(), "Bearer sk-test-fixture");
    assert_eq!(h.get("accept").unwrap().as_str(), "text/event-stream");
    assert_eq!(h.get("content-type").unwrap().as_str(), "application/json");
    assert!(h.get("content-encoding").is_none());
    let names: Vec<_> = h.iter().map(|(n, _)| n.as_str().to_string()).collect();
    assert_eq!(names.len(), 3, "unexpected extra headers: {names:?}");
  }

  #[test]
  fn deepseek_patch_headers_chat_non_streaming() {
    let p = provider();
    let mut h = HeaderMap::new();
    p.patch_headers(&mut h, &patch_ctx(Endpoint::ChatCompletions, false, None))
      .unwrap();
    assert_eq!(h.get("authorization").unwrap().as_str(), "Bearer sk-test-fixture");
    assert_eq!(h.get("accept").unwrap().as_str(), "application/json");
    assert_eq!(h.get("content-type").unwrap().as_str(), "application/json");
    assert!(h.get("content-encoding").is_none());
  }

  #[test]
  fn deepseek_patch_headers_messages_streaming_matches_chat_shape() {
    let p = provider();
    let mut h = HeaderMap::new();
    p.patch_headers(&mut h, &patch_ctx(Endpoint::Messages, true, None))
      .unwrap();
    assert_eq!(h.get("authorization").unwrap().as_str(), "Bearer sk-test-fixture");
    assert_eq!(h.get("accept").unwrap().as_str(), "text/event-stream");
    assert_eq!(h.get("content-type").unwrap().as_str(), "application/json");
  }

  #[test]
  fn deepseek_patch_headers_round_trips_content_encoding() {
    let p = provider();
    let mut h = HeaderMap::new();
    p.patch_headers(&mut h, &patch_ctx(Endpoint::ChatCompletions, false, Some("gzip")))
      .unwrap();
    assert_eq!(h.get("content-encoding").unwrap().as_str(), "gzip");
  }
}
