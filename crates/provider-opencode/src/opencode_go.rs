use crate::util::secret::Secret;
use async_trait::async_trait;
use reqwest::Method;
use serde_json::Value;
use std::sync::Arc;
use tokn_core::account::AccountConfig;
use tokn_core::provider::ProviderTarget;
use tokn_core::upstream_url::{CanonicalUpstreamUrl, CleartextHttpPolicy};
#[cfg(test)]
use tokn_headers::{keys::X_OPENCODE_SESSION, HeaderValue};
use tokn_headers::HeaderMap;
use tracing::{debug, instrument, warn};

use crate::{
  error, AuthKind, Endpoint, HeaderPatchCtx, Provider, ProviderInfo, ProviderRequestKind, RequestCtx, Result,
  TemplateVars, ID_OPENCODE_GO,
};

pub struct OpenCodeGoProvider {
  pub id: String,
  api_key: Secret<String>,
  target: ProviderTarget,
  info: ProviderInfo,
}

impl OpenCodeGoProvider {
  pub fn validate_account(account: &AccountConfig) -> Result<()> {
    if account.provider != ID_OPENCODE_GO {
      return error::ProviderMismatchSnafu {
        expected: ID_OPENCODE_GO,
        got: account.provider.clone(),
      }
      .fail();
    }
    account
      .api_key
      .as_ref()
      .filter(|key| !key.expose().trim().is_empty())
      .ok_or_else(|| error::Error::MissingCredential {
        account: account.id.clone(),
        what: "api_key",
      })?;
    Ok(())
  }

  pub fn from_account(account: Arc<AccountConfig>) -> Result<Self> {
    let base_url = account.base_url.as_deref().unwrap_or(crate::OPENCODE_GO_BASE_URL);
    let target = ProviderTarget::parse(base_url, CleartextHttpPolicy::LoopbackOnly).map_err(|source| {
      error::Error::InvalidUpstreamUrl {
        account: account.id.clone(),
        source,
      }
    })?;
    Self::from_account_at(account, target)
  }

  pub fn from_account_at(account: Arc<AccountConfig>, target: ProviderTarget) -> Result<Self> {
    Self::validate_account(&account)?;
    CanonicalUpstreamUrl::parse(target.base_url().as_str(), CleartextHttpPolicy::LoopbackOnly).map_err(|source| {
      error::Error::InvalidUpstreamUrl {
        account: account.id.clone(),
        source,
      }
    })?;
    let api_key = account.api_key.clone().expect("validated api_key");
    let upstream_url = target.base_url().to_string();
    let model_cache = target.model_cache().clone();
    Ok(Self {
      id: format!("{ID_OPENCODE_GO}:{}", account.id),
      api_key,
      target,
      info: ProviderInfo {
        id: ID_OPENCODE_GO.to_string(),
        aliases: &[ID_OPENCODE_GO],
        display_name: "OpenCode Go",
        upstream_url,
        auth_kind: AuthKind::StaticApiKey,
        default_models: crate::catalogue::default_models_for(ID_OPENCODE_GO),
        default_endpoints: crate::DEFAULT_ENDPOINTS,
        model_cache,
      },
    })
  }

  fn operation_url(&self, endpoint: Endpoint) -> Result<reqwest::Url> {
    crate::operation_url(&self.target, endpoint)
  }

  async fn upstream_post(&self, ctx: RequestCtx<'_>, what: &'static str) -> Result<reqwest::Response> {
    let url = self.operation_url(ctx.endpoint)?;
    debug!(%url, "POST OpenCode Go upstream");
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
    crate::util::http::send(
      ctx.http,
      Method::POST,
      url.as_str(),
      headers,
      Some(ctx.request_body_bytes()),
      ctx.outbound.as_ref(),
      what,
    )
    .await
  }
}

#[async_trait]
impl Provider for OpenCodeGoProvider {
  fn id(&self) -> &str {
    &self.id
  }

  fn info(&self) -> &ProviderInfo {
    &self.info
  }

  fn endpoint_rules(&self) -> Option<&'static [tokn_core::provider::EndpointRule]> {
    None
  }

  fn has_endpoint(&self, model: &str, endpoint: Endpoint) -> bool {
    crate::native::supports_model_endpoint_with_catalogue(
      model,
      endpoint,
      self.target.base_url().as_str(),
      crate::catalogue::endpoint_for_model(ID_OPENCODE_GO, model),
    )
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
    let url = self.target.base_url().operation_url(["models"])?;
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
    let response = crate::util::http::send(
      http,
      Method::GET,
      url.as_str(),
      headers,
      None,
      None,
      "OpenCode Go /models",
    )
    .await?;
    crate::util::http::read_json(response, "OpenCode Go /models").await
  }

  #[instrument(name = "opencode_go_chat", skip_all, fields(account = %self.id, stream = ctx.stream))]
  async fn chat(&self, ctx: RequestCtx<'_>) -> Result<reqwest::Response> {
    self.upstream_post(ctx, "OpenCode Go chat").await
  }

  #[instrument(name = "opencode_go_responses", skip_all, fields(account = %self.id, stream = ctx.stream))]
  async fn responses(&self, ctx: RequestCtx<'_>) -> Result<reqwest::Response> {
    self.upstream_post(ctx, "OpenCode Go responses").await
  }

  #[instrument(name = "opencode_go_messages", skip_all, fields(account = %self.id, stream = ctx.stream))]
  async fn messages(&self, ctx: RequestCtx<'_>) -> Result<reqwest::Response> {
    self.upstream_post(ctx, "OpenCode Go messages").await
  }

  fn on_unauthorized(&self) {
    warn!(account = %self.id, "OpenCode Go rejected the API key");
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tokn_core::account::AccountTier;
  use tokn_mock_server::{HeaderExpectation, MockAuthConfig, MockLlmConfig, MockLlmServer, MockRoute};

  fn account(key: Option<&str>) -> AccountConfig {
    AccountConfig {
      id: "test".into(),
      provider: ID_OPENCODE_GO.into(),
      enabled: true,
      tier: AccountTier::Active,
      tags: Vec::new(),
      label: None,
      base_url: None,
      headers: Default::default(),
      auth_type: None,
      username: None,
      api_key: key.map(|key| Secret::new(key.to_string())),
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

  fn request_ctx<'a>(
    endpoint: Endpoint,
    http: &'a reqwest::Client,
    body: &'a Value,
    inbound: &'a HeaderMap,
  ) -> RequestCtx<'a> {
    RequestCtx {
      endpoint,
      http,
      body,
      body_bytes: None,
      content_encoding: None,
      stream: false,
      initiator: "user",
      inbound_headers: inbound,
      client_headers: None,
      outbound: None,
      vars: TemplateVars {
        session_id: Some("session-42".into()),
        ..Default::default()
      },
      agent_id: tokn_core::AgentId::Opencode,
    }
  }

  #[test]
  fn requires_api_key() {
    let error = OpenCodeGoProvider::from_account(Arc::new(account(None))).err().unwrap();
    assert!(error.to_string().contains("api_key"));
  }

  #[test]
  fn rejects_cleartext_non_loopback_targets() {
    let mut configured = account(Some("sk-test"));
    configured.base_url = Some("http://api.example.com/zen/go/v1".into());
    let error = OpenCodeGoProvider::from_account(Arc::new(configured.clone()))
      .err()
      .unwrap();
    assert!(error.to_string().contains("non-loopback HTTP"));

    let prebuilt = ProviderTarget::parse(configured.base_url.as_deref().unwrap(), CleartextHttpPolicy::Allow).unwrap();
    let error = OpenCodeGoProvider::from_account_at(Arc::new(configured), prebuilt)
      .err()
      .unwrap();
    assert!(error.to_string().contains("non-loopback HTTP"));
  }

  #[test]
  fn fallback_endpoint_rules_cover_each_protocol() {
    let provider = OpenCodeGoProvider::from_account(Arc::new(account(Some("sk-test")))).unwrap();
    assert!(provider.has_endpoint("gpt-5.6-luna", Endpoint::Responses));
    assert!(provider.has_endpoint("minimax-m3", Endpoint::Messages));
    assert!(provider.has_endpoint("qwen3.7-max", Endpoint::Messages));
    assert!(!provider.has_endpoint("qwen3.7-max", Endpoint::ChatCompletions));
    assert!(provider.has_endpoint("glm-5.3", Endpoint::ChatCompletions));
    assert!(!provider.has_endpoint("gpt-5.6-luna", Endpoint::ChatCompletions));
  }

  #[test]
  fn request_id_is_used_as_the_session_fallback() {
    let provider = OpenCodeGoProvider::from_account(Arc::new(account(Some("sk-test")))).unwrap();
    let mut headers = HeaderMap::new();
    let body = Value::Null;
    let inbound = HeaderMap::new();
    let vars = TemplateVars {
      request_id: Some("request-42".into()),
      ..Default::default()
    };
    provider
      .patch_headers(
        &mut headers,
        &HeaderPatchCtx {
          request_kind: ProviderRequestKind::Operation(Endpoint::Responses),
          body: &body,
          bearer_token: None,
          content_encoding: None,
          stream: false,
          initiator: "user",
          inbound_headers: &inbound,
          vars: &vars,
          agent_id: &tokn_core::AgentId::Opencode,
        },
      )
      .unwrap();

    assert_eq!(
      headers.get(&X_OPENCODE_SESSION).map(HeaderValue::as_str),
      Some("request-42")
    );
  }

  #[tokio::test]
  async fn sends_all_protocols_with_required_identity_headers() {
    let openai_server = MockLlmServer::start(
      MockLlmConfig {
        routes: vec![MockRoute::chat_completions(), MockRoute::responses()],
        ..Default::default()
      }
      .with_auth(MockAuthConfig::bearer(["sk-test"]))
      .require_header(HeaderExpectation::equals("x-opencode-session", "session-42"))
      .require_header(HeaderExpectation::present("user-agent")),
    )
    .await;
    let mut configured = account(Some("sk-test"));
    configured.base_url = Some(openai_server.base_url().to_string());
    let provider = OpenCodeGoProvider::from_account(Arc::new(configured)).unwrap();
    let http = reqwest::Client::new();
    let body = serde_json::json!({"model": "fixture"});
    let inbound = HeaderMap::new();

    let chat = provider
      .chat(request_ctx(Endpoint::ChatCompletions, &http, &body, &inbound))
      .await
      .unwrap();
    assert_eq!(chat.status(), reqwest::StatusCode::OK);
    let responses = provider
      .responses(request_ctx(Endpoint::Responses, &http, &body, &inbound))
      .await
      .unwrap();
    assert_eq!(responses.status(), reqwest::StatusCode::OK);

    let messages_server = MockLlmServer::start(
      MockLlmConfig {
        routes: vec![MockRoute::messages()],
        ..Default::default()
      }
      .with_auth(MockAuthConfig::header("x-api-key", ["sk-test"]))
      .require_header(HeaderExpectation::equals("x-opencode-session", "session-42"))
      .require_header(HeaderExpectation::present("user-agent"))
      .forbid_header("authorization"),
    )
    .await;
    let mut configured = account(Some("sk-test"));
    configured.base_url = Some(messages_server.base_url().to_string());
    let provider = OpenCodeGoProvider::from_account(Arc::new(configured)).unwrap();
    let messages = provider
      .messages(request_ctx(Endpoint::Messages, &http, &body, &inbound))
      .await
      .unwrap();
    assert_eq!(messages.status(), reqwest::StatusCode::OK);
  }
}
