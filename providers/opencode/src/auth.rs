use async_trait::async_trait;
use tokn_auth::{ProviderAuth, QuotaSnapshot, RefreshOutcome, Result, VerifyOutcome};
use tokn_core::account::AccountConfig;

pub struct OpenCodeGoAuth;

pub fn provider_auth() -> &'static dyn ProviderAuth {
  &OpenCodeGoAuth
}

#[async_trait]
impl ProviderAuth for OpenCodeGoAuth {
  fn id(&self) -> &'static str {
    crate::ID_OPENCODE_GO
  }

  fn supports_static_key(&self) -> bool {
    true
  }

  fn default_base_url(&self) -> Option<&'static str> {
    Some(crate::OPENCODE_GO_BASE_URL)
  }

  async fn refresh_credential(&self, _client: &reqwest::Client, _account: &AccountConfig) -> Result<RefreshOutcome> {
    Ok(RefreshOutcome::NotApplicable)
  }

  async fn verify_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<VerifyOutcome> {
    crate::quota::verify(client, account).await?;
    Ok(VerifyOutcome::default())
  }

  async fn probe_quota(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<QuotaSnapshot> {
    crate::quota::fetch(client, account).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use reqwest::Method;
  use serde_json::json;
  use tokn_mock_server::{MockAuthConfig, MockEndpoint, MockLlmConfig, MockLlmServer, MockResponse, MockRoute};

  async fn server() -> MockLlmServer {
    MockLlmServer::start(
      MockLlmConfig::default()
        .with_auth(MockAuthConfig::bearer(["sk-test"]))
        .with_route(MockRoute::new(
          MockEndpoint::Custom {
            method: Method::GET,
            path: "/usage".into(),
          },
          MockResponse::json(json!({
            "usage": {
              "rolling": { "status": "ok", "percent": 10, "resetsAt": "2030-03-17T12:00:00Z" },
              "weekly": { "status": "ok", "percent": 20, "resetsAt": "2030-03-18T00:00:00Z" },
              "monthly": { "status": "ok", "percent": 30, "resetsAt": "2030-04-01T00:00:00Z" }
            }
          })),
        )),
    )
    .await
  }

  fn account(base_url: &str) -> AccountConfig {
    serde_json::from_value(json!({
      "id": "opencode-go-test",
      "provider": "opencode-go",
      "base_url": base_url,
      "api_key": "sk-test"
    }))
    .unwrap()
  }

  #[tokio::test]
  async fn exposes_static_key_capabilities_and_authenticated_status() {
    let server = server().await;
    let account = account(server.base_url());
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let auth = provider_auth();

    assert_eq!(auth.id(), crate::ID_OPENCODE_GO);
    assert!(auth.supports_static_key());
    assert_eq!(auth.default_base_url(), Some(crate::OPENCODE_GO_BASE_URL));
    assert!(matches!(
      auth.refresh_credential(&client, &account).await.unwrap(),
      RefreshOutcome::NotApplicable
    ));
    auth.verify_credential(&client, &account).await.unwrap();
    let quota = auth.probe_quota(&client, &account).await.unwrap();
    assert_eq!(quota.secondary.len(), 3);
  }
}
