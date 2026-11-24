//! OpenAI provider authentication: a thin wrapper around the static
//! API key (`Authorization: Bearer sk-…`). Codex (ChatGPT account)
//! authentication lives in [`codex_auth`].

use async_trait::async_trait;
use tokn_auth::{AuthError, ProviderAuth, QuotaSnapshot, RefreshOutcome, Result, VerifyOutcome};
use tokn_core::account::AccountConfig;

pub struct OpenAiAuth;

static OPENAI: OpenAiAuth = OpenAiAuth;

pub fn openai_auth() -> &'static dyn ProviderAuth {
  &OPENAI
}

#[async_trait]
impl ProviderAuth for OpenAiAuth {
  fn id(&self) -> &'static str {
    crate::ID_OPENAI
  }

  fn supports_static_key(&self) -> bool {
    true
  }

  fn default_account_id(&self) -> &'static str {
    crate::ID_OPENAI
  }

  fn default_base_url(&self) -> Option<&'static str> {
    Some(crate::openai::OPENAI_BASE_URL)
  }

  async fn refresh_credential(&self, _client: &reqwest::Client, _account: &AccountConfig) -> Result<RefreshOutcome> {
    Ok(RefreshOutcome::NotApplicable)
  }

  async fn verify_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<VerifyOutcome> {
    let token = account.api_key.as_ref().ok_or(AuthError::MissingCredential {
      account: account.id.clone(),
      field: "api_key",
    })?;
    let base = account
      .base_url
      .clone()
      .unwrap_or_else(|| crate::openai::OPENAI_BASE_URL.to_string());
    let url = format!("{}/models", base.trim_end_matches('/'));
    let resp = client
      .get(url)
      .header("authorization", format!("Bearer {}", token.expose()))
      .header("accept", "application/json")
      .send()
      .await
      .map_err(|e| AuthError::Network(e.to_string()))?;
    if resp.status().is_success() {
      Ok(VerifyOutcome::default())
    } else {
      let status = resp.status();
      let body = resp.text().await.unwrap_or_default();
      Err(AuthError::Upstream(format!(
        "openai rejected the credential (HTTP {status}): {}",
        body.chars().take(200).collect::<String>()
      )))
    }
  }

  async fn probe_quota(&self, _client: &reqwest::Client, _account: &AccountConfig) -> Result<QuotaSnapshot> {
    Ok(QuotaSnapshot::default())
  }
}
