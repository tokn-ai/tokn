use async_trait::async_trait;
use tokn_auth::{AuthError, ProviderAuth, QuotaSnapshot, RefreshOutcome, Result, VerifyOutcome};
use tokn_core::account::AccountConfig;

pub struct LlamaCppAuth;

pub fn provider_auth() -> &'static dyn ProviderAuth {
  &LlamaCppAuth
}

#[async_trait]
impl ProviderAuth for LlamaCppAuth {
  fn id(&self) -> &'static str {
    crate::ID_LLAMA_CPP
  }

  fn supports_static_key(&self) -> bool {
    true
  }

  fn default_account_id(&self) -> &'static str {
    crate::ID_LLAMA_CPP
  }

  fn default_base_url(&self) -> Option<&'static str> {
    Some(crate::llama_cpp::DEFAULT_BASE_URL)
  }

  async fn refresh_credential(&self, _client: &reqwest::Client, _account: &AccountConfig) -> Result<RefreshOutcome> {
    Ok(RefreshOutcome::NotApplicable)
  }

  async fn verify_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<VerifyOutcome> {
    let base = account
      .base_url
      .clone()
      .unwrap_or_else(|| crate::llama_cpp::DEFAULT_BASE_URL.to_string());
    let mut req = client
      .get(format!("{}/models", base.trim_end_matches('/')))
      .header("accept", "application/json");
    if let Some(key) = account.api_key.as_ref().filter(|key| !key.expose().trim().is_empty()) {
      req = req.header("authorization", format!("Bearer {}", key.expose()));
    }
    let resp = req.send().await.map_err(|e| AuthError::Network(e.to_string()))?;
    if resp.status().is_success() {
      Ok(VerifyOutcome::default())
    } else {
      let status = resp.status();
      let body = resp.text().await.unwrap_or_default();
      Err(AuthError::Upstream(format!(
        "llama.cpp rejected the credential probe (HTTP {status}): {}",
        body.chars().take(200).collect::<String>()
      )))
    }
  }

  async fn probe_quota(&self, _client: &reqwest::Client, _account: &AccountConfig) -> Result<QuotaSnapshot> {
    Ok(QuotaSnapshot::default())
  }
}
