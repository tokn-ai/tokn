use async_trait::async_trait;
use tokn_auth::{AuthError, ProviderAuth, QuotaSnapshot, RefreshOutcome, Result, VerifyOutcome};
use tokn_core::account::AccountConfig;

pub struct DeepSeekAuth;

pub fn provider_auth() -> &'static dyn ProviderAuth {
  &DeepSeekAuth
}

#[async_trait]
impl ProviderAuth for DeepSeekAuth {
  fn id(&self) -> &'static str {
    crate::ID_DEEPSEEK
  }

  fn supports_static_key(&self) -> bool {
    true
  }

  fn default_base_url(&self) -> Option<&'static str> {
    Some(crate::deepseek::DEFAULT_BASE_URL)
  }

  async fn refresh_credential(&self, _client: &reqwest::Client, _account: &AccountConfig) -> Result<RefreshOutcome> {
    Ok(RefreshOutcome::NotApplicable)
  }

  async fn verify_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<VerifyOutcome> {
    let key = account.api_key.as_ref().ok_or(AuthError::MissingCredential {
      account: account.id.clone(),
      field: "api_key",
    })?;
    let base = account
      .base_url
      .clone()
      .unwrap_or_else(|| crate::deepseek::DEFAULT_BASE_URL.to_string());
    let resp = client
      .get(format!("{}/models", base.trim_end_matches('/')))
      .header("authorization", format!("Bearer {}", key.expose()))
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
        "DeepSeek rejected the key (HTTP {status}): {}",
        body.chars().take(200).collect::<String>()
      )))
    }
  }

  async fn probe_quota(&self, _client: &reqwest::Client, _account: &AccountConfig) -> Result<QuotaSnapshot> {
    Ok(QuotaSnapshot::default())
  }
}
