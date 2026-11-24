//! [`ProviderAuth`] impl for github-copilot.
//!
//! Wraps the existing `oauth`, `token`, and `user` modules in the
//! provider-agnostic contract defined by `tokn-core::auth`. Holds no state;
//! exposed via [`provider_auth()`].

use crate::config::CopilotHeaders;
use async_trait::async_trait;
use tokn_auth::{
  default_import_from, AuthError, CredentialResult, CredentialSource, DeviceCodeHandle, DeviceFlowOutcome,
  ProviderAuth, QuotaSnapshot, RefreshOutcome, Result, VerifyOutcome,
};
use tokn_core::account::AccountConfig;

/// Singleton impl. Zero-sized; safe to hand out as `&'static`.
pub struct CopilotAuth;

/// Static accessor used by `tokn-auth`'s dispatch table.
pub fn provider_auth() -> &'static dyn ProviderAuth {
  &CopilotAuth
}

#[async_trait]
impl ProviderAuth for CopilotAuth {
  fn id(&self) -> &'static str {
    crate::ID_GITHUB_COPILOT
  }

  fn supports_device_flow(&self) -> bool {
    true
  }

  fn default_account_id(&self) -> &'static str {
    // Copilot login attempts a GitHub username lookup; this is only used
    // when that lookup fails.
    "default"
  }

  fn default_refresh_url(&self) -> Option<&'static str> {
    Some(crate::COPILOT_TOKEN_EXCHANGE_URL)
  }

  fn custom_credential_sources(&self) -> &'static [&'static str] {
    &["gh", "copilot-plugin"]
  }

  async fn import_from(&self, source: &CredentialSource) -> Result<CredentialResult> {
    match source {
      CredentialSource::Custom { key: "gh", .. } => crate::import::from_gh()
        .map(CredentialResult::Refresh)
        .map_err(AuthError::Other),
      CredentialSource::Custom {
        key: "copilot-plugin", ..
      } => crate::import::from_copilot_plugin()
        .map(CredentialResult::Refresh)
        .map_err(AuthError::Other),
      CredentialSource::Custom { key, .. } => Err(AuthError::Unsupported(format!(
        "github-copilot does not support custom credential source `{key}`"
      ))),
      // Env / String / File / Login → fall through to the default impl,
      // which uses the source's `flavor` to wrap correctly.
      _ => default_import_from(self.id(), source),
    }
  }

  async fn request_device_code(&self, client: &reqwest::Client) -> Result<DeviceCodeHandle> {
    let dc = crate::oauth::request_device_code(client)
      .await
      .map_err(|e| AuthError::Upstream(e.to_string()))?;
    tracing::info!(
      target: "tokn_auth::login",
      verification_uri = %dc.verification_uri,
      user_code = %dc.user_code,
      expires_in = dc.expires_in,
      "device code issued"
    );
    Ok(DeviceCodeHandle {
      device_code: dc.device_code,
      user_code: dc.user_code,
      verification_uri: dc.verification_uri,
      expires_in: dc.expires_in,
      interval: dc.interval,
    })
  }

  async fn poll_device_code(&self, client: &reqwest::Client, handle: DeviceCodeHandle) -> Result<DeviceFlowOutcome> {
    // Reconstruct the upstream DeviceCode from our public handle. Both
    // structs are field-for-field identical; we keep them separate so the
    // trait surface stays free of provider types.
    let dc = crate::oauth::DeviceCode {
      device_code: handle.device_code,
      user_code: handle.user_code,
      verification_uri: handle.verification_uri,
      expires_in: handle.expires_in,
      interval: handle.interval,
    };
    let gh_token = crate::oauth::poll_for_token(client, &dc)
      .await
      .map_err(|e| AuthError::Upstream(e.to_string()))?;

    // Exchange long-lived OAuth token for a short-lived access token.
    let headers = CopilotHeaders::default();
    let exchange = crate::token::exchange(client, &gh_token, &headers)
      .await
      .map_err(|e| AuthError::Upstream(e.to_string()))?;

    // Best-effort GitHub username lookup for id suggestion.
    let username = fetch_github_username(client, &gh_token).await.ok();

    Ok(DeviceFlowOutcome {
      refresh_token: gh_token,
      access_token: exchange.token,
      access_token_expires_at: exchange.expires_at,
      username,
      provider_account_id: None,
    })
  }

  async fn refresh_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<RefreshOutcome> {
    let gh_token = account.refresh_token.as_ref().ok_or(AuthError::MissingCredential {
      account: account.id.clone(),
      field: "refresh_token",
    })?;
    let headers = headers_from_settings(account)?;
    let resp = crate::token::exchange(client, gh_token.expose(), &headers)
      .await
      .map_err(|e| AuthError::Upstream(e.to_string()))?;
    let username = fetch_github_username(client, gh_token.expose()).await.ok();
    Ok(RefreshOutcome::Refreshed {
      access_token: resp.token,
      expires_at: resp.expires_at,
      refresh_token: None,
      id_token: None,
      username,
      provider_account_id: None,
    })
  }

  async fn verify_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<VerifyOutcome> {
    // A successful refresh proves the long-lived token still works.
    let refreshed = self.refresh_credential(client, account).await?;
    let mut username = match refreshed {
      RefreshOutcome::Refreshed { username, .. } => username,
      RefreshOutcome::Unchanged | RefreshOutcome::NotApplicable => None,
    };
    if username.is_none() {
      if let Some(gh_token) = account.refresh_token.as_ref() {
        username = fetch_github_username(client, gh_token.expose()).await.ok();
      }
    }
    Ok(VerifyOutcome { username })
  }

  async fn probe_quota(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<QuotaSnapshot> {
    let gh_token = account.refresh_token.as_ref().ok_or(AuthError::MissingCredential {
      account: account.id.clone(),
      field: "refresh_token",
    })?;
    let headers = headers_from_settings(account)?;
    let info = crate::user::fetch(client, gh_token.expose(), &headers)
      .await
      .map_err(|e| AuthError::Upstream(e.to_string()))?;

    // Pick the most informative metered bucket. Preference order matches
    // the legacy CLI: premium_interactions > chat > completions > any
    // other metered bucket > preferred unmetered (rendered as unlimited).
    let snaps = &info.quota_snapshots;
    let preferred = ["premium_interactions", "chat", "completions"];
    let metered: Option<tokn_auth::MeteredBucket> = preferred
      .iter()
      .find_map(|k| {
        snaps.get(*k).and_then(|s| {
          if s.unlimited {
            None
          } else {
            Some(tokn_auth::MeteredBucket {
              label: k.to_string(),
              remaining: s.remaining.unwrap_or(0),
              entitlement: s.entitlement,
            })
          }
        })
      })
      .or_else(|| {
        snaps.iter().find_map(|(k, s)| {
          if !s.unlimited && s.entitlement.is_some() {
            Some(tokn_auth::MeteredBucket {
              label: k.clone(),
              remaining: s.remaining.unwrap_or(0),
              entitlement: s.entitlement,
            })
          } else {
            None
          }
        })
      })
      .or_else(|| {
        preferred.iter().find_map(|k| {
          snaps.get(*k).and_then(|s| {
            if s.unlimited {
              Some(tokn_auth::MeteredBucket {
                label: k.to_string(),
                remaining: 0,
                entitlement: None,
              })
            } else {
              None
            }
          })
        })
      });

    // Headline mirrors the metered bucket for compact display.
    let headline = metered.as_ref().map(|m| match m.entitlement {
      Some(e) => format!("{}: {}/{}", m.label, m.remaining, e),
      None => format!("{}: unlimited", m.label),
    });

    Ok(QuotaSnapshot {
      plan: info.copilot_plan.clone(),
      headline,
      reset_date: info.quota_reset_date.clone(),
      metered,
      secondary: Vec::new(),
      // Forward the full snapshot blob for any UI that wants per-feature
      // detail. `serde_json::to_value` is infallible for our types.
      provider_extra: serde_json::to_value(&info).unwrap_or(serde_json::Value::Null),
    })
  }
}

/// Decode the `[settings]` table from an account into a `CopilotHeaders`
/// struct. Falls back to defaults when missing.
fn headers_from_settings(account: &AccountConfig) -> Result<CopilotHeaders> {
  let value = serde_json::to_value(toml::Value::Table(account.settings.clone())).unwrap_or(serde_json::Value::Null);
  CopilotHeaders::from_value(&value).map_err(|e| AuthError::Decode(e.to_string()))
}

/// Best-effort `GET /user` to discover the upstream login. Used only to
/// suggest an account id during interactive login.
async fn fetch_github_username(client: &reqwest::Client, gh_token: &str) -> Result<String> {
  #[derive(serde::Deserialize)]
  struct Me {
    login: String,
  }
  let me: Me = client
    .get("https://api.github.com/user")
    .header("authorization", format!("token {gh_token}"))
    .header("accept", "application/json")
    .header("user-agent", tokn_core::util::version::tokn_router_user_agent())
    .send()
    .await
    .map_err(|e| AuthError::Network(e.to_string()))?
    .error_for_status()
    .map_err(|e| AuthError::Upstream(e.to_string()))?
    .json()
    .await
    .map_err(|e| AuthError::Decode(e.to_string()))?;
  Ok(me.login)
}
