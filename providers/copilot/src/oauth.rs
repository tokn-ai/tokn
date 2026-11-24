//! GitHub OAuth Device Flow for the Copilot client_id.
//!
//! Reference: <https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps#device-flow>

use crate::provider::{error, Result};
use crate::util::redact::token_fingerprint;
use crate::{COPILOT_ACCESS_TOKEN_URL, COPILOT_DEVICE_CODE_URL};
use serde::Deserialize;
use snafu::ResultExt;
use std::time::Duration;
use tracing::{debug, info, instrument, warn};

/// VS Code Copilot Chat client ID. Public, well-known.
pub const COPILOT_CLIENT_ID: &str = "Iv1.b507a08c87ecfe98";

#[derive(Debug, Deserialize)]
pub struct DeviceCode {
  pub device_code: String,
  pub user_code: String,
  pub verification_uri: String,
  pub expires_in: u64,
  pub interval: u64,
}

#[derive(Debug, Deserialize)]
struct TokenPending {
  error: String,
  #[serde(default)]
  interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TokenOk {
  access_token: String,
  #[serde(default)]
  token_type: Option<String>,
  #[serde(default)]
  scope: Option<String>,
}

#[instrument(name = "device_code", skip_all, fields(status = tracing::field::Empty))]
pub async fn request_device_code(client: &reqwest::Client) -> Result<DeviceCode> {
  debug!("requesting GitHub device code");
  let resp = client
    .post(COPILOT_DEVICE_CODE_URL)
    .header("accept", "application/json")
    .form(&[("client_id", COPILOT_CLIENT_ID), ("scope", "read:user")])
    .send()
    .await
    .context(error::HttpSnafu { what: "device code" })?;
  let status = resp.status();
  tracing::Span::current().record("status", status.as_u16());
  let body = resp.text().await.unwrap_or_default();
  if !status.is_success() {
    return error::HttpStatusSnafu {
      what: "device code",
      status,
      body,
    }
    .fail();
  }
  let dc: DeviceCode = serde_json::from_str(&body).context(error::JsonSnafu {
    what: "device code",
    body: body.clone(),
  })?;
  info!(
    user_code = %dc.user_code,
    verification_uri = %dc.verification_uri,
    expires_in = dc.expires_in,
    "GitHub device code issued"
  );
  Ok(dc)
}

#[instrument(
  name = "oauth_poll",
  skip_all,
  fields(
    user_code = %dc.user_code,
    interval = dc.interval,
    expires_in = dc.expires_in,
    polls = tracing::field::Empty,
    token_fp = tracing::field::Empty,
  ),
)]
pub async fn poll_for_token(client: &reqwest::Client, dc: &DeviceCode) -> Result<String> {
  let mut interval = Duration::from_secs(dc.interval.max(1));
  let deadline = std::time::Instant::now() + Duration::from_secs(dc.expires_in);
  let mut polls: u64 = 0;

  loop {
    if std::time::Instant::now() >= deadline {
      warn!(polls, "device code expired before authorization");
      return error::DeviceCodeExpiredSnafu.fail();
    }
    tokio::time::sleep(interval).await;
    polls += 1;

    let resp = client
      .post(COPILOT_ACCESS_TOKEN_URL)
      .header("accept", "application/json")
      .form(&[
        ("client_id", COPILOT_CLIENT_ID),
        ("device_code", dc.device_code.as_str()),
        ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
      ])
      .send()
      .await
      .context(error::HttpSnafu {
        what: "access_token poll",
      })?;
    let body = resp.text().await.unwrap_or_default();

    if let Ok(ok) = serde_json::from_str::<TokenOk>(&body) {
      if !ok.access_token.is_empty() {
        let span = tracing::Span::current();
        span.record("polls", polls);
        span.record("token_fp", tracing::field::display(token_fingerprint(&ok.access_token)));
        info!(token_type = ?ok.token_type, scope = ?ok.scope, polls, "received GitHub access token");
        return Ok(ok.access_token);
      }
    }
    if let Ok(p) = serde_json::from_str::<TokenPending>(&body) {
      match p.error.as_str() {
        "authorization_pending" => {
          debug!(polls, "authorization still pending");
        }
        "slow_down" => {
          let next = p.interval.unwrap_or(interval.as_secs() + 5);
          debug!(polls, slow_to_secs = next, "slow_down from upstream");
          interval = Duration::from_secs(next);
        }
        "expired_token" => {
          warn!(polls, "upstream reports device token expired");
          return error::DeviceCodeExpiredSnafu.fail();
        }
        "access_denied" => {
          warn!(polls, "user denied authorization");
          return error::AccessDeniedSnafu.fail();
        }
        other => {
          warn!(polls, code = %other, "unexpected oauth error");
          return error::OAuthSnafu {
            code: other.to_string(),
            body,
          }
          .fail();
        }
      }
      continue;
    }
    warn!(polls, "unparseable oauth response");
    return error::OAuthUnexpectedSnafu { body }.fail();
  }
}
