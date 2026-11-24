//! ChatGPT Codex authentication.
//!
//! Mirrors the OAuth flow implemented by `opencode/src/plugin/codex.ts`:
//!
//! * **Device flow** (no browser available on the host) — `POST` to
//!   `https://auth.openai.com/api/accounts/deviceauth/usercode` to obtain a
//!   `user_code`, ask the user to visit `/codex/device`, then poll
//!   `/api/accounts/deviceauth/token` until the user authorises. The
//!   final response carries an `authorization_code` + `code_verifier`
//!   that we exchange at `/oauth/token` for `access_token` / `refresh_token`
//!   / `id_token`.
//!
//! * **Manual API key** — surfaced as the static-key onboarding path.
//!
//! Token refresh uses `grant_type=refresh_token` against the same
//! `/oauth/token` endpoint. The `id_token` is parsed (no signature
//! verification) so we can persist `chatgpt_account_id` for the
//! outbound `ChatGPT-Account-Id` header.
//!
//! Verification ([`ProviderAuth::verify_credential`]) probes the codex
//! responses endpoint and treats any non-`401`/`403` response as a
//! healthy credential — a true 200 would require a real model
//! invocation.

use async_trait::async_trait;
use serde::Deserialize;
use std::time::Duration;
use tokn_auth::{
  AuthError, DeviceCodeHandle, DeviceFlowOutcome, ProviderAuth, QuotaSnapshot, RefreshOutcome, Result, VerifyOutcome,
};
use tokn_core::account::AccountConfig;

use crate::jwt;
use crate::{
  CODEX_DEVICE_REDIRECT_URL, CODEX_DEVICE_TOKEN_URL, CODEX_DEVICE_USERCODE_URL, CODEX_DEVICE_VERIFY_URL,
  CODEX_OAUTH_TOKEN_URL,
};

pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const ISSUER: &str = "https://auth.openai.com";
const DEFAULT_EXPIRES_IN_SECS: u64 = 3600;
const REFRESH_SKEW_SECS: i64 = 60;

pub struct CodexAuth;

static CODEX: CodexAuth = CodexAuth;

pub fn codex_auth() -> &'static dyn ProviderAuth {
  &CODEX
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct UserCodeResponse {
  device_auth_id: String,
  user_code: String,
  #[serde(default)]
  interval: Option<serde_json::Value>,
  #[serde(default)]
  expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DevicePollResponse {
  authorization_code: String,
  code_verifier: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
  access_token: String,
  #[serde(default)]
  refresh_token: Option<String>,
  #[serde(default)]
  id_token: Option<String>,
  #[serde(default)]
  expires_in: Option<u64>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_unix() -> i64 {
  time::OffsetDateTime::now_utc().unix_timestamp()
}

fn parse_interval(raw: &Option<serde_json::Value>) -> u64 {
  match raw {
    Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(5).max(1),
    Some(serde_json::Value::String(s)) => s.parse::<u64>().unwrap_or(5).max(1),
    _ => 5,
  }
}

async fn http_form(
  client: &reqwest::Client,
  url: &str,
  body: Vec<(&'static str, String)>,
) -> Result<reqwest::Response> {
  client
    .post(url)
    .header("content-type", "application/x-www-form-urlencoded")
    .header("user-agent", tokn_core::util::version::tokn_router_user_agent())
    .form(&body)
    .send()
    .await
    .map_err(|e| AuthError::Network(e.to_string()))
}

async fn http_json(client: &reqwest::Client, url: &str, body: serde_json::Value) -> Result<reqwest::Response> {
  client
    .post(url)
    .header("content-type", "application/json")
    .header("user-agent", tokn_core::util::version::tokn_router_user_agent())
    .json(&body)
    .send()
    .await
    .map_err(|e| AuthError::Network(e.to_string()))
}

async fn exchange_authorization_code(
  client: &reqwest::Client,
  code: &str,
  code_verifier: &str,
) -> Result<TokenResponse> {
  let resp = http_form(
    client,
    CODEX_OAUTH_TOKEN_URL,
    vec![
      ("grant_type", "authorization_code".into()),
      ("code", code.into()),
      ("redirect_uri", CODEX_DEVICE_REDIRECT_URL.into()),
      ("client_id", CLIENT_ID.into()),
      ("code_verifier", code_verifier.into()),
    ],
  )
  .await?;
  decode_token_response(resp, "authorization_code exchange").await
}

async fn refresh_with_token(client: &reqwest::Client, url: &str, refresh_token: &str) -> Result<TokenResponse> {
  let resp = http_form(
    client,
    url,
    vec![
      ("grant_type", "refresh_token".into()),
      ("refresh_token", refresh_token.into()),
      ("client_id", CLIENT_ID.into()),
    ],
  )
  .await?;
  decode_token_response(resp, "refresh_token exchange").await
}

async fn decode_token_response(resp: reqwest::Response, what: &str) -> Result<TokenResponse> {
  let status = resp.status();
  let body = resp.text().await.unwrap_or_default();
  if !status.is_success() {
    return Err(AuthError::Upstream(format!(
      "{what} failed (HTTP {status}): {}",
      body.chars().take(300).collect::<String>()
    )));
  }
  serde_json::from_str(&body).map_err(|e| AuthError::Decode(format!("{what}: {e}")))
}

// ---------------------------------------------------------------------------
// ProviderAuth impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ProviderAuth for CodexAuth {
  fn id(&self) -> &'static str {
    crate::ID_CODEX
  }

  fn supports_device_flow(&self) -> bool {
    true
  }

  /// Codex also accepts a manually-pasted API key (the third method in
  /// `opencode/src/plugin/codex.ts`).
  fn supports_static_key(&self) -> bool {
    true
  }

  fn default_account_id(&self) -> &'static str {
    crate::ID_CODEX
  }

  fn default_base_url(&self) -> Option<&'static str> {
    Some(crate::codex::CODEX_BASE_URL)
  }

  fn default_refresh_url(&self) -> Option<&'static str> {
    Some(CODEX_OAUTH_TOKEN_URL)
  }

  async fn request_device_code(&self, client: &reqwest::Client) -> Result<DeviceCodeHandle> {
    let resp = http_json(
      client,
      CODEX_DEVICE_USERCODE_URL,
      serde_json::json!({"client_id": CLIENT_ID}),
    )
    .await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
      return Err(AuthError::Upstream(format!(
        "codex device-auth usercode failed (HTTP {status}): {}",
        body.chars().take(300).collect::<String>()
      )));
    }
    let parsed: UserCodeResponse =
      serde_json::from_str(&body).map_err(|e| AuthError::Decode(format!("codex usercode: {e}")))?;
    let interval = parse_interval(&parsed.interval);
    Ok(DeviceCodeHandle {
      device_code: parsed.device_auth_id,
      user_code: parsed.user_code,
      verification_uri: CODEX_DEVICE_VERIFY_URL.to_string(),
      expires_in: parsed.expires_in.unwrap_or(900),
      interval,
    })
  }

  async fn poll_device_code(&self, client: &reqwest::Client, handle: DeviceCodeHandle) -> Result<DeviceFlowOutcome> {
    let interval = Duration::from_secs(handle.interval.max(1) + 3 /* opencode SAFETY_MARGIN_MS / 1000 */);
    let deadline = std::time::Instant::now() + Duration::from_secs(handle.expires_in.max(60));
    loop {
      if std::time::Instant::now() >= deadline {
        return Err(AuthError::Other("codex device-auth poll timed out".into()));
      }
      let resp = http_json(
        client,
        CODEX_DEVICE_TOKEN_URL,
        serde_json::json!({
          "device_auth_id": handle.device_code,
          "user_code": handle.user_code,
        }),
      )
      .await?;
      let status = resp.status();
      if status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let poll: DevicePollResponse =
          serde_json::from_str(&body).map_err(|e| AuthError::Decode(format!("codex device-token: {e}")))?;
        let tokens = exchange_authorization_code(client, &poll.authorization_code, &poll.code_verifier).await?;
        return Ok(make_outcome(tokens));
      }
      let code = status.as_u16();
      if code != 403 && code != 404 {
        let body = resp.text().await.unwrap_or_default();
        return Err(AuthError::Upstream(format!(
          "codex device-auth poll failed (HTTP {status}): {}",
          body.chars().take(300).collect::<String>()
        )));
      }
      tokio::time::sleep(interval).await;
    }
  }

  async fn refresh_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<RefreshOutcome> {
    let refresh = match account
      .refresh_token
      .as_ref()
      .filter(|token| !token.expose().trim().is_empty())
    {
      Some(refresh) => refresh,
      None if account.access_token.is_none() && account.api_key.is_some() => {
        return Ok(RefreshOutcome::NotApplicable);
      }
      None => {
        return Err(AuthError::MissingCredential {
          account: account.id.clone(),
          field: "refresh_token",
        })
      }
    };
    let url = account.refresh_url.as_deref().unwrap_or(CODEX_OAUTH_TOKEN_URL);
    let tokens = refresh_with_token(client, url, refresh.expose()).await?;
    let refresh_token = tokens.refresh_token.clone();
    let id_token = tokens.id_token.clone();
    let outcome = make_outcome(tokens);
    Ok(RefreshOutcome::Refreshed {
      access_token: outcome.access_token,
      expires_at: outcome.access_token_expires_at,
      refresh_token,
      id_token,
      username: outcome.username,
      provider_account_id: outcome.provider_account_id,
    })
  }

  async fn refresh_credential_if_needed(
    &self,
    client: &reqwest::Client,
    account: &AccountConfig,
  ) -> Result<RefreshOutcome> {
    let access_token = account
      .access_token
      .as_ref()
      .filter(|token| !token.expose().trim().is_empty());
    let expires_at = account.access_token_expires_at.or_else(|| {
      access_token
        .and_then(|token| jwt::parse_jwt_claims(token.expose()))
        .and_then(|claims| claims.exp)
    });
    let refresh_available = account
      .refresh_token
      .as_ref()
      .is_some_and(|token| !token.expose().trim().is_empty());
    let fresh = expires_at.map_or(!refresh_available, |exp| {
      exp > now_unix().saturating_add(REFRESH_SKEW_SECS)
    });
    if access_token.is_some() && fresh {
      return Ok(RefreshOutcome::Unchanged);
    }
    self.refresh_credential(client, account).await
  }

  async fn verify_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<VerifyOutcome> {
    let token = account
      .access_token
      .as_ref()
      .or(account.api_key.as_ref())
      .ok_or(AuthError::MissingCredential {
        account: account.id.clone(),
        field: "access_token",
      })?;
    let base = account
      .base_url
      .clone()
      .unwrap_or_else(|| crate::codex::CODEX_BASE_URL.to_string());
    let url = format!("{}/responses", base.trim_end_matches('/'));
    let mut req = client
      .post(url)
      .header("authorization", format!("Bearer {}", token.expose()))
      .header("content-type", "application/json")
      .header("accept", "application/json")
      .json(&serde_json::json!({}));
    if let Some(pid) = jwt::account_id(account) {
      req = req.header("chatgpt-account-id", pid);
    }
    let resp = req.send().await.map_err(|e| AuthError::Network(e.to_string()))?;
    let status = resp.status();
    // 200/4xx-but-not-401/403 → credential is at least authenticated; the
    // upstream rejected the body we sent, which is expected because we
    // intentionally posted an empty payload.
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
      let body = resp.text().await.unwrap_or_default();
      return Err(AuthError::Upstream(format!(
        "codex rejected the credential (HTTP {status}): {}",
        body.chars().take(200).collect::<String>()
      )));
    }
    Ok(VerifyOutcome::default())
  }

  async fn probe_quota(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<QuotaSnapshot> {
    crate::quota_codex::fetch(client, account).await
  }
}

fn make_outcome(tokens: TokenResponse) -> DeviceFlowOutcome {
  let TokenResponse {
    access_token,
    refresh_token,
    id_token,
    expires_in,
  } = tokens;
  let id_claims = id_token.as_deref().and_then(jwt::parse_jwt_claims);
  let access_claims = jwt::parse_jwt_claims(&access_token);
  let provider_account_id = id_claims
    .as_ref()
    .and_then(jwt::extract_account_id)
    .or_else(|| access_claims.as_ref().and_then(jwt::extract_account_id));
  let username = id_claims.and_then(|claims| claims.email);
  let access_token_expires_at = expires_in
    .and_then(|seconds| i64::try_from(seconds).ok())
    .map(|seconds| now_unix().saturating_add(seconds))
    .or_else(|| access_claims.and_then(|claims| claims.exp))
    .unwrap_or_else(|| now_unix() + DEFAULT_EXPIRES_IN_SECS as i64);
  DeviceFlowOutcome {
    refresh_token: refresh_token.unwrap_or_default(),
    access_token,
    access_token_expires_at,
    username,
    provider_account_id,
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use base64::Engine;
  use tokn_mock_server::{MockEndpoint, MockLlmConfig, MockLlmServer, MockResponse, MockRoute};

  fn account() -> AccountConfig {
    serde_json::from_value(serde_json::json!({
      "id": "codex-test",
      "provider": crate::ID_CODEX,
      "access_token": "old-access",
      "refresh_token": "old-refresh",
      "access_token_expires_at": now_unix() + 3600,
    }))
    .unwrap()
  }

  async fn token_server(response: MockResponse) -> MockLlmServer {
    MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::new(
      MockEndpoint::Custom {
        method: reqwest::Method::POST,
        path: "/oauth/token".into(),
      },
      response,
    )))
    .await
  }

  #[tokio::test]
  async fn explicit_refresh_exchanges_fresh_token_and_returns_rotated_credentials() {
    let id_token = jwt_with(serde_json::json!({"chatgpt_account_id": "acc-new", "email": "user@example.com"}));
    let server = token_server(MockResponse::json(serde_json::json!({
      "access_token": "new-access",
      "refresh_token": "new-refresh",
      "id_token": id_token,
      "expires_in": 7200,
    })))
    .await;
    let mut account = account();
    account.refresh_url = Some(server.url("/oauth/token"));

    let result = CodexAuth
      .refresh_credential(&reqwest::Client::new(), &account)
      .await
      .unwrap();

    let RefreshOutcome::Refreshed {
      access_token,
      expires_at,
      refresh_token,
      id_token: refreshed_id,
      username,
      provider_account_id,
    } = result
    else {
      panic!("explicit refresh must exchange even a fresh cached access token");
    };
    assert_eq!(access_token, "new-access");
    assert_eq!(refresh_token.as_deref(), Some("new-refresh"));
    assert_eq!(refreshed_id.as_deref(), Some(id_token.as_str()));
    assert_eq!(provider_account_id.as_deref(), Some("acc-new"));
    assert_eq!(username.as_deref(), Some("user@example.com"));
    assert!((7190..=7200).contains(&(expires_at - now_unix())));
    let request = server.last_request().unwrap();
    assert_eq!(
      request.header("content-type"),
      Some("application/x-www-form-urlencoded")
    );
    let form = reqwest::Url::parse(&format!("http://localhost/?{}", String::from_utf8_lossy(&request.body))).unwrap();
    let fields: std::collections::BTreeMap<_, _> = form.query_pairs().into_owned().collect();
    assert_eq!(fields.get("grant_type").map(String::as_str), Some("refresh_token"));
    assert_eq!(fields.get("refresh_token").map(String::as_str), Some("old-refresh"));
    assert_eq!(fields.get("client_id").map(String::as_str), Some(CLIENT_ID));
  }

  #[tokio::test]
  async fn conditional_refresh_skips_fresh_access_tokens() {
    let server = token_server(MockResponse::json(serde_json::json!({"access_token": "unexpected"}))).await;
    let mut account = account();
    account.refresh_url = Some(server.url("/oauth/token"));

    let result = CodexAuth
      .refresh_credential_if_needed(&reqwest::Client::new(), &account)
      .await
      .unwrap();

    assert!(matches!(result, RefreshOutcome::Unchanged));
    assert!(server.requests().is_empty());
  }

  #[tokio::test]
  async fn conditional_refresh_exchanges_missing_expired_or_unknown_expiry_tokens() {
    let server = token_server(MockResponse::json(
      serde_json::json!({"access_token": "new-access", "expires_in": 3600}),
    ))
    .await;
    let client = reqwest::Client::new();
    for (access, expiry) in [
      (None, Some(now_unix() + 3600)),
      (Some(""), Some(now_unix() + 3600)),
      (Some("old-access"), Some(now_unix() - 10)),
      (Some("old-access"), Some(now_unix() + 30)),
      (Some("old-access"), None),
    ] {
      let mut account = account();
      account.access_token = access.map(|token| tokn_core::account::Secret::new(token.into()));
      account.access_token_expires_at = expiry;
      account.refresh_url = Some(server.url("/oauth/token"));

      let result = CodexAuth.refresh_credential_if_needed(&client, &account).await.unwrap();

      assert!(matches!(
        result,
        RefreshOutcome::Refreshed {
          refresh_token: None,
          id_token: None,
          ..
        }
      ));
    }
    assert_eq!(server.requests().len(), 5);
  }

  #[tokio::test]
  async fn access_token_without_refresh_token_is_not_a_static_api_key() {
    let mut account = account();
    account.refresh_token = None;
    let client = reqwest::Client::new();
    let err = CodexAuth.refresh_credential(&client, &account).await.unwrap_err();
    assert!(matches!(
      err,
      AuthError::MissingCredential {
        field: "refresh_token",
        ..
      }
    ));
    assert!(matches!(
      CodexAuth.refresh_credential_if_needed(&client, &account).await.unwrap(),
      RefreshOutcome::Unchanged
    ));

    account.access_token = None;
    account.api_key = Some(tokn_core::account::Secret::new("static-key".into()));
    assert!(matches!(
      CodexAuth.refresh_credential(&client, &account).await.unwrap(),
      RefreshOutcome::NotApplicable
    ));
  }

  #[tokio::test]
  async fn conditional_refresh_uses_jwt_expiry_when_imported_expiry_is_missing() {
    let mut account = account();
    account.access_token_expires_at = None;
    account.access_token = Some(tokn_core::account::Secret::new(jwt_with(
      serde_json::json!({"exp": now_unix() + 3600}),
    )));
    // A network request here would fail; the JWT is still valid.
    account.refresh_url = Some("http://127.0.0.1:1/oauth/token".into());
    assert!(matches!(
      CodexAuth
        .refresh_credential_if_needed(&reqwest::Client::new(), &account)
        .await
        .unwrap(),
      RefreshOutcome::Unchanged
    ));
  }

  #[tokio::test]
  async fn refresh_surfaces_upstream_failures() {
    let mut response = MockResponse::json(serde_json::json!({"error": "invalid_grant"}));
    response.status = reqwest::StatusCode::UNAUTHORIZED;
    let server = token_server(response).await;
    let mut account = account();
    account.refresh_url = Some(server.url("/oauth/token"));
    let err = CodexAuth
      .refresh_credential(&reqwest::Client::new(), &account)
      .await
      .unwrap_err();
    assert!(matches!(err, AuthError::Upstream(_)));
  }

  fn jwt_with(payload: serde_json::Value) -> String {
    let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
    let body = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    format!("{header}.{body}.")
  }

  #[test]
  fn make_outcome_extracts_account_id_from_id_token() {
    let tok = jwt_with(serde_json::json!({"chatgpt_account_id": "acc-9", "email": "u@x"}));
    let out = make_outcome(TokenResponse {
      access_token: "atk".into(),
      refresh_token: Some("rtk".into()),
      id_token: Some(tok),
      expires_in: Some(120),
    });
    assert_eq!(out.access_token, "atk");
    assert_eq!(out.refresh_token, "rtk");
    assert_eq!(out.provider_account_id.as_deref(), Some("acc-9"));
    assert_eq!(out.username.as_deref(), Some("u@x"));
    let drift = out.access_token_expires_at - now_unix();
    assert!((110..=130).contains(&drift), "expires_at drift {drift}");
  }

  #[test]
  fn make_outcome_without_id_token_yields_no_account_id() {
    let out = make_outcome(TokenResponse {
      access_token: "atk".into(),
      refresh_token: None,
      id_token: None,
      expires_in: None,
    });
    assert!(out.provider_account_id.is_none());
    assert!(out.username.is_none());
  }

  #[test]
  fn make_outcome_recovers_account_id_from_access_token_when_identity_token_omits_it() {
    for id_token in [None, Some(jwt_with(serde_json::json!({"email": "user@example.test"})))] {
      let out = make_outcome(TokenResponse {
        access_token: jwt_with(serde_json::json!({
          "https://api.openai.com/auth": {"chatgpt_account_id": "access-account"}
        })),
        refresh_token: None,
        id_token,
        expires_in: None,
      });
      assert_eq!(out.provider_account_id.as_deref(), Some("access-account"));
    }
  }

  #[test]
  fn make_outcome_uses_access_token_expiry_when_expires_in_is_omitted() {
    let expires_at = now_unix() + 7200;
    let out = make_outcome(TokenResponse {
      access_token: jwt_with(serde_json::json!({"exp": expires_at})),
      refresh_token: None,
      id_token: None,
      expires_in: None,
    });
    assert_eq!(out.access_token_expires_at, expires_at);
  }

  #[test]
  fn parse_interval_accepts_string_and_number() {
    assert_eq!(parse_interval(&Some(serde_json::json!(7))), 7);
    assert_eq!(parse_interval(&Some(serde_json::json!("4"))), 4);
    assert_eq!(parse_interval(&None), 5);
    assert_eq!(parse_interval(&Some(serde_json::json!(0))), 1);
  }
}
