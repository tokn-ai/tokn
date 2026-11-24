//! ChatGPT account usage as exposed by the Codex backend client.
//!
//! The upstream `openai/codex` backend client reads `/backend-api/wham/usage`;
//! its `RateLimitWindowSnapshot` reports percentages and epoch-second resets,
//! not request or token counts.
//! See `codex-rs/backend-client/src/client/rate_limit_resets.rs` and
//! `codex-rs/codex-backend-openapi-models/src/models/` (`rate_limit_window_snapshot.rs`
//! and `additional_rate_limit_details.rs`) in the upstream Codex repository.

use reqwest::{Client, Url};
use serde::Deserialize;
use tokn_auth::{AuthError, QuotaSnapshot, Result, UsageBucket};
use tokn_core::account::AccountConfig;
use tokn_core::upstream_url::{CanonicalUpstreamUrl, CleartextHttpPolicy};

#[derive(Deserialize)]
struct UsageResponse {
  plan_type: String,
  rate_limit: Option<RateLimit>,
  additional_rate_limits: Option<Vec<AdditionalRateLimit>>,
}

#[derive(Deserialize)]
struct AdditionalRateLimit {
  limit_name: Option<String>,
  metered_feature: Option<String>,
  rate_limit: Option<RateLimit>,
}

#[derive(Deserialize)]
struct RateLimit {
  primary_window: Option<UsageWindow>,
  secondary_window: Option<UsageWindow>,
}

#[derive(Deserialize)]
struct UsageWindow {
  used_percent: f64,
  limit_window_seconds: i64,
  reset_at: i64,
}

pub(crate) async fn fetch(client: &Client, account: &AccountConfig) -> Result<QuotaSnapshot> {
  let token = account
    .access_token
    .as_ref()
    .filter(|token| !token.expose().trim().is_empty())
    .or_else(|| {
      account
        .api_key
        .as_ref()
        .filter(|token| !token.expose().trim().is_empty())
    })
    .ok_or_else(|| AuthError::MissingCredential {
      account: account.id.clone(),
      field: "access_token",
    })?;
  let url = usage_url(account.base_url.as_deref().unwrap_or(crate::codex::CODEX_BASE_URL))?;
  let mut request = client
    .get(url)
    .bearer_auth(token.expose())
    .header("accept", "application/json")
    .header("user-agent", tokn_core::util::version::tokn_router_user_agent());
  if let Some(account_id) = crate::jwt::account_id(account) {
    request = request.header("chatgpt-account-id", account_id);
  }
  let response = request
    .send()
    .await
    .map_err(|error| AuthError::Network(error.to_string()))?;
  let status = response.status();
  if !status.is_success() {
    return Err(AuthError::Upstream(format!(
      "codex usage request failed (HTTP {status})"
    )));
  }
  let raw: serde_json::Value = response
    .json()
    .await
    .map_err(|error| AuthError::Decode(format!("codex usage: {error}")))?;
  let usage: UsageResponse =
    serde_json::from_value(raw.clone()).map_err(|error| AuthError::Decode(format!("codex usage: {error}")))?;
  let mut secondary = Vec::new();
  if let Some(rate_limit) = usage.rate_limit {
    append_windows(&mut secondary, rate_limit, None)?;
  }
  for additional in usage.additional_rate_limits.into_iter().flatten() {
    if let Some(rate_limit) = additional.rate_limit {
      let label = additional
        .limit_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
          additional
            .metered_feature
            .as_deref()
            .filter(|name| !name.trim().is_empty())
        })
        .unwrap_or("additional");
      append_windows(&mut secondary, rate_limit, Some(label))?;
    }
  }
  let headline = secondary.first().map(|bucket| {
    format!(
      "{}: {:.1}% used",
      bucket.label,
      bucket.percent_used.expect("Codex reports percentage usage")
    )
  });
  Ok(QuotaSnapshot {
    plan: Some(usage.plan_type),
    headline,
    secondary,
    provider_extra: raw,
    ..QuotaSnapshot::default()
  })
}

fn append_windows(buckets: &mut Vec<UsageBucket>, rate_limit: RateLimit, prefix: Option<&str>) -> Result<()> {
  for (name, window) in [
    ("primary", rate_limit.primary_window),
    ("secondary", rate_limit.secondary_window),
  ] {
    if let Some(window) = window {
      let label = window_label(name, window.limit_window_seconds);
      buckets.push(UsageBucket {
        label: match prefix {
          Some(prefix) => format!("{prefix} {label}"),
          None => label,
        },
        used: None,
        total: None,
        percent_used: Some(window.used_percent),
        reset_at_ms: Some(
          window
            .reset_at
            .checked_mul(1000)
            .ok_or_else(|| AuthError::Decode(format!("codex {name} usage reset timestamp exceeds supported range")))?,
        ),
      });
    }
  }
  Ok(())
}

fn usage_url(base: &str) -> Result<Url> {
  let base = CanonicalUpstreamUrl::parse(base, CleartextHttpPolicy::Allow)
    .map_err(|error| AuthError::Other(format!("invalid codex usage base URL: {error}")))?;
  let mut url = base.as_url().clone();
  let has_codex_suffix = url.path().trim_end_matches('/').ends_with("/codex");
  let mut path = url
    .path_segments_mut()
    .map_err(|()| AuthError::Other("codex usage base URL cannot contain path segments".into()))?;
  path.pop_if_empty();
  if has_codex_suffix {
    path.pop();
  }
  path.extend(["wham", "usage"]);
  drop(path);
  Ok(url)
}

fn window_label(fallback: &str, seconds: i64) -> String {
  match seconds {
    604_800 => "weekly usage".into(),
    n if n > 0 && n % 86_400 == 0 => format!("{}d usage", n / 86_400),
    n if n > 0 && n % 3_600 == 0 => format!("{}h usage", n / 3_600),
    n if n > 0 && n % 60 == 0 => format!("{}m usage", n / 60),
    n if n > 0 => format!("{n}s usage"),
    _ => format!("{fallback} usage"),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use base64::Engine;
  use reqwest::{Method, StatusCode};
  use serde_json::json;
  use tokn_mock_server::{
    HeaderExpectation, MockAuthConfig, MockEndpoint, MockLlmConfig, MockLlmServer, MockResponse, MockRoute,
  };

  fn account(base_url: &str) -> AccountConfig {
    serde_json::from_value(json!({
      "id": "codex-test",
      "provider": "codex",
      "base_url": base_url,
      "access_token": "access-test",
      "provider_account_id": "account-test"
    }))
    .unwrap()
  }

  async fn server(response: MockResponse) -> MockLlmServer {
    MockLlmServer::start(
      MockLlmConfig::default()
        .with_auth(MockAuthConfig::bearer(["access-test"]))
        .require_header(HeaderExpectation::equals("chatgpt-account-id", "account-test"))
        .with_route(MockRoute::new(
          MockEndpoint::Custom {
            method: Method::GET,
            path: "/backend-api/wham/usage".into(),
          },
          response,
        )),
    )
    .await
  }

  fn client() -> Client {
    Client::builder().no_proxy().build().unwrap()
  }

  #[tokio::test]
  async fn fetches_usage_windows_with_account_auth_and_independent_resets() {
    let server = server(MockResponse::json(json!({
      "plan_type": "plus",
      "rate_limit": {
        "allowed": true,
        "limit_reached": false,
        "primary_window": {
          "used_percent": 24,
          "limit_window_seconds": 18000,
          "reset_after_seconds": 3600,
          "reset_at": 1900000000
        },
        "secondary_window": {
          "used_percent": 57.5,
          "limit_window_seconds": 604800,
          "reset_after_seconds": 172800,
          "reset_at": 1900172800
        }
      },
      "credits": {"has_credits": false}
    })))
    .await;
    let quota = fetch(&client(), &account(&server.url("/backend-api/codex/")))
      .await
      .unwrap();

    assert_eq!(quota.plan.as_deref(), Some("plus"));
    assert_eq!(quota.headline.as_deref(), Some("5h usage: 24.0% used"));
    assert!(quota.metered.is_none());
    assert_eq!(quota.secondary.len(), 2);
    let primary = &quota.secondary[0];
    assert_eq!(primary.label, "5h usage");
    assert_eq!(primary.percent_used, Some(24.0));
    assert_eq!(primary.reset_at_ms, Some(1_900_000_000_000));
    let secondary = &quota.secondary[1];
    assert_eq!(secondary.label, "weekly usage");
    assert_eq!(secondary.percent_used, Some(57.5));
    assert_eq!(secondary.reset_at_ms, Some(1_900_172_800_000));
    assert!(quota
      .secondary
      .iter()
      .all(|bucket| bucket.used.is_none() && bucket.total.is_none()));
    assert_eq!(quota.provider_extra["credits"]["has_credits"], false);
    assert_eq!(server.requests().len(), 1);
    assert_eq!(server.last_request().unwrap().path, "/backend-api/wham/usage");
  }

  #[tokio::test]
  async fn missing_and_null_windows_do_not_become_zero_usage() {
    for rate_limit in [
      json!(null),
      json!({}),
      json!({"primary_window": null, "secondary_window": null}),
    ] {
      let server = server(MockResponse::json(
        json!({"plan_type": "pro", "rate_limit": rate_limit, "additional_rate_limits": null}),
      ))
      .await;
      let quota = fetch(&client(), &account(&server.url("/backend-api/codex")))
        .await
        .unwrap();
      assert_eq!(quota.plan.as_deref(), Some("pro"));
      assert!(quota.secondary.is_empty());
      assert!(quota.headline.is_none());
      assert!(quota.metered.is_none());
    }
  }

  #[tokio::test]
  async fn supports_a_secondary_window_without_a_primary() {
    let server = server(MockResponse::json(json!({
      "plan_type": "free",
      "rate_limit": {"secondary_window": {
        "used_percent": 100,
        "limit_window_seconds": 604800,
        "reset_at": 1900000000
      }}
    })))
    .await;
    let quota = fetch(&client(), &account(&server.url("/backend-api/codex")))
      .await
      .unwrap();
    assert_eq!(quota.secondary.len(), 1);
    assert_eq!(quota.headline.as_deref(), Some("weekly usage: 100.0% used"));
  }

  #[tokio::test]
  async fn additional_model_limits_follow_main_windows_and_keep_their_names_and_resets() {
    let window = |used_percent, limit_window_seconds, reset_at| {
      json!({
        "used_percent": used_percent,
        "limit_window_seconds": limit_window_seconds,
        "reset_at": reset_at
      })
    };
    let server = server(MockResponse::json(json!({
      "plan_type": "pro",
      "rate_limit": {
        "primary_window": window(10, 18000, 1900000000),
        "secondary_window": window(20, 604800, 1900100000)
      },
      "additional_rate_limits": [
        {
          "limit_name": "Model-specific allowance",
          "metered_feature": "codex_model",
          "rate_limit": {
            "primary_window": window(70, 18000, 1900200000),
            "secondary_window": window(84, 604800, 1900300000)
          }
        },
        {
          "limit_name": " ",
          "metered_feature": "codex_other_model",
          "rate_limit": {"primary_window": window(5, 3600, 1900400000)}
        },
        {"limit_name": "Unavailable allowance", "metered_feature": "unavailable", "rate_limit": null},
        {"limit_name": "Missing allowance", "metered_feature": "missing"}
      ]
    })))
    .await;
    let quota = fetch(&client(), &account(&server.url("/backend-api/codex")))
      .await
      .unwrap();
    let buckets: Vec<_> = quota
      .secondary
      .iter()
      .map(|bucket| (bucket.label.as_str(), bucket.percent_used, bucket.reset_at_ms))
      .collect();

    assert_eq!(
      buckets,
      vec![
        ("5h usage", Some(10.0), Some(1_900_000_000_000)),
        ("weekly usage", Some(20.0), Some(1_900_100_000_000)),
        ("Model-specific allowance 5h usage", Some(70.0), Some(1_900_200_000_000)),
        (
          "Model-specific allowance weekly usage",
          Some(84.0),
          Some(1_900_300_000_000)
        ),
        ("codex_other_model 1h usage", Some(5.0), Some(1_900_400_000_000))
      ]
    );
    assert_eq!(quota.headline.as_deref(), Some("5h usage: 10.0% used"));
    assert!(quota
      .secondary
      .iter()
      .all(|bucket| bucket.used.is_none() && bucket.total.is_none()));
  }

  #[tokio::test]
  async fn derives_the_account_header_from_an_imported_id_token() {
    let server = server(MockResponse::json(json!({"plan_type": "plus"}))).await;
    let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
      .encode(json!({"https://api.openai.com/auth": {"chatgpt_account_id": "account-test"}}).to_string());
    let mut account = account(&server.url("/backend-api/codex"));
    account.provider_account_id = Some("  ".to_string());
    account.id_token = Some(format!("e30.{claims}.").into());

    let quota = fetch(&client(), &account).await.unwrap();

    assert_eq!(quota.plan.as_deref(), Some("plus"));
    assert_eq!(server.requests().len(), 1);
    assert_eq!(
      server.last_request().unwrap().header("chatgpt-account-id"),
      Some("account-test")
    );
  }

  #[tokio::test]
  async fn http_errors_and_unrecognized_payloads_are_reported() {
    for status in [
      StatusCode::UNAUTHORIZED,
      StatusCode::FORBIDDEN,
      StatusCode::INTERNAL_SERVER_ERROR,
    ] {
      let mut response = MockResponse::json(json!({"error": "failed"}));
      response.status = status;
      let server = server(response).await;
      let error = fetch(&client(), &account(&server.url("/backend-api/codex")))
        .await
        .unwrap_err();
      assert!(matches!(error, AuthError::Upstream(message) if message.contains(status.as_str())));
    }
    let server = server(MockResponse::json(json!({"unexpected": "payload"}))).await;
    let error = fetch(&client(), &account(&server.url("/backend-api/codex")))
      .await
      .unwrap_err();
    assert!(matches!(error, AuthError::Decode(_)));
  }

  #[tokio::test]
  async fn missing_credential_fails_before_a_request() {
    let server = server(MockResponse::json(json!({"plan_type": "plus"}))).await;
    let mut account = account(&server.url("/backend-api/codex"));
    account.access_token = Some(" \t ".to_string().into());
    let error = fetch(&client(), &account).await.unwrap_err();
    assert!(matches!(
      error,
      AuthError::MissingCredential {
        field: "access_token",
        ..
      }
    ));
    assert!(server.requests().is_empty());
  }

  #[tokio::test]
  async fn uses_a_manually_imported_access_token_and_omits_a_blank_account_id() {
    let server = MockLlmServer::start(
      MockLlmConfig::default()
        .with_auth(MockAuthConfig::bearer(["imported-access-test"]))
        .forbid_header("chatgpt-account-id")
        .with_route(MockRoute::new(
          MockEndpoint::Custom {
            method: Method::GET,
            path: "/backend-api/wham/usage".into(),
          },
          MockResponse::json(json!({"plan_type": "plus"})),
        )),
    )
    .await;
    let mut account = account(&server.url("/backend-api/codex"));
    account.access_token = None;
    account.api_key = Some("imported-access-test".to_string().into());
    account.provider_account_id = Some("  ".to_string());

    let quota = fetch(&client(), &account).await.unwrap();

    assert_eq!(quota.plan.as_deref(), Some("plus"));
    assert!(quota.secondary.is_empty());
    assert_eq!(server.requests().len(), 1);
  }

  #[test]
  fn usage_url_preserves_the_configured_origin_and_path_prefix() {
    for (base, expected) in [
      (
        crate::codex::CODEX_BASE_URL,
        "https://chatgpt.com/backend-api/wham/usage",
      ),
      (
        "https://gateway.example/prefix/backend-api/codex/",
        "https://gateway.example/prefix/backend-api/wham/usage",
      ),
      (
        "https://gateway.example/backend-api",
        "https://gateway.example/backend-api/wham/usage",
      ),
      ("http://127.0.0.1:1234", "http://127.0.0.1:1234/wham/usage"),
    ] {
      assert_eq!(usage_url(base).unwrap().as_str(), expected);
    }
    assert!(usage_url("https://gateway.example/backend-api/codex?next=https://chatgpt.com").is_err());
    assert!(usage_url("https://user:password@gateway.example/backend-api/codex").is_err());
  }
}
