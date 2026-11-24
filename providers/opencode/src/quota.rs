use reqwest::{Client, Url};
use serde::Deserialize;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokn_auth::{AuthError, QuotaSnapshot, Result, UsageBucket};
use tokn_core::account::AccountConfig;
use tokn_core::upstream_url::{CanonicalUpstreamUrl, CleartextHttpPolicy};

#[derive(Deserialize)]
struct UsageResponse {
  usage: Usage,
}

#[derive(Deserialize)]
struct Usage {
  rolling: UsageWindow,
  weekly: UsageWindow,
  monthly: UsageWindow,
}

#[derive(Deserialize)]
struct UsageWindow {
  percent: f64,
  #[serde(rename = "resetsAt")]
  resets_at: String,
}

pub(crate) async fn fetch(client: &Client, account: &AccountConfig) -> Result<QuotaSnapshot> {
  let raw = request(client, account).await?;
  let usage: UsageResponse =
    serde_json::from_value(raw.clone()).map_err(|error| AuthError::Decode(format!("OpenCode Go usage: {error}")))?;
  let secondary = vec![
    usage_bucket("5h usage", usage.usage.rolling)?,
    usage_bucket("weekly usage", usage.usage.weekly)?,
    usage_bucket("monthly usage", usage.usage.monthly)?,
  ];
  let headline = secondary.first().map(|bucket| {
    format!(
      "{}: {:.1}% used",
      bucket.label,
      bucket.percent_used.expect("OpenCode Go reports percentage usage")
    )
  });

  Ok(QuotaSnapshot {
    plan: Some("OpenCode Go".into()),
    headline,
    secondary,
    provider_extra: raw,
    ..QuotaSnapshot::default()
  })
}

pub(crate) async fn verify(client: &Client, account: &AccountConfig) -> Result<()> {
  request(client, account).await?;
  Ok(())
}

async fn request(client: &Client, account: &AccountConfig) -> Result<serde_json::Value> {
  let key = account
    .api_key
    .as_ref()
    .filter(|key| !key.expose().trim().is_empty())
    .ok_or_else(|| AuthError::MissingCredential {
      account: account.id.clone(),
      field: "api_key",
    })?;
  let url = usage_url(account.base_url.as_deref().unwrap_or(crate::OPENCODE_GO_BASE_URL))?;
  let response = client
    .get(url)
    .bearer_auth(key.expose())
    .header("accept", "application/json")
    .header("user-agent", tokn_core::util::version::tokn_router_user_agent())
    .send()
    .await
    .map_err(|error| AuthError::Network(error.to_string()))?;
  let status = response.status();
  let body = response
    .text()
    .await
    .map_err(|error| AuthError::Network(error.to_string()))?;
  if !status.is_success() {
    return Err(AuthError::Upstream(format!(
      "OpenCode Go usage request failed (HTTP {status}): {}",
      body.chars().take(200).collect::<String>()
    )));
  }

  serde_json::from_str(&body).map_err(|error| AuthError::Decode(format!("OpenCode Go usage: {error}")))
}

fn usage_url(base: &str) -> Result<Url> {
  let base = CanonicalUpstreamUrl::parse(base, CleartextHttpPolicy::LoopbackOnly)
    .map_err(|error| AuthError::Other(format!("invalid OpenCode Go base URL: {error}")))?;
  base
    .operation_url(["usage"])
    .map_err(|error| AuthError::Other(format!("invalid OpenCode Go usage URL: {error}")))
}

fn usage_bucket(label: &str, window: UsageWindow) -> Result<UsageBucket> {
  if !window.percent.is_finite() || !(0.0..=100.0).contains(&window.percent) {
    return Err(AuthError::Decode(format!(
      "OpenCode Go {label} percentage is outside 0..=100"
    )));
  }
  let reset = OffsetDateTime::parse(&window.resets_at, &Rfc3339)
    .map_err(|error| AuthError::Decode(format!("OpenCode Go {label} reset timestamp: {error}")))?;
  let reset_at_ms = i64::try_from(reset.unix_timestamp_nanos() / 1_000_000)
    .map_err(|_| AuthError::Decode(format!("OpenCode Go {label} reset timestamp exceeds supported range")))?;
  Ok(UsageBucket {
    label: label.into(),
    used: None,
    total: None,
    percent_used: Some(window.percent),
    reset_at_ms: Some(reset_at_ms),
  })
}

#[cfg(test)]
mod tests {
  use super::*;
  use reqwest::Method;
  use serde_json::json;
  use tokn_mock_server::{MockAuthConfig, MockEndpoint, MockLlmConfig, MockLlmServer, MockResponse, MockRoute};

  fn account(base_url: &str, api_key: &str) -> AccountConfig {
    serde_json::from_value(json!({
      "id": "opencode-go-test",
      "provider": "opencode-go",
      "base_url": base_url,
      "api_key": api_key
    }))
    .unwrap()
  }

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
              "rolling": {
                "status": "ok",
                "percent": 12.5,
                "resetsAt": "2030-03-17T12:34:56.789Z"
              },
              "weekly": {
                "status": "ok",
                "percent": 34,
                "resetsAt": "2030-03-18T00:00:00Z"
              },
              "monthly": {
                "status": "rate-limited",
                "percent": 100,
                "resetsAt": "2030-04-01T00:00:00Z"
              }
            }
          })),
        )),
    )
    .await
  }

  fn client() -> Client {
    Client::builder().no_proxy().build().unwrap()
  }

  #[tokio::test]
  async fn fetches_all_usage_windows_with_bearer_auth() {
    let server = server().await;
    let snapshot = fetch(&client(), &account(server.base_url(), "sk-test")).await.unwrap();

    assert_eq!(snapshot.plan.as_deref(), Some("OpenCode Go"));
    assert_eq!(snapshot.headline.as_deref(), Some("5h usage: 12.5% used"));
    assert_eq!(snapshot.secondary.len(), 3);
    assert_eq!(snapshot.secondary[0].label, "5h usage");
    assert_eq!(snapshot.secondary[0].percent_used, Some(12.5));
    assert_eq!(snapshot.secondary[0].reset_at_ms, Some(1_899_981_296_789));
    assert_eq!(snapshot.secondary[1].percent_used, Some(34.0));
    assert_eq!(snapshot.secondary[2].percent_used, Some(100.0));
    assert_eq!(snapshot.provider_extra["usage"]["monthly"]["status"], "rate-limited");
  }

  #[tokio::test]
  async fn rejects_invalid_credentials_through_authenticated_usage_endpoint() {
    let server = server().await;
    let error = fetch(&client(), &account(server.base_url(), "wrong-key"))
      .await
      .unwrap_err();

    assert!(matches!(error, AuthError::Upstream(message) if message.contains("HTTP 401")));
  }

  #[tokio::test]
  async fn verifies_credentials_through_authenticated_usage_endpoint() {
    let server = server().await;
    verify(&client(), &account(server.base_url(), "sk-test")).await.unwrap();
  }

  #[tokio::test]
  async fn rejects_missing_credentials_before_network_access() {
    let error = fetch(&client(), &account("https://opencode.ai/zen/go/v1", ""))
      .await
      .unwrap_err();
    assert!(matches!(error, AuthError::MissingCredential { field: "api_key", .. }));
  }

  #[test]
  fn usage_url_requires_https_or_a_literal_loopback() {
    assert_eq!(
      usage_url("https://opencode.ai/zen/go/v1").unwrap().as_str(),
      "https://opencode.ai/zen/go/v1/usage"
    );
    assert_eq!(
      usage_url("http://127.0.0.1:8080/v1").unwrap().as_str(),
      "http://127.0.0.1:8080/v1/usage"
    );
    assert!(usage_url("http://opencode.ai/zen/go/v1").is_err());
  }

  #[test]
  fn rejects_invalid_usage_window_values() {
    let invalid_percent = usage_bucket(
      "weekly usage",
      UsageWindow {
        percent: 101.0,
        resets_at: "2030-03-18T00:00:00Z".into(),
      },
    )
    .unwrap_err();
    assert!(matches!(invalid_percent, AuthError::Decode(message) if message.contains("percentage")));

    let invalid_reset = usage_bucket(
      "weekly usage",
      UsageWindow {
        percent: 25.0,
        resets_at: "tomorrow".into(),
      },
    )
    .unwrap_err();
    assert!(matches!(invalid_reset, AuthError::Decode(message) if message.contains("reset timestamp")));
  }
}
