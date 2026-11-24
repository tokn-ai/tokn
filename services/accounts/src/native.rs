//! Built-in provider registry for shared request preparation.
//!
//! Platform-independent driver selection is kept alongside the standard
//! registry. Provider modules own their wire contract; this module only dispatches.

use alloc::string::{String, ToString};
use core::fmt;
use serde_json::Value;
use tokn_core::{
  headers::HeaderMap,
  provider::{PrepareError, PreparedRequest},
  types::SmolStr,
  upstream_url::CleartextHttpPolicy,
  Endpoint,
};

#[derive(Debug)]
pub enum Error {
  UnknownDriver(SmolStr),
  Prepare(PrepareError),
  Credential(CredentialError),
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::UnknownDriver(driver) => write!(f, "unknown provider driver '{driver}'"),
      Self::Prepare(error) => error.fmt(f),
      Self::Credential(error) => error.fmt(f),
    }
  }
}

impl core::error::Error for Error {
  fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
    match self {
      Self::Prepare(error) => Some(error),
      Self::Credential(error) => Some(error),
      Self::UnknownDriver(_) => None,
    }
  }
}

/// Whether a built-in driver implements a native upstream API operation.
pub fn supports_endpoint(driver: &str, endpoint: Endpoint, base_url: &str) -> bool {
  match driver {
    "openai" => tokn_provider_openai::native::operation_segments(endpoint, base_url).is_some(),
    "codex" => endpoint == Endpoint::Responses,
    "github-copilot" => tokn_provider_copilot::native::operation_segments(endpoint, base_url).is_some(),
    "deepseek" => tokn_provider_deepseek::native::operation_segments(endpoint, base_url).is_some(),
    "llama-cpp" => tokn_provider_llama_cpp::native::operation_segments(endpoint, base_url).is_some(),
    "opencode-go" => tokn_provider_opencode::native::operation_segments(endpoint, base_url).is_some(),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::operation_segments(endpoint, base_url).is_some()
    }
    _ => false,
  }
}

/// Apply the selected driver's actual transforms, URL construction, headers,
/// credential scheme, and serialization. No transport is selected here.
pub fn prepare(
  driver: &str,
  endpoint: Endpoint,
  base_url: &str,
  credential: &str,
  body: Value,
  headers: HeaderMap,
) -> Result<PreparedRequest, Error> {
  prepare_with_policy(
    driver,
    endpoint,
    base_url,
    credential,
    body,
    headers,
    CleartextHttpPolicy::LoopbackOnly,
  )
}

/// Prepare using the cleartext policy explicitly admitted by configuration.
#[allow(clippy::too_many_arguments)]
pub fn prepare_with_policy(
  driver: &str,
  endpoint: Endpoint,
  base_url: &str,
  credential: &str,
  body: Value,
  headers: HeaderMap,
  cleartext: CleartextHttpPolicy,
) -> Result<PreparedRequest, Error> {
  prepare_with_metadata(driver, endpoint, base_url, credential, body, headers, cleartext, None)
}

/// Apply provider preparation with model metadata supplied by the execution snapshot.
#[allow(clippy::too_many_arguments)]
pub fn prepare_with_metadata(
  driver: &str,
  endpoint: Endpoint,
  base_url: &str,
  credential: &str,
  body: Value,
  headers: HeaderMap,
  cleartext: CleartextHttpPolicy,
  reasoning_supported: Option<bool>,
) -> Result<PreparedRequest, Error> {
  let body = transform_body(driver, endpoint, body, reasoning_supported)?;
  let segments = operation_segments(driver, endpoint, base_url)
    .ok_or(Error::Prepare(PrepareError::UnsupportedEndpoint(endpoint)))?;
  let mut headers = headers;
  inject_driver_credentials(driver, endpoint, &mut headers, credential)?;
  headers.insert("content-type", "application/json");
  headers.insert(
    "accept",
    if body["stream"].as_bool().unwrap_or(false) {
      "text/event-stream"
    } else {
      "application/json"
    },
  );
  tokn_core::provider::prepare_request_with_policy(base_url, segments, endpoint, &body, headers, cleartext)
    .map_err(Error::Prepare)
}

/// Apply only provider payload policy, before any credential or network work.
pub fn transform_body(
  driver: &str,
  endpoint: Endpoint,
  body: Value,
  reasoning_supported: Option<bool>,
) -> Result<Value, Error> {
  Ok(match driver {
    "openai" => tokn_provider_openai::native::transform_input(endpoint, body),
    "codex" => tokn_provider_openai::native::transform_codex_input(endpoint, body),
    "github-copilot" => tokn_provider_copilot::native::transform_input(endpoint, body),
    "deepseek" => tokn_provider_deepseek::native::transform_input(endpoint, body),
    "llama-cpp" => tokn_provider_llama_cpp::native::transform_input(endpoint, body),
    "opencode-go" => tokn_provider_opencode::native::transform_input(endpoint, body),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::transform_input_with_reasoning(body, reasoning_supported)
    }
    _ => return Err(Error::UnknownDriver(SmolStr::new(driver))),
  })
}

fn inject_driver_credentials(
  driver: &str,
  endpoint: Endpoint,
  headers: &mut HeaderMap,
  token: &str,
) -> Result<(), Error> {
  if driver != "llama-cpp" || !token.is_empty() {
    tokn_core::provider::validate_credential(token).map_err(Error::Prepare)?;
  }
  match driver {
    "openai" | "codex" => tokn_provider_openai::native::inject_credentials(headers, endpoint, token),
    "github-copilot" => tokn_provider_copilot::native::inject_credentials(headers, endpoint, token),
    "deepseek" => tokn_provider_deepseek::native::inject_credentials(headers, endpoint, token),
    "llama-cpp" => tokn_provider_llama_cpp::native::inject_credentials(headers, endpoint, token),
    "opencode-go" => tokn_provider_opencode::native::inject_credentials(headers, endpoint, token),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::inject_credentials(headers, endpoint, token)
    }
    _ => return Err(Error::UnknownDriver(SmolStr::new(driver))),
  }
  Ok(())
}

/// Official destination defaults shared with the standard provider constructors.
/// Prefer the configured provider's preset identity, then its driver identity.
pub fn default_base_url(provider_or_driver: &str) -> Option<&'static str> {
  match provider_or_driver {
    "openai" => Some(tokn_provider_openai::native::OPENAI_BASE_URL),
    "codex" => Some(tokn_provider_openai::native::CODEX_BASE_URL),
    "github-copilot" => Some(tokn_provider_copilot::native::COPILOT_BASE_URL),
    "deepseek" => Some(tokn_provider_deepseek::native::DEFAULT_BASE_URL),
    "llama-cpp" => Some(tokn_provider_llama_cpp::native::DEFAULT_BASE_URL),
    "opencode-go" => Some(tokn_provider_opencode::native::OPENCODE_GO_BASE_URL),
    "zai" => Some(tokn_provider_zai::native::ZAI_BASE_URL),
    "zai-coding-plan" => Some(tokn_provider_zai::native::ZAI_CODING_PLAN_BASE_URL),
    "zhipuai" => Some(tokn_provider_zai::native::ZHIPUAI_BASE_URL),
    "zhipuai-coding-plan" => Some(tokn_provider_zai::native::ZHIPUAI_CODING_PLAN_BASE_URL),
    _ => None,
  }
}

/// Destination defaults for the portable linker. Official providers retain
/// their named destination while custom mappings inherit their driver's URL.
pub fn provider_default_base_url(
  provider: &tokn_policy::ProviderId,
  driver: &tokn_policy::DriverId,
) -> Option<&'static str> {
  let preset =
    tokn_core::provider::official_provider_preset(provider.as_str()).filter(|preset| preset.driver == driver.as_str());
  default_base_url(preset.map_or(driver.as_str(), |preset| preset.id))
}

/// Apply each driver's actual per-model API rules. Known models can force a
/// native operation even when the provider supports several operations overall.
pub fn supports_model_endpoint(driver: &str, model: &str, endpoint: Endpoint, base_url: &str) -> bool {
  match driver {
    "openai" => tokn_provider_openai::native::supports_model_endpoint(model, endpoint, base_url),
    "codex" => endpoint == Endpoint::Responses,
    "github-copilot" => tokn_provider_copilot::native::supports_model_endpoint(model, endpoint, base_url),
    "deepseek" => tokn_provider_deepseek::native::supports_model_endpoint(model, endpoint, base_url),
    "llama-cpp" => tokn_provider_llama_cpp::native::supports_model_endpoint(model, endpoint, base_url),
    "opencode-go" => tokn_provider_opencode::native::supports_model_endpoint(model, endpoint, base_url),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::supports_model_endpoint(model, endpoint, base_url)
    }
    _ => false,
  }
}

/// Enforce the driver's shared header normalization after credential injection.
/// The pipeline's request id remains authoritative when a driver rebuilds an
/// allowlisted header map. Codex also needs its account context from credentials.
pub fn normalize_headers(
  driver: &str,
  headers: &mut HeaderMap,
  ctx: &tokn_core::provider::HeaderPatchCtx<'_>,
  account: &tokn_core::account::AccountConfig,
) -> Result<(), Error> {
  let request_id = headers.get("x-request-id").cloned();
  let normalized = match driver {
    "openai" => tokn_provider_openai::native::normalize_headers(headers, ctx),
    "codex" => {
      let account_id = tokn_provider_openai::jwt::account_id(account);
      tokn_provider_openai::native::inject_codex_credentials(
        headers,
        ctx.bearer_token.or_else(|| credential(driver, account)).unwrap_or(""),
        account_id.as_deref(),
      );
      tokn_provider_openai::native::normalize_codex_headers(headers, ctx)
    }
    "github-copilot" => {
      let identity = tokn_provider_copilot::config::CopilotHeaders::from_account(account)
        .map_err(CredentialError::Settings)
        .map_err(Error::Credential)?;
      let endpoint = match ctx.request_kind {
        tokn_core::provider::ProviderRequestKind::Operation(endpoint) => endpoint,
        tokn_core::provider::ProviderRequestKind::Models | tokn_core::provider::ProviderRequestKind::Opaque => {
          Endpoint::ChatCompletions
        }
      };
      let initiator = tokn_provider_copilot::native::resolve_initiator(
        endpoint,
        ctx.body,
        ctx.inbound_headers,
        ctx.initiator,
        identity.initiator_mode,
      );
      tokn_provider_copilot::native::normalize_headers(
        headers,
        &tokn_core::provider::HeaderPatchCtx {
          initiator: &initiator,
          ..*ctx
        },
      )
    }
    "deepseek" => tokn_provider_deepseek::native::normalize_headers(headers, ctx),
    "llama-cpp" => tokn_provider_llama_cpp::native::normalize_headers(headers, ctx),
    "opencode-go" => tokn_provider_opencode::native::normalize_headers(headers, ctx),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::normalize_headers(headers, ctx)
    }
    _ => return Err(Error::UnknownDriver(SmolStr::new(driver))),
  };
  if let Some(normalized) = normalized {
    *headers = normalized;
  }
  if let Some(request_id) = request_id {
    headers.insert("x-request-id", request_id);
  }
  Ok(())
}

/// Apply account credentials and the driver's final wire header contract.
/// Relays call this without transforming their payload; managed requests call
/// it after body preparation so both paths use identical credential authority.
pub fn patch_headers(
  driver: &str,
  headers: &mut HeaderMap,
  ctx: &tokn_core::provider::HeaderPatchCtx<'_>,
  account: &tokn_core::account::AccountConfig,
) -> Result<(), Error> {
  let token = ctx.bearer_token.or_else(|| credential(driver, account)).unwrap_or("");
  inject_driver_credentials(
    driver,
    ctx.endpoint().unwrap_or(Endpoint::ChatCompletions),
    headers,
    token,
  )?;
  normalize_headers(driver, headers, ctx, account)
}

/// A credential HTTP exchange; the session supplies transport and cache ownership.
pub struct CredentialRefreshRequest {
  pub method: &'static str,
  pub url: String,
  pub headers: HeaderMap,
  pub body: tokn_core::types::Bytes,
}

pub struct CredentialUpdate {
  pub access_token: tokn_core::account::Secret<String>,
  pub expires_at: i64,
}

#[derive(Debug)]
pub enum CredentialError {
  Settings(serde_json::Error),
  InvalidCredential(PrepareError),
  InvalidUrl(tokn_core::upstream_url::InvalidUpstreamUrl),
  Response(serde_json::Error),
  ExpiredToken,
  UnsupportedDriver(SmolStr),
}

impl fmt::Display for CredentialError {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Settings(error) => write!(formatter, "invalid provider settings: {error}"),
      Self::InvalidCredential(error) => error.fmt(formatter),
      Self::InvalidUrl(error) => write!(formatter, "invalid credential exchange URL: {error}"),
      Self::Response(error) => write!(formatter, "invalid credential exchange response: {error}"),
      Self::ExpiredToken => formatter.write_str("credential exchange returned an expired token"),
      Self::UnsupportedDriver(driver) => write!(formatter, "driver '{driver}' has no in-band credential exchange"),
    }
  }
}

impl core::error::Error for CredentialError {
  fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
    match self {
      Self::Settings(error) | Self::Response(error) => Some(error),
      Self::InvalidCredential(error) => Some(error),
      Self::InvalidUrl(error) => Some(error),
      Self::ExpiredToken | Self::UnsupportedDriver(_) => None,
    }
  }
}

/// Decide whether the account needs an in-band exchange and prepare its wire request.
/// Explicit caller-supplied access tokens without a refresh credential remain valid
/// inputs; their lifecycle is owned by the caller.
pub fn credential_refresh_request(
  driver: &str,
  account: &tokn_core::account::AccountConfig,
  now: i64,
  cleartext: CleartextHttpPolicy,
) -> Result<Option<CredentialRefreshRequest>, CredentialError> {
  if driver != "github-copilot" {
    return Ok(None);
  }
  let Some(refresh_token) = account.refresh_token.as_ref() else {
    return Ok(None);
  };
  if account.access_token.is_some()
    && account
      .access_token_expires_at
      .is_some_and(|expiry| tokn_provider_copilot::native::api_token_valid_at(expiry, now))
  {
    return Ok(None);
  }
  tokn_core::provider::validate_credential(refresh_token.expose()).map_err(CredentialError::InvalidCredential)?;
  let identity =
    tokn_provider_copilot::config::CopilotHeaders::from_account(account).map_err(CredentialError::Settings)?;
  let url = account
    .refresh_url
    .as_deref()
    .unwrap_or(tokn_provider_copilot::native::COPILOT_TOKEN_EXCHANGE_URL);
  let canonical =
    tokn_core::upstream_url::CanonicalUpstreamUrl::parse(url, cleartext).map_err(CredentialError::InvalidUrl)?;
  let url = if url.ends_with('/') {
    canonical.as_str().to_string()
  } else {
    canonical.as_str().trim_end_matches('/').to_string()
  };
  Ok(Some(CredentialRefreshRequest {
    method: "GET",
    url,
    headers: tokn_provider_copilot::native::token_exchange_headers(refresh_token.expose(), &identity),
    body: tokn_core::types::Bytes::new(),
  }))
}

/// Validate an exchange response before publishing it to shared account state.
pub fn decode_credential_refresh(driver: &str, body: &[u8], now: i64) -> Result<CredentialUpdate, CredentialError> {
  if driver != "github-copilot" {
    return Err(CredentialError::UnsupportedDriver(SmolStr::new(driver)));
  }
  let response: tokn_provider_copilot::native::CopilotTokenResponse =
    serde_json::from_slice(body).map_err(CredentialError::Response)?;
  tokn_core::provider::validate_credential(&response.token).map_err(CredentialError::InvalidCredential)?;
  if response.expires_at <= now {
    return Err(CredentialError::ExpiredToken);
  }
  Ok(CredentialUpdate {
    access_token: tokn_core::account::Secret::new(response.token),
    expires_at: response.expires_at,
  })
}

/// Select the driver's preferred credential flavor after any refresh completes.
pub fn credential<'a>(driver: &str, account: &'a tokn_core::account::AccountConfig) -> Option<&'a str> {
  let secret = if matches!(driver, "codex" | "github-copilot") {
    account.access_token.as_ref().or(account.api_key.as_ref())
  } else {
    account.api_key.as_ref().or(account.access_token.as_ref())
  };
  secret.map(|secret| secret.expose().as_str())
}

/// Native API path without selecting credentials or transforming payload bytes.
/// Used by fixed-destination relays, whose caller owns the wire request.
pub fn operation_segments(driver: &str, endpoint: Endpoint, base_url: &str) -> Option<&'static [&'static str]> {
  match driver {
    "openai" => tokn_provider_openai::native::operation_segments(endpoint, base_url),
    "codex" if endpoint == Endpoint::Responses => Some(&["responses"]),
    "github-copilot" => tokn_provider_copilot::native::operation_segments(endpoint, base_url),
    "deepseek" => tokn_provider_deepseek::native::operation_segments(endpoint, base_url),
    "llama-cpp" => tokn_provider_llama_cpp::native::operation_segments(endpoint, base_url),
    "opencode-go" => tokn_provider_opencode::native::operation_segments(endpoint, base_url),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::operation_segments(endpoint, base_url)
    }
    _ => None,
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;
  use tokn_core::account::AccountConfig;

  fn copilot() -> AccountConfig {
    serde_json::from_value(json!({
      "id": "copilot-test",
      "provider": "github-copilot",
      "refresh_token": "test-github-token",
      "access_token": "test-api-token",
      "access_token_expires_at": 1_301,
      "refresh_url": "http://127.0.0.1:1234/token",
      "settings": { "editor_version": "editor/test" }
    }))
    .unwrap()
  }

  #[test]
  fn credential_exchange_respects_cached_expiry_skew_and_configured_endpoint() {
    let mut account = copilot();
    assert!(
      credential_refresh_request("github-copilot", &account, 1_000, CleartextHttpPolicy::LoopbackOnly)
        .unwrap()
        .is_none()
    );
    account.access_token_expires_at = Some(1_300);
    let request = credential_refresh_request("github-copilot", &account, 1_000, CleartextHttpPolicy::LoopbackOnly)
      .unwrap()
      .unwrap();
    assert_eq!(request.method, "GET");
    assert_eq!(request.url, "http://127.0.0.1:1234/token");
    assert!(request.body.is_empty());
    assert_eq!(
      request.headers.get("authorization").unwrap().as_str(),
      "token test-github-token"
    );
    assert_eq!(request.headers.get("editor-version").unwrap().as_str(), "editor/test");
    assert_eq!(request.headers.get("accept").unwrap().as_str(), "application/json");
    account.refresh_token = None;
    assert!(
      credential_refresh_request("github-copilot", &account, 1_000, CleartextHttpPolicy::LoopbackOnly)
        .unwrap()
        .is_none()
    );
  }

  #[test]
  fn credential_exchange_rejects_invalid_tokens_expiry_and_urls() {
    for body in [
      br#"{"token":"","expires_at":2000}"#.as_slice(),
      br#"{"token":"contains space","expires_at":2000}"#,
      br#"{"token":"valid-token","expires_at":1000}"#,
      br#"{"expires_at":2000}"#,
      br#"not json"#,
    ] {
      assert!(decode_credential_refresh("github-copilot", body, 1_000).is_err());
    }
    let update =
      decode_credential_refresh("github-copilot", br#"{"token":"new-token","expires_at":2000}"#, 1_000).unwrap();
    assert_eq!(update.access_token.expose(), "new-token");
    assert_eq!(update.expires_at, 2_000);
    let mut account = copilot();
    account.access_token = None;
    for url in [
      "http://example.com/token",
      "https://example.com/token?secret=bad",
      "https://user@example.com/token",
    ] {
      account.refresh_url = Some(url.into());
      assert!(
        credential_refresh_request("github-copilot", &account, 1_000, CleartextHttpPolicy::LoopbackOnly).is_err()
      );
    }
  }

  #[test]
  fn provider_preparation_uses_reasoning_metadata_before_model_name_fallback() {
    for (model, reasoning, expected) in [
      ("glm-future", Some(false), false),
      ("custom-model", Some(true), true),
      ("glm-future", None, true),
    ] {
      let request = prepare_with_metadata(
        "zai",
        Endpoint::ChatCompletions,
        "https://example.com/v1",
        "test-key",
        json!({"model": model, "messages": []}),
        HeaderMap::new(),
        CleartextHttpPolicy::LoopbackOnly,
        reasoning,
      )
      .unwrap();
      let body: Value = serde_json::from_slice(&request.body).unwrap();
      assert_eq!(body.get("thinking").is_some(), expected);
    }
  }

  #[test]
  fn account_header_patch_injects_credentials_without_rewriting_relay_bodies() {
    let account: AccountConfig = serde_json::from_value(json!({
      "id": "account", "provider": "upstream", "api_key": "account-key",
      "provider_account_id": "codex-account"
    }))
    .unwrap();
    let body = json!({"model":"unmodified", "unknown_provider_field": 7});
    let inbound = HeaderMap::new();
    let vars = tokn_core::headers::TemplateVars::default();
    let identity = tokn_core::AgentId::default();
    for driver in [
      "openai",
      "codex",
      "github-copilot",
      "deepseek",
      "llama-cpp",
      "opencode-go",
      "zai",
    ] {
      let mut headers = HeaderMap::new();
      headers.insert("authorization", "Bearer caller-key");
      headers.insert("x-request-id", "request");
      let context = tokn_core::provider::HeaderPatchCtx {
        request_kind: tokn_core::provider::ProviderRequestKind::Operation(Endpoint::ChatCompletions),
        body: &body,
        bearer_token: Some("account-key"),
        content_encoding: None,
        stream: false,
        initiator: "user",
        inbound_headers: &inbound,
        vars: &vars,
        agent_id: &identity,
      };
      patch_headers(driver, &mut headers, &context, &account).unwrap();
      assert_eq!(
        headers.get("authorization").unwrap().as_str(),
        "Bearer account-key",
        "{driver}"
      );
      assert_eq!(headers.get("x-request-id").unwrap().as_str(), "request", "{driver}");
      assert_eq!(body["unknown_provider_field"], 7);
    }
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer caller-key");
    let context = tokn_core::provider::HeaderPatchCtx {
      request_kind: tokn_core::provider::ProviderRequestKind::Operation(Endpoint::Messages),
      body: &body,
      bearer_token: Some("account-key"),
      content_encoding: None,
      stream: false,
      initiator: "user",
      inbound_headers: &inbound,
      vars: &vars,
      agent_id: &identity,
    };
    patch_headers("opencode-go", &mut headers, &context, &account).unwrap();
    assert!(!headers.contains_key("authorization"));
    assert_eq!(headers.get("x-api-key").unwrap().as_str(), "account-key");
  }
}
