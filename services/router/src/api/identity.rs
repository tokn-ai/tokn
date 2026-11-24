use axum::http::HeaderMap;
use std::collections::HashMap;
use tokn_core::account::AccountConfig;
use tokn_core::provider::ID_GITHUB_COPILOT;

use tokn_accounts::registry::Registry;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountIdentity {
  pub account_id: Option<String>,
  pub provider_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct AccountIdentityResolver {
  by_fingerprint: HashMap<String, AccountIdentity>,
}

impl AccountIdentityResolver {
  pub fn from_accounts(accounts: &[AccountConfig]) -> Self {
    let mut resolver = Self::default();
    for account in accounts {
      for secret in [
        account.api_key.as_ref(),
        account.access_token.as_ref(),
        account.id_token.as_ref(),
      ]
      .into_iter()
      .flatten()
      {
        resolver.insert_fingerprint(secret.fingerprint(), account);
      }
      if account.provider == ID_GITHUB_COPILOT {
        if let Some(secret) = account.refresh_token.as_ref() {
          resolver.insert_fingerprint(secret.fingerprint(), account);
        }
      }
    }
    resolver
  }

  pub fn resolve(&self, headers: &HeaderMap, url_or_host: &str, registry: &Registry) -> AccountIdentity {
    if let Some(identity) = credential_candidates(headers).find_map(|candidate| self.match_secret(candidate)) {
      return identity.clone();
    }
    let account_id = credential_candidates(headers).find_map(fallback_account_id_for_secret);
    AccountIdentity {
      account_id,
      provider_id: registry.provider_id_for_url(url_or_host).map(str::to_string),
    }
  }

  fn insert_fingerprint(&mut self, fingerprint: String, account: &AccountConfig) {
    if fingerprint == "fp:<empty>" {
      return;
    }
    self.by_fingerprint.insert(
      fingerprint,
      AccountIdentity {
        account_id: Some(account.id.clone()),
        provider_id: Some(account.provider.clone()),
      },
    );
  }

  fn match_secret(&self, secret: &str) -> Option<&AccountIdentity> {
    let secret = secret.trim();
    if secret.is_empty() {
      return None;
    }
    self
      .by_fingerprint
      .get(&tokn_core::util::redact::token_fingerprint(secret))
  }
}

fn credential_candidates(headers: &HeaderMap) -> impl Iterator<Item = &str> {
  let authorization = headers
    .get(reqwest::header::AUTHORIZATION)
    .and_then(|v| v.to_str().ok())
    .into_iter()
    .flat_map(|value| {
      let bearer = value
        .trim()
        .strip_prefix("Bearer ")
        .or_else(|| value.trim().strip_prefix("bearer "));
      bearer.into_iter().chain(std::iter::once(value.trim()))
    });
  let x_api_key = headers
    .get("x-api-key")
    .and_then(|v| v.to_str().ok())
    .into_iter()
    .map(str::trim);
  authorization.chain(x_api_key)
}

fn fallback_account_id_for_secret(secret: &str) -> Option<String> {
  let secret = secret.trim();
  if secret.len() < 32 {
    return None;
  }
  let fingerprint = tokn_core::util::redact::token_fingerprint(secret);
  let suffix = fingerprint
    .strip_prefix("fp:")?
    .chars()
    .rev()
    .take(4)
    .collect::<Vec<_>>();
  Some(format!("account_fp_{}", suffix.into_iter().rev().collect::<String>()))
}

#[cfg(test)]
mod tests {
  use super::*;
  use tokn_core::account::{AccountConfig, AuthType, Secret};

  fn account(
    id: &str,
    provider: &str,
    api_key: Option<&str>,
    access_token: Option<&str>,
    refresh_token: Option<&str>,
  ) -> AccountConfig {
    AccountConfig {
      id: id.into(),
      provider: provider.into(),
      enabled: true,
      tier: Default::default(),
      tags: Vec::new(),
      label: None,
      base_url: None,
      headers: Default::default(),
      auth_type: Some(AuthType::Bearer),
      username: None,
      api_key: api_key.map(|s| Secret::new(s.to_string())),
      api_key_expires_at: None,
      access_token: access_token.map(|s| Secret::new(s.to_string())),
      access_token_expires_at: None,
      id_token: None,
      refresh_token: refresh_token.map(|s| Secret::new(s.to_string())),
      provider_account_id: None,
      extra: Default::default(),
      refresh_url: None,
      last_refresh: None,
      settings: Default::default(),
    }
  }

  #[test]
  fn resolves_credentials_before_provider_url() {
    let resolver = AccountIdentityResolver::from_accounts(&[account("acct", "zai", Some("secret"), None, None)]);
    let registry = Registry::builtin();
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", "secret".parse().unwrap());

    let identity = resolver.resolve(&headers, "https://api.githubcopilot.com/chat", &registry);
    assert_eq!(identity.account_id.as_deref(), Some("acct"));
    assert_eq!(identity.provider_id.as_deref(), Some("zai"));
  }

  #[test]
  fn resolves_copilot_refresh_token_as_api_key() {
    let resolver = AccountIdentityResolver::from_accounts(&[account(
      "copilot-acct",
      ID_GITHUB_COPILOT,
      None,
      None,
      Some("ghu-refresh"),
    )]);
    let registry = Registry::builtin();
    let mut headers = HeaderMap::new();
    headers.insert(reqwest::header::AUTHORIZATION, "Bearer ghu-refresh".parse().unwrap());

    let identity = resolver.resolve(&headers, "https://api.githubcopilot.com/chat", &registry);
    assert_eq!(identity.account_id.as_deref(), Some("copilot-acct"));
    assert_eq!(identity.provider_id.as_deref(), Some(ID_GITHUB_COPILOT));
  }

  #[test]
  fn does_not_resolve_non_copilot_refresh_token() {
    let resolver =
      AccountIdentityResolver::from_accounts(&[account("zai-acct", "zai", None, None, Some("zai-refresh"))]);
    let registry = Registry::builtin();
    let mut headers = HeaderMap::new();
    headers.insert(reqwest::header::AUTHORIZATION, "Bearer zai-refresh".parse().unwrap());

    let identity = resolver.resolve(&headers, "https://api.z.ai/api/paas/v4", &registry);
    assert_eq!(identity.account_id, None);
    assert_eq!(identity.provider_id.as_deref(), Some("zai"));
  }

  #[test]
  fn unmatched_long_credential_gets_fingerprint_account_id() {
    let resolver = AccountIdentityResolver::default();
    let registry = Registry::builtin();
    let secret = "abcdefghijklmnopqrstuvwxyz012345";
    let mut headers = HeaderMap::new();
    headers.insert(
      reqwest::header::AUTHORIZATION,
      format!("Bearer {secret}").parse().unwrap(),
    );

    let identity = resolver.resolve(&headers, "https://api.z.ai/api/paas/v4", &registry);
    let fp = tokn_core::util::redact::token_fingerprint(secret);
    let want_suffix = &fp[fp.len() - 4..];
    assert_eq!(
      identity.account_id.as_deref(),
      Some(format!("account_fp_{want_suffix}").as_str())
    );
    assert_eq!(identity.provider_id.as_deref(), Some("zai"));
  }

  #[test]
  fn unmatched_short_credential_has_no_account_id() {
    let resolver = AccountIdentityResolver::default();
    let registry = Registry::builtin();
    let mut headers = HeaderMap::new();
    headers.insert(reqwest::header::AUTHORIZATION, "Bearer short-token".parse().unwrap());

    let identity = resolver.resolve(&headers, "https://api.z.ai/api/paas/v4", &registry);
    assert_eq!(identity.account_id, None);
    assert_eq!(identity.provider_id.as_deref(), Some("zai"));
  }

  #[test]
  fn falls_back_to_provider_registry() {
    let resolver = AccountIdentityResolver::default();
    let identity = resolver.resolve(&HeaderMap::new(), "https://api.z.ai/api/paas/v4", &Registry::builtin());
    assert_eq!(identity.account_id, None);
    assert_eq!(identity.provider_id.as_deref(), Some("zai"));
  }
}
