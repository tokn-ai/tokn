//! Account lifecycle operations shared by interactive frontends.
//! Responses deliberately omit stored credentials and provider-specific raw payloads.
use crate::context::{ConfigContext, ResolvedProviderAuth};
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use std::{
  path::Path,
  time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokn_auth::{
  AccountConfig, AccountTier, AuthStore, AuthStoreLock, CredentialFlavor, CredentialResult, CredentialSource,
  DeviceFlowOutcome, MeteredBucket, UsageBucket,
};
use tokn_core::account::{AuthType, Secret};

#[derive(Serialize)]
pub struct ProviderOptions {
  pub id: String,
  pub device_login: bool,
  pub api_key: bool,
  pub refresh_token: bool,
  pub sources: Vec<String>,
  pub default_flavor: &'static str,
}

pub fn providers(context: &ConfigContext) -> Result<Vec<ProviderOptions>> {
  context
    .provider_ids()
    .into_iter()
    .map(|id| {
      let provider = context.resolve_provider(&id)?;
      let auth = provider.auth();
      Ok(ProviderOptions {
        id,
        default_flavor: match auth.default_auth_flavor() {
          CredentialFlavor::ApiKey => "api_key",
          CredentialFlavor::RefreshToken => "refresh_token",
        },
        device_login: auth.supports_device_flow(),
        api_key: auth.supports_auth_flavor(CredentialFlavor::ApiKey),
        refresh_token: auth.supports_auth_flavor(CredentialFlavor::RefreshToken),
        sources: auth
          .credential_sources()
          .iter()
          .filter(|source| source.as_str() != "login")
          .map(|source| source.as_str().to_string())
          .collect(),
      })
    })
    .collect()
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Activation {
  Active,
  Fallback,
  Disabled,
}

#[derive(Serialize)]
pub struct AccountSummary {
  pub id: String,
  pub provider: String,
  pub label: Option<String>,
  pub username: Option<String>,
  pub enabled: bool,
  pub tier: AccountTier,
  pub credential_kind: &'static str,
  pub credential_status: &'static str,
  pub expires_at: Option<i64>,
  pub last_refresh: Option<i64>,
  pub can_refresh: bool,
}

pub fn summary(account: &AccountConfig) -> AccountSummary {
  let oauth = account.refresh_token.is_some();
  let expires_at = if account.api_key.is_some() {
    account.api_key_expires_at
  } else {
    account.access_token_expires_at
  };
  let present = account.api_key.is_some() || account.access_token.is_some() || oauth;
  AccountSummary {
    id: account.id.clone(),
    provider: account.provider.clone(),
    label: account.label.clone(),
    username: account.username.clone(),
    enabled: account.enabled,
    tier: account.tier,
    credential_kind: if oauth {
      "oauth"
    } else if account.api_key.is_some() {
      "api_key"
    } else {
      "access_token"
    },
    credential_status: if !present {
      "missing"
    } else if expires_at.is_some_and(|expires| expires <= now()) {
      "expired"
    } else {
      "stored"
    },
    expires_at,
    last_refresh: account.last_refresh,
    can_refresh: oauth,
  }
}

pub fn list(auth_path: Option<&Path>) -> Result<Vec<AccountSummary>> {
  let store = AuthStore::load(auth_path, None)?;
  let mut accounts: Vec<_> = store.accounts.iter().map(summary).collect();
  accounts.sort_by(|a, b| (&a.provider, &a.id).cmp(&(&b.provider, &b.id)));
  Ok(accounts)
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum AccountEdit {
  Update {
    id: String,
    label: Option<String>,
    activation: Activation,
  },
  Remove {
    id: String,
  },
}

pub fn edit(auth_path: Option<&Path>, edit: AccountEdit) -> Result<()> {
  let lock = AuthStoreLock::acquire(auth_path)?;
  let mut store = AuthStore::load_locked(&lock)?;
  match edit {
    AccountEdit::Update { id, label, activation } => {
      let account = store
        .accounts
        .iter_mut()
        .find(|account| account.id == id)
        .ok_or_else(|| anyhow!("Account no longer exists"))?;
      account.label = label
        .map(|label| label.trim().to_owned())
        .filter(|label| !label.is_empty());
      account.enabled = !matches!(activation, Activation::Disabled);
      if account.enabled {
        account.tier = if matches!(activation, Activation::Active) {
          AccountTier::Active
        } else {
          AccountTier::Fallback
        };
      }
    }
    AccountEdit::Remove { id } => {
      store.remove(&id).ok_or_else(|| anyhow!("Account no longer exists"))?;
    }
  }
  store.save_locked(&lock)
}

pub fn validate_new_id(auth_path: Option<&Path>, id: &str) -> Result<()> {
  if id.trim().is_empty() || id != id.trim() || id.len() > 128 || id.chars().any(char::is_control) {
    bail!("Use an account ID of 1–128 characters without leading/trailing whitespace or control characters");
  }
  if AuthStore::load(auth_path, None)?
    .accounts
    .iter()
    .any(|account| account.id == id)
  {
    bail!("An account with this ID already exists; choose another ID");
  }
  Ok(())
}

pub fn insert(auth_path: Option<&Path>, account: AccountConfig) -> Result<()> {
  insert_with_id(auth_path, account, false)
}

/// Allocate a unique inferred ID while holding the same lock used to save it.
pub fn insert_generated(auth_path: Option<&Path>, account: AccountConfig) -> Result<()> {
  insert_with_id(auth_path, account, true)
}

fn insert_with_id(auth_path: Option<&Path>, mut account: AccountConfig, generated: bool) -> Result<()> {
  let lock = AuthStoreLock::acquire(auth_path)?;
  let mut store = AuthStore::load_locked(&lock)?;
  if generated {
    let base = suggested_id(&account);
    account.id = base.clone();
    let mut suffix = 2;
    while store.accounts.iter().any(|existing| existing.id == account.id) {
      account.id = format!("{base}-{suffix}");
      suffix += 1;
    }
  }
  if store.accounts.iter().any(|existing| existing.id == account.id) {
    bail!("An account with this ID already exists; nothing was replaced");
  }
  store.upsert_in_main(account)?;
  store.save_locked(&lock)
}

fn suggested_id(account: &AccountConfig) -> String {
  [
    account.username.as_deref(),
    account.provider_account_id.as_deref(),
    Some(account.provider.as_str()),
  ]
  .into_iter()
  .flatten()
  .map(str::trim)
  .find(|value| !value.is_empty() && value.len() <= 100 && !value.chars().any(char::is_control))
  .unwrap_or("account")
  .to_string()
}

pub fn empty_account(provider: &ResolvedProviderAuth, id: String) -> AccountConfig {
  let auth = provider.auth();
  AccountConfig {
    id,
    provider: provider.provider_id().into(),
    enabled: true,
    tier: AccountTier::Active,
    tags: Vec::new(),
    label: None,
    base_url: auth.default_base_url().map(str::to_owned),
    headers: Default::default(),
    auth_type: Some(AuthType::Bearer),
    username: None,
    api_key: None,
    api_key_expires_at: None,
    access_token: None,
    access_token_expires_at: None,
    id_token: None,
    refresh_token: None,
    provider_account_id: None,
    extra: Default::default(),
    refresh_url: auth.default_refresh_url().map(str::to_owned),
    last_refresh: None,
    settings: toml::Table::new(),
  }
}

pub fn device_account(provider: &ResolvedProviderAuth, id: String, outcome: DeviceFlowOutcome) -> AccountConfig {
  let mut account = empty_account(provider, id);
  account.access_token = Some(Secret::new(outcome.access_token));
  account.access_token_expires_at = Some(outcome.access_token_expires_at);
  account.refresh_token = Some(Secret::new(outcome.refresh_token));
  account.username = outcome.username;
  account.provider_account_id = outcome.provider_account_id;
  account.last_refresh = Some(now());
  provider.finish_account(&mut account);
  account
}

/// Import through the provider's advertised source; validate before saving.
pub async fn import_account(
  client: &reqwest::Client,
  provider: &ResolvedProviderAuth,
  id: String,
  source: CredentialSource,
) -> Result<AccountConfig> {
  let auth = provider.auth();
  if !auth.supports_credential_source(&source) {
    bail!("This credential source is not supported by the provider");
  }
  let result = auth.import_from(&source).await?;
  let mut account = empty_account(provider, id);
  match result {
    CredentialResult::ApiKey(key) => {
      account.api_key = Some(Secret::new(key));
      account.username = auth
        .verify_credential(client, &provider.account_for_auth(&account))
        .await?
        .username;
    }
    CredentialResult::Refresh(token) => {
      account.refresh_token = Some(Secret::new(token));
      auth
        .refresh_credential(client, &provider.account_for_auth(&account))
        .await?
        .apply_to(&mut account);
    }
  }
  provider.finish_account(&mut account);
  Ok(account)
}

#[derive(Serialize, Default)]
pub struct AccountProbe {
  pub checked_at: i64,
  pub authentication: String,
  pub quota_status: String,
  pub plan: Option<String>,
  pub headline: Option<String>,
  pub reset_date: Option<String>,
  pub metered: Option<MeteredBucket>,
  pub secondary: Vec<UsageBucket>,
  pub message: Option<String>,
}

/// Hold the cooperative lock through refresh, and save rotation *before* probing
/// quota. A failed quota endpoint must never discard a completed token exchange.
pub async fn probe(context: &ConfigContext, auth_path: Option<&Path>, id: &str, force: bool) -> Result<AccountProbe> {
  let lock = AuthStoreLock::acquire(auth_path)?;
  let store = AuthStore::load_locked(&lock)?;
  let index = store
    .accounts
    .iter()
    .position(|account| account.id == id)
    .ok_or_else(|| anyhow!("Account no longer exists"))?;
  let provider = context.resolve_account_provider(&store.accounts[index])?;
  let auth = provider.auth();
  let client = context.build_http_client(false)?;
  let account = provider.account_for_auth(&store.accounts[index]);
  probe_locked(store, lock, index, auth, client, account, force).await
}

async fn probe_locked(
  mut store: AuthStore,
  lock: AuthStoreLock,
  index: usize,
  auth: &dyn tokn_auth::ProviderAuth,
  client: reqwest::Client,
  mut account: AccountConfig,
  force: bool,
) -> Result<AccountProbe> {
  let exchange = async {
    if force {
      auth.refresh_credential(&client, &account).await
    } else {
      auth.refresh_credential_if_needed(&client, &account).await
    }
  };
  let mut result = AccountProbe {
    checked_at: now(),
    authentication: "unverified".into(),
    quota_status: "unavailable".into(),
    ..Default::default()
  };
  match tokio::time::timeout(Duration::from_secs(20), exchange).await {
    Ok(Ok(refresh)) => {
      if refresh.apply_to(&mut store.accounts[index]) {
        store.save_locked(&lock)?;
        refresh.apply_to(&mut account);
        result.authentication = "verified".into();
      } else if account.refresh_token.is_some() {
        result.authentication = "current".into();
      }
    }
    _ => {
      result.authentication = "failed".into();
      result.message = Some("Credential refresh failed or timed out. Try signing in again.".into());
      return Ok(result);
    }
  }
  drop(lock);
  // Report remote failures without forwarding upstream bodies that may contain secrets.
  if account.refresh_token.is_none() {
    match tokio::time::timeout(Duration::from_secs(15), auth.verify_credential(&client, &account)).await {
      Ok(Ok(_)) => result.authentication = "verified".into(),
      Ok(Err(tokn_auth::AuthError::Unsupported(_))) => {}
      _ => {
        result.authentication = "failed".into();
        result.message = Some("Credential verification failed or timed out.".into());
      }
    }
  }
  match tokio::time::timeout(auth.quota_timeout(), auth.probe_quota(&client, &account)).await {
    Ok(Ok(quota)) => {
      result.quota_status =
        if quota.plan.is_none() && quota.headline.is_none() && quota.metered.is_none() && quota.secondary.is_empty() {
          "unsupported"
        } else {
          "available"
        }
        .into();
      result.plan = quota.plan;
      result.headline = quota.headline;
      result.reset_date = quota.reset_date;
      result.metered = quota.metered;
      result.secondary = quota.secondary;
    }
    Ok(Err(tokn_auth::AuthError::Unsupported(_))) => result.quota_status = "unsupported".into(),
    _ => {
      result.message = Some("Provider quota is currently unavailable. Local usage is still available.".into());
    }
  }
  Ok(result)
}

fn now() -> i64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_secs() as i64
}

#[cfg(test)]
mod tests;
