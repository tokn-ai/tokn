pub use crate::util::secret::Secret;
use alloc::{
  collections::BTreeMap,
  string::{String, ToString},
  vec::Vec,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
  pub id: String,
  #[serde(default = "default_provider")]
  pub provider: String,
  #[serde(default = "default_true")]
  pub enabled: bool,
  /// Activation tier within an enabled account. `Active` accounts are
  /// tried first by the pool; `Fallback` accounts are only consulted when
  /// every active account in the same provider is cooled or exhausted.
  /// Disabled accounts (`enabled = false`) are skipped entirely; this
  /// field is then meaningless.
  #[serde(default)]
  pub tier: AccountTier,
  #[serde(default)]
  pub tags: Vec<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub label: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub base_url: Option<String>,
  #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
  pub headers: BTreeMap<String, String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub auth_type: Option<AuthType>,
  #[serde(default)]
  pub username: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub api_key: Option<Secret<String>>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub api_key_expires_at: Option<i64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub access_token: Option<Secret<String>>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub access_token_expires_at: Option<i64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub id_token: Option<Secret<String>>,
  #[serde(default)]
  pub refresh_token: Option<Secret<String>>,
  /// Provider-specific account identifier (e.g. ChatGPT account id from a
  /// codex JWT). Stored verbatim and surfaced to providers via
  /// provider-owned credential injection for use in outbound headers.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub provider_account_id: Option<String>,
  #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
  pub extra: BTreeMap<String, Secret<String>>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub refresh_url: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub last_refresh: Option<i64>,
  #[serde(default)]
  pub settings: crate::settings::Table,
}

pub type Account = AccountConfig;

/// Activation tier of an account within its provider bucket.
///
/// `Active` accounts serve traffic in normal flow; `Fallback` accounts are
/// only acquired by the pool once every `Active` account in the same
/// provider has been cooled down or otherwise rejected. The CLI exposes
/// this via `tokn-router account switch` (see `gateway-cli`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AccountTier {
  #[default]
  Active,
  Fallback,
}

/// Effective activation state surfaced by the CLI. Derived from
/// `(enabled, tier)`; not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountState {
  Active,
  Fallback,
  Disabled,
}

impl AccountConfig {
  /// Compute the effective activation state for display / pool decisions.
  pub fn state(&self) -> AccountState {
    if !self.enabled {
      AccountState::Disabled
    } else {
      match self.tier {
        AccountTier::Active => AccountState::Active,
        AccountTier::Fallback => AccountState::Fallback,
      }
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AuthType {
  Bearer,
  XApiKey,
  Header(String),
}

fn default_provider() -> String {
  crate::provider::ID_GITHUB_COPILOT.to_string()
}

fn default_true() -> bool {
  true
}
