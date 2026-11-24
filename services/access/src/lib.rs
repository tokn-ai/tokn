//! Inbound gateway API-key authentication and provider authorization.
//!
//! This is intentionally separate from `tokn-auth`, which owns credentials
//! used by the gateway to authenticate *to upstream providers*.

use anyhow::{bail, Context, Result};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use subtle::ConstantTimeEq;
use time::OffsetDateTime;
use tokn_persistence::{AccessDb, NewApiKeyRecord};
use uuid::Uuid;

const KEY_PREFIX: &str = "tokn";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderAccess {
  All,
  Only(BTreeSet<String>),
}

impl ProviderAccess {
  pub fn from_provider_ids(provider_ids: Vec<String>) -> Result<Self> {
    if provider_ids.is_empty() {
      return Ok(Self::All);
    }
    if provider_ids.iter().any(|provider| provider.trim() == "*") {
      if provider_ids.len() != 1 {
        bail!("provider `*` cannot be combined with specific provider ids");
      }
      return Ok(Self::All);
    }
    let providers = provider_ids
      .into_iter()
      .map(|provider| provider.trim().to_string())
      .collect::<BTreeSet<_>>();
    if providers.iter().any(String::is_empty) {
      bail!("provider ids must not be empty");
    }
    Ok(Self::Only(providers))
  }

  pub fn allows(&self, provider_id: &str) -> bool {
    match self {
      Self::All => true,
      Self::Only(providers) => providers.contains(provider_id),
    }
  }

  pub fn provider_ids(&self) -> Option<&BTreeSet<String>> {
    match self {
      Self::All => None,
      Self::Only(providers) => Some(providers),
    }
  }

  pub fn display(&self) -> String {
    match self {
      Self::All => "*".to_string(),
      Self::Only(providers) => providers.iter().cloned().collect::<Vec<_>>().join(","),
    }
  }

  fn to_json(&self) -> Result<String> {
    let providers = match self {
      Self::All => vec!["*".to_string()],
      Self::Only(providers) => providers.iter().cloned().collect(),
    };
    Ok(serde_json::to_string(&providers)?)
  }

  fn from_json(value: &str) -> Result<Self> {
    Self::from_provider_ids(serde_json::from_str(value).context("decode allowed providers")?)
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessContext {
  pub key_id: Option<String>,
  pub key_name: Option<String>,
  pub providers: ProviderAccess,
}

impl AccessContext {
  pub fn unrestricted() -> Self {
    Self {
      key_id: None,
      key_name: None,
      providers: ProviderAccess::All,
    }
  }
}

#[derive(Clone, PartialEq, Eq)]
pub struct CreatedApiKey {
  pub id: String,
  pub name: String,
  pub token: String,
  pub providers: ProviderAccess,
  pub created_at: i64,
}

impl std::fmt::Debug for CreatedApiKey {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("CreatedApiKey")
      .field("id", &self.id)
      .field("name", &self.name)
      .field("token", &"<redacted>")
      .field("providers", &self.providers)
      .field("created_at", &self.created_at)
      .finish()
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKeySummary {
  pub id: String,
  pub name: String,
  pub providers: ProviderAccess,
  pub created_at: i64,
  pub revoked_at: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthenticationError {
  Missing,
  Invalid,
  Revoked,
}

/// SQLite-backed store shared by CLI management and HTTP verification.
///
/// Verification is deliberately synchronous and performs one indexed local
/// lookup. The connection mutex also allows a running server to observe keys
/// created or revoked by a separate CLI process without a restart.
pub struct AccessStore {
  path: PathBuf,
  database: Mutex<AccessDb>,
}

impl std::fmt::Debug for AccessStore {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.debug_struct("AccessStore").field("path", &self.path).finish()
  }
}

impl AccessStore {
  pub fn disabled() -> Self {
    let database = AccessDb::open_in_memory().expect("open in-memory access store");
    Self {
      path: PathBuf::from(":memory:"),
      database: Mutex::new(database),
    }
  }

  pub fn open(path: impl AsRef<Path>) -> Result<Self> {
    let path = path.as_ref().to_path_buf();
    let database = AccessDb::open(&path).with_context(|| format!("open {}", path.display()))?;
    Ok(Self {
      path,
      database: Mutex::new(database),
    })
  }

  pub fn open_default() -> Result<Self> {
    Self::open(default_access_path()?)
  }

  pub fn path(&self) -> &Path {
    &self.path
  }

  pub fn create_key(&self, name: impl Into<String>, provider_ids: Vec<String>) -> Result<CreatedApiKey> {
    let name = name.into();
    if name.trim().is_empty() {
      bail!("API key name must not be empty");
    }
    let providers = ProviderAccess::from_provider_ids(provider_ids)?;
    let created_at = OffsetDateTime::now_utc().unix_timestamp();

    for _ in 0..4 {
      let id = Uuid::new_v4().simple().to_string()[..16].to_string();
      let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
      let token = format!("{KEY_PREFIX}_{id}_{secret}");
      let hash = hash_secret(secret.as_bytes());
      let allowed_providers = providers.to_json()?;
      let inserted = self.database.lock().insert_key(&NewApiKeyRecord {
        id: &id,
        name: &name,
        secret_hash: hash.as_slice(),
        allowed_providers: &allowed_providers,
        created_at,
      })?;
      if inserted {
        return Ok(CreatedApiKey {
          id,
          name,
          token,
          providers,
          created_at,
        });
      }
    }
    bail!("could not allocate a unique API key id")
  }

  pub fn list_keys(&self) -> Result<Vec<ApiKeySummary>> {
    self
      .database
      .lock()
      .list_keys()?
      .into_iter()
      .map(|record| {
        Ok(ApiKeySummary {
          id: record.id,
          name: record.name,
          providers: ProviderAccess::from_json(&record.allowed_providers)?,
          created_at: record.created_at,
          revoked_at: record.revoked_at,
        })
      })
      .collect()
  }

  pub fn revoke_key(&self, id: &str) -> Result<bool> {
    let revoked_at = OffsetDateTime::now_utc().unix_timestamp();
    Ok(self.database.lock().revoke_key(id, revoked_at)?)
  }

  pub fn authenticate(&self, token: Option<&str>) -> Result<AccessContext, AuthenticationError> {
    let token = token.ok_or(AuthenticationError::Missing)?;
    let (id, secret) = parse_token(token).ok_or(AuthenticationError::Invalid)?;
    let record = self
      .database
      .lock()
      .find_key(id)
      .map_err(|_| AuthenticationError::Invalid)?
      .ok_or(AuthenticationError::Invalid)?;
    if record.revoked_at.is_some() {
      return Err(AuthenticationError::Revoked);
    }
    let actual_hash = hash_secret(secret.as_bytes());
    if record.secret_hash.len() != actual_hash.len()
      || !bool::from(record.secret_hash.as_slice().ct_eq(actual_hash.as_slice()))
    {
      return Err(AuthenticationError::Invalid);
    }
    let providers = ProviderAccess::from_json(&record.allowed_providers).map_err(|_| AuthenticationError::Invalid)?;
    Ok(AccessContext {
      key_id: Some(id.to_string()),
      key_name: Some(record.name),
      providers,
    })
  }
}

pub fn default_access_path() -> Result<PathBuf> {
  tokn_core::util::paths::router_home()
    .map(|home| home.join("access.db"))
    .context("could not determine the tokn router home")
}

fn parse_token(token: &str) -> Option<(&str, &str)> {
  let mut parts = token.trim().split('_');
  match (parts.next(), parts.next(), parts.next(), parts.next()) {
    (Some(KEY_PREFIX), Some(id), Some(secret), None) if id.len() == 16 && secret.len() == 64 => Some((id, secret)),
    _ => None,
  }
}

fn hash_secret(secret: &[u8]) -> [u8; 32] {
  Sha256::digest(secret).into()
}

#[cfg(test)]
mod tests {
  use super::*;

  fn store() -> (tempfile::TempDir, AccessStore) {
    let temp = tempfile::tempdir().unwrap();
    let store = AccessStore::open(temp.path().join("access.db")).unwrap();
    (temp, store)
  }

  #[test]
  fn empty_provider_list_defaults_to_all() {
    assert_eq!(
      ProviderAccess::from_provider_ids(Vec::new()).unwrap(),
      ProviderAccess::All
    );
  }

  #[test]
  fn trims_provider_ids_before_wildcard_detection() {
    assert_eq!(
      ProviderAccess::from_provider_ids(vec!["  *  ".into()]).unwrap(),
      ProviderAccess::All
    );
    assert!(ProviderAccess::from_provider_ids(vec![" * ".into(), "openai".into()]).is_err());
  }

  #[test]
  fn created_key_debug_output_redacts_the_token() {
    let (_temp, store) = store();
    let created = store.create_key("client", Vec::new()).unwrap();
    let output = format!("{created:?}");

    assert!(output.contains("<redacted>"));
    assert!(!output.contains(&created.token));
  }

  #[test]
  fn key_round_trip_and_revocation() {
    let (_temp, store) = store();
    let created = store
      .create_key("client", vec!["openai".into(), "github-copilot".into()])
      .unwrap();

    let context = store.authenticate(Some(&created.token)).unwrap();
    assert_eq!(context.key_id.as_deref(), Some(created.id.as_str()));
    assert!(context.providers.allows("openai"));
    assert!(!context.providers.allows("deepseek"));

    assert!(store.revoke_key(&created.id).unwrap());
    assert_eq!(
      store.authenticate(Some(&created.token)),
      Err(AuthenticationError::Revoked)
    );
  }

  #[test]
  fn wildcard_is_the_default_and_matches_future_providers() {
    let (_temp, store) = store();
    let created = store.create_key("client", Vec::new()).unwrap();
    let context = store.authenticate(Some(&created.token)).unwrap();
    assert!(context.providers.allows("provider-added-later"));
    assert_eq!(created.providers.display(), "*");
  }

  #[test]
  fn malformed_and_unknown_keys_are_rejected() {
    let (_temp, store) = store();
    assert_eq!(store.authenticate(None), Err(AuthenticationError::Missing));
    assert_eq!(store.authenticate(Some("not-a-key")), Err(AuthenticationError::Invalid));
  }
}
