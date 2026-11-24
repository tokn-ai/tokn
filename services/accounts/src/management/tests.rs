use super::*;
use tokn_auth::AuthSource;

fn fixture() -> (tempfile::TempDir, std::path::PathBuf, ResolvedProviderAuth) {
  let dir = tempfile::tempdir().unwrap();
  let path = dir.path().join("auth.yaml");
  (dir, path, ResolvedProviderAuth::legacy("openai").unwrap())
}

#[test]
fn edits_preserve_shards_and_unrelated_credentials() {
  let (_dir, path, provider) = fixture();
  let mut store = AuthStore::load(Some(&path), None).unwrap();
  let mut account = empty_account(&provider, "from-agent".into());
  account.api_key = Some(Secret::new("test-secret".into()));
  store.upsert_in_shard("agent", account).unwrap();
  store.upsert_in_main(empty_account(&provider, "other".into())).unwrap();
  store.save().unwrap();
  let root_before = std::fs::read(&path).unwrap();
  edit(
    Some(&path),
    AccountEdit::Update {
      id: "from-agent".into(),
      label: Some(" Work ".into()),
      activation: Activation::Fallback,
    },
  )
  .unwrap();
  let store = AuthStore::load(Some(&path), None).unwrap();
  assert_eq!(std::fs::read(&path).unwrap(), root_before);
  assert_eq!(
    store.account_source("from-agent"),
    Some(AuthSource::Shard("agent".into()))
  );
  let account = store
    .accounts
    .iter()
    .find(|account| account.id == "from-agent")
    .unwrap();
  assert_eq!(account.tier, AccountTier::Fallback);
  assert_eq!(account.label.as_deref(), Some("Work"));
  assert_eq!(account.api_key.as_ref().unwrap().expose(), "test-secret");
  edit(
    Some(&path),
    AccountEdit::Remove {
      id: "from-agent".into(),
    },
  )
  .unwrap();
  assert_eq!(list(Some(&path)).unwrap().len(), 1);
  assert_eq!(std::fs::read(&path).unwrap(), root_before);
}

#[test]
fn duplicate_insert_never_replaces_credentials() {
  let (_dir, path, provider) = fixture();
  let mut account = empty_account(&provider, "same".into());
  account.api_key = Some(Secret::new("original-secret".into()));
  insert(Some(&path), account).unwrap();
  assert!(validate_new_id(Some(&path), "same").is_err());
  assert!(insert(Some(&path), empty_account(&provider, "same".into())).is_err());
  assert_eq!(
    AuthStore::load(Some(&path), None).unwrap().accounts[0]
      .api_key
      .as_ref()
      .unwrap()
      .expose(),
    "original-secret"
  );
  for invalid in ["", "  ", " space", "new\nline"] {
    assert!(validate_new_id(Some(&path), invalid).is_err());
  }
}

#[test]
fn serialized_summary_excludes_all_secrets() {
  let (_dir, _path, provider) = fixture();
  let mut account = empty_account(&provider, "test".into());
  account.api_key = Some(Secret::new("api-secret".into()));
  account.refresh_token = Some(Secret::new("refresh-secret".into()));
  account.access_token = Some(Secret::new("access-secret".into()));
  account.id_token = Some(Secret::new("identity-secret".into()));
  account.headers.insert("x-secret".into(), "header-secret".into());
  let json = serde_json::to_string(&summary(&account)).unwrap();
  assert!(!json.contains("secret"));
  assert!(!json.contains("headers"));
  assert!(json.contains("can_refresh"));
}

#[test]
fn disabled_state_retains_tier_and_reactivation_sets_requested_tier() {
  let (_dir, path, provider) = fixture();
  insert(Some(&path), empty_account(&provider, "test".into())).unwrap();
  for activation in [Activation::Fallback, Activation::Disabled, Activation::Active] {
    edit(
      Some(&path),
      AccountEdit::Update {
        id: "test".into(),
        label: None,
        activation,
      },
    )
    .unwrap();
    let account = AuthStore::load(Some(&path), None).unwrap().accounts.remove(0);
    match activation {
      Activation::Fallback => {
        assert!(account.enabled);
        assert_eq!(account.tier, AccountTier::Fallback);
      }
      Activation::Disabled => {
        assert!(!account.enabled);
        assert_eq!(account.tier, AccountTier::Fallback);
      }
      Activation::Active => {
        assert!(account.enabled);
        assert_eq!(account.tier, AccountTier::Active);
      }
    }
  }
}

#[test]
fn busy_store_rejects_changes_without_overwriting() {
  let (_dir, path, provider) = fixture();
  insert(Some(&path), empty_account(&provider, "test".into())).unwrap();
  let _lock = AuthStoreLock::acquire(Some(&path)).unwrap();
  assert!(edit(Some(&path), AccountEdit::Remove { id: "test".into() }).is_err());
  assert_eq!(list(Some(&path)).unwrap().len(), 1);
}

#[tokio::test]
async fn custom_provider_import_verifies_at_configured_endpoint() {
  use tokio::io::{AsyncReadExt, AsyncWriteExt};
  let (dir, _path, _) = fixture();
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let address = listener.local_addr().unwrap();
  let server = tokio::spawn(async move {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut buffer = vec![0; 4096];
    let length = stream.read(&mut buffer).await.unwrap();
    let request = String::from_utf8_lossy(&buffer[..length]).to_lowercase();
    assert!(request.starts_with("get /v1/models "));
    assert!(request.contains("authorization: bearer test-credential"));
    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"data\":[]}").await.unwrap();
  });
  let config_path = dir.path().join("config.toml");
  std::fs::write(
    &config_path,
    format!(
      r#"schema_version = 2
[listeners.local]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"
[providers.work]
driver = "openai"
base_url = "http://{address}/v1"
"#
    ),
  )
  .unwrap();
  let context = ConfigContext::load(Some(&config_path)).unwrap();
  let provider = context.resolve_provider("work").unwrap();
  let client = reqwest::Client::builder().no_proxy().build().unwrap();
  let account = import_account(
    &client,
    &provider,
    "work-key".into(),
    CredentialSource::String {
      value: "test-credential".into(),
      flavor: CredentialFlavor::ApiKey,
    },
  )
  .await
  .unwrap();
  assert_eq!(account.provider, "work");
  assert!(account.base_url.is_none());
  server.await.unwrap();
}

struct RotatingAuth {
  path: std::path::PathBuf,
  fail: bool,
}
#[async_trait::async_trait]
impl tokn_auth::ProviderAuth for RotatingAuth {
  fn id(&self) -> &'static str {
    "fixture"
  }
  async fn refresh_credential(
    &self,
    _: &reqwest::Client,
    _: &AccountConfig,
  ) -> tokn_auth::Result<tokn_auth::RefreshOutcome> {
    Ok(tokn_auth::RefreshOutcome::Refreshed {
      access_token: "new-access".into(),
      expires_at: now() + 3600,
      refresh_token: Some("new-refresh".into()),
      id_token: None,
      username: None,
      provider_account_id: None,
    })
  }
  async fn verify_credential(
    &self,
    _: &reqwest::Client,
    _: &AccountConfig,
  ) -> tokn_auth::Result<tokn_auth::VerifyOutcome> {
    panic!("OAuth verification must not trigger another unpersisted exchange");
  }
  fn quota_timeout(&self) -> Duration {
    Duration::from_millis(10)
  }
  async fn probe_quota(
    &self,
    _: &reqwest::Client,
    account: &AccountConfig,
  ) -> tokn_auth::Result<tokn_auth::QuotaSnapshot> {
    assert_eq!(account.access_token.as_ref().unwrap().expose(), "new-access");
    let saved = AuthStore::load(Some(&self.path), None).unwrap();
    assert_eq!(
      saved.accounts[0].refresh_token.as_ref().unwrap().expose(),
      "new-refresh"
    );
    if self.fail {
      return Err(tokn_auth::AuthError::Other("secret-upstream-body".into()));
    }
    tokio::time::sleep(Duration::from_secs(10)).await;
    unreachable!()
  }
}

#[tokio::test]
async fn rotation_is_persisted_before_quota_failure_or_timeout() {
  for fail in [true, false] {
    let (_dir, path, provider) = fixture();
    let mut account = empty_account(&provider, "rotating".into());
    account.refresh_token = Some(Secret::new("old-refresh".into()));
    insert(Some(&path), account.clone()).unwrap();
    let lock = AuthStoreLock::acquire(Some(&path)).unwrap();
    let store = AuthStore::load_locked(&lock).unwrap();
    let auth = RotatingAuth {
      path: path.clone(),
      fail,
    };
    let result = probe_locked(store, lock, 0, &auth, reqwest::Client::new(), account, true)
      .await
      .unwrap();
    assert_eq!(result.authentication, "verified");
    assert_eq!(result.quota_status, "unavailable");
    assert!(!serde_json::to_string(&result).unwrap().contains("secret-upstream-body"));
    assert_eq!(
      AuthStore::load(Some(&path), None).unwrap().accounts[0]
        .refresh_token
        .as_ref()
        .unwrap()
        .expose(),
      "new-refresh"
    );
  }
}

#[test]
fn inferred_ids_use_identity_and_resolve_collisions_without_overwriting() {
  let (_dir, path, provider) = fixture();
  let mut account = empty_account(&provider, String::new());
  account.username = Some(" person@example.test ".into());
  insert_generated(Some(&path), account.clone()).unwrap();
  insert_generated(Some(&path), account).unwrap();
  let ids: Vec<_> = list(Some(&path))
    .unwrap()
    .into_iter()
    .map(|account| account.id)
    .collect();
  assert_eq!(ids, ["person@example.test", "person@example.test-2"]);
}

#[test]
fn inferred_ids_fall_back_to_provider_identity_then_provider() {
  let (_dir, path, provider) = fixture();
  let mut account = empty_account(&provider, String::new());
  account.username = Some("\n".into());
  account.provider_account_id = Some("upstream-account".into());
  insert_generated(Some(&path), account).unwrap();
  insert_generated(Some(&path), empty_account(&provider, String::new())).unwrap();
  let ids: Vec<_> = list(Some(&path))
    .unwrap()
    .into_iter()
    .map(|account| account.id)
    .collect();
  assert_eq!(ids, ["openai", "upstream-account"]);
}
