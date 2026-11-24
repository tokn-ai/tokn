use crate::agent::AgentKind;
use crate::migration::{annotate_imported_account, EditKind, MigrationPlan, PlannedEdit};
use anyhow::{Context, Result};
use serde_json::Value;
use std::path::PathBuf;
use tokn_config::{Account, AuthType};
use tokn_core::account::AccountTier;
use tokn_core::provider::ID_CODEX;
use tokn_core::util::secret::Secret;

pub(crate) fn plan(timestamp: &str, profile: &str, target_base_url: &str, home: PathBuf) -> Result<MigrationPlan> {
  let auth_path = home.join(".codex/auth.json");
  let config_path = home.join(".codex/config.toml");

  let mut imported_accounts = Vec::new();
  let mut edits = Vec::new();

  if auth_path.exists() {
    let raw = std::fs::read_to_string(&auth_path).with_context(|| format!("reading {}", auth_path.display()))?;
    let mut json: Value = serde_json::from_str(&raw).with_context(|| format!("parsing {}", auth_path.display()))?;
    if let Some(account) = account_from_auth_json(&json) {
      imported_accounts.push(annotate_imported_account(
        account,
        AgentKind::CodexCli,
        &auth_path,
        "auth.tokens",
        timestamp,
      ));
    }
    if let Some(obj) = json.as_object_mut() {
      obj.insert("auth_mode".into(), Value::String("api_key".into()));
      obj.insert("OPENAI_API_KEY".into(), Value::String("tokn-router".into()));
    }
    edits.push(PlannedEdit {
      path: auth_path,
      kind: EditKind::Json(json),
    });
  }

  let doc = if config_path.exists() {
    std::fs::read_to_string(&config_path)
      .with_context(|| format!("reading {}", config_path.display()))?
      .parse::<toml_edit::DocumentMut>()
      .with_context(|| format!("parsing {}", config_path.display()))?
  } else {
    toml_edit::DocumentMut::new()
  };
  let mut doc = doc;
  rewrite_config(&mut doc, target_base_url);
  edits.push(PlannedEdit {
    path: config_path,
    kind: EditKind::Toml(doc),
  });

  Ok(MigrationPlan::new(
    AgentKind::CodexCli,
    timestamp,
    profile,
    target_base_url,
    imported_accounts,
    edits,
  ))
}

fn rewrite_config(doc: &mut toml_edit::DocumentMut, target_base_url: &str) {
  doc["model_provider"] = toml_edit::value("tokn-router");
  let providers = doc["model_providers"].or_insert(toml_edit::table());
  let provider = providers["tokn-router"].or_insert(toml_edit::table());
  provider["name"] = toml_edit::value("tokn-router");
  provider["base_url"] = toml_edit::value(target_base_url);
  provider["env_key"] = toml_edit::value("OPENAI_API_KEY");
  provider["wire_api"] = toml_edit::value("responses");
}

fn account_from_auth_json(json: &Value) -> Option<Account> {
  let tokens = json.get("tokens")?;
  let refresh = tokens.get("refresh_token").and_then(Value::as_str)?.trim();
  if refresh.is_empty() {
    return None;
  }
  let access = tokens
    .get("access_token")
    .and_then(Value::as_str)
    .filter(|s| !s.trim().is_empty());
  let account_id = tokens
    .get("account_id")
    .and_then(Value::as_str)
    .filter(|s| !s.trim().is_empty());
  let username = tokens
    .get("email")
    .and_then(Value::as_str)
    .filter(|s| !s.trim().is_empty());
  Some(Account {
    id: "codex-cli-codex".into(),
    provider: ID_CODEX.into(),
    enabled: true,
    tier: AccountTier::Active,
    tags: vec!["agent-migrated".into(), "codex-cli".into()],
    label: Some("Codex CLI migration".into()),
    base_url: Some(tokn_provider_openai::codex::CODEX_BASE_URL.into()),
    headers: Default::default(),
    auth_type: Some(AuthType::Bearer),
    username: username.map(str::to_string),
    api_key: None,
    api_key_expires_at: None,
    access_token: access.map(|value| Secret::new(value.to_string())),
    access_token_expires_at: None,
    id_token: tokens
      .get("id_token")
      .and_then(Value::as_str)
      .filter(|s| !s.trim().is_empty())
      .map(|value| Secret::new(value.to_string())),
    refresh_token: Some(Secret::new(refresh.to_string())),
    provider_account_id: account_id.map(str::to_string),
    extra: Default::default(),
    refresh_url: Some(tokn_provider_openai::CODEX_OAUTH_TOKEN_URL.into()),
    last_refresh: None,
    settings: toml::Table::new(),
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn account_imports_refresh_and_account_id() {
    let account = account_from_auth_json(&serde_json::json!({
      "tokens": {
        "refresh_token": "rt",
        "access_token": "at",
        "id_token": "id",
        "account_id": "acc"
      }
    }))
    .unwrap();
    assert_eq!(account.id, "codex-cli-codex");
    assert_eq!(account.provider, "codex");
    assert_eq!(account.provider_account_id.as_deref(), Some("acc"));
    assert_eq!(account.refresh_token.unwrap().expose(), "rt");
  }

  #[test]
  fn config_rewrite_preserves_existing_keys() {
    let mut doc: toml_edit::DocumentMut = "model = \"gpt-5\"\n".parse().unwrap();
    rewrite_config(&mut doc, "http://127.0.0.1:4141/codex/v1");
    assert_eq!(doc["model"].as_str(), Some("gpt-5"));
    assert_eq!(doc["model_provider"].as_str(), Some("tokn-router"));
    assert_eq!(
      doc["model_providers"]["tokn-router"]["base_url"].as_str(),
      Some("http://127.0.0.1:4141/codex/v1")
    );
  }

  #[test]
  fn account_import_ignores_missing_refresh_token() {
    assert!(account_from_auth_json(&serde_json::json!({"tokens": {}})).is_none());
    assert!(account_from_auth_json(&serde_json::json!({"tokens": {"refresh_token": "  "}})).is_none());
  }

  #[test]
  fn plan_reads_existing_auth_and_config() {
    let dir = tempfile::tempdir().unwrap();
    let auth_path = dir.path().join(".codex/auth.json");
    let config_path = dir.path().join(".codex/config.toml");
    std::fs::create_dir_all(auth_path.parent().unwrap()).unwrap();
    std::fs::write(
      &auth_path,
      serde_json::json!({
        "tokens": {
          "refresh_token": "rt",
          "access_token": "at",
          "email": "user@example.test"
        }
      })
      .to_string(),
    )
    .unwrap();
    std::fs::write(&config_path, "model = \"gpt-5\"\n").unwrap();

    let plan = plan(
      "20260604T153012Z",
      "codex",
      "http://127.0.0.1:4141/codex/v1",
      dir.path().to_path_buf(),
    )
    .unwrap();

    assert_eq!(plan.agent, AgentKind::CodexCli);
    assert_eq!(plan.imported_accounts.len(), 1);
    assert_eq!(plan.imported_accounts[0].username.as_deref(), Some("user@example.test"));
    assert_eq!(plan.edits.len(), 2);
    assert!(plan.edits.iter().any(|edit| edit.path == auth_path));
    assert!(plan.edits.iter().any(|edit| edit.path == config_path));
  }
}
