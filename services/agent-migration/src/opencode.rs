use crate::agent::AgentKind;
use crate::migration::{annotate_imported_account, EditKind, MigrationPlan, PlannedEdit};
use anyhow::{Context, Result};
use serde_json::Value;
use std::path::PathBuf;
use tokn_config::{Account, AuthType};
use tokn_core::account::AccountTier;
use tokn_core::util::secret::Secret;

pub(crate) fn plan(timestamp: &str, profile: &str, target_base_url: &str, home: PathBuf) -> Result<MigrationPlan> {
  let auth_path = home.join(".local/share/opencode/auth.json");
  let config_path = home.join(".config/opencode/opencode.json");
  let imported_accounts = if auth_path.exists() {
    let raw = std::fs::read_to_string(&auth_path).with_context(|| format!("reading {}", auth_path.display()))?;
    let json = serde_json::from_str(&raw).with_context(|| format!("parsing {}", auth_path.display()))?;
    accounts_from_auth_json(&json, &auth_path, timestamp)
  } else {
    Vec::new()
  };
  let mut json = if config_path.exists() {
    let raw = std::fs::read_to_string(&config_path).with_context(|| format!("reading {}", config_path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parsing {}", config_path.display()))?
  } else {
    Value::Object(serde_json::Map::new())
  };
  rewrite_config(&mut json, target_base_url);

  Ok(MigrationPlan::new(
    AgentKind::Opencode,
    timestamp,
    profile,
    target_base_url,
    imported_accounts,
    vec![PlannedEdit {
      path: config_path,
      kind: EditKind::Json(json),
    }],
  ))
}

fn rewrite_config(json: &mut Value, target_base_url: &str) {
  if !json.is_object() {
    *json = Value::Object(serde_json::Map::new());
  }
  let obj = json.as_object_mut().expect("object ensured");
  obj
    .entry("$schema")
    .or_insert_with(|| Value::String("https://opencode.ai/config.json".into()));
  let model_ids = ["model", "small_model"]
    .into_iter()
    .filter_map(|key| rewrite_model_provider(obj, key))
    .collect::<Vec<_>>();
  obj.remove("providers");

  let provider = obj
    .entry("provider")
    .or_insert_with(|| Value::Object(serde_json::Map::new()));
  if !provider.is_object() {
    *provider = Value::Object(serde_json::Map::new());
  }
  let mut router_provider = serde_json::json!({
    "name": "tokn-router",
    "npm": "@ai-sdk/openai-compatible",
    "options": {
      "baseURL": target_base_url,
      "apiKey": "tokn-router"
    }
  });
  if !model_ids.is_empty() {
    router_provider["models"] = model_map(&model_ids);
  }
  provider
    .as_object_mut()
    .expect("object ensured")
    .insert("tokn-router".into(), router_provider);
}

fn rewrite_model_provider(obj: &mut serde_json::Map<String, Value>, key: &str) -> Option<String> {
  let model = obj.get(key).and_then(Value::as_str).map(str::to_string)?;
  let (provider, model_id) = model.split_once('/')?;
  if is_routeable_opencode_provider(provider) && !model_id.trim().is_empty() {
    *obj.get_mut(key).expect("model exists") = Value::String(format!("tokn-router/{model_id}"));
    return Some(model_id.to_string());
  }
  None
}

fn is_routeable_opencode_provider(provider: &str) -> bool {
  matches!(
    provider,
    tokn_core::provider::ID_OPENAI | tokn_core::provider::ID_GITHUB_COPILOT
  )
}

fn model_map(model_ids: &[String]) -> Value {
  let mut models = serde_json::Map::new();
  for model_id in model_ids {
    models.insert(model_id.clone(), serde_json::json!({"name": model_id}));
  }
  Value::Object(models)
}

fn accounts_from_auth_json(json: &Value, auth_path: &std::path::Path, timestamp: &str) -> Vec<Account> {
  let Some(providers) = json.as_object() else {
    return Vec::new();
  };

  providers
    .iter()
    .filter_map(|(provider, auth)| {
      account_from_provider_auth(provider, auth).map(|account| {
        annotate_imported_account(
          account,
          AgentKind::Opencode,
          auth_path,
          &format!("auth.{provider}"),
          timestamp,
        )
      })
    })
    .collect()
}

fn account_from_provider_auth(provider: &str, auth: &Value) -> Option<Account> {
  let auth = auth.as_object()?;
  match (provider, auth.get("type").and_then(Value::as_str)?) {
    (tokn_core::provider::ID_OPENAI, "api") => {
      let api_key = auth.get("key").and_then(Value::as_str)?.trim();
      if api_key.is_empty() {
        return None;
      }
      Some(openai_account_from_key(api_key))
    }
    (tokn_core::provider::ID_OPENAI, "oauth") => oauth_account_from_auth(
      "opencode-codex",
      tokn_core::provider::ID_CODEX,
      "opencode Codex migration",
      Some(tokn_provider_openai::codex::CODEX_BASE_URL),
      Some(tokn_provider_openai::CODEX_OAUTH_TOKEN_URL),
      auth,
      auth
        .get("accountId")
        .or_else(|| auth.get("account_id"))
        .and_then(Value::as_str),
    ),
    (tokn_core::provider::ID_GITHUB_COPILOT, "oauth") => oauth_account_from_auth(
      "opencode-github-copilot",
      tokn_core::provider::ID_GITHUB_COPILOT,
      "opencode GitHub Copilot migration",
      Some(tokn_provider_copilot::COPILOT_BASE_URL),
      Some(tokn_provider_copilot::COPILOT_TOKEN_EXCHANGE_URL),
      auth,
      None,
    ),
    _ => None,
  }
}

fn openai_account_from_key(api_key: &str) -> Account {
  Account {
    id: "opencode-openai".into(),
    provider: tokn_core::provider::ID_OPENAI.into(),
    enabled: true,
    tier: AccountTier::Active,
    tags: vec!["agent-migrated".into(), "opencode".into()],
    label: Some("opencode migration".into()),
    base_url: Some(tokn_provider_openai::openai::OPENAI_BASE_URL.into()),
    headers: Default::default(),
    auth_type: Some(AuthType::Bearer),
    username: None,
    api_key: Some(Secret::new(api_key.to_string())),
    api_key_expires_at: None,
    access_token: None,
    access_token_expires_at: None,
    id_token: None,
    refresh_token: None,
    provider_account_id: None,
    extra: Default::default(),
    refresh_url: None,
    last_refresh: None,
    settings: toml::Table::new(),
  }
}

fn oauth_account_from_auth(
  id: &str,
  provider: &str,
  label: &str,
  base_url: Option<&str>,
  refresh_url: Option<&str>,
  auth: &serde_json::Map<String, Value>,
  provider_account_id: Option<&str>,
) -> Option<Account> {
  let refresh = auth.get("refresh").and_then(Value::as_str)?.trim();
  if refresh.is_empty() {
    return None;
  }
  let access = auth.get("access").and_then(Value::as_str).and_then(non_empty_string);
  let expires = auth.get("expires").and_then(expires_at);
  Some(Account {
    id: id.into(),
    provider: provider.into(),
    enabled: true,
    tier: AccountTier::Active,
    tags: vec!["agent-migrated".into(), "opencode".into()],
    label: Some(label.into()),
    base_url: base_url.map(str::to_string),
    headers: Default::default(),
    auth_type: Some(AuthType::Bearer),
    username: None,
    api_key: None,
    api_key_expires_at: None,
    access_token: access.map(Secret::new),
    access_token_expires_at: expires,
    id_token: None,
    refresh_token: Some(Secret::new(refresh.to_string())),
    provider_account_id: provider_account_id.and_then(non_empty_string),
    extra: Default::default(),
    refresh_url: refresh_url.map(str::to_string),
    last_refresh: None,
    settings: toml::Table::new(),
  })
}

fn non_empty_string(value: &str) -> Option<String> {
  let value = value.trim();
  (!value.is_empty()).then(|| value.to_string())
}

fn expires_at(value: &Value) -> Option<i64> {
  match value {
    Value::Number(n) => n
      .as_i64()
      .or_else(|| n.as_u64().and_then(|value| i64::try_from(value).ok())),
    Value::String(s) => s.trim().parse().ok(),
    _ => None,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rewrite_preserves_mcp() {
    let mut json = serde_json::json!({
      "mcp": {"x": true},
      "model": "openai/gpt-5",
      "small_model": "github-copilot/gpt-5-mini",
      "providers": {
        "old": {}
      }
    });
    rewrite_config(&mut json, "http://127.0.0.1:4141/opencode/v1");
    assert_eq!(json["mcp"]["x"], true);
    assert_eq!(json["model"], "tokn-router/gpt-5");
    assert_eq!(json["small_model"], "tokn-router/gpt-5-mini");
    assert_eq!(json["providers"], Value::Null);
    assert_eq!(json["$schema"], "https://opencode.ai/config.json");
    assert_eq!(json["provider"]["tokn-router"]["models"]["gpt-5"]["name"], "gpt-5");
    assert_eq!(
      json["provider"]["tokn-router"]["models"]["gpt-5-mini"]["name"],
      "gpt-5-mini"
    );
    assert_eq!(
      json["provider"]["tokn-router"]["options"]["baseURL"],
      "http://127.0.0.1:4141/opencode/v1"
    );
  }

  #[test]
  fn accounts_from_auth_json_imports_openai_api_key() {
    let json = serde_json::json!({
      "openai": {
        "type": "api",
        "key": "sk-test"
      }
    });

    let auth_path = std::path::PathBuf::from("/tmp/opencode-auth.json");
    let accounts = accounts_from_auth_json(&json, &auth_path, "20260604T153012Z");

    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].id, "opencode-openai");
    assert_eq!(accounts[0].provider, tokn_core::provider::ID_OPENAI);
    assert_eq!(accounts[0].api_key.as_ref().unwrap().expose(), "sk-test");
    assert!(accounts[0].tags.iter().any(|tag| tag == "source:opencode"));
    assert_eq!(
      accounts[0]
        .settings
        .get("import")
        .and_then(toml::Value::as_table)
        .and_then(|table| table.get("source_key"))
        .and_then(toml::Value::as_str),
      Some("auth.openai")
    );
  }

  #[test]
  fn accounts_from_auth_json_imports_oauth_records() {
    let json = serde_json::json!({
      "github-copilot": {
        "type": "oauth",
        "access": "at",
        "refresh": "ghu_rt",
        "expires": 0
      },
      "openai": {
        "type": "oauth",
        "access": "codex_at",
        "refresh": "codex_rt",
        "expires": "42",
        "accountId": "acc"
      }
    });

    let accounts = accounts_from_auth_json(
      &json,
      std::path::Path::new("/tmp/opencode-auth.json"),
      "20260604T153012Z",
    );

    assert_eq!(accounts.len(), 2);
    let copilot = accounts
      .iter()
      .find(|account| account.id == "opencode-github-copilot")
      .unwrap();
    assert_eq!(copilot.provider, tokn_core::provider::ID_GITHUB_COPILOT);
    assert_eq!(copilot.refresh_token.as_ref().unwrap().expose(), "ghu_rt");
    assert_eq!(copilot.access_token.as_ref().unwrap().expose(), "at");
    assert_eq!(copilot.access_token_expires_at, Some(0));

    let codex = accounts.iter().find(|account| account.id == "opencode-codex").unwrap();
    assert_eq!(codex.provider, tokn_core::provider::ID_CODEX);
    assert_eq!(codex.refresh_token.as_ref().unwrap().expose(), "codex_rt");
    assert_eq!(codex.access_token.as_ref().unwrap().expose(), "codex_at");
    assert_eq!(codex.access_token_expires_at, Some(42));
    assert_eq!(codex.provider_account_id.as_deref(), Some("acc"));
  }

  #[test]
  fn accounts_from_auth_json_ignores_unsupported_and_incomplete_records() {
    let json = serde_json::json!({
      "anthropic": {
        "type": "oauth",
        "access": "at",
        "refresh": "rt"
      },
      "openai": {
        "type": "oauth",
        "access": "at"
      }
    });

    assert!(accounts_from_auth_json(
      &json,
      std::path::Path::new("/tmp/opencode-auth.json"),
      "20260604T153012Z",
    )
    .is_empty());
  }

  #[test]
  fn plan_reads_existing_auth_imports_account_and_rewrites_config() {
    let dir = tempfile::tempdir().unwrap();
    let auth_path = dir.path().join(".local/share/opencode/auth.json");
    let config_path = dir.path().join(".config/opencode/opencode.json");
    std::fs::create_dir_all(auth_path.parent().unwrap()).unwrap();
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    std::fs::write(
      &auth_path,
      serde_json::json!({
        "openai": {
          "type": "api",
          "key": "sk-test"
        }
      })
      .to_string(),
    )
    .unwrap();
    std::fs::write(
      &config_path,
      serde_json::json!({
        "mcp": {"x": true}
      })
      .to_string(),
    )
    .unwrap();

    let plan = plan(
      "20260604T153012Z",
      "opencode",
      "http://127.0.0.1:4141/opencode/v1",
      dir.path().to_path_buf(),
    )
    .unwrap();

    assert_eq!(plan.agent, AgentKind::Opencode);
    assert_eq!(plan.profile, "opencode");
    assert_eq!(plan.imported_accounts.len(), 1);
    assert_eq!(plan.edits.len(), 1);
    assert_eq!(plan.edits[0].path, config_path);
  }
}
