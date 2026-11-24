use crate::agent::AgentKind;
use crate::manifest::{self, FileBackup, MigrationManifest};
use crate::{codex, opencode};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use time::format_description::well_known::Rfc3339;
use tokn_auth::{default_auth_path, AuthStore};
use tokn_config::{Account, Config, RouteMode};

#[derive(Debug)]
pub struct MigrateRequest {
  pub agent: AgentKind,
  pub profile: String,
  pub gateway_config_path: Option<PathBuf>,
  pub agent_home: Option<PathBuf>,
}

#[derive(Debug)]
pub struct RollbackRequest {
  pub agent: AgentKind,
  pub backup_id: Option<String>,
}

#[derive(Debug)]
pub struct MigrationPlan {
  pub agent: AgentKind,
  pub timestamp: String,
  pub profile: String,
  pub gateway_config_path: PathBuf,
  pub gateway_auth_path: PathBuf,
  pub target_base_url: String,
  pub imported_accounts: Vec<Account>,
  pub edits: Vec<PlannedEdit>,
}

impl MigrationPlan {
  pub(crate) fn new(
    agent: AgentKind,
    timestamp: &str,
    profile: &str,
    target_base_url: &str,
    imported_accounts: Vec<Account>,
    edits: Vec<PlannedEdit>,
  ) -> Self {
    Self {
      agent,
      timestamp: timestamp.to_string(),
      profile: profile.to_string(),
      gateway_config_path: PathBuf::new(),
      gateway_auth_path: PathBuf::new(),
      target_base_url: target_base_url.to_string(),
      imported_accounts,
      edits,
    }
  }
}

#[derive(Debug)]
pub struct PlannedEdit {
  pub path: PathBuf,
  pub(crate) kind: EditKind,
}

#[derive(Debug)]
pub(crate) enum EditKind {
  Json(Value),
  Toml(toml_edit::DocumentMut),
}

#[derive(Debug)]
pub struct ApplyReport {
  pub manifest_path: PathBuf,
  pub files: Vec<FileBackup>,
}

#[derive(Debug)]
pub struct RollbackReport {
  pub manifest_path: PathBuf,
  pub timestamp: String,
  pub actions: Vec<FileAction>,
}

#[derive(Debug)]
pub enum FileAction {
  Removed(PathBuf),
  Restored { original: PathBuf, backup: PathBuf },
}

pub fn plan_migration(request: MigrateRequest) -> Result<MigrationPlan> {
  validate_profile_name(&request.profile)?;
  let (cfg, gateway_config_path) = Config::load(request.gateway_config_path.as_deref())?;
  let gateway_auth_path = default_gateway_auth_path()?;
  let timestamp = timestamp()?;
  let target_base_url = gateway_profile_base_url(&cfg, &request.profile);
  let home = match request.agent_home {
    Some(home) => home,
    None => home_dir()?,
  };
  let mut plan = match request.agent {
    AgentKind::CodexCli => codex::plan(&timestamp, &request.profile, &target_base_url, home)?,
    AgentKind::Opencode => opencode::plan(&timestamp, &request.profile, &target_base_url, home)?,
  };
  plan.gateway_config_path = gateway_config_path;
  plan.gateway_auth_path = gateway_auth_path;
  validate_migration_plan(&plan)?;
  Ok(plan)
}

pub fn apply_migration(plan: MigrationPlan) -> Result<ApplyReport> {
  let manifest_path = manifest::manifest_path(&plan.timestamp, plan.agent)?;
  apply_migration_to_manifest_path(plan, manifest_path)
}

fn apply_migration_to_manifest_path(plan: MigrationPlan, manifest_path: PathBuf) -> Result<ApplyReport> {
  let mut files = Vec::new();
  let imported_account_ids = plan
    .imported_accounts
    .iter()
    .map(|account| account.id.clone())
    .collect::<BTreeSet<_>>();

  let gateway_auth_existed = plan.gateway_auth_path.exists();
  manifest::backup_path_for(&plan.gateway_auth_path, &plan.timestamp, &mut files)?;
  let gateway_config_existed = plan.gateway_config_path.exists();
  manifest::backup_path_for(&plan.gateway_config_path, &plan.timestamp, &mut files)?;
  manifest::mark_created(&mut files, &plan.gateway_auth_path, gateway_auth_existed);
  manifest::mark_created(&mut files, &plan.gateway_config_path, gateway_config_existed);

  for edit in &plan.edits {
    let existed = edit.path.exists();
    manifest::backup_path_for(&edit.path, &plan.timestamp, &mut files)?;
    manifest::mark_created(&mut files, &edit.path, existed);
  }

  let manifest = MigrationManifest {
    version: 1,
    completed: true,
    agent: plan.agent,
    timestamp: plan.timestamp.clone(),
    profile: plan.profile.clone(),
    target_base_url: plan.target_base_url.clone(),
    imported_account_ids: plan
      .imported_accounts
      .iter()
      .map(|account| account.id.clone())
      .collect(),
    files,
  };
  manifest::write_manifest(&manifest_path, &manifest.clone().in_progress())?;

  let mut store = AuthStore::load(Some(&plan.gateway_auth_path), Some(&plan.gateway_config_path))?;
  disable_missing_source_accounts(&mut store, plan.agent, &imported_account_ids);
  for account in &plan.imported_accounts {
    store.upsert(account.clone());
  }
  store.save()?;

  upsert_agent_and_profiles(
    &plan.gateway_config_path,
    &plan.profile,
    plan.agent,
    &plan.imported_accounts,
  )?;

  for edit in &plan.edits {
    write_edit(edit)?;
  }

  let manifest = manifest.complete();
  manifest::write_manifest(&manifest_path, &manifest)?;
  Ok(ApplyReport {
    manifest_path,
    files: manifest.files,
  })
}

pub(crate) fn annotate_imported_account(
  mut account: Account,
  agent: AgentKind,
  source_path: &Path,
  source_key: &str,
  imported_at: &str,
) -> Account {
  let source_tag = format!("source:{}", agent.slug());
  for tag in ["imported", "agent-managed", agent.slug(), source_tag.as_str()] {
    if !account.tags.iter().any(|existing| existing == tag) {
      account.tags.push(tag.to_string());
    }
  }

  let mut import = toml::Table::new();
  import.insert("source_agent".into(), toml::Value::String(agent.slug().into()));
  import.insert(
    "source_path".into(),
    toml::Value::String(source_path.display().to_string()),
  );
  import.insert("source_key".into(), toml::Value::String(source_key.into()));
  import.insert("imported_at".into(), toml::Value::String(imported_at.into()));
  import.insert("last_seen_at".into(), toml::Value::String(imported_at.into()));
  import.insert("sync_managed".into(), toml::Value::Boolean(true));
  import.insert("missing_from_source".into(), toml::Value::Boolean(false));
  account.settings.insert("import".into(), toml::Value::Table(import));
  account.enabled = true;
  account
}

fn disable_missing_source_accounts(store: &mut AuthStore, agent: AgentKind, seen_ids: &BTreeSet<String>) {
  for account in &mut store.accounts {
    if seen_ids.contains(&account.id) || !is_source_managed_account(account, agent) {
      continue;
    }
    account.enabled = false;
    if !account.tags.iter().any(|tag| tag == "source:missing") {
      account.tags.push("source:missing".into());
    }
    if let Some(import) = account.settings.get_mut("import").and_then(toml::Value::as_table_mut) {
      import.insert("missing_from_source".into(), toml::Value::Boolean(true));
    }
  }
}

fn is_source_managed_account(account: &Account, agent: AgentKind) -> bool {
  let source_agent = account
    .settings
    .get("import")
    .and_then(toml::Value::as_table)
    .and_then(|import| import.get("source_agent"))
    .and_then(toml::Value::as_str);
  if source_agent == Some(agent.slug()) {
    return true;
  }
  let source_tag = format!("source:{}", agent.slug());
  account.tags.iter().any(|tag| tag == &source_tag)
}

pub fn rollback_migration(request: RollbackRequest) -> Result<RollbackReport> {
  let manifest_path = manifest::resolve_manifest(request.agent, request.backup_id.as_deref())?;
  let raw = std::fs::read_to_string(&manifest_path).with_context(|| format!("reading {}", manifest_path.display()))?;
  let manifest: MigrationManifest =
    serde_json::from_str(&raw).with_context(|| format!("parsing {}", manifest_path.display()))?;
  if manifest.agent != request.agent {
    bail!(
      "manifest {} is for {}, not {}",
      manifest_path.display(),
      manifest.agent.slug(),
      request.agent.slug()
    );
  }

  let mut actions = Vec::new();
  for file in manifest.files.iter().rev() {
    if file.created_by_migration {
      if file.original.exists() {
        std::fs::remove_file(&file.original).with_context(|| format!("removing {}", file.original.display()))?;
        actions.push(FileAction::Removed(file.original.clone()));
      }
      continue;
    }
    let Some(backup) = &file.backup else {
      continue;
    };
    if let Some(parent) = file.original.parent() {
      std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::copy(backup, &file.original)
      .with_context(|| format!("restoring {} from {}", file.original.display(), backup.display()))?;
    actions.push(FileAction::Restored {
      original: file.original.clone(),
      backup: backup.clone(),
    });
  }

  Ok(RollbackReport {
    manifest_path,
    timestamp: manifest.timestamp,
    actions,
  })
}

fn validate_migration_plan(plan: &MigrationPlan) -> Result<()> {
  if plan.imported_accounts.is_empty() {
    bail!(
      "no credentials were discovered for {}; refusing to rewrite agent config without an account to route",
      plan.agent.slug()
    );
  }
  Ok(())
}

fn write_edit(edit: &PlannedEdit) -> Result<()> {
  if let Some(parent) = edit.path.parent() {
    std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
  }
  match &edit.kind {
    EditKind::Json(value) => {
      std::fs::write(&edit.path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("writing {}", edit.path.display()))?;
    }
    EditKind::Toml(doc) => {
      std::fs::write(&edit.path, doc.to_string()).with_context(|| format!("writing {}", edit.path.display()))?;
    }
  }
  Ok(())
}

fn upsert_agent_and_profiles(path: &Path, profile: &str, agent: AgentKind, accounts: &[Account]) -> Result<()> {
  Ok(Config::edit_in_place(path, |doc| {
    let agent_mode = upsert_agent(doc, profile, agent);
    upsert_profile_item(doc, profile, agent, agent_mode, accounts);
    if agent_mode == RouteMode::Switch {
      upsert_switch_profiles(doc, profile, agent, accounts);
    }
    Ok(())
  })?)
}

fn upsert_agent(doc: &mut toml_edit::DocumentMut, profile: &str, agent: AgentKind) -> RouteMode {
  let agents = doc["agents"].or_insert(toml_edit::table());
  let agent_item = agents[agent.slug()].or_insert(toml_edit::table());
  if agent_item.get("mode").is_none() {
    agent_item["mode"] = toml_edit::value("route");
  }
  agent_item["profile"] = toml_edit::value(profile);
  agent_item["sync"] = toml_edit::value(true);
  agent_item
    .get("mode")
    .and_then(toml_edit::Item::as_str)
    .and_then(parse_route_mode)
    .unwrap_or(RouteMode::Route)
}

fn upsert_profile_item(
  doc: &mut toml_edit::DocumentMut,
  profile: &str,
  agent: AgentKind,
  mode: RouteMode,
  accounts: &[Account],
) {
  let account_ids = accounts.iter().map(|account| account.id.clone()).collect::<Vec<_>>();
  let mut providers = accounts
    .iter()
    .map(|account| account.provider.clone())
    .collect::<Vec<_>>();
  if providers.is_empty() {
    providers.push(agent.default_provider_id().to_string());
  }
  providers.sort();
  providers.dedup();
  let profiles = doc["profiles"].or_insert(toml_edit::table());
  let profile_item = profiles[profile].or_insert(toml_edit::table());
  profile_item["mode"] = toml_edit::value(route_mode_as_str(mode));
  profile_item["agent_id"] = toml_edit::value(agent.agent_id());
  profile_item["providers"] = array_value(&providers);
  if accounts.is_empty() {
    profile_item.as_table_mut().map(|table| table.remove("accounts"));
  } else {
    profile_item["accounts"] = array_value(&account_ids);
  }
}

fn upsert_switch_profiles(doc: &mut toml_edit::DocumentMut, profile: &str, agent: AgentKind, accounts: &[Account]) {
  let mut by_provider: BTreeMap<String, Vec<String>> = BTreeMap::new();
  for account in accounts {
    by_provider
      .entry(account.provider.clone())
      .or_default()
      .push(account.id.clone());
  }
  let profiles = doc["profiles"].or_insert(toml_edit::table());
  for (provider, account_ids) in by_provider {
    let synthetic_profile = format!("{profile}-{provider}");
    let item = profiles[synthetic_profile.as_str()].or_insert(toml_edit::table());
    item["mode"] = toml_edit::value("switch");
    item["agent_id"] = toml_edit::value(agent.agent_id());
    item["providers"] = array_value(std::slice::from_ref(&provider));
    item["accounts"] = array_value(&account_ids);
  }
}

fn array_value(values: &[String]) -> toml_edit::Item {
  let mut arr = toml_edit::Array::new();
  for value in values {
    arr.push(value.as_str());
  }
  toml_edit::value(arr)
}

fn parse_route_mode(mode: &str) -> Option<RouteMode> {
  match mode.trim() {
    "passthrough" => Some(RouteMode::Passthrough),
    "switch" => Some(RouteMode::Switch),
    "exact" => Some(RouteMode::Exact),
    "route" => Some(RouteMode::Route),
    "fuzzy" => Some(RouteMode::Fuzzy),
    _ => None,
  }
}

fn route_mode_as_str(mode: RouteMode) -> &'static str {
  match mode {
    RouteMode::Passthrough => "passthrough",
    RouteMode::Switch => "switch",
    RouteMode::Exact => "exact",
    RouteMode::Route => "route",
    RouteMode::Fuzzy => "fuzzy",
  }
}

fn validate_profile_name(profile: &str) -> Result<()> {
  if profile.trim().is_empty() || profile.contains('/') {
    bail!("profile name must be non-empty and must not contain '/'");
  }
  Ok(())
}

fn gateway_profile_base_url(cfg: &Config, profile: &str) -> String {
  format!("http://{}:{}/{profile}/v1", cfg.server.host, cfg.server.port)
}

fn timestamp() -> Result<String> {
  let now = time::OffsetDateTime::now_utc();
  let rfc3339 = now.format(&Rfc3339)?;
  Ok(compact_rfc3339_timestamp(&rfc3339))
}

fn compact_rfc3339_timestamp(rfc3339: &str) -> String {
  let compact = rfc3339
    .replace(['-', ':'], "")
    .replace('.', "")
    .trim_end_matches('Z')
    .to_string();
  format!("{compact}Z")
}

fn home_dir() -> Result<PathBuf> {
  directories::BaseDirs::new()
    .map(|dirs| dirs.home_dir().to_path_buf())
    .ok_or_else(|| anyhow!("cannot resolve home directory"))
}

fn default_gateway_auth_path() -> Result<PathBuf> {
  default_auth_path()
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sample_account(id: &str, provider: &str) -> Account {
    Account {
      id: id.into(),
      provider: provider.into(),
      enabled: true,
      tier: tokn_core::account::AccountTier::Active,
      tags: Vec::new(),
      label: None,
      base_url: None,
      headers: Default::default(),
      auth_type: None,
      username: None,
      api_key: None,
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

  #[test]
  fn default_gateway_auth_path_uses_auth_store_default() {
    assert_eq!(default_gateway_auth_path().unwrap(), default_auth_path().unwrap());
  }

  #[test]
  fn compact_timestamp_preserves_fractional_seconds() {
    assert_eq!(
      compact_rfc3339_timestamp("2026-06-05T10:11:12.123456789Z"),
      "20260605T101112123456789Z"
    );
  }

  #[test]
  fn plan_migration_uses_explicit_agent_home() {
    let dir = tempfile::tempdir().unwrap();
    let gateway_config_path = dir.path().join("config.toml");
    let agent_home = dir.path().join("agent-home");
    let opencode_auth_path = agent_home.join(".local/share/opencode/auth.json");
    let opencode_config_path = agent_home.join(".config/opencode/opencode.json");
    std::fs::create_dir_all(opencode_auth_path.parent().unwrap()).unwrap();
    std::fs::create_dir_all(opencode_config_path.parent().unwrap()).unwrap();
    std::fs::write(
      &gateway_config_path,
      r#"
[server]
host = "127.0.0.1"
port = 4141
"#,
    )
    .unwrap();
    std::fs::write(
      &opencode_auth_path,
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
      &opencode_config_path,
      serde_json::json!({
        "mcp": {}
      })
      .to_string(),
    )
    .unwrap();

    let plan = plan_migration(MigrateRequest {
      agent: AgentKind::Opencode,
      profile: "opencode".into(),
      gateway_config_path: Some(gateway_config_path.clone()),
      agent_home: Some(agent_home),
    })
    .unwrap();

    assert_eq!(plan.agent, AgentKind::Opencode);
    assert_eq!(plan.gateway_config_path, gateway_config_path);
    assert_eq!(plan.target_base_url, "http://127.0.0.1:4141/opencode/v1");
    assert_eq!(plan.imported_accounts.len(), 1);
  }

  #[test]
  fn migration_plan_rejects_missing_imported_accounts() {
    let plan = MigrationPlan {
      agent: AgentKind::Opencode,
      timestamp: "20260604T153012Z".into(),
      profile: "opencode".into(),
      gateway_config_path: PathBuf::from("config.toml"),
      gateway_auth_path: PathBuf::from("auth.yaml"),
      target_base_url: "http://127.0.0.1:4141/opencode/v1".into(),
      imported_accounts: Vec::new(),
      edits: Vec::new(),
    };

    let err = validate_migration_plan(&plan).unwrap_err();
    assert!(err.to_string().contains("no credentials were discovered"));
  }

  #[test]
  fn validate_profile_name_rejects_empty_and_path_like_names() {
    assert!(validate_profile_name("").is_err());
    assert!(validate_profile_name("   ").is_err());
    assert!(validate_profile_name("agent/profile").is_err());
    assert!(validate_profile_name("agent").is_ok());
  }

  #[test]
  fn upsert_agent_and_profiles_without_imported_accounts_scopes_to_agent_provider() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");

    upsert_agent_and_profiles(&path, "opencode", AgentKind::Opencode, &[]).unwrap();

    let (cfg, _) = Config::load(Some(&path)).unwrap();
    let agent = cfg.agents.get("opencode").unwrap();
    assert_eq!(agent.mode, Some(RouteMode::Route));
    assert_eq!(agent.profile.as_deref(), Some("opencode"));
    assert!(agent.sync);
    let profile = cfg.profiles.get("opencode").unwrap();
    assert_eq!(profile.agent_id, Some(tokn_core::AgentId::Opencode));
    assert_eq!(
      profile.providers.as_deref(),
      Some(&[tokn_core::provider::ID_OPENAI.to_string()][..])
    );
    assert_eq!(profile.accounts, None);
  }

  #[test]
  fn upsert_agent_and_profiles_with_imported_accounts_scopes_to_accounts_and_providers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let accounts = vec![Account {
      id: "codex-cli-codex".into(),
      provider: tokn_core::provider::ID_CODEX.into(),
      enabled: true,
      tier: tokn_core::account::AccountTier::Active,
      tags: Vec::new(),
      label: None,
      base_url: None,
      headers: Default::default(),
      auth_type: None,
      username: None,
      api_key: None,
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
    }];

    upsert_agent_and_profiles(&path, "codex", AgentKind::CodexCli, &accounts).unwrap();

    let (cfg, _) = Config::load(Some(&path)).unwrap();
    let agent = cfg.agents.get("codex-cli").unwrap();
    assert_eq!(agent.profile.as_deref(), Some("codex"));
    let profile = cfg.profiles.get("codex").unwrap();
    assert_eq!(profile.agent_id, Some(tokn_core::AgentId::CodexCli));
    assert_eq!(
      profile.providers.as_deref(),
      Some(&[tokn_core::provider::ID_CODEX.to_string()][..])
    );
    assert_eq!(profile.accounts.as_deref(), Some(&["codex-cli-codex".to_string()][..]));
  }

  #[test]
  fn upsert_agent_and_profiles_respects_switch_mode_with_synthetic_profiles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(
      &path,
      r#"
        [agents.opencode]
        mode = "switch"
      "#,
    )
    .unwrap();
    let mut openai = sample_account("opencode-openai", tokn_core::provider::ID_OPENAI);
    let mut codex = sample_account("opencode-codex", tokn_core::provider::ID_CODEX);
    openai.tags.push("source:opencode".into());
    codex.tags.push("source:opencode".into());

    upsert_agent_and_profiles(&path, "opencode", AgentKind::Opencode, &[openai, codex]).unwrap();

    let (cfg, _) = Config::load(Some(&path)).unwrap();
    assert_eq!(cfg.agents["opencode"].mode, Some(RouteMode::Switch));
    assert_eq!(cfg.profiles["opencode"].mode, Some(RouteMode::Switch));
    assert_eq!(
      cfg.profiles["opencode-openai"].accounts.as_deref(),
      Some(&["opencode-openai".to_string()][..])
    );
    assert_eq!(
      cfg.profiles["opencode-codex"].accounts.as_deref(),
      Some(&["opencode-codex".to_string()][..])
    );
  }

  #[test]
  fn disable_missing_source_accounts_disables_previously_imported_accounts() {
    let dir = tempfile::tempdir().unwrap();
    let auth_path = dir.path().join("auth.yaml");
    let mut store = AuthStore::load(Some(&auth_path), None).unwrap();
    store.accounts = vec![
      annotate_imported_account(
        sample_account("opencode-openai", tokn_core::provider::ID_OPENAI),
        AgentKind::Opencode,
        Path::new("/tmp/opencode-auth.json"),
        "auth.openai",
        "20260604T153012Z",
      ),
      sample_account("manual-openai", tokn_core::provider::ID_OPENAI),
    ];

    disable_missing_source_accounts(&mut store, AgentKind::Opencode, &BTreeSet::new());

    let imported = store.get("opencode-openai").unwrap();
    assert!(!imported.enabled);
    assert!(imported.tags.iter().any(|tag| tag == "source:missing"));
    assert!(store.get("manual-openai").unwrap().enabled);
  }

  #[test]
  fn write_edit_creates_parent_directories_for_json_and_toml() {
    let dir = tempfile::tempdir().unwrap();
    let json_path = dir.path().join("nested/auth.json");
    let toml_path = dir.path().join("nested/config.toml");
    let mut doc = toml_edit::DocumentMut::new();
    doc["model_provider"] = toml_edit::value("tokn-router");

    write_edit(&PlannedEdit {
      path: json_path.clone(),
      kind: EditKind::Json(serde_json::json!({"auth_mode": "api_key"})),
    })
    .unwrap();
    write_edit(&PlannedEdit {
      path: toml_path.clone(),
      kind: EditKind::Toml(doc),
    })
    .unwrap();

    let json: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(json_path).unwrap()).unwrap();
    assert_eq!(json["auth_mode"], "api_key");
    assert!(std::fs::read_to_string(toml_path).unwrap().contains("model_provider"));
  }

  #[test]
  fn apply_migration_writes_gateway_state_agent_edits_and_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let gateway_config_path = dir.path().join("gateway/config.toml");
    let gateway_auth_path = dir.path().join("gateway/auth.yaml");
    let agent_config_path = dir.path().join("agent/config.json");
    let manifest_path = dir.path().join("manifest.json");
    let account = Account {
      id: "opencode-openai".into(),
      provider: tokn_core::provider::ID_OPENAI.into(),
      enabled: true,
      tier: tokn_core::account::AccountTier::Active,
      tags: Vec::new(),
      label: None,
      base_url: None,
      headers: Default::default(),
      auth_type: None,
      username: None,
      api_key: Some(tokn_core::util::secret::Secret::new("sk-test".to_string())),
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
    };
    let plan = MigrationPlan {
      agent: AgentKind::Opencode,
      timestamp: "20260604T153012Z".into(),
      profile: "opencode".into(),
      gateway_config_path: gateway_config_path.clone(),
      gateway_auth_path: gateway_auth_path.clone(),
      target_base_url: "http://127.0.0.1:4141/opencode/v1".into(),
      imported_accounts: vec![account],
      edits: vec![PlannedEdit {
        path: agent_config_path.clone(),
        kind: EditKind::Json(serde_json::json!({"provider": "tokn-router"})),
      }],
    };

    let report = apply_migration_to_manifest_path(plan, manifest_path.clone()).unwrap();

    assert_eq!(report.manifest_path, manifest_path);
    assert!(gateway_config_path.exists());
    assert!(gateway_auth_path.exists());
    assert_eq!(
      serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(agent_config_path).unwrap()).unwrap()
        ["provider"],
      "tokn-router"
    );
    let manifest: MigrationManifest = serde_json::from_str(&std::fs::read_to_string(manifest_path).unwrap()).unwrap();
    assert!(manifest.completed);
    assert_eq!(manifest.imported_account_ids, vec!["opencode-openai"]);
    assert!(report.files.iter().any(|file| file.original == gateway_config_path));
    assert!(report.files.iter().any(|file| file.original == gateway_auth_path));
  }

  #[test]
  fn apply_migration_leaves_in_progress_manifest_if_later_edit_fails() {
    let dir = tempfile::tempdir().unwrap();
    let gateway_config_path = dir.path().join("gateway/config.toml");
    let gateway_auth_path = dir.path().join("gateway/auth.yaml");
    let edit_path = dir.path().join("agent/config.json");
    let manifest_path = dir.path().join("manifest.json");
    std::fs::write(dir.path().join("agent"), "not a directory").unwrap();
    let account = Account {
      id: "opencode-openai".into(),
      provider: tokn_core::provider::ID_OPENAI.into(),
      enabled: true,
      tier: tokn_core::account::AccountTier::Active,
      tags: Vec::new(),
      label: None,
      base_url: None,
      headers: Default::default(),
      auth_type: None,
      username: None,
      api_key: Some(tokn_core::util::secret::Secret::new("sk-test".to_string())),
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
    };
    let plan = MigrationPlan {
      agent: AgentKind::Opencode,
      timestamp: "20260604T153012Z".into(),
      profile: "opencode".into(),
      gateway_config_path,
      gateway_auth_path,
      target_base_url: "http://127.0.0.1:4141/opencode/v1".into(),
      imported_accounts: vec![account],
      edits: vec![PlannedEdit {
        path: edit_path.clone(),
        kind: EditKind::Json(serde_json::json!({"provider": "tokn-router"})),
      }],
    };

    let err = apply_migration_to_manifest_path(plan, manifest_path.clone()).unwrap_err();

    assert!(format!("{err:#}").contains("creating"));
    let manifest: MigrationManifest = serde_json::from_str(&std::fs::read_to_string(manifest_path).unwrap()).unwrap();
    assert!(!manifest.completed);
    assert!(manifest.files.iter().any(|file| file.original == edit_path));
  }

  #[test]
  fn rollback_restores_file_from_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let original = dir.path().join("config.toml");
    let backup = dir.path().join("config.toml.bak.20260604T153012Z");
    let manifest_path = dir.path().join("20260604T153012Z-codex-cli.json");
    std::fs::write(&original, "mutated").unwrap();
    std::fs::write(&backup, "original").unwrap();
    let manifest = MigrationManifest {
      version: 1,
      completed: true,
      agent: AgentKind::CodexCli,
      timestamp: "20260604T153012Z".into(),
      profile: "codex".into(),
      target_base_url: "http://127.0.0.1:4141/codex/v1".into(),
      imported_account_ids: vec!["codex-cli-codex".into()],
      files: vec![FileBackup {
        original: original.clone(),
        backup: Some(backup),
        existed: true,
        created_by_migration: false,
      }],
    };
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let report = rollback_migration(RollbackRequest {
      agent: AgentKind::CodexCli,
      backup_id: Some(manifest_path.display().to_string()),
    })
    .unwrap();

    assert_eq!(std::fs::read_to_string(original).unwrap(), "original");
    assert_eq!(report.actions.len(), 1);
  }

  #[test]
  fn rollback_removes_files_created_by_migration() {
    let dir = tempfile::tempdir().unwrap();
    let original = dir.path().join("created.toml");
    let manifest_path = dir.path().join("20260604T153012Z-opencode.json");
    std::fs::write(&original, "created").unwrap();
    let manifest = MigrationManifest {
      version: 1,
      completed: true,
      agent: AgentKind::Opencode,
      timestamp: "20260604T153012Z".into(),
      profile: "opencode".into(),
      target_base_url: "http://127.0.0.1:4141/opencode/v1".into(),
      imported_account_ids: vec!["opencode-openai".into()],
      files: vec![FileBackup {
        original: original.clone(),
        backup: None,
        existed: false,
        created_by_migration: true,
      }],
    };
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let report = rollback_migration(RollbackRequest {
      agent: AgentKind::Opencode,
      backup_id: Some(manifest_path.display().to_string()),
    })
    .unwrap();

    assert!(!original.exists());
    assert!(matches!(report.actions.as_slice(), [FileAction::Removed(path)] if path == &original));
  }

  #[test]
  fn rollback_rejects_manifest_for_different_agent() {
    let dir = tempfile::tempdir().unwrap();
    let manifest_path = dir.path().join("20260604T153012Z-codex-cli.json");
    let manifest = MigrationManifest {
      version: 1,
      completed: true,
      agent: AgentKind::CodexCli,
      timestamp: "20260604T153012Z".into(),
      profile: "codex".into(),
      target_base_url: "http://127.0.0.1:4141/codex/v1".into(),
      imported_account_ids: vec!["codex-cli-codex".into()],
      files: Vec::new(),
    };
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let err = rollback_migration(RollbackRequest {
      agent: AgentKind::Opencode,
      backup_id: Some(manifest_path.display().to_string()),
    })
    .unwrap_err();

    assert!(err.to_string().contains("not opencode"));
  }
}
