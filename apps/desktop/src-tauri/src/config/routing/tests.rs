use super::*;

fn value<'a>(fields: &'a [RoutingSummaryField], label: &str) -> &'a str {
  &fields
    .iter()
    .find(|field| field.label == label)
    .expect("summary field missing")
    .value
}

fn read(contents: &str) -> super::super::RoutingDocument {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("config.toml");
  std::fs::write(&path, contents).unwrap();
  super::super::read_at(&path).unwrap()
}

#[test]
fn defaults_expand_into_a_profile_and_route_with_authored_provenance() {
  let document = read(
    r#"schema_version = 2
[listeners.local]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"
[defaults]
providers = ["openai"]
[defaults.account_pool]
accounts = ["work", "personal"]
strategy = "round_robin"
[model_scores."gpt-*"]
openai = 10
"#,
  );
  assert!(document.overview_error.is_none());
  let overview = document.overview.unwrap();
  assert_eq!(overview.model_score_count, 1);
  assert_eq!(overview.profiles.len(), 1);
  assert_eq!(overview.routes.len(), 1);
  let profile = &overview.profiles[0];
  assert_eq!(profile.id, "default");
  assert_eq!(profile.route_id.as_deref(), Some("default"));
  assert_eq!(profile.api_path.as_deref(), Some("/v1"));
  assert_eq!(profile.source.section, "defaults");
  assert!(profile.source.generated);
  assert_eq!(value(&profile.fields, "Account selection"), "personal, work");
  assert_eq!(value(&profile.fields, "Pool strategy"), "Round robin");
  let route = &overview.routes[0];
  assert_eq!(route.used_by, ["default"]);
  assert_eq!(route.source.section, "defaults");
  assert!(route.source.generated);
  assert_eq!(value(&route.fields, "Provider restrictions"), "openai");
  assert!(!route.fields.iter().any(|field| field.label == "Account selection"));
}

#[test]
fn shared_routes_keep_profile_pools_separate_and_expose_retry_policy() {
  let document = read(
    r#"schema_version = 2
[listeners.local]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"
[profiles.work]
route = "shared"
[profiles.work.account_pool]
accounts = ["work-account"]
[profiles."personal.daily"]
route = "shared"
[profiles."personal.daily".account_pool]
accounts = ["personal-account"]
[routes.shared]
kind = "managed"
providers = ["openai"]
provider = { kind = "fixed", provider = "openai" }
model = { kind = "qualified", namespace = "provider" }
operation = "preserve"
retry = { kind = "recoverable", policy = "standard" }
[retry_policies.standard]
max_retries = 3
initial_backoff_ms = 25
"#,
  );
  assert!(document.overview_error.is_none(), "{:?}", document.overview_error);
  let overview = document.overview.unwrap();
  assert_eq!(overview.routes.len(), 1);
  let route = &overview.routes[0];
  assert_eq!(route.target, "openai");
  assert_eq!(route.used_by, ["personal.daily", "work"]);
  assert_eq!(value(&route.fields, "Model selection"), "Provider-qualified model");
  assert_eq!(
    value(&route.fields, "Retry"),
    "Recoverable · standard (3 retries, 25 ms initial backoff)"
  );
  let work = overview.profiles.iter().find(|profile| profile.id == "work").unwrap();
  assert_eq!(value(&work.fields, "Account selection"), "work-account");
  assert_eq!(work.api_path.as_deref(), Some("/work/v1"));
  let personal = overview
    .profiles
    .iter()
    .find(|profile| profile.id == "personal.daily")
    .unwrap();
  assert_eq!(value(&personal.fields, "Account selection"), "personal-account");
  assert_eq!(personal.source.section, "profiles.\"personal.daily\"");
}

#[test]
fn original_destination_client_relay_is_proxy_only_and_has_no_pool() {
  let document = read(
    r#"schema_version = 2
[listeners.proxy]
kind = "forward_proxy"
bind = "127.0.0.1:4142"
client_auth = "none"
default_http_action = { kind = "route", profile = "transparent" }
default_connect = "reject"
[profiles.transparent]
route = "relay"
[routes.relay]
kind = "relay"
providers = ["local"]
destination = { kind = "original" }
credentials = { kind = "client" }
[providers.local]
driver = "openai"
base_url = "https://api.example.test/v1"
"#,
  );
  assert!(document.overview_error.is_none(), "{:?}", document.overview_error);
  let overview = document.overview.unwrap();
  let profile = &overview.profiles[0];
  assert!(profile.api_path.is_none());
  assert_eq!(profile.route_kind, "relay");
  assert_eq!(
    value(&profile.fields, "Account selection"),
    "Client credentials · no account pool"
  );
  assert!(!profile.fields.iter().any(|field| field.label == "Pool strategy"));
  let route = &overview.routes[0];
  assert_eq!(route.target, "Original request destination");
  assert_eq!(value(&route.fields, "Credentials"), "Client credentials");
}

#[test]
fn legacy_overlays_override_profiles_and_scores_with_real_source_paths() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("work.toml");
  std::fs::write(
    &path,
    r#"[defaults]
providers = ["openai"]
accounts = ["default-account"]
[profiles.coding]
agent_id = "codex-cli"
mode = "exact"
accounts = ["primary-account"]
[model_scores."gpt-*"]
openai = 1
[copilot]
api_key = "never-expose-this-credential"
"#,
  )
  .unwrap();
  let fragments = directory.path().join("work.d");
  std::fs::create_dir(&fragments).unwrap();
  let fragment = fragments.join("codex-cli.toml");
  std::fs::write(
    &fragment,
    r#"[profiles.coding]
agent_id = "codex-cli"
mode = "fuzzy"
accounts = ["fragment-account"]
[model_scores."gpt-*"]
openai = 7
[model_scores."claude-*"]
openai = 2
"#,
  )
  .unwrap();
  let document = super::super::read_at(&path).unwrap();
  assert!(document.overview_error.is_none(), "{:?}", document.overview_error);
  let overview = document.overview.unwrap();
  assert!(overview.routes.is_empty());
  assert_eq!(overview.model_score_count, 2);
  let profile = overview.profiles.iter().find(|profile| profile.id == "coding").unwrap();
  assert_eq!(profile.route_kind, "fuzzy");
  assert_eq!(profile.source.path, fragment.display().to_string());
  assert_eq!(profile.source.section, "profiles.coding");
  assert!(!profile.source.generated);
  assert_eq!(value(&profile.fields, "Account selection"), "fragment-account");
  assert_eq!(value(&profile.fields, "Provider restrictions"), "openai");
  assert_eq!(profile.api_path.as_deref(), Some("/coding/v1"));
  assert!(!serde_json::to_string(&overview)
    .unwrap()
    .contains("never-expose-this-credential"));
}

#[test]
fn invalid_semantics_do_not_hide_the_editor_or_change_its_file() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("config.toml");
  let contents = "schema_version = 2\n[listeners.local]\nkind = 'llm_api'\nbind = '127.0.0.1:4141'\nclient_auth = 'none'\n[profiles.broken]\nroute = 'missing'\n";
  std::fs::write(&path, contents).unwrap();
  let document = super::super::read_at(&path).unwrap();
  assert!(document.overview.is_none());
  assert!(document.overview_error.unwrap().contains("missing"));
  assert!(document.routing_toml.contains("[profiles.broken]"));
  assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);
}

#[test]
fn overview_uses_captured_bytes_when_live_files_change() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("config.toml");
  let contents = "[defaults]\naccounts = ['captured-account']\n";
  let overlay_path = directory.path().join("config.d/codex-cli.toml");
  let overlays = vec![(
    overlay_path.clone(),
    b"[profiles.coding]\nagent_id = 'codex-cli'\naccounts = ['captured-profile']\n".to_vec(),
  )];
  std::fs::write(&path, "this is not the captured configuration").unwrap();
  std::fs::create_dir(overlay_path.parent().unwrap()).unwrap();
  std::fs::write(&overlay_path, "this is not the captured fragment").unwrap();
  let document: DocumentMut = contents.parse().unwrap();
  let overview = summarize(&path, contents, &document, &overlays, ConfigSchema::Legacy).unwrap();
  assert_eq!(
    value(&overview.profiles[0].fields, "Account selection"),
    "captured-account"
  );
  let profile = overview.profiles.iter().find(|profile| profile.id == "coding").unwrap();
  assert_eq!(value(&profile.fields, "Account selection"), "captured-profile");
  assert_eq!(profile.source.path, overlay_path.display().to_string());
}

#[test]
fn legacy_modes_and_model_families_follow_runtime_inheritance() {
  let document = read(
    r#"[server]
route_mode = "switch"
[defaults]
default_provider_id = "openai"
[[model_families]]
name = "coding"
members = ["gpt-5"]
[profiles.work]
"#,
  );
  assert!(document.overview_error.is_none());
  let overview = document.overview.unwrap();
  let work = overview.profiles.iter().find(|profile| profile.id == "work").unwrap();
  assert_eq!(work.route_kind, "switch");
  assert_eq!(value(&work.fields, "Default provider"), "openai");
  assert_eq!(value(&work.fields, "Model families"), "coding: gpt-5");
}

#[test]
fn legacy_defaults_and_a_named_default_profile_have_distinct_sources() {
  let document = read("[profiles.default]\n");
  let overview = document.overview.unwrap();
  assert_eq!(overview.profiles.len(), 2);
  assert_eq!(overview.profiles[0].id, "default");
  assert_eq!(overview.profiles[1].id, "default");
  assert_eq!(overview.profiles[0].source.section, "defaults");
  assert_eq!(overview.profiles[1].source.section, "profiles.default");
  assert_eq!(overview.profiles[0].api_path.as_deref(), Some("/v1"));
  assert_eq!(overview.profiles[1].api_path.as_deref(), Some("/default/v1"));
}

#[test]
fn invalid_legacy_overlay_keeps_the_primary_editor_available() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("config.toml");
  let contents = "[defaults]\naccounts = ['primary-account']\n";
  std::fs::write(&path, contents).unwrap();
  let fragments = directory.path().join("config.d");
  std::fs::create_dir(&fragments).unwrap();
  let fragment = fragments.join("codex-cli.toml");
  std::fs::write(&fragment, "[profiles.coding]\nmode = 'exact'\n").unwrap();
  let document = super::super::read_at(&path).unwrap();
  assert!(document.overview.is_none());
  let error = document.overview_error.unwrap();
  assert!(error.contains(&fragment.display().to_string()));
  assert!(error.contains("agent_id"));
  assert!(document.routing_toml.contains("primary-account"));
  assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);
}

#[test]
fn legacy_profile_api_mounts_escape_reserved_bytes_like_the_runtime() {
  let document = read("[profiles.'team blue']\n[profiles.'a%2Fb']\n[profiles.'研究']\n");
  let overview = document.overview.unwrap();
  for (id, expected) in [
    ("team blue", "/team%20blue/v1"),
    ("a%2Fb", "/a%252Fb/v1"),
    ("研究", "/%E7%A0%94%E7%A9%B6/v1"),
  ] {
    let profile = overview.profiles.iter().find(|profile| profile.id == id).unwrap();
    assert_eq!(profile.api_path.as_deref(), Some(expected));
  }
  assert!(legacy_api_path(".").is_err());
  assert!(legacy_api_path("..").is_err());
}

#[test]
fn saving_rebuilds_the_overview_and_preserves_unrelated_configuration() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("config.toml");
  std::fs::write(
    &path,
    r#"schema_version = 2
# Keep listener and service settings intact.
[listeners.local]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"
[service.logging]
target = "stderr"
[profiles.work]
route = "first"
[profiles.work.account_pool]
accounts = ["before"]
[routes.first]
kind = "managed"
provider = { kind = "fixed", provider = "openai" }
model = { kind = "capability" }
operation = "preserve"
"#,
  )
  .unwrap();
  let original = super::super::read_at(&path).unwrap();
  assert_eq!(original.overview.as_ref().unwrap().routes[0].target, "openai");
  let saved = super::super::save_at(
    &path,
    &original.revision,
    r#"[profiles.work]
route = "second"
[profiles.work.account_pool]
accounts = ["after"]
[routes.second]
kind = "managed"
providers = ["deepseek"]
provider = { kind = "fixed", provider = "deepseek" }
model = { kind = "qualified", namespace = "provider" }
operation = "translate_compatible"
"#,
  )
  .unwrap();
  assert!(saved.overview_error.is_none());
  assert_ne!(saved.revision, original.revision);
  let overview = saved.overview.unwrap();
  assert_eq!(overview.routes.len(), 1);
  assert_eq!(overview.routes[0].id, "second");
  assert_eq!(overview.routes[0].target, "deepseek");
  assert_eq!(overview.routes[0].used_by, ["work"]);
  assert_eq!(overview.profiles[0].route_id.as_deref(), Some("second"));
  assert_eq!(value(&overview.profiles[0].fields, "Account selection"), "after");
  let persisted = std::fs::read_to_string(&path).unwrap();
  assert!(persisted.contains("# Keep listener and service settings intact."));
  assert!(persisted.contains("bind = \"127.0.0.1:4141\""));
  assert!(persisted.contains("[service.logging]\ntarget = \"stderr\""));
  assert!(!persisted.contains("[routes.first]"));
}
