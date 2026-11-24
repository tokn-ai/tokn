use serde_json::json;
use tokn_config::v2::{compile_execution, CompileError, RawConfig};
use tokn_policy::{ProfileId, RouteId, RoutePlan};

fn raw_config() -> RawConfig {
  serde_json::from_value(json!({
    "schema_version": 2,
    "defaults": {"provider": {"kind": "fixed", "provider": "local"}},
    "providers": {"local": {"driver": "openai", "base_url": "https://example.com/v1"}}
  }))
  .unwrap()
}

#[test]
fn execution_compiles_without_listener_or_source_path() {
  let raw = raw_config();
  assert!(raw.listeners.is_empty());
  let plan = compile_execution(&raw).unwrap();
  let profile = plan.profile(&ProfileId::new("default").unwrap()).unwrap();
  assert_eq!(profile.route().as_str(), "default");
  assert!(profile.account_pool().is_some());
  assert!(matches!(
    plan.route(&RouteId::new("default").unwrap()),
    Some(RoutePlan::Managed(_))
  ));
  assert_eq!(
    plan
      .providers()
      .get(&tokn_policy::ProviderId::new("local").unwrap())
      .unwrap()
      .base_url(),
    Some("https://example.com/v1/")
  );
}

#[test]
fn execution_keeps_semantic_validation_and_authored_diagnostics() {
  let mut raw = raw_config();
  raw.defaults.as_mut().unwrap().account_pool.session_ttl_secs = 0;
  raw
    .defaults
    .as_mut()
    .unwrap()
    .account_pool
    .session_expired_retention_secs = 1;
  let error = compile_execution(&raw).unwrap_err();
  assert!(matches!(error, CompileError::InvalidValue {location, ..} if location.starts_with("defaults.")));
  let mut raw = raw_config();
  raw.schema_version = 99;
  assert!(
    matches!(compile_execution(&raw), Err(CompileError::InvalidValue {location, ..}) if location == "schema_version")
  );
}

#[cfg(feature = "std")]
#[test]
fn gateway_and_sdk_share_identical_execution_resources() {
  let mut raw = raw_config();
  raw.listeners.insert(
    "api".into(),
    serde_json::from_value(json!({"kind":"llm_api", "bind":"127.0.0.1:4141", "client_auth":"none"})).unwrap(),
  );
  let execution = compile_execution(&raw).unwrap();
  let gateway = tokn_config::v2::compile(&raw, std::path::Path::new("same-config.toml")).unwrap();
  assert_eq!(gateway.execution(), &execution);
}
