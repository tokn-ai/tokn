use super::*;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use std::sync::Mutex as StdMutex;

#[derive(Clone)]
struct MockReply {
  status: StatusCode,
  body: Value,
  delay: Duration,
}

impl MockReply {
  fn models(ids: &[&str]) -> Self {
    Self {
      status: StatusCode::OK,
      body: json!({"data": ids.iter().map(|id| json!({"id": id, "owned_by": "upstream"})).collect::<Vec<_>>()}),
      delay: Duration::ZERO,
    }
  }
}

#[derive(Default)]
struct MockState {
  replies: BTreeMap<String, MockReply>,
  calls: Vec<String>,
}

async fn mock_models(State(state): State<Arc<StdMutex<MockState>>>, headers: HeaderMap) -> (StatusCode, Json<Value>) {
  let account = headers["authorization"]
    .to_str()
    .unwrap()
    .strip_prefix("Bearer ")
    .unwrap();
  let reply = {
    let mut state = state.lock().unwrap();
    state.calls.push(account.to_owned());
    state.replies[account].clone()
  };
  tokio::time::sleep(reply.delay).await;
  (reply.status, Json(reply.body))
}

fn discovery(base_url: &str) -> DiscoveryRuntime {
  let config = format!(
    r#"
schema_version = 2

[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"

[providers.local]
driver = "openai"
base_url = "{base_url}/v1"

[profiles.all]
route = "managed"
binding = {{ path = "/all/v1" }}
account_pool = {{ accounts = ["a", "b"] }}

[profiles.first]
route = "managed"
binding = {{ path = "/first/v1" }}
account_pool = {{ accounts = ["a"] }}

[routes.managed]
kind = "managed"
providers = ["local"]
provider = {{ kind = "fixed", provider = "local" }}
model = {{ kind = "capability" }}
operation = "preserve"
"#
  );
  let plan = tokn_config::v2::parse(&config, std::path::Path::new("discovery-refresh.toml")).unwrap();
  let accounts =
    ["a", "b"].map(|id| serde_json::from_value(json!({"id": id, "provider": "local", "api_key": id})).unwrap());
  let registry = Registry::builtin();
  let graph = tokn_accounts::link::link_provider_graph(&plan, &accounts, &registry).unwrap();
  let pools = tokn_accounts::link::link_account_pools(&plan, &graph).unwrap();
  DiscoveryRuntime::new(
    &plan,
    &graph,
    &pools,
    &registry,
    reqwest::Client::builder().no_proxy().build().unwrap(),
    &plan.profiles().keys().cloned().collect(),
  )
  .unwrap()
}

#[tokio::test]
async fn refresh_unions_accounts_and_preserves_successes_after_errors_empty_lists_and_timeouts() {
  let state = Arc::new(StdMutex::new(MockState {
    replies: BTreeMap::from([
      ("a".into(), MockReply::models(&["account-a", "gpt-4o"])),
      ("b".into(), MockReply::models(&["account-b"])),
    ]),
    calls: Vec::new(),
  }));
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let base_url = format!("http://{}", listener.local_addr().unwrap());
  let app = Router::new()
    .route("/v1/models", get(mock_models))
    .with_state(state.clone());
  let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
  let runtime = discovery(&base_url);
  let provider_id = ProviderId::new("local").unwrap();
  let cache = &runtime.metadata[&provider_id].model_cache;

  runtime.refresh_upstream(Duration::from_secs(1)).await;
  assert!(cache.contains("account-a"));
  assert!(cache.contains("account-b"));
  // Account a appears in both profiles, but background discovery queries it once.
  assert_eq!(state.lock().unwrap().calls.len(), 2);

  // A slow background batch must not hold discovery readers behind network I/O.
  let refreshing = runtime.refresh_gate.lock().await;
  let snapshot = tokio::time::timeout(
    Duration::from_millis(100),
    runtime.models(&ProfileId::new("all").unwrap(), &AccessContext::unrestricted()),
  )
  .await
  .expect("cached discovery should not wait for a refresh")
  .unwrap();
  assert!(snapshot["data"]
    .as_array()
    .unwrap()
    .iter()
    .any(|model| model["id"] == "account-b"));
  assert_eq!(state.lock().unwrap().calls.len(), 2);
  drop(refreshing);

  let all = runtime
    .models(&ProfileId::new("all").unwrap(), &AccessContext::unrestricted())
    .await
    .unwrap();
  let models = all["data"].as_array().unwrap();
  assert!(models.iter().any(|model| model["id"] == "account-a"));
  assert!(models.iter().any(|model| model["id"] == "account-b"));
  // Live records win over catalogue suggestions for the same ID.
  assert_eq!(
    models.iter().find(|model| model["id"] == "gpt-4o").unwrap()["owned_by"],
    "upstream"
  );
  assert!(models.len() > 3, "catalogue-only suggestions must also be listed");

  {
    let mut state = state.lock().unwrap();
    state.replies.insert("a".into(), MockReply::models(&["account-a-new"]));
    state.replies.get_mut("b").unwrap().status = StatusCode::SERVICE_UNAVAILABLE;
  }
  runtime.refresh_upstream(Duration::from_secs(1)).await;
  assert!(cache.contains("account-a-new"));
  assert!(!cache.contains("account-a"));
  assert!(cache.contains("account-b"));

  state.lock().unwrap().replies.insert("a".into(), MockReply::models(&[]));
  let first = runtime
    .models(&ProfileId::new("first").unwrap(), &AccessContext::unrestricted())
    .await
    .unwrap();
  let models = first["data"].as_array().unwrap();
  assert!(models.iter().any(|model| model["id"] == "account-a-new"));
  assert!(
    !models.iter().any(|model| model["id"] == "account-b"),
    "other profile's accounts must stay private"
  );
  assert!(
    cache.contains("account-b"),
    "a scoped GET must not erase another account's cache"
  );

  state.lock().unwrap().replies.get_mut("a").unwrap().delay = Duration::from_secs(1);
  runtime.refresh_upstream(Duration::from_millis(10)).await;
  assert!(cache.contains("account-a-new"));
  assert!(cache.contains("account-b"));

  let calls = state.lock().unwrap().calls.len();
  let restricted = AccessContext {
    providers: tokn_access::ProviderAccess::from_provider_ids(vec!["other".into()]).unwrap(),
    ..AccessContext::unrestricted()
  };
  let denied = runtime
    .models(&ProfileId::new("all").unwrap(), &restricted)
    .await
    .unwrap();
  assert_eq!(denied["data"], json!([]));
  assert_eq!(
    state.lock().unwrap().calls.len(),
    calls,
    "denied providers must not be queried"
  );
  server.abort();
}

#[test]
fn discovered_ids_use_the_routes_qualification_namespace() {
  let runtime = discovery("http://127.0.0.1:1");
  let provider = ProviderId::new("local").unwrap();
  for (namespace, expected) in [
    (QualificationNamespace::Provider, "local/organization/custom-model"),
    (QualificationNamespace::Driver, "openai/organization/custom-model"),
  ] {
    let mut models = Vec::new();
    merge_models(
      &mut models,
      &mut HashSet::new(),
      vec![json!({"id": "organization/custom-model"})],
      &provider,
      &runtime.metadata[&provider],
      false,
      Some(namespace),
    );
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["id"], expected);
    assert_eq!(models[0]["x_tokn_router"]["upstream_id"], "organization/custom-model");
  }
}

#[test]
fn current_catalogue_replaces_metadata_in_existing_destinations() {
  let runtime = discovery("http://127.0.0.1:1");
  let provider_id = ProviderId::new("local").unwrap();
  let metadata = &runtime.metadata[&provider_id];
  assert!(
    metadata.model_cache.catalogue_models().is_some(),
    "construction must seed current catalogue"
  );
  let mut model = metadata.catalogue_models().into_iter().next().unwrap();
  model.id = "newly-catalogued".into();
  model.name = "Current catalogue name".into();
  metadata.model_cache.set_catalogue(vec![model]);
  assert_eq!(
    metadata.local_models(),
    vec![json!({"id": "newly-catalogued", "object": "model"})]
  );
  let mut entry = json!({"id": "newly-catalogued"});
  enrich(
    &mut entry,
    "newly-catalogued",
    "newly-catalogued",
    &provider_id,
    metadata,
  );
  assert_eq!(entry["x_tokn_router"]["name"], "Current catalogue name");

  runtime.apply_catalogue();
  assert!(metadata.model_cache.catalogue_contains("gpt-4o").unwrap());
  assert!(!metadata.model_cache.catalogue_contains("newly-catalogued").unwrap());
}

#[test]
fn discovery_efforts_follow_live_metadata_catalogue_and_unknown_precedence() {
  let provider_id = ProviderId::new("deepseek").unwrap();
  let metadata = ProviderMetadata {
    driver_id: "deepseek".into(),
    display_name: "DeepSeek",
    upstream_url: "https://api.deepseek.com/".into(),
    auth_kind: Value::Null,
    endpoints: vec!["chat_completions"],
    models: tokn_catalogue::catalogue::default_models_for("deepseek"),
    model_cache: Arc::new(ModelCache::default()),
  };
  let mut entry = json!({"id": "deepseek-v4-flash"});
  enrich(
    &mut entry,
    "deepseek-v4-flash",
    "deepseek-v4-flash",
    &provider_id,
    &metadata,
  );
  assert_eq!(
    entry["x_tokn_router"]["capabilities"]["reasoning_efforts"],
    json!(["low", "high", "max"])
  );

  let mut live = json!({"id": "deepseek-v4-flash", "capabilities": {"supports": {"reasoning_effort": []}}});
  metadata.model_cache.set_models(&[live.clone()]);
  enrich(
    &mut live,
    "deepseek-v4-flash",
    "deepseek-v4-flash",
    &provider_id,
    &metadata,
  );
  assert_eq!(live["x_tokn_router"]["capabilities"]["reasoning_efforts"], json!([]));
  assert_eq!(live["capabilities"]["supports"]["reasoning_effort"], json!([]));
  // A subsequent local fallback still reports the cached support used by validation.
  enrich(
    &mut entry,
    "deepseek-v4-flash",
    "deepseek-v4-flash",
    &provider_id,
    &metadata,
  );
  assert_eq!(entry["x_tokn_router"]["capabilities"]["reasoning_efforts"], json!([]));

  let mut unknown = json!({"id": "future"});
  enrich(&mut unknown, "future", "future", &provider_id, &metadata);
  assert_eq!(
    unknown["x_tokn_router"]["capabilities"]["reasoning_efforts"],
    Value::Null
  );
}
