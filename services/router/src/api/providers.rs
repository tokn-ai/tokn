use super::error::ApiError;
use super::{AppState, LiveAppState, RequestPolicyRuntime};
use axum::extract::{Extension, Path, State};
use axum::Json;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokn_access::AccessContext;
use tokn_accounts::routing::route_mode_as_str;

pub async fn list_providers(
  State(s): State<LiveAppState>,
  Extension(access): Extension<AccessContext>,
) -> Result<Json<Value>, ApiError> {
  let s = s.current();
  let policy = s.default_policy.clone();
  list_providers_for_policy(policy, &access)
}

pub async fn list_providers_with_profile(
  State(s): State<LiveAppState>,
  Extension(access): Extension<AccessContext>,
  Path(profile): Path<String>,
) -> Result<Json<Value>, ApiError> {
  let s = s.current();
  let policy = profile_policy(&s, &profile)?;
  list_providers_for_policy(policy, &access)
}

fn profile_policy(s: &AppState, profile: &str) -> Result<Arc<RequestPolicyRuntime>, ApiError> {
  s.profiles
    .get(profile)
    .cloned()
    .ok_or_else(|| ApiError::bad_request(format!("unknown profile '{profile}'")))
}

fn list_providers_for_policy(
  policy: Arc<RequestPolicyRuntime>,
  access: &AccessContext,
) -> Result<Json<Value>, ApiError> {
  let mut providers: BTreeMap<String, ProviderSummary> = BTreeMap::new();

  for account in policy.pool.all() {
    if !access.providers.allows(account.provider.info().id.as_str()) {
      continue;
    }
    let info = account.provider.info();
    providers
      .entry(info.id.clone())
      .and_modify(|summary| summary.accounts += 1)
      .or_insert_with(|| ProviderSummary {
        id: info.id.clone(),
        display_name: info.display_name,
        auth_kind: serde_json::to_value(info.auth_kind).unwrap_or(Value::Null),
        upstream_url: info.upstream_url.clone(),
        accounts: 1,
        endpoints: info
          .default_endpoints
          .iter()
          .map(|endpoint| endpoint.as_str())
          .collect(),
      });
  }

  let data = providers
    .into_values()
    .map(|provider| {
      json!({
        "id": provider.id,
        "object": "provider",
        "display_name": provider.display_name,
        "auth_kind": provider.auth_kind,
        "upstream_url": provider.upstream_url,
        "accounts": provider.accounts,
        "endpoints": provider.endpoints,
      })
    })
    .collect::<Vec<_>>();

  Ok(Json(json!({
    "object": "list",
    "route_mode": route_mode_as_str(policy.mode),
    "data": data,
  })))
}

struct ProviderSummary {
  id: String,
  display_name: &'static str,
  auth_kind: Value,
  upstream_url: String,
  accounts: usize,
  endpoints: Vec<&'static str>,
}
