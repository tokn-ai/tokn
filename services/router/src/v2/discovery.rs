use crate::api::error::ApiError;
use futures_util::{stream, StreamExt};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokn_access::AccessContext;
use tokn_accounts::link::{LinkedAccountPools, ProviderBinding, ProviderBindingKey, ProviderGraph};
use tokn_accounts::registry::Registry;
use tokn_core::provider::{ModelCache, ModelInfo, Provider};
use tokn_policy::{
  GatewayPlan, ModelSelector, ProfileId, ProviderId, ProviderSelector, QualificationNamespace, RelayCredentials,
  RelayDestination, RoutePlan,
};
use tracing::{debug, warn};

const MODEL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const REFRESH_CONCURRENCY: usize = 4;
const CATALOGUE_URL: &str = "https://models.dev/api.json";

type BindingModels = BTreeMap<ProviderBindingKey, Vec<Value>>;

pub(super) struct DiscoveryRuntime {
  http: reqwest::Client,
  metadata: BTreeMap<ProviderId, ProviderMetadata>,
  profiles: BTreeMap<ProfileId, ProfileDiscovery>,
  // Only refreshers serialize; readers can use last-good snapshots while
  // network work is pending. Retain account records before merging targets.
  refresh_gate: Mutex<()>,
  upstream: RwLock<BindingModels>,
}

struct ProviderMetadata {
  driver_id: String,
  display_name: &'static str,
  upstream_url: String,
  auth_kind: Value,
  endpoints: Vec<&'static str>,
  models: Vec<ModelInfo>,
  model_cache: Arc<ModelCache>,
}

struct ProfileDiscovery {
  mode: &'static str,
  providers: BTreeMap<ProviderId, ProfileProvider>,
}

#[derive(Default)]
struct ProfileProvider {
  bindings: BTreeMap<ProviderBindingKey, Arc<ProviderBinding>>,
  plain_model_ids: bool,
  qualified_model_ids: Option<QualificationNamespace>,
}

impl DiscoveryRuntime {
  pub(super) fn new(
    plan: &GatewayPlan,
    providers: &ProviderGraph,
    pools: &LinkedAccountPools,
    registry: &Registry,
    http: reqwest::Client,
    reachable_profiles: &BTreeSet<ProfileId>,
  ) -> anyhow::Result<Self> {
    let mut bindings_by_provider = BTreeMap::<ProviderId, Vec<Arc<ProviderBinding>>>::new();
    for binding in providers.bindings() {
      bindings_by_provider
        .entry(binding.provider_id().clone())
        .or_default()
        .push(binding.clone());
    }

    let mut metadata = BTreeMap::new();
    for (provider_id, destination) in providers.destinations() {
      let provider_plan = plan
        .provider(provider_id)
        .ok_or_else(|| anyhow::anyhow!("linked provider '{provider_id}' is missing from the compiled plan"))?;
      let descriptor = registry
        .resolve_provider_descriptor(provider_id.as_str(), provider_plan.driver().as_str())
        .ok_or_else(|| {
          anyhow::anyhow!(
            "linked provider '{provider_id}' references missing driver '{}'",
            provider_plan.driver()
          )
        })?;
      let first_binding = bindings_by_provider
        .get(provider_id)
        .and_then(|bindings| bindings.first());
      let auth_kind = first_binding
        .map(|binding| binding.driver().info().auth_kind)
        .map(|kind| serde_json::to_value(kind).unwrap_or(Value::Null))
        .unwrap_or(Value::Null);
      let models = first_binding
        .map(|binding| binding.driver().info().default_models.clone())
        .unwrap_or_else(|| catalogue_models(provider_id, provider_plan.driver().as_str()));
      metadata.insert(
        provider_id.clone(),
        ProviderMetadata {
          driver_id: provider_plan.driver().to_string(),
          display_name: descriptor.display_name,
          upstream_url: destination.target().base_url().to_string(),
          auth_kind,
          endpoints: descriptor
            .endpoints
            .iter()
            .map(|endpoint| endpoint.endpoint.as_str())
            .collect(),
          models,
          model_cache: Arc::clone(destination.target().model_cache()),
        },
      );
    }

    let mut profiles = BTreeMap::new();
    for profile_id in reachable_profiles {
      let profile = plan
        .profile(profile_id)
        .ok_or_else(|| anyhow::anyhow!("reachable profile '{profile_id}' is missing from the compiled plan"))?;
      let route = plan.route(profile.route()).ok_or_else(|| {
        anyhow::anyhow!(
          "reachable profile '{profile_id}' references missing route '{}'",
          profile.route()
        )
      })?;
      let mut discovery = ProfileDiscovery {
        mode: super::request_record_mode(route),
        providers: BTreeMap::new(),
      };
      match route {
        RoutePlan::Managed(managed) => {
          let pool_id = profile
            .account_pool()
            .ok_or_else(|| anyhow::anyhow!("profile '{profile_id}' has no account pool"))?;
          let pool = pools
            .pool(pool_id)
            .ok_or_else(|| anyhow::anyhow!("profile '{profile_id}' references missing account pool '{}'", pool_id))?;
          let qualified = match managed.target().model() {
            ModelSelector::Qualified { namespace } => Some(*namespace),
            _ => None,
          };
          for account in pool.active().iter().chain(pool.fallback()) {
            let binding = account.binding();
            if !route.allows_provider(binding.provider_id()) {
              continue;
            }
            if matches!(managed.target().provider(), ProviderSelector::Fixed(provider) if provider != binding.provider_id())
            {
              continue;
            }
            add_binding(
              &mut discovery.providers,
              binding.clone(),
              qualified.is_none(),
              qualified,
            );
          }
        }
        RoutePlan::Relay(route) => {
          let RelayDestination::FixedProvider(provider_id) = route.destination() else {
            profiles.insert(profile_id.clone(), discovery);
            continue;
          };
          match route.credentials() {
            RelayCredentials::Client => {
              discovery
                .providers
                .entry(provider_id.clone())
                .or_default()
                .plain_model_ids = true;
            }
            RelayCredentials::AccountPool => {
              let pool_id = profile
                .account_pool()
                .ok_or_else(|| anyhow::anyhow!("profile '{profile_id}' has no account pool"))?;
              let pool = pools
                .pool(pool_id)
                .ok_or_else(|| anyhow::anyhow!("profile '{profile_id}' references missing account pool '{pool_id}'"))?;
              for account in pool.active().iter().chain(pool.fallback()) {
                let binding = account.binding();
                if binding.provider_id() == provider_id {
                  add_binding(&mut discovery.providers, binding.clone(), true, None);
                }
              }
            }
          }
        }
      }
      profiles.insert(profile_id.clone(), discovery);
    }

    let runtime = Self {
      http,
      metadata,
      profiles,
      refresh_gate: Mutex::new(()),
      upstream: RwLock::new(BTreeMap::new()),
    };
    runtime.apply_catalogue();
    Ok(runtime)
  }

  pub(super) fn providers(&self, profile_id: &ProfileId, access: &AccessContext) -> Result<Value, ApiError> {
    let profile = self.profile(profile_id)?;
    let providers = profile.allowed_providers(access);
    let data = providers
      .filter_map(|(provider_id, provider)| {
        let metadata = self.metadata.get(provider_id)?;
        Some(json!({
          "id": provider_id.as_str(),
          "object": "provider",
          "display_name": metadata.display_name,
          "driver": metadata.driver_id,
          "auth_kind": metadata.auth_kind,
          "upstream_url": metadata.upstream_url,
          "accounts": provider.bindings.len(),
          "endpoints": metadata.endpoints,
          "route_modes": [profile.mode],
        }))
      })
      .collect::<Vec<_>>();
    Ok(list_response(profile.mode, data))
  }

  /// Refresh each reachable account once, even when several profiles use it.
  pub(super) async fn refresh_upstream(&self, timeout: Duration) {
    let bindings = self
      .profiles
      .values()
      .flat_map(|profile| profile.providers.values())
      .flat_map(|provider| provider.bindings.iter())
      .map(|(key, binding)| (key.clone(), binding.clone()))
      .collect::<BTreeMap<_, _>>();
    let _guard = self.refresh_gate.lock().await;
    self.refresh_bindings(bindings.into_values().collect(), timeout).await;
  }

  /// Publish a validated catalogue to every destination in this generation.
  pub(super) async fn refresh_catalogue(&self, timeout: Duration) {
    match tokio::time::timeout(
      timeout,
      tokn_catalogue::loader::fetch_and_persist(&self.http, CATALOGUE_URL),
    )
    .await
    {
      Ok(Ok(_)) => self.apply_catalogue(),
      Ok(Err(error)) => warn!(%error, "model catalogue refresh failed; retaining previous metadata"),
      Err(error) => warn!(%error, "model catalogue refresh timed out; retaining previous metadata"),
    }
  }

  pub(super) fn apply_catalogue(&self) {
    for (provider_id, metadata) in &self.metadata {
      metadata
        .model_cache
        .set_catalogue(catalogue_models(provider_id, &metadata.driver_id));
    }
  }

  pub(super) async fn models(&self, profile_id: &ProfileId, access: &AccessContext) -> Result<Value, ApiError> {
    let profile = self.profile(profile_id)?;
    let bindings = profile
      .allowed_providers(access)
      .flat_map(|(_, provider)| provider.bindings.values().cloned())
      .collect::<Vec<_>>();
    let queried_account = !bindings.is_empty();
    let last_error = if let Ok(_guard) = self.refresh_gate.try_lock() {
      match tokio::time::timeout(
        MODEL_REQUEST_TIMEOUT,
        self.refresh_bindings(bindings, MODEL_REQUEST_TIMEOUT),
      )
      .await
      {
        Ok(error) => error,
        Err(error) => Some(error.to_string()),
      }
    } else {
      None
    };
    let cached = self.upstream.read();
    let mut data = Vec::new();
    let mut seen = HashSet::new();

    for (provider_id, provider) in profile.allowed_providers(access) {
      let Some(metadata) = self.metadata.get(provider_id) else {
        continue;
      };
      // Both sources are advisory. Keep upstream records first so advertised
      // metadata wins for duplicate IDs, then add catalogue-only suggestions.
      let mut provider_models = provider
        .bindings
        .keys()
        .filter_map(|key| cached.get(key))
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
      provider_models.extend(metadata.local_models());
      merge_models(
        &mut data,
        &mut seen,
        provider_models,
        provider_id,
        metadata,
        provider.plain_model_ids,
        provider.qualified_model_ids,
      );
    }

    if data.is_empty() && queried_account {
      return Err(ApiError::upstream(
        axum::http::StatusCode::BAD_GATEWAY,
        last_error.unwrap_or_else(|| "no models available".into()),
      ));
    }
    Ok(list_response(profile.mode, data))
  }

  async fn refresh_bindings(&self, bindings: Vec<Arc<ProviderBinding>>, timeout: Duration) -> Option<String> {
    let requests = bindings.into_iter().map(|binding| async move {
      debug!(
        account = binding.account_id(),
        provider = %binding.provider_id(),
        "v2 model discovery: querying account"
      );
      let result = match tokio::time::timeout(timeout, remote_models(binding.driver().as_ref(), &self.http)).await {
        Ok(Ok(models)) if !models.is_empty() => Ok(models),
        Ok(Ok(_)) => Err("upstream returned no model IDs".to_string()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(error) => Err(error.to_string()),
      };
      (binding, result)
    });
    let mut requests = stream::iter(requests).buffer_unordered(REFRESH_CONCURRENCY);
    let mut last_error = None;
    while let Some((binding, result)) = requests.next().await {
      match result {
        Ok(models) => {
          let mut cached = self.upstream.write();
          cached.insert(binding.key().clone(), models);
          self.publish_upstream(&cached);
        }
        Err(error) => {
          warn!(
            account = binding.account_id(),
            provider = %binding.provider_id(),
            %error,
            "v2 model discovery failed; retaining previous models"
          );
          last_error = Some(error);
        }
      }
    }
    last_error
  }

  fn publish_upstream(&self, cached: &BindingModels) {
    // Account bindings under one provider share a target. Every successful
    // refresh publishes the union, including retained data for other accounts.
    for (provider_id, metadata) in &self.metadata {
      let models = cached
        .iter()
        .filter(|(key, _)| key.provider_id() == provider_id)
        .flat_map(|(_, models)| models.iter().cloned())
        .collect::<Vec<_>>();
      if !models.is_empty() {
        metadata.model_cache.set_models(&models);
      }
    }
  }

  fn profile(&self, id: &ProfileId) -> Result<&ProfileDiscovery, ApiError> {
    self
      .profiles
      .get(id)
      .ok_or_else(|| ApiError::internal("API mount references a missing discovery profile"))
  }
}

impl ProviderMetadata {
  fn catalogue_models(&self) -> Vec<ModelInfo> {
    self
      .model_cache
      .catalogue_models()
      .unwrap_or_else(|| self.models.clone())
  }

  fn local_models(&self) -> Vec<Value> {
    local_models(&self.catalogue_models())
  }
}

impl ProfileDiscovery {
  fn allowed_providers<'a>(
    &'a self,
    access: &'a AccessContext,
  ) -> impl Iterator<Item = (&'a ProviderId, &'a ProfileProvider)> {
    self
      .providers
      .iter()
      .filter(|(id, _)| access.providers.allows(id.as_str()))
  }
}

fn add_binding(
  providers: &mut BTreeMap<ProviderId, ProfileProvider>,
  binding: Arc<ProviderBinding>,
  plain_model_ids: bool,
  qualified_model_ids: Option<QualificationNamespace>,
) {
  let provider = providers.entry(binding.provider_id().clone()).or_default();
  provider.plain_model_ids |= plain_model_ids;
  provider.qualified_model_ids = qualified_model_ids;
  provider.bindings.insert(binding.key().clone(), binding);
}

fn catalogue_models(provider_id: &ProviderId, driver_id: &str) -> Vec<ModelInfo> {
  let models = catalogue_for(provider_id.as_str());
  if !models.is_empty() {
    return models;
  }
  let catalogue_driver = if driver_id == tokn_core::provider::ID_CODEX {
    tokn_core::provider::ID_OPENAI
  } else {
    driver_id
  };
  catalogue_for(catalogue_driver)
}

fn catalogue_for(provider_id: &str) -> Vec<ModelInfo> {
  if tokn_core::provider::ZAI_PROVIDERS.contains(&provider_id) {
    tokn_provider_zai::models::catalogue_for(provider_id)
  } else {
    tokn_catalogue::catalogue::default_models_for(provider_id)
  }
}

async fn remote_models(provider: &dyn Provider, http: &reqwest::Client) -> tokn_core::provider::Result<Vec<Value>> {
  let response = provider.list_models(http).await?;
  Ok(
    response
      .get("data")
      .and_then(Value::as_array)
      .into_iter()
      .flatten()
      .filter(|model| model.get("id").and_then(Value::as_str).is_some_and(|id| !id.is_empty()))
      .cloned()
      .collect(),
  )
}

fn local_models(models: &[ModelInfo]) -> Vec<Value> {
  models
    .iter()
    .map(|model| {
      json!({
        "id": model.id,
        "object": "model",
      })
    })
    .collect()
}

#[allow(clippy::too_many_arguments)]
fn merge_models(
  output: &mut Vec<Value>,
  seen: &mut HashSet<String>,
  models: Vec<Value>,
  provider_id: &ProviderId,
  metadata: &ProviderMetadata,
  plain_model_ids: bool,
  qualified_model_ids: Option<QualificationNamespace>,
) {
  for model in models {
    let upstream_id = model.get("id").and_then(Value::as_str).unwrap_or("");
    if upstream_id.is_empty() {
      continue;
    }
    let rendered_ids = plain_model_ids
      .then(|| upstream_id.to_string())
      .into_iter()
      .chain(qualified_model_ids.map(|namespace| {
        let qualifier = match namespace {
          QualificationNamespace::Provider => provider_id.as_str(),
          QualificationNamespace::Driver => metadata.driver_id.as_str(),
        };
        format!("{qualifier}/{upstream_id}")
      }));
    for rendered_id in rendered_ids {
      if !seen.insert(rendered_id.clone()) {
        continue;
      }
      let mut rendered = model.clone();
      if let Some(object) = rendered.as_object_mut() {
        object.insert("id".into(), Value::String(rendered_id.clone()));
      }
      enrich(&mut rendered, upstream_id, &rendered_id, provider_id, metadata);
      output.push(rendered);
    }
  }
}

fn enrich(
  entry: &mut Value,
  upstream_id: &str,
  rendered_id: &str,
  provider_id: &ProviderId,
  metadata: &ProviderMetadata,
) {
  let mut extension = Map::new();
  extension.insert("provider".into(), json!(provider_id.as_str()));
  extension.insert("provider_display_name".into(), json!(metadata.display_name));
  extension.insert("driver".into(), json!(metadata.driver_id));
  extension.insert("upstream_id".into(), json!(upstream_id));
  extension.insert("model_id".into(), json!(rendered_id));
  extension.insert("auth_kind".into(), metadata.auth_kind.clone());

  let model = metadata
    .model_cache
    .catalogue_model(upstream_id)
    .unwrap_or_else(|| metadata.models.iter().find(|model| model.id == upstream_id).cloned());
  if let Some(model) = &model {
    extension.insert("name".into(), json!(model.name));
    extension.insert(
      "capabilities".into(),
      serde_json::to_value(&model.capabilities).unwrap_or(Value::Null),
    );
    if let Some(cost) = &model.cost {
      extension.insert("cost".into(), serde_json::to_value(cost).unwrap_or(Value::Null));
    }
    extension.insert(
      "limit".into(),
      serde_json::to_value(&model.limit).unwrap_or(Value::Null),
    );
    if let Some(release_date) = &model.release_date {
      extension.insert("release_date".into(), json!(release_date));
    }
  }

  let efforts = tokn_core::provider::upstream_reasoning_efforts(entry)
    .or_else(|| metadata.model_cache.reasoning_efforts(upstream_id))
    .or_else(|| model.as_ref()?.capabilities.reasoning_efforts.clone());
  let capabilities = extension.entry("capabilities").or_insert_with(|| json!({}));
  capabilities["reasoning_efforts"] = json!(efforts);

  if let Some(object) = entry.as_object_mut() {
    object.insert("x_tokn_router".into(), Value::Object(extension));
  }
}

fn list_response(mode: &str, data: Vec<Value>) -> Value {
  json!({
    "object": "list",
    "route_mode": mode,
    "route_modes": [mode],
    "data": data,
  })
}

#[cfg(test)]
mod tests;
