//! Read-only summaries of the exact configuration snapshot shown by the editor.
use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tokn_config::{ConfigSchema, RouteMode};
use tokn_policy::{
  AccountSelectionStrategy, GatewayPlan, ManagedRetry, ModelSelector, OperationPolicy, ProviderSelector,
  QualificationNamespace, RelayCredentials, RelayDestination, RelayRetry, RoutePlan, WireIdentity,
};
use toml_edit::DocumentMut;

#[derive(Debug, Serialize)]
pub struct RoutingOverview {
  pub profiles: Vec<RoutingProfileSummary>,
  pub routes: Vec<RoutingRouteSummary>,
  pub model_score_count: usize,
}

#[derive(Debug, Serialize)]
pub struct RoutingSource {
  pub path: String,
  pub section: String,
  pub generated: bool,
}

#[derive(Debug, Serialize)]
pub struct RoutingSummaryField {
  pub label: String,
  pub value: String,
}

#[derive(Debug, Serialize)]
pub struct RoutingProfileSummary {
  pub id: String,
  pub route_id: Option<String>,
  pub route_kind: String,
  pub api_path: Option<String>,
  pub source: RoutingSource,
  pub fields: Vec<RoutingSummaryField>,
}

#[derive(Debug, Serialize)]
pub struct RoutingRouteSummary {
  pub id: String,
  pub kind: String,
  pub target: String,
  pub source: RoutingSource,
  pub fields: Vec<RoutingSummaryField>,
  pub used_by: Vec<String>,
}

pub(super) fn summarize(
  path: &Path,
  contents: &str,
  document: &DocumentMut,
  overlays: &[(PathBuf, Vec<u8>)],
  schema: ConfigSchema,
) -> Result<RoutingOverview> {
  match schema {
    ConfigSchema::V2 => v2_summary(path, contents),
    ConfigSchema::Legacy => legacy_summary(path, contents, document, overlays),
  }
}

fn field(label: &str, value: impl Into<String>) -> RoutingSummaryField {
  RoutingSummaryField {
    label: label.to_owned(),
    value: value.into(),
  }
}

fn section(group: &str, id: &str) -> String {
  format!("{group}.{}", toml_edit::Key::new(id))
}

fn source(path: &Path, section: String, generated: bool) -> RoutingSource {
  RoutingSource {
    path: path.display().to_string(),
    section,
    generated,
  }
}

fn join_names(values: impl Iterator<Item = impl AsRef<str>>, empty: &str) -> String {
  let values = values.map(|value| value.as_ref().to_owned()).collect::<Vec<_>>();
  if values.is_empty() {
    empty.to_owned()
  } else {
    values.join(", ")
  }
}

fn v2_summary(path: &Path, contents: &str) -> Result<RoutingOverview> {
  let raw = tokn_config::v2::decode(contents, path)?;
  let compiled = tokn_config::v2::compile_config(&raw, path)?;
  let plan = compiled.gateway();
  let generated_default = raw.defaults.is_some();
  let authored_source = |group: &str, id: &str| {
    let generated = generated_default && id == "default";
    source(
      path,
      if generated {
        "defaults".to_owned()
      } else {
        section(group, id)
      },
      generated,
    )
  };

  let mut profiles = Vec::with_capacity(plan.profiles().len());
  for (id, profile) in plan.profiles() {
    let route = plan.route(profile.route()).context("Compiled profile has no route")?;
    let mut fields = vec![field(
      "Wire identity",
      match profile.wire_identity() {
        WireIdentity::None => "None".to_owned(),
        WireIdentity::ProviderDefault => "Provider default".to_owned(),
        WireIdentity::Named(id) => id.to_string(),
      },
    )];
    if let Some(pool_id) = profile.account_pool() {
      let pool = plan
        .account_pool(pool_id)
        .context("Compiled profile has no account pool")?;
      let accounts = pool.selector().accounts().map_or_else(
        || "All accounts".to_owned(),
        |accounts| join_names(accounts.iter().map(|id| id.as_str()), "No accounts"),
      );
      fields.extend([
        field("Account selection", accounts),
        field(
          "Pool strategy",
          match pool.strategy() {
            AccountSelectionStrategy::RoundRobin => "Round robin",
          },
        ),
        field(
          "Failure cooldown",
          format!("{} seconds", pool.failure_cooldown().as_secs()),
        ),
      ]);
      if let Some(affinity) = pool.session_affinity() {
        fields.push(field(
          "Session affinity",
          format!("{} seconds", affinity.ttl().as_secs()),
        ));
        fields.push(field(
          "Expired session retention",
          format!("{} seconds", affinity.expired_retention().as_secs()),
        ));
      }
    } else {
      fields.push(field("Account selection", "Client credentials · no account pool"));
    }
    if let Some(binding) = profile.api_binding() {
      fields.push(field(
        "API operations",
        join_names(binding.endpoints().iter().map(|id| id.as_str()), "None"),
      ));
    }
    profiles.push(RoutingProfileSummary {
      id: id.to_string(),
      route_id: Some(profile.route().to_string()),
      route_kind: route_kind(route).to_owned(),
      api_path: profile.api_binding().map(|binding| binding.path().to_owned()),
      source: authored_source("profiles", id.as_str()),
      fields,
    });
  }

  let routes = plan
    .routes()
    .iter()
    .map(|(id, route)| {
      let (target, mut fields) = route_fields(plan, route);
      fields.push(field(
        "Provider restrictions",
        route.providers().map_or_else(
          || "All configured providers".to_owned(),
          |providers| join_names(providers.iter().map(|id| id.as_str()), "No providers"),
        ),
      ));
      RoutingRouteSummary {
        id: id.to_string(),
        kind: route_kind(route).to_owned(),
        target,
        source: authored_source("routes", id.as_str()),
        fields,
        used_by: plan
          .profiles()
          .iter()
          .filter(|(_, profile)| profile.route() == id)
          .map(|(id, _)| id.to_string())
          .collect(),
      }
    })
    .collect();
  Ok(RoutingOverview {
    profiles,
    routes,
    model_score_count: raw.model_scores.len(),
  })
}

fn route_kind(route: &RoutePlan) -> &'static str {
  match route {
    RoutePlan::Managed(_) => "managed",
    RoutePlan::Relay(_) => "relay",
  }
}

fn retry_policy(plan: &GatewayPlan, kind: &str, id: &tokn_policy::RetryPolicyId) -> String {
  let settings = plan
    .retry_policy(id)
    .map(|policy| {
      format!(
        " ({} retries, {} ms initial backoff)",
        policy.max_retries(),
        policy.initial_backoff().as_millis()
      )
    })
    .unwrap_or_default();
  format!("{kind} · {id}{settings}")
}

fn route_fields(plan: &GatewayPlan, route: &RoutePlan) -> (String, Vec<RoutingSummaryField>) {
  match route {
    RoutePlan::Managed(route) => {
      let target = match route.target().provider() {
        ProviderSelector::Any => "Automatic provider".to_owned(),
        ProviderSelector::Fixed(id) => id.to_string(),
      };
      let model = match route.target().model() {
        ModelSelector::Capability => "Model capability".to_owned(),
        ModelSelector::Qualified { namespace } => match namespace {
          QualificationNamespace::Driver => "Driver-qualified model".to_owned(),
          QualificationNamespace::Provider => "Provider-qualified model".to_owned(),
        },
        ModelSelector::Family(families) => families
          .iter()
          .map(|family| {
            format!(
              "{}: {}",
              family.name(),
              join_names(family.members().iter(), "No models")
            )
          })
          .collect::<Vec<_>>()
          .join("; "),
      };
      let retry = match route.retry() {
        ManagedRetry::Never => "Never".to_owned(),
        ManagedRetry::Recoverable(id) => retry_policy(plan, "Recoverable", id),
      };
      (
        target,
        vec![
          field("Model selection", model),
          field(
            "Operation handling",
            match route.operation() {
              OperationPolicy::Preserve => "Preserve",
              OperationPolicy::TranslateCompatible => "Translate compatible operations",
            },
          ),
          field("Credentials", "Profile account pool"),
          field("Retry", retry),
        ],
      )
    }
    RoutePlan::Relay(route) => {
      let target = match route.destination() {
        RelayDestination::Original => "Original request destination".to_owned(),
        RelayDestination::FixedProvider(id) => id.to_string(),
      };
      let retry = match route.retry() {
        RelayRetry::Never => "Never".to_owned(),
        RelayRetry::SafeMethods(id) => retry_policy(plan, "Safe methods", id),
        RelayRetry::Buffered(id) => retry_policy(plan, "Buffered replay", id),
      };
      (
        target,
        vec![
          field("Payload handling", "Preserve payload bytes"),
          field(
            "Credentials",
            match route.credentials() {
              RelayCredentials::Client => "Client credentials",
              RelayCredentials::AccountPool => "Profile account pool",
            },
          ),
          field("Retry", retry),
        ],
      )
    }
  }
}

pub(super) fn load_legacy_snapshot(
  path: &Path,
  contents: &str,
  overlays: &[(PathBuf, Vec<u8>)],
) -> Result<tokn_config::Config> {
  // The public legacy loader owns overlay validation and merge semantics. Feed
  // it the already-read bytes so the summary cannot race a second live read.
  let stage = tempfile::tempdir()?;
  let filename = path.file_name().context("Configuration path has no filename")?;
  let staged_path = stage.path().join(filename);
  std::fs::write(&staged_path, contents)?;
  if !overlays.is_empty() {
    let fragment_dir = tokn_config::paths::config_fragment_dir(&staged_path);
    std::fs::create_dir(&fragment_dir)?;
    for (source, contents) in overlays {
      std::fs::write(
        fragment_dir.join(source.file_name().context("Fragment path has no filename")?),
        contents,
      )?;
    }
  }
  tokn_config::Config::load(Some(&staged_path))
    .map(|(config, _)| config)
    .map_err(|error| {
      let original_parent = path.parent().unwrap_or_else(|| Path::new("."));
      anyhow::anyhow!(error.to_string().replace(
        &stage.path().display().to_string(),
        &original_parent.display().to_string()
      ))
    })
}

fn legacy_summary(
  path: &Path,
  contents: &str,
  document: &DocumentMut,
  overlays: &[(PathBuf, Vec<u8>)],
) -> Result<RoutingOverview> {
  let config = load_legacy_snapshot(path, contents, overlays)?;
  let mut profile_sources = BTreeMap::new();
  for (overlay_path, bytes) in overlays {
    let overlay: DocumentMut = std::str::from_utf8(bytes)?.parse()?;
    if let Some(profiles) = overlay.get("profiles").and_then(toml_edit::Item::as_table_like) {
      for (id, _) in profiles.iter() {
        profile_sources.insert(id.to_owned(), source(overlay_path, section("profiles", id), false));
      }
    }
  }
  let defaults = &config.defaults;
  let default_mode = if defaults.mode == RouteMode::Route && config.server.route_mode != RouteMode::Route {
    config.server.route_mode
  } else {
    defaults.mode
  };
  let default_families = if defaults.model_families.is_empty() {
    &config.model_families
  } else {
    &defaults.model_families
  };
  let mut profiles = vec![RoutingProfileSummary {
    id: "default".to_owned(),
    route_id: None,
    route_kind: legacy_mode(default_mode).to_owned(),
    api_path: Some("/v1".to_owned()),
    source: source(path, "defaults".to_owned(), !document.contains_key("defaults")),
    fields: legacy_fields(
      &config,
      defaults.default_provider_id.as_deref(),
      defaults.providers.as_deref(),
      defaults.accounts.as_deref(),
      default_families,
      defaults.agent_id.as_ref(),
    ),
  }];
  for (id, profile) in &config.profiles {
    let mode = profile.mode.unwrap_or(default_mode);
    profiles.push(RoutingProfileSummary {
      id: id.clone(),
      route_id: None,
      route_kind: legacy_mode(mode).to_owned(),
      api_path: Some(legacy_api_path(id)?),
      source: profile_sources
        .remove(id)
        .unwrap_or_else(|| source(path, section("profiles", id), false)),
      fields: legacy_fields(
        &config,
        profile
          .default_provider_id
          .as_deref()
          .or(defaults.default_provider_id.as_deref()),
        profile.providers.as_deref().or(defaults.providers.as_deref()),
        profile.accounts.as_deref().or(defaults.accounts.as_deref()),
        profile.model_families.as_deref().unwrap_or(default_families),
        profile.agent_id.as_ref().or(defaults.agent_id.as_ref()),
      ),
    });
  }
  Ok(RoutingOverview {
    profiles,
    routes: Vec::new(),
    model_score_count: config.model_scores.len(),
  })
}

fn legacy_mode(mode: RouteMode) -> &'static str {
  match mode {
    RouteMode::Passthrough => "passthrough",
    RouteMode::Switch => "switch",
    RouteMode::Exact => "exact",
    RouteMode::Route => "route",
    RouteMode::Fuzzy => "fuzzy",
  }
}

fn legacy_api_path(profile: &str) -> Result<String> {
  // Match the legacy runtime projection's RFC 3986 path-segment encoding in
  // crates/legacy-config/src/v2/analysis.rs, including literal percent signs.
  anyhow::ensure!(
    profile != "." && profile != "..",
    "Profile `{profile}` has no valid API path"
  );
  let mut encoded = String::with_capacity(profile.len());
  for byte in profile.bytes() {
    if byte.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@".contains(&byte) {
      encoded.push(char::from(byte));
    } else {
      use std::fmt::Write;
      write!(&mut encoded, "%{byte:02X}").expect("writing to a String cannot fail");
    }
  }
  Ok(format!("/{encoded}/v1"))
}

fn legacy_fields(
  config: &tokn_config::Config,
  provider: Option<&str>,
  providers: Option<&[String]>,
  accounts: Option<&[String]>,
  families: &[tokn_config::ModelFamily],
  agent: Option<&tokn_config::AgentId>,
) -> Vec<RoutingSummaryField> {
  let selection = |values: Option<&[String]>, all: &str| {
    values.map_or_else(|| all.to_owned(), |values| join_names(values.iter(), "None"))
  };
  let mut fields = vec![
    field("Default provider", provider.unwrap_or("Not set")),
    field(
      "Provider restrictions",
      selection(providers, "All configured providers"),
    ),
    field("Account selection", selection(accounts, "All accounts")),
    field("Pool strategy", config.pool.strategy.replace('_', " ")),
  ];
  if let Some(agent) = agent {
    fields.push(field("Agent identity", agent.as_str()));
  }
  if !families.is_empty() {
    fields.push(field(
      "Model families",
      families
        .iter()
        .map(|family| format!("{}: {}", family.name, join_names(family.members.iter(), "No models")))
        .collect::<Vec<_>>()
        .join("; "),
    ));
  }
  fields
}

#[cfg(test)]
mod tests;
