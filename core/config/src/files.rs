//! Capture legacy configuration fragments and evaluate primary TOML against their contents.

use crate::{
  error, paths, standard::validated_primary_config, AgentConfig, AgentId, Config, ConfigSchema, ConfigSources, Error,
  LoadedConfig, ProfileConfig, Result,
};
use serde::Deserialize;
use snafu::ResultExt;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Ordered fragment paths and bytes captured for a primary configuration.
pub struct ConfigFragments {
  primary_path: PathBuf,
  fragments: Vec<(PathBuf, Vec<u8>)>,
}

impl ConfigFragments {
  /// Capture sorted legacy fragments. Version 2 has no fragment directory.
  pub fn read(primary_path: &Path, schema: ConfigSchema) -> Result<Self> {
    let mut fragments = Vec::new();
    if schema == ConfigSchema::Legacy {
      for path in fragment_paths(primary_path)? {
        let bytes = std::fs::read(&path).context(error::ReadSnafu { path: &path })?;
        fragments.push((path, bytes));
      }
    }
    Ok(Self {
      primary_path: primary_path.to_path_buf(),
      fragments,
    })
  }

  /// Iterate over the original paths and bytes in merge order.
  pub fn iter(&self) -> impl Iterator<Item = (&Path, &[u8])> {
    self
      .fragments
      .iter()
      .map(|(path, contents)| (path.as_path(), contents.as_slice()))
  }

  /// Validate the supplied primary TOML and merge the captured fragments without accessing disk.
  pub fn parse_legacy(&self, primary_text: &str) -> Result<LegacyConfigSnapshot<'_>> {
    let path = &self.primary_path;
    let document = toml::from_str(primary_text).context(error::ParseSnafu { path })?;
    let mut snapshot = LegacyConfigSnapshot {
      config: validated_primary_config(document, path)?,
      agents: BTreeMap::new(),
      profiles: BTreeMap::new(),
      model_scores: BTreeMap::new(),
    };
    let mut profile_owners = BTreeMap::new();
    for (path, contents) in self.iter() {
      let fragment = parse_fragment(path, contents)?;
      record_sources(&mut snapshot.agents, &fragment.agents, path);
      record_sources(&mut snapshot.profiles, &fragment.profiles, path);
      record_sources(&mut snapshot.model_scores, &fragment.model_scores, path);
      apply_config_fragment(&mut snapshot.config, path, fragment, &mut profile_owners)?;
    }
    snapshot.config.validate()?;
    Ok(snapshot)
  }
}

fn record_sources<'a, T>(sources: &mut BTreeMap<String, &'a Path>, entries: &BTreeMap<String, T>, path: &'a Path) {
  for name in entries.keys() {
    sources.entry(name.clone()).or_insert(path);
  }
}

/// The merged legacy configuration and the fragments that supplied its authored entries.
#[derive(Debug)]
pub struct LegacyConfigSnapshot<'a> {
  pub config: Config,
  agents: BTreeMap<String, &'a Path>,
  profiles: BTreeMap<String, &'a Path>,
  model_scores: BTreeMap<String, &'a Path>,
}

impl LegacyConfigSnapshot<'_> {
  /// Return the first fragment defining this agent, profile, or model-score key in load order.
  pub fn fragment_source(&self, section: &str, name: &str) -> Option<&Path> {
    let sources = match section {
      "agents" => &self.agents,
      "profiles" => &self.profiles,
      "model_scores" => &self.model_scores,
      _ => return None,
    };
    sources.get(name).copied()
  }
}

/// List the sorted TOML files in the primary configuration's matching fragment directory.
pub fn fragment_paths(primary_path: &Path) -> Result<Vec<PathBuf>> {
  let directory = paths::config_fragment_dir(primary_path);
  let entries = match std::fs::read_dir(&directory) {
    Ok(entries) => entries,
    Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
    Err(source) => {
      return Err(Error::Read {
        path: directory,
        source,
      })
    }
  };
  let mut fragments = Vec::new();
  for entry in entries {
    let path = entry.context(error::ReadSnafu { path: &directory })?.path();
    if path.is_file() && path.extension().is_some_and(|extension| extension == "toml") {
      fragments.push(path);
    }
  }
  fragments.sort();
  Ok(fragments)
}

fn parse_fragment(path: &Path, contents: &[u8]) -> Result<ConfigFragment> {
  let text = std::str::from_utf8(contents).map_err(|source| Error::Read {
    path: path.to_owned(),
    source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
  })?;
  toml::from_str(text).context(error::ParseSnafu { path })
}

/// A deliberately narrow runtime configuration overlay. Agent integration
/// tooling owns agent/profile entries, while operators may keep model scores
/// in a separate fragment. Other root configuration remains primary-file only.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFragment {
  #[serde(default)]
  agents: BTreeMap<String, AgentConfig>,
  #[serde(default)]
  profiles: BTreeMap<String, ProfileConfig>,
  #[serde(default)]
  model_scores: BTreeMap<String, BTreeMap<String, i32>>,
}

pub(crate) fn load_config_overlays(path: &Path, mut config: Config) -> Result<LoadedConfig> {
  let fragment_dir = paths::config_fragment_dir(path);
  let fragments = fragment_paths(path)?;
  let sources = ConfigSources {
    root: path.to_path_buf(),
    fragment_dir,
    fragments,
  };
  let mut fragment_profile_owners = BTreeMap::new();
  for fragment_path in &sources.fragments {
    let contents = std::fs::read(fragment_path).context(error::ReadSnafu { path: fragment_path })?;
    let fragment = parse_fragment(fragment_path, &contents)?;
    apply_config_fragment(&mut config, fragment_path, fragment, &mut fragment_profile_owners)?;
  }
  Ok(LoadedConfig { config, sources })
}

fn apply_config_fragment(
  cfg: &mut Config,
  path: &Path,
  fragment: ConfigFragment,
  fragment_profile_owners: &mut BTreeMap<String, AgentId>,
) -> Result<()> {
  if fragment.agents.is_empty() && fragment.profiles.is_empty() {
    merge_model_scores(&mut cfg.model_scores, fragment.model_scores);
    return Ok(());
  }

  let agent_name = path
    .file_stem()
    .and_then(|name| name.to_str())
    .filter(|name| !name.is_empty())
    .ok_or_else(|| Error::Other {
      message: format!("agent config fragment has no valid filename: {}", path.display()),
    })?;
  let agent = AgentId::from(agent_name);
  if agent.as_str() != agent_name {
    return Err(Error::Other {
      message: format!(
        "agent config fragment {} must use the canonical agent filename {}.toml",
        path.display(),
        agent.as_str()
      ),
    });
  }
  if fragment.agents.len() > 1 || (!fragment.agents.is_empty() && !fragment.agents.contains_key(agent_name)) {
    return Err(Error::Other {
      message: format!(
        "agent config fragment {} may define only the legacy [agents.{agent_name}] binding",
        path.display()
      ),
    });
  }

  for (profile_name, profile) in &fragment.profiles {
    if profile.agent_id.as_ref() != Some(&agent) {
      return Err(Error::Other {
        message: format!(
          "profile '{profile_name}' in {} must set agent_id = '{}'",
          path.display(),
          agent.as_str()
        ),
      });
    }
    if let Some(owner) = fragment_profile_owners.insert(profile_name.clone(), agent.clone()) {
      return Err(Error::Other {
        message: format!(
          "profile '{profile_name}' is managed by both {} and {} agent fragments",
          owner.as_str(),
          agent.as_str()
        ),
      });
    }
    if let Some(existing) = cfg.profiles.get(profile_name) {
      if existing.agent_id.as_ref() != Some(&agent) {
        return Err(Error::Other {
          message: format!(
            "profile '{profile_name}' in {} conflicts with a profile not owned by {}",
            path.display(),
            agent.as_str()
          ),
        });
      }
    }
  }

  let binding = fragment.agents.get(agent_name).cloned();
  if let Some(profile_name) = binding.as_ref().and_then(|binding| binding.profile.as_deref()) {
    let Some(profile) = fragment.profiles.get(profile_name) else {
      return Err(Error::Other {
        message: format!(
          "agent config fragment {} must define [profiles.{profile_name}] for its legacy [agents.{agent_name}].profile",
          path.display()
        ),
      });
    };
    if profile.agent_id.as_ref() != Some(&agent) {
      return Err(Error::Other {
        message: format!(
          "profile '{profile_name}' in {} must set agent_id = '{}'",
          path.display(),
          agent.as_str()
        ),
      });
    }
  }

  // A legacy root binding materialized its base profile and provider-specific
  // children under the binding name. Mask precisely that set while the
  // sidecar is active, so an old route cannot remain reachable after a
  // relink without hiding unrelated profiles that merely share an agent
  // persona.
  let legacy_profile = cfg
    .agents
    .get(agent_name)
    .and_then(|existing| existing.profile.as_deref())
    .map(str::to_string);
  if let Some(legacy_profile) = legacy_profile.as_deref() {
    remove_legacy_agent_profiles(cfg, legacy_profile, &agent);
  }
  if let Some(binding) = binding {
    cfg.agents.insert(agent_name.to_string(), binding);
  } else {
    cfg.agents.remove(agent_name);
  }
  cfg.profiles.extend(fragment.profiles);
  merge_model_scores(&mut cfg.model_scores, fragment.model_scores);
  Ok(())
}

fn merge_model_scores(
  scores: &mut BTreeMap<String, BTreeMap<String, i32>>,
  fragment: BTreeMap<String, BTreeMap<String, i32>>,
) {
  for (model, provider_scores) in fragment {
    scores.entry(model).or_default().extend(provider_scores);
  }
}

fn remove_legacy_agent_profiles(cfg: &mut Config, profile: &str, agent: &AgentId) {
  let prefix = format!("{profile}-");
  cfg.profiles.retain(|name, existing| {
    // Provider children created by the old link writer always carried their
    // account allow-list. Keep a same-persona, similarly named user profile
    // without that migration shape visible rather than treating `agent_id`
    // itself as ownership evidence.
    !(name == profile
      || (name.starts_with(&prefix)
        && existing.agent_id.as_ref() == Some(agent)
        && existing.accounts.as_ref().is_some_and(|accounts| !accounts.is_empty())))
  });
}
