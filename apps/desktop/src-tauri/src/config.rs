use anyhow::{bail, Context, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
  net::SocketAddr,
  path::{Path, PathBuf},
};
use toml_edit::DocumentMut;

mod routing;
pub use routing::RoutingOverview;

const SECTIONS: &[&str] = &["defaults", "profiles", "routes", "model_scores"];

#[derive(Serialize)]
pub struct RoutingDocument {
  pub config_path: String,
  pub revision: String,
  pub schema: String,
  pub overlay_paths: Vec<String>,
  pub routing_toml: String,
  pub overview: Option<RoutingOverview>,
  pub overview_error: Option<String>,
}

pub fn path() -> Result<PathBuf> {
  // An environment override supports isolated local testing without exposing
  // arbitrary filesystem writes to the webview.
  Ok(
    std::env::var_os("TOKN_DESKTOP_CONFIG")
      .map(PathBuf::from)
      .unwrap_or(tokn_config::paths::config_path()?),
  )
}

pub fn load() -> Result<tokn_config::SchemaConfig> {
  Ok(tokn_config::load_config(Some(&path()?))?)
}

pub fn address() -> Result<SocketAddr> {
  match load()? {
    tokn_config::SchemaConfig::Legacy(loaded) => {
      let host = &loaded.config.server.host;
      let ip = if host == "localhost" {
        "127.0.0.1".parse()?
      } else {
        host.parse()?
      };
      let address = SocketAddr::new(ip, loaded.config.server.port);
      if !address.ip().is_loopback() || address.port() == 0 {
        bail!("Configure a fixed loopback server address to manage the gateway");
      }
      Ok(address)
    }
    tokn_config::SchemaConfig::V2 { .. } => {
      let raw = tokn_config::v2::load_raw(&path()?)?;
      raw
        .listeners
        .values()
        .find_map(|listener| match listener {
          tokn_config::v2::RawListener::LlmApi { bind, .. } => bind
            .parse::<SocketAddr>()
            .ok()
            .filter(|address| address.ip().is_loopback() && address.port() != 0),
          _ => None,
        })
        .context("Configure a loopback llm_api listener to manage the gateway from Tokn Desktop")
    }
  }
}

fn overlays(path: &Path, schema: tokn_config::ConfigSchema) -> Result<Vec<(PathBuf, Vec<u8>)>> {
  if schema != tokn_config::ConfigSchema::Legacy {
    return Ok(Vec::new());
  }
  let directory = tokn_config::paths::config_fragment_dir(path);
  if !directory.exists() {
    return Ok(Vec::new());
  }
  let mut paths = std::fs::read_dir(directory)?
    .map(|entry| entry.map(|entry| entry.path()))
    .collect::<std::io::Result<Vec<_>>>()?;
  paths.retain(|path| path.extension().is_some_and(|extension| extension == "toml"));
  paths.sort();
  paths
    .into_iter()
    .map(|path| Ok((path.clone(), std::fs::read(path)?)))
    .collect()
}

fn snapshot_revision(contents: &str, overlays: &[(PathBuf, Vec<u8>)]) -> String {
  let mut hash = Sha256::new();
  hash.update(revision(contents));
  for (path, bytes) in overlays {
    hash.update(path.as_os_str().as_encoded_bytes());
    hash.update(Sha256::digest(bytes));
  }
  format!("{:x}", hash.finalize())
}

fn revision(contents: &str) -> String {
  format!("{:x}", Sha256::digest(contents.as_bytes()))
}

pub fn read_at(path: &Path) -> Result<RoutingDocument> {
  let contents = std::fs::read_to_string(path)?;
  let document: DocumentMut = contents.parse()?;
  let schema = document_schema(&document)?;
  let overlays = overlays(path, schema)?;
  let (overview, overview_error) = match routing::summarize(path, &contents, &document, &overlays, schema) {
    Ok(overview) => (Some(overview), None),
    Err(error) => (None, Some(format!("{error:#}"))),
  };
  let mut routing = DocumentMut::new();
  for section in SECTIONS {
    if let Some(value) = document.get(section) {
      routing[section] = value.clone();
    }
  }
  Ok(RoutingDocument {
    config_path: path.display().to_string(),
    revision: snapshot_revision(&contents, &overlays),
    schema: if schema == tokn_config::ConfigSchema::V2 {
      "v2"
    } else {
      "legacy"
    }
    .into(),
    overlay_paths: overlays.iter().map(|(path, _)| path.display().to_string()).collect(),
    routing_toml: routing.to_string(),
    overview,
    overview_error,
  })
}

fn document_schema(document: &DocumentMut) -> Result<tokn_config::ConfigSchema> {
  match document.get("schema_version") {
    None => Ok(tokn_config::ConfigSchema::Legacy),
    Some(marker) => match marker.as_integer() {
      Some(version) if version == i64::from(tokn_config::v2::SCHEMA_VERSION) => Ok(tokn_config::ConfigSchema::V2),
      Some(version) => bail!("Unsupported configuration schema version: {version}"),
      None => bail!("Configuration schema_version must be an integer"),
    },
  }
}

pub fn save_at(path: &Path, expected_revision: &str, routing_toml: &str) -> Result<RoutingDocument> {
  let lock = tokn_config::lock_config_file(path)?;
  let original = std::fs::read_to_string(path)?;
  let mut document: DocumentMut = original.parse()?;
  let schema = document_schema(&document)?;
  let overlays = overlays(path, schema)?;
  if snapshot_revision(&original, &overlays) != expected_revision {
    bail!("Configuration changed on disk. Reload before saving; your draft has been retained.");
  }
  let routing: DocumentMut = routing_toml.parse()?;
  for (key, _) in routing.iter() {
    if !SECTIONS.contains(&key) {
      bail!("Only defaults, profiles, routes and model_scores can be edited here (found {key})");
    }
  }
  for section in SECTIONS {
    document.remove(section);
    if let Some(value) = routing.get(section) {
      document[section] = value.clone();
    }
  }
  let candidate = document.to_string();
  match schema {
    tokn_config::ConfigSchema::V2 => {
      tokn_config::v2::parse_config(&candidate, path)?;
    }
    tokn_config::ConfigSchema::Legacy => {
      if routing.contains_key("routes") {
        bail!("Legacy configuration uses defaults and profiles, not routes");
      }
      routing::load_legacy_snapshot(path, &candidate, &overlays)?;
    }
  }
  if snapshot_revision(&original, &self::overlays(path, schema)?) != expected_revision {
    bail!("Configuration fragments changed during validation. Reload before saving.");
  }
  lock.replace_contents_if_unchanged(Some(original.as_bytes()), candidate.as_bytes())?;
  read_at(path)
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn legacy_edits_validate_fragments_and_detect_overlay_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "[server]\nport = 4141\n").unwrap();
    let fragments = dir.path().join("config.d");
    std::fs::create_dir(&fragments).unwrap();
    let fragment = fragments.join("scores.toml");
    std::fs::write(&fragment, "[model_scores.\"gpt-*\"]\ncodex = 2\n").unwrap();
    let first = read_at(&path).unwrap();
    assert_eq!(first.schema, "legacy");
    assert_eq!(first.overlay_paths.len(), 1);
    save_at(&path, &first.revision, "[model_scores.\"deepseek-*\"]\ndeepseek = 10\n").unwrap();
    let second = read_at(&path).unwrap();
    std::fs::write(&fragment, "[model_scores.\"gpt-*\"]\ncodex = 3\n").unwrap();
    assert!(save_at(&path, &second.revision, "").is_err());
    assert!(std::fs::read_to_string(&path).unwrap().contains("deepseek"));
  }

  #[test]
  fn edits_preserve_service_settings_and_reject_stale_or_invalid_drafts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "schema_version = 2\n# retained\n[service]\n[defaults]\n[listeners.local]\nkind = 'llm_api'\nbind = '127.0.0.1:4141'\nclient_auth = 'none'\n").unwrap();
    let first = read_at(&path).unwrap();
    let saved = save_at(
      &path,
      &first.revision,
      "[defaults]\n[model_scores.\"gpt-*\"]\ncodex = 10\n",
    )
    .unwrap();
    assert!(std::fs::read_to_string(&path).unwrap().contains("# retained"));
    assert!(save_at(&path, &first.revision, "[defaults]\n").is_err());
    assert!(save_at(&path, &saved.revision, "[profiles.broken]\nroute = 'missing'\n").is_err());
    assert!(save_at(&path, &saved.revision, "[service]\n").is_err());
    assert_eq!(read_at(&path).unwrap().revision, saved.revision);
  }
}
