use crate::cli::config_context::{compile_effective_v2_config, EffectiveV2Config};
use anyhow::Result;
use clap::{Subcommand, ValueEnum};
use std::path::{Path, PathBuf};
use tokn_router_legacy_config::v2::{V2ProjectionOptions, V2ProjectionWarning};

mod model;
mod provider;
mod send;

pub use model::ModelArgs;
pub use provider::ProviderArgs;
pub use send::SendArgs;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
  Text,
  Json,
}

#[derive(Subcommand, Debug)]
pub enum SmokeCmd {
  /// Send a request through a configured LLM API listener.
  Send(SendArgs),
  /// Show providers that support a model.
  Model(ModelArgs),
  /// Show configuration, driver metadata, and models for a provider.
  Provider(ProviderArgs),
}

pub async fn run_cmd(cfg_path: Option<PathBuf>, cmd: SmokeCmd) -> Result<()> {
  match cmd {
    SmokeCmd::Send(args) => send::run(cfg_path, args).await,
    SmokeCmd::Model(args) => model::run(args).await,
    SmokeCmd::Provider(args) => provider::run(cfg_path, args).await,
  }
}

fn load_effective_v2_config(explicit: Option<&Path>) -> Result<EffectiveV2Config> {
  let config = tokn_config::load_config(explicit)?;
  let projected_legacy = config.schema() == tokn_config::ConfigSchema::Legacy;
  let config_path = config.path().to_path_buf();
  let accounts = crate::server_runtime::load_accounts(Some(&config_path))?;
  let effective = compile_effective_v2_config(config, accounts, V2ProjectionOptions::default())?;
  if projected_legacy {
    log_projection_warnings(&effective.config_path, &effective.warnings);
  }
  Ok(effective)
}

fn log_projection_warnings(config_path: &Path, warnings: &[V2ProjectionWarning]) {
  tracing::warn!(
    config = %config_path.display(),
    warning_count = warnings.len(),
    "legacy config is running through the in-memory v2 smoke runtime"
  );
  for warning in warnings {
    tracing::warn!(config = %config_path.display(), warning = %warning, "legacy-to-v2 projection warning");
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn legacy_config_is_projected_instead_of_entering_the_strict_v2_loader() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.toml");
    std::fs::write(&path, "[server]\nport = 4242\n").unwrap();
    let config = tokn_config::load_config(Some(&path)).unwrap();
    let account = toml::from_str(
      r#"id = "primary"
provider = "github-copilot"
enabled = true
"#,
    )
    .unwrap();

    let effective = compile_effective_v2_config(config, vec![account], V2ProjectionOptions::default()).unwrap();

    assert_eq!(effective.accounts.len(), 1);
    assert_eq!(
      effective
        .compiled
        .gateway()
        .listeners()
        .values()
        .next()
        .unwrap()
        .bind()
        .port(),
      4242
    );
  }
}
