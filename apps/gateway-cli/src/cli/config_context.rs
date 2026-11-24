use anyhow::Result;
use std::path::PathBuf;
pub use tokn_accounts::context::{AccountView, ConfigContext, ResolvedProviderAuth};
use tokn_config::SchemaConfig;
use tokn_core::account::AccountConfig;
use tokn_router_legacy_config::v2::{project_v2_config, V2ProjectionOptions, V2ProjectionWarning};
/// A native or projected configuration ready for the v2 runtime pipeline.
pub(crate) struct EffectiveV2Config {
  pub compiled: tokn_config::v2::CompiledConfig,
  pub accounts: Vec<AccountConfig>,
  pub config_path: PathBuf,
  pub warnings: Vec<V2ProjectionWarning>,
}

/// Convert the result of the common schema loader into one effective v2
/// runtime configuration. Native v2 accounts pass through unchanged; legacy
/// accounts are normalized together with the in-memory projection.
pub(crate) fn compile_effective_v2_config(
  config: SchemaConfig,
  accounts: Vec<AccountConfig>,
  projection_options: V2ProjectionOptions,
) -> Result<EffectiveV2Config> {
  let config_path = config.path().to_path_buf();
  match config {
    SchemaConfig::Legacy(loaded) => {
      let projection = project_v2_config(&loaded.config, &accounts, projection_options)?;
      let (_, compiled, accounts, warnings) = projection.into_parts();
      Ok(EffectiveV2Config {
        compiled,
        accounts,
        config_path,
        warnings,
      })
    }
    SchemaConfig::V2 { config, .. } => Ok(EffectiveV2Config {
      compiled: *config,
      accounts,
      config_path,
      warnings: Vec::new(),
    }),
  }
}
