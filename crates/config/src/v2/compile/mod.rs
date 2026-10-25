#[cfg(feature = "std")]
mod listeners;
mod path;
mod resources;
#[cfg(feature = "std")]
mod service;

use crate::v2::{CompileError, RawConfig};
#[cfg(feature = "std")]
use crate::v2::{CompiledConfig, CompiledExecutionConfig};
#[cfg(feature = "std")]
use std::path::Path;
use tokn_policy::ExecutionRules;
#[cfg(feature = "std")]
use tokn_policy::GatewayPlan;

#[cfg(feature = "std")]
pub(super) fn compile_execution_config(raw: &RawConfig) -> Result<CompiledExecutionConfig, CompileError> {
  Ok(CompiledExecutionConfig::new(
    compile_execution(raw)?,
    service::compile_service(&raw.service)?,
  ))
}

#[cfg(feature = "std")]
pub(super) fn compile_config(raw: &RawConfig, source: &Path) -> Result<CompiledConfig, CompileError> {
  let expanded = super::defaults::expand(raw)?;
  let gateway = compile_gateway(&expanded, source).map_err(|error| {
    if raw.defaults.is_some() {
      super::defaults::source_error(raw, error)
    } else {
      error
    }
  })?;
  let service = service::compile_service(&raw.service)?;
  Ok(CompiledConfig::new(gateway, service))
}

#[cfg(feature = "std")]
fn compile_gateway(raw: &RawConfig, source: &Path) -> Result<GatewayPlan, CompileError> {
  if raw.listeners.is_empty() {
    return Err(CompileError::EmptyRegistry { resource: "listener" });
  }

  let resources = resources::compile_resources(raw)?;
  let listeners = listeners::compile_listeners(raw, source, &resources.profiles)?;

  Ok(GatewayPlan::new(
    listeners,
    resources.profiles,
    resources.routes,
    resources.retry_policies,
    resources.account_pools,
    resources.providers,
  ))
}

/// Compile shared resources without requiring a gateway listener or source path.
pub(super) fn compile_execution(raw: &RawConfig) -> Result<ExecutionRules, CompileError> {
  let expanded = super::defaults::expand(raw)?;
  let resources = resources::compile_resources(&expanded).map_err(|error| {
    if raw.defaults.is_some() {
      super::defaults::source_error(raw, error)
    } else {
      error
    }
  })?;
  Ok(ExecutionRules::new(
    resources.profiles,
    resources.routes,
    resources.retry_policies,
    resources.account_pools,
    resources.providers,
  ))
}
