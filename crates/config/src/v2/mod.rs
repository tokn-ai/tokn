//! Strict version 2 configuration decoding and compilation.
//!
//! This module is deliberately separate from the legacy [`crate::Config`]
//! loader. Callers must opt into v2 until the runtime cutover and explicit
//! migration command are complete.

mod compile;
#[cfg(feature = "std")]
mod compiled;
mod cors;
mod defaults;
mod error;
mod logging;
mod models;
mod raw;

#[cfg(feature = "std")]
pub use compiled::{
  CompiledConfig, CompiledExecutionConfig, OutboundPlan, PersistencePaths, PersistencePlan, RequestLimitsPlan,
  ServiceSettings,
};
pub use cors::RawCors;
pub use defaults::RawDefaultPolicy;
pub use error::CompileError;
#[cfg(feature = "std")]
pub use error::{Error, Result};
pub use logging::RawLogging;
pub use models::{ModelRefreshPlan, RawModelRefresh};
pub use raw::{
  RawAccountPool, RawBinding, RawBindingAction, RawClientAuth, RawConfig, RawConnectAction, RawConnectRule,
  RawListener, RawModelSelector, RawOperationPolicy, RawOutbound, RawPersistence, RawPoolStrategy, RawProfile,
  RawProfileBinding, RawProvider, RawProviderSelector, RawQualificationNamespace, RawRelayCredentials,
  RawRelayDestination, RawRequestLimits, RawRetryPolicy, RawRoute, RawRouteRetry, RawService, RawWireIdentity,
};
pub use raw::{
  DEFAULT_ARCHIVE_AFTER_DAYS, DEFAULT_BODY_MAX_BYTES, DEFAULT_FORWARD_PROXY_REQUEST_BODY_MAX_BYTES,
  DEFAULT_MAX_DECODED_BYTES, DEFAULT_MAX_WIRE_BYTES, DEFAULT_PRUNE_AFTER_DAYS, DEFAULT_WRITE_QUEUE_CAPACITY,
  SCHEMA_VERSION,
};

#[cfg(feature = "std")]
mod standard;
#[cfg(feature = "std")]
pub use standard::*;

/// Compile the in-memory configuration into the same execution policy used by
/// the gateway, without requiring listeners, filesystem paths, or a network runtime.
pub fn compile_execution(raw: &RawConfig) -> core::result::Result<tokn_policy::ExecutionRules, CompileError> {
  if raw.schema_version != SCHEMA_VERSION {
    return Err(CompileError::InvalidValue {
      location: "schema_version".into(),
      message: alloc::format!(
        "unsupported schema version {}; expected {}",
        raw.schema_version,
        SCHEMA_VERSION
      ),
    });
  }
  compile::compile_execution(raw)
}
