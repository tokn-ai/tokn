use std::path::PathBuf;
use tokn_policy::{ExecutionRules, GatewayPlan};

/// Version 2 execution policy and service settings without gateway admission.
///
/// SDKs use this form without requiring or initializing listeners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledExecutionConfig {
  execution: ExecutionRules,
  service: ServiceSettings,
}

impl CompiledExecutionConfig {
  pub(super) fn new(execution: ExecutionRules, service: ServiceSettings) -> Self {
    Self { execution, service }
  }

  pub fn execution(&self) -> &ExecutionRules {
    &self.execution
  }

  pub fn service(&self) -> &ServiceSettings {
    &self.service
  }

  pub fn into_parts(self) -> (ExecutionRules, ServiceSettings) {
    (self.execution, self.service)
  }
}

/// A fully decoded and semantically compiled version 2 configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledConfig {
  gateway: GatewayPlan,
  service: ServiceSettings,
}

impl CompiledConfig {
  pub(super) fn new(gateway: GatewayPlan, service: ServiceSettings) -> Self {
    Self { gateway, service }
  }

  pub fn gateway(&self) -> &GatewayPlan {
    &self.gateway
  }

  pub fn service(&self) -> &ServiceSettings {
    &self.service
  }

  pub fn into_parts(self) -> (GatewayPlan, ServiceSettings) {
    (self.gateway, self.service)
  }
}

/// Process-wide serving settings independent of the routing graph.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ServiceSettings {
  logging: crate::LoggingConfig,
  outbound: OutboundPlan,
  request_limits: RequestLimitsPlan,
  persistence: PersistencePlan,
  models: super::ModelRefreshPlan,
}

impl ServiceSettings {
  pub(super) fn new(
    logging: crate::LoggingConfig,
    outbound: OutboundPlan,
    request_limits: RequestLimitsPlan,
    persistence: PersistencePlan,
    models: super::ModelRefreshPlan,
  ) -> Self {
    Self {
      logging,
      outbound,
      request_limits,
      persistence,
      models,
    }
  }

  pub fn logging(&self) -> &crate::LoggingConfig {
    &self.logging
  }

  pub fn outbound(&self) -> &OutboundPlan {
    &self.outbound
  }

  pub const fn request_limits(&self) -> RequestLimitsPlan {
    self.request_limits
  }

  pub const fn persistence(&self) -> &PersistencePlan {
    &self.persistence
  }

  pub const fn models(&self) -> super::ModelRefreshPlan {
    self.models
  }
}

/// Shared outbound proxy policy for managed, opaque, and tunnel clients.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OutboundPlan {
  proxy_url: Option<String>,
  no_proxy: Box<[String]>,
  use_system_proxy: bool,
}

impl OutboundPlan {
  pub(super) fn new(proxy_url: Option<String>, no_proxy: Box<[String]>, use_system_proxy: bool) -> Self {
    Self {
      proxy_url,
      no_proxy,
      use_system_proxy,
    }
  }

  pub fn proxy_url(&self) -> Option<&str> {
    self.proxy_url.as_deref()
  }

  pub fn no_proxy(&self) -> &[String] {
    &self.no_proxy
  }

  pub const fn use_system_proxy(&self) -> bool {
    self.use_system_proxy
  }

  pub fn to_http_client_options(&self) -> tokn_core::util::http::HttpClientOptions {
    tokn_core::util::http::HttpClientOptions {
      url: self.proxy_url.clone(),
      no_proxy: self.no_proxy.to_vec(),
      system: self.use_system_proxy,
    }
  }
}

/// Independent admission bounds; `usize::MAX` represents an omitted limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestLimitsPlan {
  max_wire_bytes: usize,
  max_decoded_bytes: usize,
}

impl Default for RequestLimitsPlan {
  fn default() -> Self {
    Self::new(usize::MAX, usize::MAX)
  }
}

impl RequestLimitsPlan {
  pub(super) const fn new(max_wire_bytes: usize, max_decoded_bytes: usize) -> Self {
    Self {
      max_wire_bytes,
      max_decoded_bytes,
    }
  }

  pub const fn max_wire_bytes(self) -> usize {
    self.max_wire_bytes
  }

  pub const fn max_decoded_bytes(self) -> usize {
    self.max_decoded_bytes
  }
}

/// Persistence settings for usage, sessions, and per-day request databases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistencePlan {
  enabled: bool,
  usage_db_path: Option<PathBuf>,
  sessions_db_path: Option<PathBuf>,
  requests_dir: Option<PathBuf>,
  record_sessions: bool,
  record_request_bodies: bool,
  body_max_bytes: usize,
  write_queue_capacity: usize,
  archive_extension: Option<String>,
  archive_after_days: i64,
  prune_after_days: i64,
}

impl Default for PersistencePlan {
  fn default() -> Self {
    Self::new(
      true,
      None,
      None,
      None,
      true,
      true,
      super::DEFAULT_BODY_MAX_BYTES as usize,
      super::DEFAULT_WRITE_QUEUE_CAPACITY as usize,
      None,
      super::DEFAULT_ARCHIVE_AFTER_DAYS as i64,
      super::DEFAULT_PRUNE_AFTER_DAYS as i64,
    )
  }
}

impl PersistencePlan {
  #[allow(clippy::too_many_arguments)]
  pub(super) fn new(
    enabled: bool,
    usage_db_path: Option<PathBuf>,
    sessions_db_path: Option<PathBuf>,
    requests_dir: Option<PathBuf>,
    record_sessions: bool,
    record_request_bodies: bool,
    body_max_bytes: usize,
    write_queue_capacity: usize,
    archive_extension: Option<String>,
    archive_after_days: i64,
    prune_after_days: i64,
  ) -> Self {
    Self {
      enabled,
      usage_db_path,
      sessions_db_path,
      requests_dir,
      record_sessions,
      record_request_bodies,
      body_max_bytes,
      write_queue_capacity,
      archive_extension,
      archive_after_days,
      prune_after_days,
    }
  }

  pub const fn enabled(&self) -> bool {
    self.enabled
  }

  pub const fn record_sessions(&self) -> bool {
    self.record_sessions
  }

  pub const fn record_request_bodies(&self) -> bool {
    self.record_request_bodies
  }

  pub const fn body_max_bytes(&self) -> usize {
    self.body_max_bytes
  }

  pub const fn write_queue_capacity(&self) -> usize {
    self.write_queue_capacity
  }

  pub fn archive_extension(&self) -> Option<&str> {
    self.archive_extension.as_deref()
  }

  pub const fn archive_after_days(&self) -> i64 {
    self.archive_after_days
  }

  pub const fn prune_after_days(&self) -> i64 {
    self.prune_after_days
  }

  pub fn resolve_paths(&self) -> crate::Result<PersistencePaths> {
    Ok(PersistencePaths {
      usage_db: self
        .usage_db_path
        .clone()
        .map(Ok)
        .unwrap_or_else(crate::paths::default_usage_db)?,
      sessions_db: self
        .sessions_db_path
        .clone()
        .map(Ok)
        .unwrap_or_else(crate::paths::default_sessions_db)?,
      requests_dir: self
        .requests_dir
        .clone()
        .map(Ok)
        .unwrap_or_else(crate::paths::default_requests_dir)?,
    })
  }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistencePaths {
  pub usage_db: PathBuf,
  pub sessions_db: PathBuf,
  pub requests_dir: PathBuf,
}
