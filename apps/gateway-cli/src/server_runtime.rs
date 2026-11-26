use crate::config::Config;
use crate::progress::{ArchiveProgressEventHandler, ProgressEventHandler, ProgressLogEventHandler};
use anyhow::Result;
use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokn_auth::AuthStore;
use tokn_config::{LoggingConfig, RouteMode};
use tokn_core::account::AccountConfig;
use tokn_core::event::{EventBus, EventHandler};
use tokn_gateway::events::EventObservers;
pub use tokn_gateway::runtime::{load_access_store, resolve_bind_addr};
use tokn_maintenance::{ArchiveEventHandler, ArchiveRuntime};

type EventBusParts = (
  Arc<EventBus>,
  broadcast::Receiver<Arc<tokn_core::event::Event>>,
  Vec<Box<dyn EventHandler>>,
  Option<ArchiveRuntime>,
);

/// Build the event bus and its persistence/progress handlers.
pub fn build_event_bus(cfg: &Config) -> Result<EventBusParts> {
  let capacity = cfg.db.write_queue_capacity.max(256);
  let bus = EventBus::new(capacity);
  let receiver = bus.subscribe();
  let mut handlers: Vec<Box<dyn EventHandler>> = Vec::new();

  let requests_dir = if cfg.db.enabled {
    let paths = cfg.db.resolve_paths()?;
    let request_handler = tokn_persistence::RequestEventHandler::with_options(
      paths.requests_dir.clone(),
      tokn_persistence::RequestPersistenceOptions {
        record_request_bodies: cfg.db.record_request_bodies,
        body_max_bytes: cfg.db.body_max_bytes,
      },
    )?;
    let usage_handler = tokn_persistence::UsageEventHandler::new(paths.usage_db)?;
    handlers.push(Box::new(request_handler));
    handlers.push(Box::new(usage_handler));
    if cfg.db.record_sessions {
      let session_handler = tokn_persistence::SessionEventHandler::new(paths.sessions_db)?;
      handlers.push(Box::new(session_handler));
    }
    Some(paths.requests_dir)
  } else {
    None
  };

  handlers.extend(progress_handlers(&cfg.logging));
  let archive_runtime = requests_dir.and_then(|requests_dir| {
    tokn_maintenance::start_request_maintenance_worker(
      requests_dir,
      cfg.db.archive_extension.as_deref(),
      tokn_config::v2::DEFAULT_ARCHIVE_AFTER_DAYS as i64,
      tokn_config::v2::DEFAULT_PRUNE_AFTER_DAYS as i64,
      maintenance_handlers(),
    )
  });
  Ok((Arc::new(bus), receiver, handlers, archive_runtime))
}

fn progress_handlers(logging: &LoggingConfig) -> Vec<Box<dyn EventHandler>> {
  let mut handlers: Vec<Box<dyn EventHandler>> = Vec::new();
  match crate::logging::resolve_logs_dir(logging) {
    Ok(dir) => match ProgressLogEventHandler::new(&dir) {
      Ok(handler) => handlers.push(Box::new(handler)),
      Err(e) => tracing::warn!(path = %dir.display(), error = %e, "progress log disabled"),
    },
    Err(e) => tracing::warn!(error = %e, "progress log disabled"),
  }

  if std::io::stdout().is_terminal() {
    handlers.push(Box::new(ProgressEventHandler::new()));
  }

  handlers
}

pub fn maintenance_handlers() -> Vec<Box<dyn ArchiveEventHandler>> {
  let mut handlers: Vec<Box<dyn ArchiveEventHandler>> = Vec::new();
  if std::io::stdout().is_terminal() {
    handlers.push(Box::new(ArchiveProgressEventHandler::new()));
  }
  handlers
}

pub fn event_observers() -> EventObservers {
  EventObservers {
    requests: progress_handlers(&LoggingConfig::default()),
    maintenance: maintenance_handlers(),
  }
}

/// Load accounts from the root `auth.yaml` and any `auth.d` fragments.
///
/// `config_path` is accepted for compatibility with call sites that already
/// have the effective config path; legacy schema migration runs before latest
/// config/auth loading.
pub fn load_accounts(config_path: Option<&Path>) -> Result<Vec<AccountConfig>> {
  let store = AuthStore::load(None, config_path)?;
  Ok(store.accounts)
}

pub fn build_proxy_state_for_route_mode(
  cfg: &Config,
  accounts: &[AccountConfig],
  events: Arc<EventBus>,
  route_mode: RouteMode,
) -> Result<tokn_router::api::AppState> {
  let mut cfg = cfg.clone();
  cfg.server.route_mode = route_mode;
  cfg.defaults.mode = route_mode;
  tokn_router::api::build_proxy_state(&cfg, accounts, events)
}

#[cfg(test)]
mod tests {
  use super::*;
  use tokn_gateway::runtime::ensure_bind_host;

  fn persistence_config(record_sessions: bool) -> (Config, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("tokn-server-runtime-test-{}", uuid::Uuid::new_v4()));
    let sessions_db = root.join("sessions.db");
    let mut cfg = Config::default();
    cfg.db.usage_db_path = Some(root.join("usage.db"));
    cfg.db.sessions_db_path = Some(sessions_db.clone());
    cfg.db.requests_dir = Some(root.join("requests"));
    cfg.db.record_sessions = record_sessions;
    cfg.logging.dir = Some(root.join("logs"));
    (cfg, sessions_db)
  }

  #[test]
  fn build_event_bus_opens_sessions_db_when_recording_is_enabled() {
    let (cfg, sessions_db) = persistence_config(true);

    let _parts = build_event_bus(&cfg).expect("event bus should initialize persistence");

    assert!(sessions_db.is_file());
  }

  #[test]
  fn build_event_bus_leaves_sessions_db_absent_when_recording_is_disabled() {
    let (cfg, sessions_db) = persistence_config(false);

    let _parts = build_event_bus(&cfg).expect("event bus should initialize other persistence");

    assert!(!sessions_db.exists());
  }

  #[test]
  fn rejects_non_loopback_without_insecure_allow_remote() {
    let err = ensure_bind_host("0.0.0.0", false).expect_err("remote bind should be rejected");
    assert!(
      err.to_string().contains("--insecure-allow-remote"),
      "unexpected error: {err}"
    );
  }

  #[test]
  fn accepts_non_loopback_with_insecure_allow_remote() {
    ensure_bind_host("0.0.0.0", true).expect("remote bind should be allowed");
  }

  #[test]
  fn accepts_loopback_without_insecure_allow_remote() {
    ensure_bind_host("127.0.0.1", false).expect("loopback bind should be allowed");
  }
}
