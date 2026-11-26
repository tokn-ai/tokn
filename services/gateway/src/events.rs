//! Request persistence and maintenance for a serving gateway.

use anyhow::Result;
use std::path::Path;
use std::sync::Arc;
use tokn_config::v2::PersistencePlan;
use tokn_core::event::{spawn_event_loop, EventBus, EventHandler};
use tokn_maintenance::{ArchiveEventHandler, ArchiveRuntime};
use tokn_persistence::{RequestEventHandler, RequestPersistenceOptions, SessionEventHandler, UsageEventHandler};

/// Optional presentation handlers provided by the caller.
#[derive(Default)]
pub struct EventObservers {
  pub requests: Vec<Box<dyn EventHandler>>,
  pub maintenance: Vec<Box<dyn ArchiveEventHandler>>,
}

/// Owns the request writers and any scheduled retention work for one process.
pub struct RuntimeEvents {
  events: Arc<EventBus>,
  archive: Option<ArchiveRuntime>,
}

enum Retention {
  Disabled,
  ArchiveOnly,
  ArchiveAndPrune,
}

impl RuntimeEvents {
  pub(crate) fn start(
    persistence: &PersistencePlan,
    frontend_maintenance_dir: Option<&Path>,
    observers: impl FnOnce() -> EventObservers,
  ) -> Result<Self> {
    // Replacements use the frontend's maintenance worker for a shared history directory.
    let retention = match frontend_maintenance_dir {
      Some(dir) if tokn_maintenance::maintenance_requests_dir(persistence)?.as_deref() == Some(dir) => {
        Retention::Disabled
      }
      _ => Retention::ArchiveAndPrune,
    };
    Self::start_with_retention(persistence, retention, observers)
  }

  /// Share the serving writers with diagnostic requests, archiving without pruning.
  pub fn start_archive_only(persistence: &PersistencePlan, observers: impl FnOnce() -> EventObservers) -> Result<Self> {
    Self::start_with_retention(persistence, Retention::ArchiveOnly, observers)
  }

  fn start_with_retention(
    persistence: &PersistencePlan,
    retention: Retention,
    observers: impl FnOnce() -> EventObservers,
  ) -> Result<Self> {
    let mut handlers: Vec<Box<dyn EventHandler>> = Vec::new();
    let requests_dir = if persistence.enabled() {
      let paths = persistence.resolve_paths()?;
      handlers.push(Box::new(RequestEventHandler::with_options(
        paths.requests_dir.clone(),
        RequestPersistenceOptions {
          record_request_bodies: persistence.record_request_bodies(),
          body_max_bytes: persistence.body_max_bytes(),
        },
      )?));
      handlers.push(Box::new(UsageEventHandler::new(paths.usage_db)?));
      if persistence.record_sessions() {
        handlers.push(Box::new(SessionEventHandler::new(paths.sessions_db)?));
      }
      Some(paths.requests_dir)
    } else {
      None
    };

    let observers = observers();
    handlers.extend(observers.requests);
    let archive = requests_dir.and_then(|requests_dir| match retention {
      Retention::Disabled => None,
      Retention::ArchiveOnly => tokn_maintenance::start_request_archive_worker(
        requests_dir,
        persistence.archive_extension(),
        persistence.archive_after_days(),
        observers.maintenance,
      ),
      Retention::ArchiveAndPrune => tokn_maintenance::start_request_maintenance_worker(
        requests_dir,
        persistence.archive_extension(),
        persistence.archive_after_days(),
        persistence.prune_after_days(),
        observers.maintenance,
      ),
    });

    let events = Arc::new(EventBus::new(persistence.write_queue_capacity().max(256)));
    spawn_event_loop(events.subscribe(), handlers);
    Ok(Self { events, archive })
  }

  pub fn events(&self) -> Arc<EventBus> {
    self.events.clone()
  }

  /// Flush queued events and stop maintenance after the request producers have drained.
  pub async fn finish(self) -> Result<()> {
    crate::finish_events(&self.events, self.archive).await
  }
}
