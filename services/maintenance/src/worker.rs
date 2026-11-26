//! Periodic maintenance and progress delivery.

use super::{archive_requests_once_with_events, resolve_format, ArchiveStats};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokn_config::v2::PersistencePlan;

const SCAN_INTERVAL: StdDuration = StdDuration::from_secs(6 * 60 * 60);

#[derive(Clone, Debug)]
pub enum ArchiveEvent {
  ScanStarted {
    dir: PathBuf,
  },
  FileStarted {
    id: String,
    path: PathBuf,
    archive: PathBuf,
    total_bytes: u64,
  },
  FileProgress {
    id: String,
    bytes_read: u64,
    total_bytes: u64,
  },
  FileCompleted {
    id: String,
    path: PathBuf,
    archive: PathBuf,
    bytes_in: u64,
    bytes_out: u64,
  },
  FileSkipped {
    path: PathBuf,
    archive: PathBuf,
  },
  FileFailed {
    id: String,
    path: PathBuf,
    archive: PathBuf,
    error: String,
  },
  ScanCompleted {
    dir: PathBuf,
    stats: ArchiveStats,
  },
}

enum ArchiveBusMessage {
  Event(ArchiveEvent),
  Shutdown(oneshot::Sender<()>),
}

pub trait ArchiveEmitter: Send + Sync {
  fn emit(&self, event: ArchiveEvent);
}

#[derive(Clone)]
pub struct ArchiveEventBus {
  tx: mpsc::Sender<ArchiveBusMessage>,
}

pub struct ArchiveEventReceiver {
  rx: mpsc::Receiver<ArchiveBusMessage>,
}

pub trait ArchiveEventHandler: Send + 'static {
  fn handle(&mut self, event: &ArchiveEvent);
  fn flush(&mut self) {}
}

pub struct ArchiveRuntime {
  bus: ArchiveEventBus,
  worker: tokio::task::JoinHandle<()>,
  cancelled: Arc<AtomicBool>,
  _event_thread: std::thread::JoinHandle<()>,
}

impl ArchiveEventBus {
  pub fn new(capacity: usize) -> (Self, ArchiveEventReceiver) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    (Self { tx }, ArchiveEventReceiver { rx })
  }

  pub async fn shutdown(&self) {
    let (tx, rx) = oneshot::channel();
    let _ = self.tx.send(ArchiveBusMessage::Shutdown(tx)).await;
    let _ = rx.await;
  }
}

impl ArchiveEmitter for ArchiveEventBus {
  fn emit(&self, event: ArchiveEvent) {
    match self.tx.try_send(ArchiveBusMessage::Event(event)) {
      Ok(()) => {}
      Err(mpsc::error::TrySendError::Full(_)) => tracing::warn!("archive event bus full, dropping event"),
      Err(mpsc::error::TrySendError::Closed(_)) => tracing::warn!("archive event bus closed, dropping event"),
    }
  }
}

impl ArchiveEventReceiver {
  fn blocking_recv(&mut self) -> Option<ArchiveBusMessage> {
    self.rx.blocking_recv()
  }
}

impl ArchiveRuntime {
  pub async fn shutdown(mut self) {
    self.cancelled.store(true, Ordering::Relaxed);
    if let Err(err) = (&mut self.worker).await {
      if !err.is_cancelled() {
        tracing::warn!(error = %err, "request db archival worker shutdown failed");
      }
    }
    self.bus.shutdown().await;
  }
}

impl Drop for ArchiveRuntime {
  fn drop(&mut self) {
    self.cancelled.store(true, Ordering::Relaxed);
  }
}

pub fn spawn_archive_event_loop(
  mut receiver: ArchiveEventReceiver,
  mut handlers: Vec<Box<dyn ArchiveEventHandler>>,
) -> std::thread::JoinHandle<()> {
  std::thread::spawn(move || {
    let mut flushed = false;
    while let Some(message) = receiver.blocking_recv() {
      match message {
        ArchiveBusMessage::Event(event) => {
          for handler in &mut handlers {
            handler.handle(&event);
          }
        }
        ArchiveBusMessage::Shutdown(done) => {
          for handler in &mut handlers {
            handler.flush();
          }
          flushed = true;
          let _ = done.send(());
          break;
        }
      }
    }
    if !flushed {
      for handler in &mut handlers {
        handler.flush();
      }
    }
  })
}

pub fn start_request_archive_worker(
  requests_dir: PathBuf,
  archive_extension: Option<&str>,
  archive_after_days: i64,
  handlers: Vec<Box<dyn ArchiveEventHandler>>,
) -> Option<ArchiveRuntime> {
  start_request_maintenance(requests_dir, archive_extension, archive_after_days, None, handlers)
}

/// Archive old request databases and prune those past the prune cutoff after verification.
pub fn start_request_maintenance_worker(
  requests_dir: PathBuf,
  archive_extension: Option<&str>,
  archive_after_days: i64,
  prune_after_days: i64,
  handlers: Vec<Box<dyn ArchiveEventHandler>>,
) -> Option<ArchiveRuntime> {
  start_request_maintenance(
    requests_dir,
    archive_extension,
    archive_after_days,
    Some(prune_after_days),
    handlers,
  )
}

fn start_request_maintenance(
  requests_dir: PathBuf,
  archive_extension: Option<&str>,
  archive_after_days: i64,
  prune_after_days: Option<i64>,
  handlers: Vec<Box<dyn ArchiveEventHandler>>,
) -> Option<ArchiveRuntime> {
  let Ok(handle) = tokio::runtime::Handle::try_current() else {
    tracing::warn!(path = %requests_dir.display(), "request db archival disabled: no tokio runtime");
    return None;
  };

  let (bus, receiver) = ArchiveEventBus::new(1024);
  let event_thread = spawn_archive_event_loop(receiver, handlers);
  let worker_bus = bus.clone();
  let cancelled = Arc::new(AtomicBool::new(false));
  let worker_cancelled = cancelled.clone();
  let format = resolve_format(archive_extension);
  let worker = handle.spawn(async move {
    loop {
      if worker_cancelled.load(Ordering::Relaxed) {
        break;
      }
      let dir = requests_dir.clone();
      let events = worker_bus.clone();
      let scan_cancelled = worker_cancelled.clone();
      match tokio::task::spawn_blocking(move || {
        archive_requests_once_with_events(
          &dir,
          OffsetDateTime::now_utc().date(),
          archive_after_days,
          prune_after_days,
          format,
          Some(&events),
          Some(scan_cancelled.as_ref()),
        )
      })
      .await
      {
        Ok(Ok(stats)) if stats.archived > 0 || stats.skipped_existing > 0 || stats.failed > 0 => {
          tracing::info!(
            path = %requests_dir.display(),
            archived = stats.archived,
            skipped_existing = stats.skipped_existing,
            failed = stats.failed,
            "request db archival scan completed"
          );
        }
        Ok(Ok(_)) => {}
        Ok(Err(err)) if err.kind() == io::ErrorKind::Interrupted => {}
        Ok(Err(err)) => tracing::warn!(path = %requests_dir.display(), error = %err, "request db archival scan failed"),
        Err(err) => tracing::warn!(path = %requests_dir.display(), error = %err, "request db archival worker failed"),
      }
      tokio::select! {
        _ = tokio::time::sleep(SCAN_INTERVAL) => {}
        _ = wait_cancelled(worker_cancelled.clone()) => break,
      }
    }
  });

  Some(ArchiveRuntime {
    bus,
    worker,
    cancelled,
    _event_thread: event_thread,
  })
}

async fn wait_cancelled(cancelled: Arc<AtomicBool>) {
  while !cancelled.load(Ordering::Relaxed) {
    tokio::time::sleep(StdDuration::from_millis(100)).await;
  }
}

/// Identify the directory whose retention jobs belong to this gateway.
pub fn maintenance_requests_dir(persistence: &PersistencePlan) -> anyhow::Result<Option<PathBuf>> {
  if !persistence.enabled() {
    return Ok(None);
  }
  let path = persistence.resolve_paths()?.requests_dir;
  let absolute = std::path::absolute(path)?;
  match absolute.canonicalize() {
    Ok(canonical) => Ok(Some(canonical)),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Some(absolute)),
    Err(error) => Err(error.into()),
  }
}

/// Start retention for the resolved directory, which the gateway advertises to its workers.
pub fn start_v2_maintenance(
  persistence: &PersistencePlan,
  handlers: Vec<Box<dyn ArchiveEventHandler>>,
) -> anyhow::Result<Option<(PathBuf, ArchiveRuntime)>> {
  let Some(dir) = maintenance_requests_dir(persistence)? else {
    return Ok(None);
  };
  Ok(
    start_request_maintenance_worker(
      dir.clone(),
      persistence.archive_extension(),
      persistence.archive_after_days(),
      persistence.prune_after_days(),
      handlers,
    )
    .map(|runtime| (dir, runtime)),
  )
}
