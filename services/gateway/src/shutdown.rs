use anyhow::Result;
use std::time::Duration;
use tokn_core::event::EventBus;
use tokn_maintenance::ArchiveRuntime;

/// Called after every listener has stopped and its connection tasks have been
/// joined. A stuck finalizer or writer must not hang process termination.
pub async fn finish_events(events: &EventBus, archive: Option<ArchiveRuntime>) -> Result<()> {
  finish_events_with_timeout(events, archive, Duration::from_secs(5)).await
}

async fn finish_events_with_timeout(
  events: &EventBus,
  archive: Option<ArchiveRuntime>,
  timeout: Duration,
) -> Result<()> {
  let result = tokio::time::timeout(timeout, async {
    tokio::join!(events.shutdown(), async {
      if let Some(archive) = archive {
        archive.shutdown().await;
      }
    });
  })
  .await;
  if result.is_err() {
    tracing::error!("shutdown persistence cleanup timed out; queued records may be incomplete");
    anyhow::bail!("shutdown persistence cleanup timed out");
  }
  tracing::info!("shutdown persistence cleanup complete");
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;
  use tokn_core::event::EventHandler;

  #[tokio::test]
  async fn cleanup_waits_for_handler_flush() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Handler(Arc<AtomicBool>);
    impl EventHandler for Handler {
      fn handle(&mut self, _: &tokn_core::event::Event) {}
      fn flush(&mut self) {
        self.0.store(true, Ordering::SeqCst);
      }
    }
    let flushed = Arc::new(AtomicBool::new(false));
    let events = EventBus::new(16);
    let worker = tokn_core::event::spawn_event_loop(events.subscribe(), vec![Box::new(Handler(flushed.clone()))]);
    finish_events(&events, None).await.unwrap();
    assert!(flushed.load(Ordering::SeqCst));
    worker.join().unwrap();
  }

  #[tokio::test]
  async fn cleanup_reports_stalled_finalizers_instead_of_waiting_forever() {
    let events = EventBus::new(16);
    let _finalizer = events.begin_finalizer();
    let error = finish_events_with_timeout(&events, None, Duration::from_millis(20))
      .await
      .unwrap_err();
    assert!(error.to_string().contains("cleanup timed out"));
  }
}
