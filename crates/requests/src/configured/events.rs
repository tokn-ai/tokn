//! Local delivery adapter for portable request events.

use super::{Lifecycle, Runtime};
use core::time::Duration;
use tokn_core::request_event::RequestEventSink;

/// Adds a typed event sink to application-supplied runtime services.
/// Neither the runtime nor the sink needs to be `Send` or `'static`.
pub struct EventRuntime<R, S> {
  runtime: R,
  sink: S,
}

impl<R, S> EventRuntime<R, S> {
  pub fn new(runtime: R, sink: S) -> Self {
    Self { runtime, sink }
  }

  pub fn into_parts(self) -> (R, S) {
    (self.runtime, self.sink)
  }
}

impl<R: Runtime, S: RequestEventSink> Runtime for EventRuntime<R, S> {
  fn now(&self) -> Duration {
    self.runtime.now()
  }

  fn unix_seconds(&self) -> u64 {
    self.runtime.unix_seconds()
  }

  fn unix_millis(&self) -> i64 {
    self.runtime.unix_millis()
  }

  async fn sleep(&mut self, duration: Duration) {
    self.runtime.sleep(duration).await;
  }

  fn event_sink(&mut self) -> Option<&mut dyn RequestEventSink> {
    Some(&mut self.sink)
  }

  fn observe(&mut self, event: Lifecycle<'_>) {
    self.runtime.observe(event);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{configured::LocalEvents, session::Events};
  use core::cell::RefCell;

  #[derive(Default)]
  struct Clock {
    observations: usize,
  }
  impl Runtime for Clock {
    fn now(&self) -> Duration {
      Duration::ZERO
    }
    fn unix_seconds(&self) -> u64 {
      u64::MAX
    }
    async fn sleep(&mut self, _: Duration) {}
    fn observe(&mut self, _: Lifecycle<'_>) {
      self.observations += 1;
    }
  }

  #[test]
  fn default_millisecond_clock_saturates() {
    assert_eq!(Clock::default().unix_millis(), i64::MAX);
  }

  #[test]
  fn absent_typed_sink_disables_payloads_without_disabling_legacy_observer() {
    let runtime = RefCell::new(Clock::default());
    let events = LocalEvents(&runtime);
    assert!(!events.enabled());
    events.observe(Lifecycle {
      request_id: "request",
      attempt: 0,
      stage: "extract",
      success: None,
    });
    assert_eq!(runtime.borrow().observations, 1);
  }
}
