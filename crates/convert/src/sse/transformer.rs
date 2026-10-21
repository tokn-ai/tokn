use super::SseEvent;
use crate::Result;
use alloc::vec::Vec;

/// Synchronous SSE transformation shared by local and concurrent transports.
pub trait EventTransformer {
  fn transform(&mut self, event: SseEvent) -> Result<Vec<SseEvent>>;

  fn finish(&mut self) -> Result<Vec<SseEvent>> {
    Ok(Vec::new())
  }
}
