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

/// An absent transformer preserves each event unchanged.
impl<T: EventTransformer> EventTransformer for Option<T> {
  fn transform(&mut self, event: SseEvent) -> Result<Vec<SseEvent>> {
    match self {
      Some(transformer) => transformer.transform(event),
      None => Ok(vec![event]),
    }
  }

  fn finish(&mut self) -> Result<Vec<SseEvent>> {
    match self {
      Some(transformer) => transformer.finish(),
      None => Ok(Vec::new()),
    }
  }
}
