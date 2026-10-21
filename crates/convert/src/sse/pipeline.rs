use super::codec::encode_event;
use super::event::SseEvent;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde_json::Value;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

type EventStream = Pin<Box<dyn Stream<Item = std::io::Result<SseEvent>> + Send>>;
type ByteStream = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send>>;

pub type ObserverSender = mpsc::UnboundedSender<ObserverMsg>;
pub type ObserverReceiver = mpsc::UnboundedReceiver<ObserverMsg>;

/// Messages sent to an observer channel during stream processing.
#[derive(Debug)]
pub enum ObserverMsg {
  /// Raw upstream bytes (before SSE parsing). For body accumulation.
  From(Bytes),
  /// Parsed SSE event JSON (before transformers). `None` for non-JSON events.
  Parsed(Option<Value>),
  /// Transformed SSE event JSON (after transformers). `None` for non-JSON events.
  Transformed(Option<Value>),
  /// Encoded bytes yielded to the client.
  To(Bytes),
  /// Stream completed successfully.
  Done,
  /// Stream error.
  Error(String),
}

pub fn observer_channel() -> (ObserverSender, ObserverReceiver) {
  mpsc::unbounded_channel()
}

pub use super::transformer::EventTransformer;

/// Per-kind opt-in mask for [`SsePipeline`] tap delivery.
///
/// `Done` and `Error` messages are always delivered when a tap is attached;
/// the four payload-bearing kinds (`From`, `Parsed`, `Transformed`, `To`)
/// are individually gated so consumers only pay for what they need.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TapKinds {
  pub from: bool,
  pub parsed: bool,
  pub transformed: bool,
  pub to: bool,
}

impl TapKinds {
  /// All four payload kinds enabled.
  pub const fn all() -> Self {
    Self {
      from: true,
      parsed: true,
      transformed: true,
      to: true,
    }
  }

  /// Only `Parsed` enabled — useful for consumers that just want to inspect
  /// SSE event JSON (e.g. usage extraction).
  pub const fn parsed_only() -> Self {
    Self {
      from: false,
      parsed: true,
      transformed: false,
      to: false,
    }
  }
}

pub struct SsePipeline {
  source: ByteStream,
  /// Shared tap sender used for raw-byte teeing and parsed/transformed/output observation.
  tap: Option<Arc<ObserverSender>>,
  tap_kinds: TapKinds,
  transformers: Vec<Box<dyn EventTransformer + Send>>,
}

impl SsePipeline {
  /// Create a pipeline from a byte stream without a tap.
  pub fn from_stream<S>(source: S) -> Self
  where
    S: Stream<Item = io::Result<Bytes>> + Send + 'static,
  {
    Self {
      source: Box::pin(source),
      tap: None,
      tap_kinds: TapKinds::default(),
      transformers: Vec::new(),
    }
  }

  /// Create a pipeline from an HTTP response without a tap (no observer overhead).
  pub fn from_response(resp: reqwest::Response) -> Self {
    Self::from_stream(resp.bytes_stream().map(|item| item.map_err(io::Error::other)))
  }

  pub fn with_transformer<T>(mut self, transformer: T) -> Self
  where
    T: EventTransformer + Send + 'static,
  {
    self.transformers.push(Box::new(transformer));
    self
  }

  /// Attach a tap channel with an explicit per-kind mask.
  ///
  /// `Done`/`Error` messages are always delivered. Each of `From`, `Parsed`,
  /// `Transformed`, `To` is only sent when the corresponding flag in `kinds`
  /// is set.
  pub fn with_tap(mut self, tap: ObserverSender, kinds: TapKinds) -> Self {
    self.tap = Some(Arc::new(tap));
    self.tap_kinds = kinds;
    self
  }

  /// Shortcut for [`with_tap`](Self::with_tap) with [`TapKinds::all`].
  pub fn with_tap_all(self, tap: ObserverSender) -> Self {
    self.with_tap(tap, TapKinds::all())
  }

  /// Shortcut for [`with_tap`](Self::with_tap) with [`TapKinds::parsed_only`].
  pub fn with_tap_parsed(self, tap: ObserverSender) -> Self {
    self.with_tap(tap, TapKinds::parsed_only())
  }

  pub fn run(self) -> ByteStream {
    let source: ByteStream = if self.tap_kinds.from {
      let tap = self.tap.clone();
      Box::pin(self.source.map(move |result| {
        if let Ok(bytes) = &result {
          if let Some(tap) = &tap {
            let _ = tap.send(ObserverMsg::From(bytes.clone()));
          }
        }
        result
      }))
    } else {
      self.source
    };
    let source: EventStream = Box::pin(DecodedStream::new(source));
    Box::pin(StreamWithFinalizer::new(
      PipelineStream::new(source, self.transformers, self.tap, self.tap_kinds),
      finalize_tap,
    ))
  }
}

/// A std stream adapter around the same bounded framer used by embedded clients.
struct DecodedStream {
  source: ByteStream,
  decoder: super::Decoder,
  chunk: Bytes,
  position: usize,
  done: bool,
}

impl DecodedStream {
  fn new(source: ByteStream) -> Self {
    Self {
      source,
      decoder: super::Decoder::new(usize::MAX),
      chunk: Bytes::new(),
      position: 0,
      done: false,
    }
  }
}

impl Stream for DecodedStream {
  type Item = io::Result<SseEvent>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = self.get_mut();
    loop {
      if this.done {
        return Poll::Ready(None);
      }
      while this.position < this.chunk.len() {
        let byte = this.chunk[this.position];
        this.position += 1;
        match this.decoder.push(byte) {
          Ok(Some(record)) => return Poll::Ready(Some(Ok(SseEvent::from(record)))),
          Ok(None) => {}
          Err(error) => {
            this.done = true;
            return Poll::Ready(Some(Err(io::Error::other(error))));
          }
        }
      }
      match this.source.as_mut().poll_next(cx) {
        Poll::Ready(Some(Ok(chunk))) => {
          this.chunk = chunk;
          this.position = 0;
        }
        Poll::Ready(Some(Err(error))) => {
          this.done = true;
          return Poll::Ready(Some(Err(error)));
        }
        Poll::Ready(None) => {
          this.done = true;
          return Poll::Ready(None);
        }
        Poll::Pending => return Poll::Pending,
      }
    }
  }
}

struct PipelineStream {
  source: EventStream,
  transformers: Vec<Box<dyn EventTransformer + Send>>,
  tap: Option<Arc<ObserverSender>>,
  tap_kinds: TapKinds,
  pending: VecDeque<std::io::Result<Bytes>>,
  source_done: bool,
}

impl PipelineStream {
  fn new(
    source: EventStream,
    transformers: Vec<Box<dyn EventTransformer + Send>>,
    tap: Option<Arc<ObserverSender>>,
    tap_kinds: TapKinds,
  ) -> Self {
    Self {
      source,
      transformers,
      tap,
      tap_kinds,
      pending: VecDeque::new(),
      source_done: false,
    }
  }

  #[inline]
  fn send_tap(&self, msg: ObserverMsg) {
    if let Some(ref tap) = self.tap {
      let _ = tap.send(msg);
    }
  }

  #[inline]
  fn send_tap_parsed(&self, value: Option<Value>) {
    if self.tap_kinds.parsed {
      self.send_tap(ObserverMsg::Parsed(value));
    }
  }

  #[inline]
  fn send_tap_transformed(&self, value: Option<Value>) {
    if self.tap_kinds.transformed {
      self.send_tap(ObserverMsg::Transformed(value));
    }
  }

  #[inline]
  fn send_tap_to(&self, bytes: Bytes) {
    if self.tap_kinds.to {
      self.send_tap(ObserverMsg::To(bytes));
    }
  }

  fn process_event(&mut self, event: SseEvent) -> std::io::Result<()> {
    // Parsed: before transformers
    self.send_tap_parsed(event.json.clone());

    let transformed = self.apply_transformers(vec![event], 0)?;
    for event in transformed {
      // Transformed: after transformers
      self.send_tap_transformed(event.json.clone());

      let encoded = encode_event(&event);
      if !encoded.is_empty() {
        // To: encoded bytes to client
        self.send_tap_to(encoded.clone());
        self.pending.push_back(Ok(encoded));
      }
    }
    Ok(())
  }

  fn apply_transformers(&mut self, mut events: Vec<SseEvent>, start: usize) -> std::io::Result<Vec<SseEvent>> {
    for idx in start..self.transformers.len() {
      let mut next = Vec::new();
      for event in events {
        next.extend(self.transformers[idx].transform(event).map_err(std::io::Error::other)?);
      }
      events = next;
    }
    Ok(events)
  }

  fn finish_transformers(&mut self) -> std::io::Result<()> {
    for idx in 0..self.transformers.len() {
      let events = self.transformers[idx].finish().map_err(std::io::Error::other)?;
      for event in self.apply_transformers(events, idx + 1)? {
        self.send_tap_transformed(event.json.clone());
        let encoded = encode_event(&event);
        if !encoded.is_empty() {
          self.send_tap_to(encoded.clone());
          self.pending.push_back(Ok(encoded));
        }
      }
    }
    Ok(())
  }

  fn signal_error(&self, err: &std::io::Error) {
    self.send_tap(ObserverMsg::Error(err.to_string()));
  }
}

impl Stream for PipelineStream {
  type Item = std::io::Result<Bytes>;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    loop {
      if let Some(item) = self.pending.pop_front() {
        return Poll::Ready(Some(item));
      }
      if self.source_done {
        return Poll::Ready(None);
      }

      match self.source.as_mut().poll_next(cx) {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Some(Ok(event))) => {
          if let Err(err) = self.process_event(event) {
            self.signal_error(&err);
            self.pending.push_back(Err(err));
            self.source_done = true;
          }
        }
        Poll::Ready(Some(Err(err))) => {
          self.signal_error(&err);
          self.pending.push_back(Err(err));
          self.source_done = true;
        }
        Poll::Ready(None) => {
          if let Err(err) = self.finish_transformers() {
            self.signal_error(&err);
            self.pending.push_back(Err(err));
          }
          self.source_done = true;
        }
      }
    }
  }
}

fn finalize_tap(stream: &mut PipelineStream) {
  stream.send_tap(ObserverMsg::Done);
}

struct StreamWithFinalizer<S, F>
where
  S: Stream + Unpin,
  F: FnOnce(&mut S) + Send + 'static,
{
  inner: S,
  fin: Option<F>,
}

impl<S, F> StreamWithFinalizer<S, F>
where
  S: Stream + Unpin,
  F: FnOnce(&mut S) + Send + 'static,
{
  fn new(inner: S, fin: F) -> Self {
    Self { inner, fin: Some(fin) }
  }
}

impl<S, F> Stream for StreamWithFinalizer<S, F>
where
  S: Stream + Unpin,
  F: FnOnce(&mut S) + Send + 'static + Unpin,
{
  type Item = S::Item;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let poll = Pin::new(&mut self.inner).poll_next(cx);
    if let Poll::Ready(None) = &poll {
      if let Some(fin) = self.fin.take() {
        fin(&mut self.inner);
      }
    }
    poll
  }
}

impl<S, F> Drop for StreamWithFinalizer<S, F>
where
  S: Stream + Unpin,
  F: FnOnce(&mut S) + Send + 'static,
{
  fn drop(&mut self) {
    if let Some(fin) = self.fin.take() {
      fin(&mut self.inner);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::error::Result;
  use bytes::BytesMut;
  use futures_util::{stream, StreamExt};

  struct AppendTransformer(&'static str);

  impl EventTransformer for AppendTransformer {
    fn transform(&mut self, mut event: SseEvent) -> Result<Vec<SseEvent>> {
      if !event.is_done() {
        event.data.push_str(self.0);
      }
      Ok(vec![event])
    }
  }

  #[test]
  fn decoded_stream_preserves_default_message_event_name() {
    let source = stream::iter([Ok(Bytes::from_static(b"data: hello\n\n"))]);
    let mut decoded = DecodedStream::new(Box::pin(source));
    let event = futures::executor::block_on(decoded.next()).unwrap().unwrap();
    assert_eq!(event.event.as_deref(), Some("message"));
    assert_eq!(event.data, "hello");
  }

  #[test]
  fn pipeline_applies_transformers_in_order() {
    let (tx, mut rx) = observer_channel();
    let body = futures::executor::block_on(async move {
      let body = SsePipeline::from_stream(stream::iter(vec![
        Ok(Bytes::from_static(b"data: hello\n\n")),
        Ok(Bytes::from_static(b"data: [DONE]\n\n")),
      ]))
      .with_transformer(AppendTransformer("-a"))
      .with_transformer(AppendTransformer("-b"))
      .with_tap_all(tx)
      .run()
      .collect::<Vec<_>>()
      .await
      .into_iter()
      .collect::<std::result::Result<Vec<_>, _>>()
      .unwrap()
      .into_iter()
      .fold(BytesMut::new(), |mut out, chunk| {
        out.extend_from_slice(&chunk);
        out
      })
      .freeze();

      // Verify observer messages received
      let mut msgs = Vec::new();
      while let Ok(msg) = rx.try_recv() {
        msgs.push(msg);
      }
      // Should have Parsed+Transformed+To for each event, then Done
      assert!(msgs.iter().any(|m| matches!(m, ObserverMsg::Done)));
      let to_count = msgs.iter().filter(|m| matches!(m, ObserverMsg::To(_))).count();
      assert_eq!(to_count, 2); // "hello-a-b" + "[DONE]"

      body
    });
    let text = std::str::from_utf8(&body).unwrap();
    assert!(text.contains("data: hello-a-b"));
    assert!(text.contains("data: [DONE]"));
  }

  #[test]
  fn pipeline_tees_raw_bytes_when_enabled() {
    let (tx, mut rx) = observer_channel();
    futures::executor::block_on(async move {
      let _ = SsePipeline::from_stream(stream::iter(vec![
        Ok(Bytes::from_static(b"data: hello\n\n")),
        Ok(Bytes::from_static(b"data: [DONE]\n\n")),
      ]))
      .with_tap_all(tx)
      .run()
      .collect::<Vec<_>>()
      .await;
    });

    let mut from_count = 0;
    while let Ok(msg) = rx.try_recv() {
      if matches!(msg, ObserverMsg::From(_)) {
        from_count += 1;
      }
    }
    assert_eq!(from_count, 2);
  }

  #[test]
  fn pipeline_parsed_only_mask_suppresses_other_kinds() {
    let (tx, mut rx) = observer_channel();
    futures::executor::block_on(async move {
      let _ = SsePipeline::from_stream(stream::iter(vec![
        Ok(Bytes::from_static(b"data: {\"x\":1}\n\n")),
        Ok(Bytes::from_static(b"data: [DONE]\n\n")),
      ]))
      .with_tap_parsed(tx)
      .run()
      .collect::<Vec<_>>()
      .await;
    });

    let mut from = 0;
    let mut parsed = 0;
    let mut transformed = 0;
    let mut to = 0;
    let mut done = 0;
    while let Ok(msg) = rx.try_recv() {
      match msg {
        ObserverMsg::From(_) => from += 1,
        ObserverMsg::Parsed(_) => parsed += 1,
        ObserverMsg::Transformed(_) => transformed += 1,
        ObserverMsg::To(_) => to += 1,
        ObserverMsg::Done => done += 1,
        ObserverMsg::Error(_) => {}
      }
    }
    assert_eq!(from, 0);
    assert_eq!(transformed, 0);
    assert_eq!(to, 0);
    assert_eq!(done, 1);
    assert!(parsed >= 1);
  }
}
