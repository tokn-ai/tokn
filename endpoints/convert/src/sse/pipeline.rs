use super::codec::encode_event;
use super::event::SseEvent;
use super::{StreamOutcome, StreamProcessor};
use crate::{provider::Endpoint, Result};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde_json::Value;
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
  endpoint: Option<Endpoint>,
  completion: Option<Box<dyn FnOnce(StreamOutcome) + Send>>,
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
      endpoint: None,
      completion: None,
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

  /// Validate a managed upstream protocol and stop reading at its terminal.
  /// Without this opt-in, arbitrary SSE streams complete at transport EOF.
  pub fn with_endpoint(mut self, endpoint: Endpoint) -> Self {
    self.endpoint = Some(endpoint);
    self
  }

  /// Observe the outcome once, after the last output is delivered or on failure
  /// or cancellation. Platform adapters can use this for request completion.
  pub fn with_completion(mut self, completion: impl FnOnce(StreamOutcome) + Send + 'static) -> Self {
    self.completion = Some(Box::new(completion));
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
    Box::pin(PipelineStream::new(
      source,
      self.endpoint,
      self.transformers,
      self.tap,
      self.tap_kinds,
      self.completion,
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
  source: Option<EventStream>,
  processor: StreamProcessor<TransformerChain>,
  tap: Option<Arc<ObserverSender>>,
  tap_kinds: TapKinds,
  completion: Option<Box<dyn FnOnce(StreamOutcome) + Send>>,
  outcome_reported: bool,
}

impl PipelineStream {
  fn new(
    source: EventStream,
    endpoint: Option<Endpoint>,
    transformers: Vec<Box<dyn EventTransformer + Send>>,
    tap: Option<Arc<ObserverSender>>,
    tap_kinds: TapKinds,
    completion: Option<Box<dyn FnOnce(StreamOutcome) + Send>>,
  ) -> Self {
    Self {
      source: Some(source),
      processor: StreamProcessor::new(endpoint, TransformerChain(transformers)),
      tap,
      tap_kinds,
      completion,
      outcome_reported: false,
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

  fn report_outcome(&mut self, error: Option<&io::Error>) {
    if self.outcome_reported {
      return;
    }
    let Some(outcome) = self.processor.outcome() else {
      return;
    };
    self.outcome_reported = true;
    match outcome {
      StreamOutcome::Complete => self.send_tap(ObserverMsg::Done),
      StreamOutcome::Failed => self.send_tap(ObserverMsg::Error(
        error.map(ToString::to_string).unwrap_or_else(|| "stream failed".into()),
      )),
      StreamOutcome::Cancelled => self.send_tap(ObserverMsg::Error("stream cancelled".into())),
    }
    if let Some(completion) = self.completion.take() {
      completion(outcome);
    }
  }

  fn fail(&mut self, error: io::Error) -> Poll<Option<io::Result<Bytes>>> {
    self.processor.fail();
    self.source = None;
    self.report_outcome(Some(&error));
    Poll::Ready(Some(Err(error)))
  }
}

struct TransformerChain(Vec<Box<dyn EventTransformer + Send>>);

impl TransformerChain {
  fn apply(&mut self, mut events: Vec<SseEvent>, start: usize) -> Result<Vec<SseEvent>> {
    for idx in start..self.0.len() {
      let mut next = Vec::new();
      for event in events {
        next.extend(self.0[idx].transform(event)?);
      }
      events = next;
    }
    Ok(events)
  }
}

impl EventTransformer for TransformerChain {
  fn transform(&mut self, event: SseEvent) -> Result<Vec<SseEvent>> {
    self.apply(vec![event], 0)
  }

  fn finish(&mut self) -> Result<Vec<SseEvent>> {
    let mut output = Vec::new();
    for idx in 0..self.0.len() {
      let events = self.0[idx].finish()?;
      output.extend(self.apply(events, idx + 1)?);
    }
    Ok(output)
  }
}

impl Stream for PipelineStream {
  type Item = std::io::Result<Bytes>;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    loop {
      if let Some(event) = self.processor.next_event() {
        self.send_tap_transformed(event.json.clone());
        let encoded = encode_event(&event);
        self.send_tap_to(encoded.clone());
        self.report_outcome(None);
        return Poll::Ready(Some(Ok(encoded)));
      }
      if self.processor.input_closed() {
        self.source = None;
        self.report_outcome(None);
        return Poll::Ready(None);
      }

      match self
        .source
        .as_mut()
        .expect("open processor has an upstream stream")
        .as_mut()
        .poll_next(cx)
      {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Some(Ok(event))) => {
          self.send_tap_parsed(event.json.clone());
          if let Err(error) = self.processor.push(event) {
            return self.fail(io::Error::other(error));
          }
          if self.processor.input_closed() {
            self.source = None;
          }
        }
        Poll::Ready(Some(Err(error))) => return self.fail(error),
        Poll::Ready(None) => {
          self.source = None;
          if let Err(error) = self.processor.finish() {
            return self.fail(io::Error::other(error));
          }
        }
      }
    }
  }
}

impl Drop for PipelineStream {
  fn drop(&mut self) {
    self.processor.cancel();
    self.report_outcome(None);
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

  #[test]
  fn managed_pipeline_completes_on_terminal_without_reading_more_input() {
    let (tx, mut rx) = observer_channel();
    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = outcomes.clone();
    let mut polls = 0;
    let source = stream::poll_fn(move |_| {
      polls += 1;
      assert_eq!(polls, 1, "transport must not be read after the terminal");
      Poll::Ready(Some(Ok(Bytes::from_static(
        b"data: [DONE]\n\nevent: error\ndata: {\"error\":\"ignored trailer\"}\n\n",
      ))))
    });
    let mut body = SsePipeline::from_stream(source)
      .with_endpoint(Endpoint::ChatCompletions)
      .with_tap_all(tx)
      .with_completion(move |outcome| observed.lock().unwrap().push(outcome))
      .run();
    assert_eq!(
      futures::executor::block_on(body.next()).unwrap().unwrap(),
      Bytes::from_static(b"data: [DONE]\n\n")
    );
    assert_eq!(outcomes.lock().unwrap().as_slice(), &[StreamOutcome::Complete]);
    assert!(futures::executor::block_on(body.next()).is_none());
    drop(body);
    assert_eq!(outcomes.lock().unwrap().len(), 1);
    let mut kinds = Vec::new();
    while let Ok(msg) = rx.try_recv() {
      kinds.push(match msg {
        ObserverMsg::From(_) => "from",
        ObserverMsg::Parsed(_) => "parsed",
        ObserverMsg::Transformed(_) => "transformed",
        ObserverMsg::To(_) => "to",
        ObserverMsg::Done => "done",
        ObserverMsg::Error(_) => "error",
      });
    }
    assert_eq!(kinds, ["from", "parsed", "transformed", "to", "done"]);
  }

  #[test]
  fn pipeline_failures_emit_one_error_and_never_done() {
    for chunks in [
      vec![Err(io::Error::other("transport failed"))],
      vec![Ok(Bytes::from_static(b"data: \xff\n\n"))],
      vec![Ok(Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
      ))],
      vec![Ok(Bytes::from_static(
        b"event: error\ndata: {\"error\":\"failed\"}\n\n",
      ))],
    ] {
      let (tx, mut rx) = observer_channel();
      let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
      let observed = outcomes.clone();
      let mut body = SsePipeline::from_stream(stream::iter(chunks))
        .with_endpoint(Endpoint::ChatCompletions)
        .with_tap_all(tx)
        .with_completion(move |outcome| observed.lock().unwrap().push(outcome))
        .run();
      let mut errors = 0;
      futures::executor::block_on(async {
        while let Some(item) = body.next().await {
          errors += usize::from(item.is_err());
        }
        assert!(body.next().await.is_none());
      });
      drop(body);
      assert_eq!(errors, 1);
      assert_eq!(outcomes.lock().unwrap().as_slice(), &[StreamOutcome::Failed]);
      let mut tap_errors = 0;
      while let Ok(msg) = rx.try_recv() {
        match msg {
          ObserverMsg::Done => panic!("failed stream cannot report successful completion"),
          ObserverMsg::Error(_) => tap_errors += 1,
          _ => {}
        }
      }
      assert_eq!(tap_errors, 1);
    }
  }

  struct Duplicate;

  impl EventTransformer for Duplicate {
    fn transform(&mut self, event: SseEvent) -> Result<Vec<SseEvent>> {
      Ok(vec![event.clone(), event])
    }
  }

  #[test]
  fn drop_reports_cancellation_until_terminal_output_is_fully_delivered() {
    for consumed in 0..=2 {
      let (tx, mut rx) = observer_channel();
      let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
      let observed = outcomes.clone();
      let mut body = SsePipeline::from_stream(stream::iter([Ok(Bytes::from_static(b"data: [DONE]\n\n"))]))
        .with_endpoint(Endpoint::ChatCompletions)
        .with_transformer(Duplicate)
        .with_tap_all(tx)
        .with_completion(move |outcome| observed.lock().unwrap().push(outcome))
        .run();
      futures::executor::block_on(async {
        for _ in 0..consumed {
          body.next().await.unwrap().unwrap();
        }
      });
      drop(body);
      let expected = if consumed == 2 {
        StreamOutcome::Complete
      } else {
        StreamOutcome::Cancelled
      };
      assert_eq!(outcomes.lock().unwrap().as_slice(), &[expected]);
      let mut terminal_taps = Vec::new();
      while let Ok(msg) = rx.try_recv() {
        match msg {
          ObserverMsg::Done => terminal_taps.push(StreamOutcome::Complete),
          ObserverMsg::Error(error) => {
            assert!(error.contains("cancelled"));
            terminal_taps.push(StreamOutcome::Cancelled);
          }
          _ => {}
        }
      }
      assert_eq!(terminal_taps, [expected]);
    }
  }

  struct Flush(&'static str);

  impl EventTransformer for Flush {
    fn transform(&mut self, mut event: SseEvent) -> Result<Vec<SseEvent>> {
      event.data.push_str(self.0);
      Ok(vec![event])
    }

    fn finish(&mut self) -> Result<Vec<SseEvent>> {
      Ok(vec![SseEvent::raw(None, self.0.into())])
    }
  }

  #[test]
  fn generic_pipeline_flushes_each_transformer_through_the_remaining_chain() {
    let output = futures::executor::block_on(
      SsePipeline::from_stream(stream::empty())
        .with_transformer(Flush("-a"))
        .with_transformer(Flush("-b"))
        .run()
        .collect::<Vec<_>>(),
    );
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].as_ref().unwrap().as_ref(), b"data: -a-b\n\n");
    assert_eq!(output[1].as_ref().unwrap().as_ref(), b"data: -b\n\n");
  }
}
