//! Shared buffered conversion, SSE delivery, and response finalization.

use super::*;
use tokn_convert::sse::{Decoder, EndpointTranslator, SseEvent, StreamOutcome, StreamProcessor};
use tokn_core::request_event::ConvertedResponseSummary;

pub struct Response<T: Transport, C: Clock, E: Events> {
  pub status: u16,
  pub headers: HeaderMap,
  pub body: ResponseBody<T, C, E>,
  pub opaque: bool,
}
pub enum ResponseBody<T: Transport, C: Clock, E: Events> {
  Buffered(Bytes),
  Stream(alloc::boxed::Box<EventStream<T, C, E>>),
}
impl<T: Transport, C: Clock, E: Events> Response<T, C, E> {
  pub fn into_json<D: serde::de::DeserializeOwned>(self) -> Result<(u16, HeaderMap, D), Error> {
    let ResponseBody::Buffered(bytes) = self.body else {
      return Err(Error::new("response", "expected a buffered response"));
    };
    let data = serde_json::from_slice(&bytes).map_err(|error| Error::new("response", error))?;
    Ok((self.status, self.headers, data))
  }
}

impl<T: Transport, C: Clock, E: Events> Session<T, C, E> {
  pub(super) async fn finish_response(
    &mut self,
    (mut head, resolved, stream, upstream_stream): (ResponseHead, Resolved, bool, bool),
  ) -> Result<Response<T, C, E>, Error> {
    let mut body = self.transport.take().expect("session owns transport");
    let decoded = body.response_is_decoded();
    if decoded {
      head.headers.remove("content-encoding");
      head.headers.remove("content-length");
    }
    head.headers.remove("transfer-encoding");
    let content_type = head.headers.get("content-type").map(HeaderValue::as_str);
    let is_sse = tokn_convert::buffered::is_sse(content_type, &[]);
    let streaming = if resolved.opaque {
      head.status < 400 && content_type.map_or(stream, |_| is_sse)
    } else {
      stream
    };
    if streaming {
      if !resolved.opaque && !is_sse {
        return Err(Error::new(
          "convert_response",
          if upstream_stream {
            "streaming upstream did not return text/event-stream"
          } else {
            "expected text/event-stream"
          },
        ));
      }
      if !resolved.opaque {
        head.headers.remove("content-length");
        head.headers.insert("content-type", "text/event-stream");
      }
      self.emit_stage(Stage::ConvertResponse, || {
        StageEvent::ConvertResponse(ConvertedResponseSummary {
          status: head.status,
          headers: head.headers.clone(),
          body: None,
        })
      });
      let events = EventStream::new(
        self.observer.take().expect("session owns observer"),
        self.limits,
        body,
        resolved.endpoint,
        self.input.endpoint,
        resolved.model,
        self.input.request_id.clone(),
        self.attempt,
        head.clone(),
        resolved.opaque,
        is_sse && (decoded || !head.headers.contains_key("content-encoding")),
      );
      self.terminal = true;
      return Ok(Response {
        status: head.status,
        headers: head.headers,
        body: ResponseBody::Stream(alloc::boxed::Box::new(events)),
        opaque: resolved.opaque,
      });
    }
    let (bytes, result) = read_buffered(&mut *body, self.limits.max_response_bytes).await;
    let bytes = Bytes::from(bytes);
    self.emit_record(|| RecordEvent::UpstreamBody {
      body: bytes.clone(),
      error: result.as_ref().err().map(|error| error.message.clone()),
    });
    result?;
    if resolved.opaque {
      self
        .observer()
        .emit_event(&self.input.request_id, self.attempt, None, || {
          tokn_convert::usage::observed_usage_json(&bytes)
            .map(|usage| RequestEventPayload::Record(RecordEvent::Usage(usage)))
        });
      self.emit_stage(Stage::ConvertResponse, || {
        StageEvent::ConvertResponse(ConvertedResponseSummary {
          status: head.status,
          headers: head.headers.clone(),
          body: None,
        })
      });
      self.emit_record(|| RecordEvent::ConvertedBody {
        body: bytes.clone(),
        error: None,
      });
      self.terminal = true;
      self
        .observer()
        .emit_complete(&self.input.request_id, self.attempt, true);
      return Ok(Response {
        status: head.status,
        headers: head.headers,
        body: ResponseBody::Buffered(bytes),
        opaque: true,
      });
    }
    let response = tokn_convert::buffered::convert_response(
      resolved.endpoint,
      self.input.endpoint,
      bytes,
      head.headers.get("content-type").map(HeaderValue::as_str),
      self.limits.max_event_bytes,
      |value| self.observer().emit_usage(&self.input.request_id, self.attempt, value),
    )
    .map_err(|error| Error::new("convert_response", error))?;
    if response.body_bytes.len() > self.limits.max_response_bytes {
      return Err(Error::new("convert_response", "converted response exceeds byte limit"));
    }
    if response.accumulated_sse {
      head.headers.insert("content-type", "application/json");
    }
    self.emit_stage(Stage::ConvertResponse, || {
      StageEvent::ConvertResponse(ConvertedResponseSummary {
        status: head.status,
        headers: head.headers.clone(),
        body: response.body_json.map(Shared::new),
      })
    });
    self.emit_record(|| RecordEvent::ConvertedBody {
      body: response.body_bytes.clone(),
      error: None,
    });
    self.terminal = true;
    self
      .observer()
      .emit_complete(&self.input.request_id, self.attempt, true);
    Ok(Response {
      status: head.status,
      headers: head.headers,
      body: ResponseBody::Buffered(response.body_bytes),
      opaque: false,
    })
  }
}

/// Preserve partial response evidence when buffering fails.
async fn read_buffered<T: Transport>(transport: &mut T, limit: usize) -> (Vec<u8>, Result<(), Error>) {
  let mut bytes = Vec::new();
  let mut buffer = [0u8; 1024];
  loop {
    let count = match transport.read(&mut buffer).await {
      Ok(count) if count <= buffer.len() => count,
      Ok(_) => {
        return (
          bytes,
          Err(Error::new(
            "convert_response",
            "transport returned an invalid byte count",
          )),
        )
      }
      Err(error) => return (bytes, Err(Error::new("convert_response", error))),
    };
    if count == 0 {
      return (bytes, Ok(()));
    }
    if count > limit.saturating_sub(bytes.len()) {
      return (
        bytes,
        Err(Error::new("convert_response", "response exceeds byte limit")),
      );
    }
    bytes.extend_from_slice(&buffer[..count]);
  }
}

pub(super) async fn read_all<T: Transport>(transport: &mut T, limit: usize) -> Result<Vec<u8>, Error> {
  let mut bytes = Vec::new();
  let mut buffer = [0u8; 1024];
  loop {
    let count = transport
      .read(&mut buffer)
      .await
      .map_err(|e| Error::new("convert_response", e))?;
    if count > buffer.len() {
      return Err(Error::new(
        "convert_response",
        "transport returned an invalid byte count",
      ));
    }
    if count == 0 {
      return Ok(bytes);
    }
    if count > limit.saturating_sub(bytes.len()) {
      return Err(Error::new("convert_response", "response exceeds byte limit"));
    }
    bytes.extend_from_slice(&buffer[..count]);
  }
}

pub(super) async fn read_error_body<T: Transport>(transport: &mut T, limit: usize) -> String {
  let mut bytes = Vec::new();
  let mut buffer = [0u8; 1024];
  loop {
    let capacity = buffer.len().min(limit.saturating_sub(bytes.len()).saturating_add(1));
    let count = match transport.read(&mut buffer[..capacity]).await {
      Ok(count) if count <= capacity => count,
      Ok(_) => return "transport returned an invalid byte count".into(),
      Err(error) => return format!("failed to read error body: {error}"),
    };
    if count == 0 {
      return String::from_utf8_lossy(&bytes).into_owned();
    }
    let remaining = limit.saturating_sub(bytes.len());
    bytes.extend_from_slice(&buffer[..count.min(remaining)]);
    if count > remaining {
      return format!("{} [truncated]", String::from_utf8_lossy(&bytes));
    }
  }
}

/// Pull-based response stream with one lifecycle for owned and borrowed HTTP exchanges.
pub struct EventStream<T: Transport, C: Clock, E: Events> {
  pub status: u16,
  pub headers: HeaderMap,
  observer: Observer<C, E>,
  body: Option<Exchange<T>>,
  decoder: Decoder,
  processor: StreamProcessor<Option<EndpointTranslator>>,
  buffer: [u8; 1024],
  position: usize,
  length: usize,
  request_id: String,
  model: SmolStr,
  endpoint: Endpoint,
  attempt: u32,
  report_terminal: bool,
  opaque: bool,
  inspect_opaque: bool,
  opaque_error: Option<Error>,
  capture: Option<StreamCapture>,
  bytes_streamed: u64,
  chunks: u64,
  last_progress: Duration,
  usage: tokn_core::usage::Usage,
}

struct StreamCapture {
  upstream: Vec<u8>,
  converted: Vec<u8>,
  limit: usize,
  upstream_truncated: bool,
  converted_truncated: bool,
}
impl StreamCapture {
  fn new(limit: usize) -> Self {
    Self {
      upstream: Vec::new(),
      converted: Vec::new(),
      limit,
      upstream_truncated: false,
      converted_truncated: false,
    }
  }
  fn append(bytes: &mut Vec<u8>, chunk: &[u8], limit: usize, truncated: &mut bool) {
    let remaining = limit.saturating_sub(bytes.len());
    bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    *truncated |= chunk.len() > remaining;
  }
}

impl<T: Transport, C: Clock, E: Events> EventStream<T, C, E> {
  #[allow(clippy::too_many_arguments)]
  fn new(
    observer: Observer<C, E>,
    limits: Limits,
    body: Exchange<T>,
    from: Endpoint,
    to: Endpoint,
    model: SmolStr,
    request_id: String,
    attempt: u32,
    head: ResponseHead,
    opaque: bool,
    inspect_opaque: bool,
  ) -> Self {
    let created_at = observer.clock.unix_seconds();
    let last_progress = observer.clock.now();
    let capture = observer
      .events
      .enabled()
      .then(|| StreamCapture::new(limits.max_response_bytes));
    observer.events.progress(StreamProgress {
      request_id: &request_id,
      model: &model,
      endpoint: to,
      usage: &Default::default(),
      bytes_streamed: 0,
      chunks: 0,
    });
    Self {
      status: head.status,
      headers: head.headers,
      observer,
      body: Some(body),
      decoder: Decoder::new(limits.max_event_bytes),
      processor: StreamProcessor::new(
        Some(from),
        (!opaque && from != to).then(|| EndpointTranslator::with_created_at(from, to, created_at)),
      ),
      buffer: [0; 1024],
      position: 0,
      length: 0,
      request_id,
      model,
      endpoint: to,
      attempt,
      report_terminal: true,
      opaque,
      inspect_opaque,
      opaque_error: None,
      capture,
      bytes_streamed: 0,
      chunks: 0,
      last_progress,
      usage: Default::default(),
    }
  }

  fn note_upstream(&mut self, bytes: &[u8]) {
    if let Some(capture) = &mut self.capture {
      StreamCapture::append(
        &mut capture.upstream,
        bytes,
        capture.limit,
        &mut capture.upstream_truncated,
      );
    }
  }

  fn note_converted(&mut self, bytes: &[u8]) {
    if let Some(capture) = &mut self.capture {
      StreamCapture::append(
        &mut capture.converted,
        bytes,
        capture.limit,
        &mut capture.converted_truncated,
      );
    }
    self.bytes_streamed = self.bytes_streamed.saturating_add(bytes.len() as u64);
    self.chunks = self.chunks.saturating_add(1);
    let now = self.observer.clock.now();
    if now.saturating_sub(self.last_progress) >= Duration::from_millis(500) {
      self.last_progress = now;
      self.observer.events.progress(StreamProgress {
        request_id: &self.request_id,
        model: &self.model,
        endpoint: self.endpoint,
        usage: &self.usage,
        bytes_streamed: self.bytes_streamed,
        chunks: self.chunks,
      });
    }
  }

  fn observe_usage(&mut self, value: &Value) {
    let Some(usage) = tokn_convert::usage::observed_usage(value) else {
      return;
    };
    self.usage = usage.clone();
    self.observer.emit_event(&self.request_id, self.attempt, None, || {
      Some(RequestEventPayload::Record(RecordEvent::Usage(usage)))
    });
  }

  fn finish(&mut self, outcome: StreamOutcome, error: Option<&Error>) {
    if !core::mem::take(&mut self.report_terminal) {
      return;
    }
    self.body.take();
    let fallback = (outcome != StreamOutcome::Complete).then(|| {
      Error::new(
        "stream",
        match outcome {
          StreamOutcome::Cancelled => "stream cancelled before completion",
          _ => "stream failed",
        },
      )
    });
    let error = error.or(fallback.as_ref());
    if let Some(capture) = self.capture.take() {
      let upstream_error = error.map(|error| error.message.clone()).or_else(|| {
        capture
          .upstream_truncated
          .then(|| SmolStr::new("upstream recording exceeded byte limit"))
      });
      let converted_error = error.map(|error| error.message.clone()).or_else(|| {
        capture
          .converted_truncated
          .then(|| SmolStr::new("converted recording exceeded byte limit"))
      });
      self.observer.emit_event(&self.request_id, self.attempt, None, || {
        Some(RequestEventPayload::Record(RecordEvent::UpstreamBody {
          body: Bytes::from(capture.upstream),
          error: upstream_error,
        }))
      });
      self.observer.emit_event(&self.request_id, self.attempt, None, || {
        Some(RequestEventPayload::Record(RecordEvent::ConvertedBody {
          body: Bytes::from(capture.converted),
          error: converted_error,
        }))
      });
    }
    if let Some(error) = error {
      self
        .observer
        .emit_error(&self.request_id, self.attempt, Stage::ConvertResponse, error, false);
    }
    self
      .observer
      .emit_complete(&self.request_id, self.attempt, outcome == StreamOutcome::Complete);
  }

  fn settle(&mut self, error: Option<&Error>) {
    if self.processor.input_closed() {
      self.body.take();
    }
    if let Some(outcome) = self.processor.outcome() {
      self.finish(outcome, error);
    }
  }

  pub fn cancel(&mut self) {
    self.processor.cancel();
    self.finish(StreamOutcome::Cancelled, None);
  }

  /// Read a parsed event, applying managed endpoint conversion when configured.
  pub async fn next_event(&mut self) -> Result<Option<SseEvent>, Error> {
    let result = self.read_event().await;
    if let Err(error) = &result {
      self.processor.fail();
      self.settle(Some(error));
    }
    result
  }

  /// Read client-facing bytes. Opaque relays preserve every byte, including
  /// SSE comments, line endings, unrecognized fields, and encoded entities.
  pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, Error> {
    if !self.opaque {
      return self
        .next_event()
        .await
        .map(|event| event.map(|event| tokn_convert::sse::codec::encode_event(&event)));
    }
    let result = self.read_opaque_chunk().await;
    if let Err(error) = &result {
      self.finish(StreamOutcome::Failed, Some(error));
    }
    result
  }

  async fn read_opaque_chunk(&mut self) -> Result<Option<Bytes>, Error> {
    let Some(body) = self.body.as_mut() else {
      return Ok(None);
    };
    let count = body
      .read(&mut self.buffer)
      .await
      .map_err(|error| Error::new("stream", error))?;
    if count > self.buffer.len() {
      return Err(Error::new("stream", "transport returned an invalid byte count"));
    }
    if count == 0 {
      if self.inspect_opaque {
        if let Err(error) = self.processor.finish() {
          self.opaque_error = Some(Error::new("stream", error));
        }
      }
      let error = self.opaque_error.take();
      self.finish(
        if error.is_some() {
          StreamOutcome::Failed
        } else {
          StreamOutcome::Complete
        },
        error.as_ref(),
      );
      return Ok(None);
    }
    let bytes = Bytes::copy_from_slice(&self.buffer[..count]);
    self.note_upstream(&bytes);
    self.note_converted(&bytes);
    if self.inspect_opaque {
      for byte in bytes.iter().copied() {
        let record = match self.decoder.push(byte) {
          Ok(record) => record,
          Err(error) => {
            self.opaque_error = Some(Error::new("stream", error));
            self.inspect_opaque = false;
            break;
          }
        };
        let Some(record) = record else {
          continue;
        };
        let event = SseEvent::from(record);
        if let Some(value) = &event.json {
          self.observe_usage(value);
        }
        if let Err(error) = self.processor.push(event) {
          self.opaque_error = Some(Error::new("stream", error));
          self.inspect_opaque = false;
          break;
        }
        while self.processor.next_event().is_some() {}
      }
    }
    Ok(Some(bytes))
  }

  async fn read_event(&mut self) -> Result<Option<SseEvent>, Error> {
    loop {
      let event = self.processor.next_event();
      if let Some(event) = &event {
        self.note_converted(&tokn_convert::sse::codec::encode_event(event));
      }
      self.settle(None);
      if event.is_some() {
        return Ok(event);
      }
      let Some(body) = self.body.as_mut() else {
        return Ok(None);
      };
      if self.position == self.length {
        let count = body
          .read(&mut self.buffer)
          .await
          .map_err(|error| Error::new("stream", error))?;
        if count > self.buffer.len() {
          return Err(Error::new("stream", "transport returned an invalid byte count"));
        }
        if count == 0 {
          self.processor.finish().map_err(|error| Error::new("stream", error))?;
          continue;
        }
        let bytes = Bytes::copy_from_slice(&self.buffer[..count]);
        self.note_upstream(&bytes);
        self.position = 0;
        self.length = count;
      }
      while self.position < self.length {
        let byte = self.buffer[self.position];
        self.position += 1;
        let Some(record) = self.decoder.push(byte).map_err(|error| Error::new("stream", error))? else {
          continue;
        };
        let event = SseEvent::from(record);
        if let Some(value) = &event.json {
          self.observe_usage(value);
        }
        self
          .processor
          .push(event)
          .map_err(|error| Error::new("stream", error))?;
        break;
      }
    }
  }
}
impl<T: Transport, C: Clock, E: Events> Drop for EventStream<T, C, E> {
  fn drop(&mut self) {
    self.cancel();
  }
}
