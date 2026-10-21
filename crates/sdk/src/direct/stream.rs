use alloc::{boxed::Box, vec::Vec};
use core::fmt;
use serde::Serialize;
use serde_json::Value;
use tokn_endpoint_chat_completions::{ChatChunk, ChatEvent, ChatRequest};
use tokn_endpoint_messages::{MessagesEvent, MessagesRequest};
use tokn_endpoint_responses::{ResponsesEvent, ResponsesRequest};

use super::{
  is_event_stream,
  sse::{Decoder, Record},
  DirectClient, Endpoint, Header, Response, StreamBody, StreamError, StreamingResponse, StreamingTransport,
};

/// Buffer limits for one streaming request.
///
/// These bound SSE records and retained HTTP error bodies, not the allocator's
/// total usage. Parsed JSON, typed events, transport buffers, and any events
/// retained by the caller require additional memory.
#[derive(Clone, Copy, Debug)]
pub struct StreamOptions {
  /// Maximum bytes in one SSE record, including fields and comments.
  /// CRLF counts as one newline. Default: 64 KiB.
  pub max_event_bytes: usize,
  /// Maximum bytes retained from a non-2xx response. Default: 8 KiB.
  pub max_error_body_bytes: usize,
}

impl Default for StreamOptions {
  fn default() -> Self {
    Self {
      max_event_bytes: 64 * 1024,
      max_error_body_bytes: 8 * 1024,
    }
  }
}

type DecodeEvent<E> = fn(Record) -> Result<(E, bool), EventError>;

enum EventError {
  Json(serde_json::Error),
  Invalid(&'static str),
  Provider(Value),
}

impl EventError {
  fn into_stream_error<E>(self) -> StreamError<E> {
    match self {
      Self::Json(error) => StreamError::Deserialize(error),
      Self::Invalid(reason) => StreamError::InvalidEvent(reason),
      Self::Provider(data) => StreamError::Provider { data },
    }
  }
}

/// Pull-based typed SSE events with bounded buffering and no executor dependency.
///
/// Consume with [`Self::next_event`]. Dropping or cancelling this stream drops
/// its body immediately; terminal events and failures also release the body.
/// No background task reads ahead. At most one record and a 1 KiB read buffer
/// are retained, in addition to the event currently being decoded.
pub struct EventStream<B, E> {
  pub status: u16,
  pub headers: Vec<Header>,
  body: Option<B>,
  decoder: Option<Decoder>,
  buffer: [u8; 1024],
  position: usize,
  length: usize,
  decode: DecodeEvent<E>,
}

impl<B, E> fmt::Debug for EventStream<B, E> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("EventStream")
      .field("status", &self.status)
      .field("headers", &self.headers)
      .field("closed", &self.body.is_none())
      .finish_non_exhaustive()
  }
}

impl<B, E> EventStream<B, E> {
  /// Stop reading and drop the response body. Drop the stream itself to release
  /// a borrow of the transport. This does not send a provider cancellation API call.
  pub fn cancel(&mut self) {
    self.body = None;
    self.decoder = None;
    self.position = 0;
    self.length = 0;
  }
}

impl<B: StreamBody, E> EventStream<B, E> {
  /// Read one endpoint event. After a terminal event or error, returns `Ok(None)`.
  ///
  /// A Chat Completions stream yields `ChatEvent::Done`; a Responses stream
  /// yields its terminal event (including failed/incomplete/error outcomes).
  /// A Messages stream yields `MessagesEvent::MessageStop` or `MessagesEvent::Error`.
  /// EOF before that event is an error. Callers must inspect terminal events
  /// to distinguish provider success from provider failure.
  ///
  /// Dropping this future preserves parser state. It can be called again
  /// provided the injected body's reads obey the cancellation-safe contract.
  pub async fn next_event(&mut self) -> Result<Option<E>, StreamError<B::Error>> {
    let result = self.read_event().await;
    if result.is_err() {
      self.cancel();
    }
    result
  }

  async fn read_event(&mut self) -> Result<Option<E>, StreamError<B::Error>> {
    while let Some(body) = &mut self.body {
      if self.position == self.length {
        let length = body.read(&mut self.buffer).await.map_err(StreamError::Transport)?;
        if length > self.buffer.len() {
          return Err(StreamError::InvalidReadSize);
        }
        if length == 0 {
          return Err(StreamError::UnexpectedEof);
        }
        self.position = 0;
        self.length = length;
      }
      while self.position < self.length {
        let byte = self.buffer[self.position];
        self.position += 1;
        let Some(record) = self
          .decoder
          .as_mut()
          .expect("open stream has decoder")
          .push(byte)
          .map_err(StreamError::Sse)?
        else {
          continue;
        };
        // Empty data fields are often used as keepalives.
        if record.data.trim().is_empty() {
          continue;
        }
        let (event, terminal) = (self.decode)(record).map_err(EventError::into_stream_error)?;
        if terminal {
          self.cancel();
        }
        return Ok(Some(event));
      }
    }
    Ok(None)
  }
}

impl<T: StreamingTransport> DirectClient<T> {
  /// Stream native Responses events. The method sets `stream: true`, including
  /// when the request's typed field or flattened extras specify otherwise.
  pub async fn responses_stream<'a>(
    &'a mut self,
    request: &ResponsesRequest,
    options: StreamOptions,
  ) -> Result<EventStream<T::Body<'a>, ResponsesEvent>, StreamError<T::Error>> {
    self
      .start_stream(Endpoint::Responses, request, options, decode_responses)
      .await
  }

  /// Stream native Chat Completions chunks followed by `ChatEvent::Done`.
  /// A chunk's `finish_reason` does not end the stream: usage may follow it.
  /// This method always sets `stream: true`.
  pub async fn chat_completions_stream<'a>(
    &'a mut self,
    request: &ChatRequest,
    options: StreamOptions,
  ) -> Result<EventStream<T::Body<'a>, ChatEvent>, StreamError<T::Error>> {
    self
      .start_stream(Endpoint::ChatCompletions, request, options, decode_chat)
      .await
  }

  /// Stream native Messages events through the configured Messages endpoint.
  /// This method always sets `stream: true`. Ping and content-block stop events
  /// do not end the stream; `MessageStop` and provider `Error` events do.
  pub async fn messages_stream<'a>(
    &'a mut self,
    request: &MessagesRequest,
    options: StreamOptions,
  ) -> Result<EventStream<T::Body<'a>, MessagesEvent>, StreamError<T::Error>> {
    self
      .start_stream(Endpoint::Messages, request, options, decode_messages)
      .await
  }

  async fn start_stream<'a, Q: Serialize, E>(
    &'a mut self,
    endpoint: Endpoint,
    request: &Q,
    options: StreamOptions,
    decode: DecodeEvent<E>,
  ) -> Result<EventStream<T::Body<'a>, E>, StreamError<T::Error>> {
    let mut body = serde_json::to_value(request).map_err(StreamError::Serialize)?;
    body["stream"] = Value::Bool(true);
    let body = serde_json::to_vec(&body).map_err(StreamError::Serialize)?;
    let request = self
      .http_request(endpoint, body, true)
      .map_err(StreamError::UnsupportedEndpoint)?;
    let response = self
      .transport
      .send_stream(request)
      .await
      .map_err(StreamError::Transport)?;
    open_stream(response, options, decode).await
  }
}

async fn open_stream<B: StreamBody, E>(
  response: StreamingResponse<B>,
  options: StreamOptions,
  decode: DecodeEvent<E>,
) -> Result<EventStream<B, E>, StreamError<B::Error>> {
  let StreamingResponse {
    status,
    headers,
    mut body,
  } = response;
  if !(200..300).contains(&status) {
    let mut bytes = Vec::new();
    let mut buffer = [0; 1024];
    let truncated = loop {
      let remaining = options.max_error_body_bytes - bytes.len();
      let capacity = buffer.len().min(remaining.saturating_add(1));
      let length = body
        .read(&mut buffer[..capacity])
        .await
        .map_err(StreamError::Transport)?;
      if length > capacity {
        return Err(StreamError::InvalidReadSize);
      }
      bytes.extend_from_slice(&buffer[..length.min(remaining)]);
      if length == 0 || length > remaining {
        break length > remaining;
      }
    };
    return Err(StreamError::HttpStatus {
      response: Response {
        status,
        headers,
        body: bytes,
      },
      truncated,
    });
  }
  if !is_event_stream(&headers) {
    return Err(StreamError::UnexpectedContentType { status, headers });
  }
  Ok(EventStream {
    status,
    headers,
    body: Some(body),
    decoder: Some(Decoder::new(options.max_event_bytes)),
    buffer: [0; 1024],
    position: 0,
    length: 0,
    decode,
  })
}

fn decode_chat(record: Record) -> Result<(ChatEvent, bool), EventError> {
  if record.data.trim() == "[DONE]" {
    if record.event.as_deref() == Some("error") {
      return Err(EventError::Invalid("error event cannot contain a completion sentinel"));
    }
    return Ok((ChatEvent::Done, true));
  }
  let value: Value = serde_json::from_str(&record.data).map_err(EventError::Json)?;
  if value.get("error").is_some_and(|error| !error.is_null()) || record.event.as_deref() == Some("error") {
    return Err(EventError::Provider(value));
  }
  if !value.get("choices").is_some_and(Value::is_array) {
    return Err(EventError::Invalid("chat chunk must contain a choices array"));
  }
  let chunk: ChatChunk = serde_json::from_value(value).map_err(EventError::Json)?;
  Ok((ChatEvent::Chunk(Box::new(chunk)), false))
}

fn decode_responses(record: Record) -> Result<(ResponsesEvent, bool), EventError> {
  let value: Value = serde_json::from_str(&record.data).map_err(EventError::Json)?;
  let kind = event_kind(&record, &value, "Responses event must contain a nonempty type")?;
  let terminal = matches!(
    kind,
    "response.completed" | "response.failed" | "response.incomplete" | "response.cancelled" | "error"
  );
  let event: ResponsesEvent = serde_json::from_value(value).map_err(EventError::Json)?;
  if matches!(&event, ResponsesEvent::Other(_)) && ResponsesEvent::is_known_kind(event.kind()) {
    return Err(EventError::Invalid("known Responses event has an invalid payload"));
  }
  Ok((event, terminal))
}

fn decode_messages(record: Record) -> Result<(MessagesEvent, bool), EventError> {
  let value: Value = serde_json::from_str(&record.data).map_err(EventError::Json)?;
  let kind = event_kind(&record, &value, "Messages event must contain a nonempty type")?;
  if (kind == "error" && !value.get("error").is_some_and(Value::is_object))
    || (kind == "message_delta" && !value.get("delta").is_some_and(Value::is_object))
  {
    return Err(EventError::Invalid("Messages event requires an object payload"));
  }
  let terminal = matches!(kind, "message_stop" | "error");
  let event: MessagesEvent = serde_json::from_value(value).map_err(EventError::Json)?;
  if matches!(&event, MessagesEvent::Other(_)) && MessagesEvent::is_known_kind(event.kind()) {
    return Err(EventError::Invalid("known Messages event has an invalid payload"));
  }
  Ok((event, terminal))
}

fn event_kind<'a>(record: &Record, value: &'a Value, missing_type: &'static str) -> Result<&'a str, EventError> {
  let kind = value
    .get("type")
    .and_then(Value::as_str)
    .filter(|kind| !kind.is_empty())
    .ok_or(EventError::Invalid(missing_type))?;
  // SSE dispatches records without an explicit name as "message".
  if record
    .event
    .as_deref()
    .is_some_and(|name| name != "message" && name != kind)
  {
    return Err(EventError::Invalid("SSE event name disagrees with the JSON type"));
  }
  Ok(kind)
}
