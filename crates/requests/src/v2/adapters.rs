//! Standard HTTP and timing adapters for configured request execution.
//!
//! These adapters know nothing about profiles, account selection, credentials,
//! provider request shaping, or retry policy.

use crate::session::{Clock, Events};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokn_core::event::EventBus;
use tokn_core::request_event::RequestEvent;
use tokn_service::transport::{Request, ResponseHead, Transport};

/// An independently owned HTTP exchange using a shared connection pool.
pub struct ReqwestExchange {
  managed: reqwest::Client,
  opaque: reqwest::Client,
  preserve_encoding: bool,
  response: Option<reqwest::Response>,
  pending: Bytes,
}

#[derive(Debug)]
pub struct TransportError(reqwest::Error);

impl std::fmt::Display for TransportError {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let mut error: &dyn std::error::Error = &self.0;
    write!(formatter, "{error}")?;
    while let Some(source) = error.source() {
      write!(formatter, ": {source}")?;
      error = source;
    }
    Ok(())
  }
}

impl ReqwestExchange {
  #[cfg(test)]
  pub fn new(http: reqwest::Client) -> Self {
    Self::with_clients(http.clone(), http)
  }

  pub fn with_clients(managed: reqwest::Client, opaque: reqwest::Client) -> Self {
    Self {
      managed,
      opaque,
      preserve_encoding: false,
      response: None,
      pending: Bytes::new(),
    }
  }
}

impl Transport for ReqwestExchange {
  type Error = TransportError;

  async fn send(&mut self, mut request: Request) -> Result<ResponseHead, Self::Error> {
    self.close_response();
    // Host/SNI and payload length belong to HTTP transport. Inbound or persona
    // values must not override the destination selected by the shared session.
    request.headers.remove("host");
    request.headers.remove("content-length");
    let method = reqwest::Method::from_bytes(request.method.as_bytes()).expect("valid execution method");
    let http = if self.preserve_encoding {
      &self.opaque
    } else {
      &self.managed
    };
    let response = http
      .request(method, request.url)
      .headers(request.headers.into())
      .body(request.body)
      .send()
      .await
      .map_err(TransportError)?;
    let head = ResponseHead {
      status: response.status().as_u16(),
      headers: response.headers().into(),
    };
    self.response = Some(response);
    Ok(head)
  }

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    if buffer.is_empty() {
      return Ok(0);
    }
    while self.pending.is_empty() {
      let Some(response) = self.response.as_mut() else {
        return Ok(0);
      };
      match response.chunk().await.map_err(TransportError)? {
        Some(chunk) => self.pending = chunk,
        None => {
          self.response = None;
          return Ok(0);
        }
      }
    }
    let count = buffer.len().min(self.pending.len());
    buffer[..count].copy_from_slice(&self.pending[..count]);
    self.pending = self.pending.slice(count..);
    Ok(count)
  }

  fn accept_encoding(&self) -> &str {
    "gzip, deflate, br, zstd"
  }

  fn response_is_decoded(&self) -> bool {
    !self.preserve_encoding
  }

  fn configure_body_mode(&mut self, preserve_encoding: bool) {
    self.preserve_encoding = preserve_encoding;
  }

  fn encode_body(&self, body: &[u8], encoding: Option<&str>) -> Result<Bytes, tokn_service::transport::EncodeError> {
    let mut headers = tokn_core::headers::HeaderMap::new();
    if let Some(encoding) = encoding {
      headers.insert("content-encoding", encoding.to_string());
    }
    let encoding = crate::utils::codec::request_content_encoding(&headers)
      .map_err(|error| tokn_service::transport::EncodeError(error.to_string()))?;
    crate::utils::codec::encode_body_bytes(body, encoding)
      .map_err(|error| tokn_service::transport::EncodeError(error.to_string()))
  }

  fn close_response(&mut self) {
    self.response = None;
    self.pending = Bytes::new();
  }

  fn recoverable(&self, error: &Self::Error) -> bool {
    !error.0.is_builder()
  }
}

/// Per-exchange clock. Clones share the monotonic origin.
#[derive(Clone)]
pub struct TokioRuntime {
  origin: Instant,
}

impl TokioRuntime {
  pub fn new() -> Self {
    Self { origin: Instant::now() }
  }
}

impl Default for TokioRuntime {
  fn default() -> Self {
    Self::new()
  }
}

impl Clock for TokioRuntime {
  fn now(&self) -> Duration {
    self.origin.elapsed()
  }

  fn unix_seconds(&self) -> u64 {
    tokn_core::util::now_unix_ms().max(0) as u64 / 1_000
  }

  fn unix_millis(&self) -> i64 {
    tokn_core::util::now_unix_ms()
  }

  async fn sleep(&mut self, duration: Duration) {
    tokio::time::sleep(duration).await;
  }
}

pub struct StandardEvents {
  bus: Arc<EventBus>,
  progress: std::sync::Mutex<Option<ProgressTask>>,
}

struct ProgressTask {
  snapshot: Arc<std::sync::Mutex<ProgressSnapshot>>,
  task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct ProgressSnapshot {
  active: bool,
  request_id: String,
  model: String,
  endpoint: String,
  usage: tokn_core::usage::Usage,
  bytes_streamed: u64,
  chunks: u64,
}

impl StandardEvents {
  pub fn new(bus: Arc<EventBus>) -> Self {
    Self {
      bus,
      progress: std::sync::Mutex::new(None),
    }
  }

  fn stop_progress(&self) {
    if let Some(progress) = self
      .progress
      .lock()
      .unwrap_or_else(std::sync::PoisonError::into_inner)
      .take()
    {
      progress
        .snapshot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .active = false;
      progress.task.abort();
    }
  }
}

impl Events for StandardEvents {
  fn progress(&self, progress: crate::session::StreamProgress<'_>) {
    let snapshot = ProgressSnapshot {
      active: true,
      request_id: progress.request_id.to_string(),
      model: progress.model.to_string(),
      endpoint: progress.endpoint.as_str().to_string(),
      usage: progress.usage.clone(),
      bytes_streamed: progress.bytes_streamed,
      chunks: progress.chunks,
    };
    let mut guard = self.progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(progress) = guard.as_ref() {
      *progress
        .snapshot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = snapshot;
      return;
    }
    let snapshot = Arc::new(std::sync::Mutex::new(snapshot));
    let latest = snapshot.clone();
    let bus = self.bus.clone();
    let task = tokio::spawn(async move {
      let mut interval = tokio::time::interval(Duration::from_millis(500));
      interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
      interval.tick().await;
      loop {
        interval.tick().await;
        let snapshot = latest.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if snapshot.active {
          bus.emit(tokn_core::event::Event::StreamProgress {
            request_id: snapshot.request_id.clone(),
            model: snapshot.model.clone(),
            endpoint: snapshot.endpoint.clone(),
            usage: snapshot.usage.clone(),
            bytes_streamed: snapshot.bytes_streamed,
            chunks: snapshot.chunks,
          });
        }
      }
    });
    *guard = Some(ProgressTask { snapshot, task });
  }

  fn enabled(&self) -> bool {
    true
  }

  fn emit(&self, event: RequestEvent) {
    if matches!(
      event.payload,
      tokn_core::request_event::RequestEventPayload::Stage(tokn_core::request_event::StageEvent::Completed { .. })
    ) {
      self.stop_progress();
    }
    self.bus.emit(tokn_core::event::Event::Requests(event));
  }
}

impl Drop for StandardEvents {
  fn drop(&mut self) {
    self.stop_progress();
  }
}

/// Adapt owned standard exchanges into the same configured session used by
/// injected callers. The service clones only immutable state and pool handles;
/// every request owns its transport, timer handle, and response lifecycle.
pub(super) fn request_service(
  state: Arc<crate::session::ExecutionState>,
  profile: tokn_policy::ProfileId,
  providers: &tokn_accounts::link::ProviderGraph,
  http: &super::HttpClients,
  events: Arc<EventBus>,
  mode: super::BuildMode,
) -> crate::RequestService {
  let bindings: Arc<[_]> = providers.bindings().cloned().collect();
  let managed = http.managed.clone();
  let opaque = http.opaque.clone();
  let clock = TokioRuntime::new();
  crate::RequestService::new(tower::service_fn(move |request: crate::ExecutionRequest| {
    let state = state.clone();
    let profile = profile.clone();
    let bindings = bindings.clone();
    let transport = ReqwestExchange::with_clients(managed.clone(), opaque.clone());
    let clock = clock.clone();
    let events = StandardEvents::new(events.clone());
    async move {
      let (inbound, config) = request.into_parts();
      let endpoint = inbound.request_endpoint.resolved().ok_or_else(|| {
        crate::PipelineError::permanent(
          crate::Stage::Extract,
          crate::pipeline::error::RequestsError::MissingResolvedEndpoint {
            request_endpoint: inbound.request_endpoint.as_str().into(),
          },
        )
      })?;
      refresh_models(&state, &bindings);
      // Opaque gateway admission can retain only the decoded bytes. Parse the
      // inspection copy here while leaving the original wire payload intact.
      let body = if inbound.body_json.is_null() {
        serde_json::from_slice(&inbound.decoded_body).unwrap_or(serde_json::Value::Null)
      } else {
        inbound.body_json
      };
      let mut input = crate::session::RequestInput::new(
        endpoint,
        body,
        inbound.headers,
        inbound.request_id.map_or_else(request_id, |id| id.to_string()),
      );
      input.raw_body = inbound.raw_body;
      input.decoded_body = inbound.decoded_body;
      input.content_encoding = input
        .headers
        .get("content-encoding")
        .map(|value| value.as_str().to_string());
      input.profile = Some(profile.to_string());
      input.generation_options = config.generation_options().cloned();
      input.allowed_providers = allowed_providers(&config)?;
      input.method = request_method(config.request_method())?;
      input.dry_run = matches!(mode, super::BuildMode::DryRun);
      let response = crate::session::Session::new(state, input, transport, clock, events)
        .with_limits(crate::session::Limits {
          max_response_bytes: usize::MAX,
          max_event_bytes: usize::MAX,
          max_error_body_bytes: 8 * 1024,
        })
        .run()
        .await
        .map_err(|error| pipeline_error(error, endpoint))?;
      Ok::<_, crate::PipelineError>(into_standard_response(response))
    }
  }))
}

fn request_id() -> String {
  use std::sync::atomic::{AtomicU64, Ordering};
  static NEXT: AtomicU64 = AtomicU64::new(0);
  format!(
    "req-{}-{}",
    tokn_core::util::now_unix_ms(),
    NEXT.fetch_add(1, Ordering::Relaxed)
  )
}

fn request_method(method: Option<&http::Method>) -> Result<&'static str, crate::PipelineError> {
  match method.map(http::Method::as_str).unwrap_or("POST") {
    "GET" => Ok("GET"),
    "HEAD" => Ok("HEAD"),
    "POST" => Ok("POST"),
    "PUT" => Ok("PUT"),
    "PATCH" => Ok("PATCH"),
    "DELETE" => Ok("DELETE"),
    "OPTIONS" => Ok("OPTIONS"),
    "CONNECT" => Ok("CONNECT"),
    "TRACE" => Ok("TRACE"),
    method => Err(super::selection::invalid_route_request(format!(
      "unsupported HTTP method '{method}'"
    ))),
  }
}

fn allowed_providers(
  config: &crate::RunConfig,
) -> Result<Option<std::collections::BTreeSet<String>>, crate::PipelineError> {
  let Some(value) = config.get(crate::stages::ACCESS_ALLOWED_PROVIDERS_KEY) else {
    return Ok(None);
  };
  value
    .as_array()
    .and_then(|values| values.iter().map(|value| value.as_str().map(str::to_owned)).collect())
    .map(Some)
    .ok_or_else(|| {
      crate::PipelineError::permanent(
        crate::Stage::Resolve,
        crate::pipeline::error::RequestsError::InvalidAccessPolicy,
      )
    })
}

fn refresh_models(state: &crate::session::ExecutionState, bindings: &[Arc<tokn_accounts::link::ProviderBinding>]) {
  use crate::session::{ModelCapabilities, ModelInventory};
  use std::collections::BTreeSet;
  for binding in bindings {
    let provider = binding.driver();
    let info = provider.info();
    let catalogue = info
      .model_cache
      .catalogue_models()
      .unwrap_or_else(|| info.default_models.clone());
    let mut ids = catalogue.iter().map(|model| model.id.clone()).collect::<BTreeSet<_>>();
    ids.extend(info.model_cache.snapshot().unwrap_or_default());
    let mut inventory = ModelInventory::new();
    for id in ids {
      let reasoning_efforts = provider.reasoning_efforts(&id);
      let reasoning_supported = if reasoning_efforts.as_ref().is_some_and(|values| !values.is_empty()) {
        Some(true)
      } else {
        provider.model_info(&id).map(|model| model.capabilities.reasoning)
      };
      let endpoints = [
        tokn_core::Endpoint::ChatCompletions,
        tokn_core::Endpoint::Responses,
        tokn_core::Endpoint::Messages,
      ]
      .into_iter()
      .filter(|endpoint| provider.has_endpoint(&id, *endpoint))
      .collect();
      inventory.insert(
        id.into(),
        ModelCapabilities {
          endpoints: Some(endpoints),
          reasoning_efforts,
          reasoning_supported,
        },
      );
    }
    state.set_binding_models(binding.provider_id(), binding.account_id(), inventory);
  }
}

fn pipeline_error(error: crate::session::Error, endpoint: tokn_core::Endpoint) -> crate::PipelineError {
  use crate::pipeline::error::RequestsError;
  use crate::session::ErrorKind;
  use crate::Stage;
  let stage = match error.stage {
    "extract" => Stage::Extract,
    "resolve" => Stage::Resolve,
    "build_headers" => Stage::BuildHeaders,
    "convert_request" => Stage::ConvertRequest,
    "send" => Stage::Send,
    _ => Stage::ConvertResponse,
  };
  if error.stop {
    return crate::PipelineError::stop(stage);
  }
  let source = match error.kind {
    ErrorKind::ProviderAccessDenied => RequestsError::ProviderAccessDenied,
    ErrorKind::NoAccount { model } => RequestsError::NoAccount { endpoint, model },
    ErrorKind::InvalidRoute => RequestsError::InvalidRouteRequest { message: error.message },
    ErrorKind::UpstreamStatus { status, body } => RequestsError::UpstreamStatus {
      status,
      body: body.to_string(),
    },
    ErrorKind::Stopped => RequestsError::Stop,
    ErrorKind::Preparation(source) => source.into(),
    ErrorKind::Other => RequestsError::Other {
      source: Box::new(std::io::Error::other(error.message.to_string())),
    },
  };
  if error.recoverable {
    crate::PipelineError::recoverable(stage, source)
  } else {
    crate::PipelineError::permanent(stage, source)
  }
}

fn into_standard_response(
  response: crate::session::Response<ReqwestExchange, TokioRuntime, StandardEvents>,
) -> crate::pipeline::stages::ConvertedResponse {
  use crate::pipeline::stages::{ConvertedBody, ConvertedResponse, ConvertedResponseKind};
  use futures_util::StreamExt;
  let kind = if response.opaque {
    ConvertedResponseKind::Opaque
  } else {
    ConvertedResponseKind::Managed
  };
  let body = match response.body {
    crate::session::ResponseBody::Buffered(body_bytes) => ConvertedBody::Buffered {
      body_json: (!response.opaque)
        .then(|| serde_json::from_slice(&body_bytes).ok().map(Arc::new))
        .flatten(),
      body_bytes,
    },
    crate::session::ResponseBody::Stream(stream) => {
      let body = futures_util::stream::unfold(Some(stream), |stream| async move {
        let mut stream = stream?;
        match stream.next_chunk().await {
          Ok(Some(bytes)) => Some((Ok(bytes), Some(stream))),
          Ok(None) => None,
          Err(error) => Some((Err(std::io::Error::other(error.to_string())), None)),
        }
      })
      .boxed();
      ConvertedBody::Stream { body }
    }
  };
  ConvertedResponse {
    status: response.status,
    headers: response.headers,
    kind,
    body,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn partial_reads_preserve_remaining_response_bytes() {
    let mut transport = ReqwestExchange::new(reqwest::Client::new());
    transport.response = Some(reqwest::Response::from(
      http::Response::builder().body("abcdefgh").unwrap(),
    ));
    let mut buffer = [0; 3];
    assert_eq!(transport.read(&mut buffer).await.unwrap(), 3);
    assert_eq!(&buffer, b"abc");
    assert_eq!(transport.read(&mut buffer).await.unwrap(), 3);
    assert_eq!(&buffer, b"def");
    assert_eq!(transport.read(&mut buffer).await.unwrap(), 2);
    assert_eq!(&buffer[..2], b"gh");
    assert_eq!(transport.read(&mut buffer).await.unwrap(), 0);
  }

  #[tokio::test]
  async fn close_releases_both_live_response_and_pending_chunk() {
    let mut transport = ReqwestExchange::new(reqwest::Client::new());
    transport.response = Some(reqwest::Response::from(http::Response::new("body")));
    transport.pending = Bytes::from_static(b"pending");
    transport.close_response();
    assert!(transport.response.is_none());
    assert!(transport.pending.is_empty());
    assert_eq!(transport.read(&mut [0; 8]).await.unwrap(), 0);
  }
}
