use crate::config::MockLlmConfig;
use crate::route::MockResponse;
use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const MAX_BODY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct CapturedRequest {
  pub method: Method,
  pub path: String,
  pub query: Option<String>,
  pub headers: Vec<(String, String)>,
  pub body: Bytes,
}

impl CapturedRequest {
  pub fn header(&self, name: &str) -> Option<&str> {
    self
      .headers
      .iter()
      .find(|(header, _)| header.eq_ignore_ascii_case(name))
      .map(|(_, value)| value.as_str())
  }
}

pub struct MockLlmServer {
  base_url: String,
  requests: Arc<Mutex<Vec<CapturedRequest>>>,
  shutdown: Option<oneshot::Sender<()>>,
  task: Option<JoinHandle<()>>,
}

impl MockLlmServer {
  pub async fn start(config: MockLlmConfig) -> Self {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
      .await
      .expect("bind mock llm listener");
    let addr = listener.local_addr().expect("read mock llm listener addr");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let state = Arc::new(MockState {
      config,
      requests: requests.clone(),
    });
    let app = Router::new().fallback(any(handle_request)).with_state(state);
    let task = tokio::spawn(async move {
      let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = shutdown_rx.await;
      });
      let _ = server.await;
    });
    Self {
      base_url: format!("http://{addr}"),
      requests,
      shutdown: Some(shutdown_tx),
      task: Some(task),
    }
  }

  pub fn base_url(&self) -> &str {
    &self.base_url
  }

  pub fn url(&self, path: &str) -> String {
    format!("{}{}", self.base_url, path)
  }

  pub fn requests(&self) -> Vec<CapturedRequest> {
    self.requests.lock().unwrap().clone()
  }

  pub fn last_request(&self) -> Option<CapturedRequest> {
    self.requests.lock().unwrap().last().cloned()
  }

  pub async fn shutdown(mut self) {
    if let Some(tx) = self.shutdown.take() {
      let _ = tx.send(());
    }
    if let Some(task) = self.task.take() {
      let _ = task.await;
    }
  }
}

impl Drop for MockLlmServer {
  fn drop(&mut self) {
    if let Some(tx) = self.shutdown.take() {
      let _ = tx.send(());
    }
    if let Some(task) = self.task.take() {
      task.abort();
    }
  }
}

struct MockState {
  config: MockLlmConfig,
  requests: Arc<Mutex<Vec<CapturedRequest>>>,
}

async fn handle_request(State(state): State<Arc<MockState>>, request: Request) -> Response {
  let (parts, body) = request.into_parts();
  let body = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
    Ok(body) => body,
    Err(err) => {
      if err.to_string().contains("length limit exceeded") {
        return (
          StatusCode::PAYLOAD_TOO_LARGE,
          format!("request body exceeds {MAX_BODY_BYTES} bytes"),
        )
          .into_response();
      }
      return (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("failed to read request body: {err}"),
      )
        .into_response();
    }
  };

  state.requests.lock().unwrap().push(CapturedRequest {
    method: parts.method.clone(),
    path: parts.uri.path().to_string(),
    query: parts.uri.query().map(str::to_string),
    headers: capture_headers(&parts.headers),
    body: body.clone(),
  });

  let Some(route) = state
    .config
    .routes
    .iter()
    .rev()
    .find(|route| route.endpoint.method() == parts.method && route.endpoint.path() == parts.uri.path())
  else {
    return (
      StatusCode::NOT_FOUND,
      format!("mock endpoint not configured for {} {}", parts.method, parts.uri.path()),
    )
      .into_response();
  };

  if let Some(auth) = &state.config.auth {
    let Some(actual) = parts
      .headers
      .get(auth.header_name.as_str())
      .and_then(|value| value.to_str().ok())
    else {
      return (StatusCode::UNAUTHORIZED, format!("missing {} header", auth.header_name)).into_response();
    };
    if !auth.accepted_values.iter().any(|expected| expected == actual) {
      return (
        StatusCode::UNAUTHORIZED,
        format!("unexpected {} header", auth.header_name),
      )
        .into_response();
    }
  }

  for required in &state.config.required_headers {
    let Some(actual) = parts
      .headers
      .get(required.name.as_str())
      .and_then(|value| value.to_str().ok())
    else {
      return (
        StatusCode::BAD_REQUEST,
        format!("missing required header {}", required.name),
      )
        .into_response();
    };
    if let Some(expected) = &required.value {
      if actual != expected {
        return (
          StatusCode::BAD_REQUEST,
          format!("unexpected value for header {}", required.name),
        )
          .into_response();
      }
    }
  }

  for forbidden in &state.config.forbidden_headers {
    if parts.headers.contains_key(forbidden.as_str()) {
      return (
        StatusCode::BAD_REQUEST,
        format!("forbidden header present: {forbidden}"),
      )
        .into_response();
    }
  }

  build_response(&route.response)
}

fn capture_headers(headers: &HeaderMap) -> Vec<(String, String)> {
  headers
    .iter()
    .map(|(name, value)| {
      (
        name.as_str().to_string(),
        value.to_str().unwrap_or_default().to_string(),
      )
    })
    .collect()
}

fn build_response(response: &MockResponse) -> Response {
  let mut out = Response::new(axum::body::Body::from(response.body.clone()));
  *out.status_mut() = response.status;
  for (name, value) in &response.headers {
    let Ok(name) = HeaderName::try_from(name.as_str()) else {
      continue;
    };
    let Ok(value) = HeaderValue::from_str(value) else {
      continue;
    };
    out.headers_mut().insert(name, value);
  }
  out
}
