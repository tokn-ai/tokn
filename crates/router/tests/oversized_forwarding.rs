//! Exercise oversized request forwarding through the real public frontend,
//! Unix IPC, worker pipeline, and default request persistence policy.
#![cfg(unix)]

use axum::body::{to_bytes, Bytes};
use axum::extract::{Request, State};
use axum::routing::post;
use axum::Router;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokn_access::AccessStore;
use tokn_core::event::{spawn_event_loop, EventBus};
use tokn_persistence::requests::read_request_row;
use tokn_persistence::{RequestEventHandler, RequestPersistenceOptions};
use tokn_router::frontend::Frontend;
use tokn_router::ipc::{serve_worker, WorkerEndpoint, WorkerPool};
use tokn_router::v2::{build_worker_runtime_states, LiveRuntime};

const CAPTURE_LIMIT: usize = 10 * 1024 * 1024;
const RESPONSE: &str = r#"{"id":"response-oversized","model":"gpt-test","output":[],"status":"completed"}"#;

struct UpstreamCapture {
  marker: String,
  content_length: usize,
  body: Bytes,
}

async fn capture_upstream(State(sender): State<mpsc::Sender<UpstreamCapture>>, request: Request) -> &'static str {
  let (parts, body) = request.into_parts();
  let capture = UpstreamCapture {
    marker: parts.headers["x-oversized-probe"].to_str().unwrap().to_string(),
    content_length: parts.headers["content-length"].to_str().unwrap().parse().unwrap(),
    // The fixture itself must not impose Axum's extractor default limit.
    body: to_bytes(body, usize::MAX).await.unwrap(),
  };
  sender.send(capture).await.unwrap();
  RESPONSE
}

#[tokio::test]
async fn default_frontend_and_worker_forward_oversized_api_and_proxy_requests_without_saving_bodies() {
  tokio::time::timeout(Duration::from_secs(30), oversized_requests_are_forwarded())
    .await
    .expect("oversized forwarding fixture timed out");
}

async fn oversized_requests_are_forwarded() {
  let (capture_tx, mut capture_rx) = mpsc::channel(2);
  let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let upstream_addr = upstream_listener.local_addr().unwrap();
  let upstream = Router::new()
    .route("/v1/responses", post(capture_upstream))
    .with_state(capture_tx);
  let upstream_task = tokio::spawn(async move { axum::serve(upstream_listener, upstream).await.unwrap() });

  // Reserve distinct addresses until the frontend is ready to bind both.
  let reserved_api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let reserved_proxy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let api_addr = reserved_api.local_addr().unwrap();
  let proxy_addr = reserved_proxy.local_addr().unwrap();
  let compiled = relay_config(api_addr, proxy_addr, upstream_addr);
  let (plan, service) = compiled.into_parts();
  let access = Arc::new(AccessStore::disabled());
  let events = Arc::new(EventBus::new(128));
  let requests_dir = tempfile::tempdir().unwrap();
  let persistence = RequestEventHandler::new(requests_dir.path().to_path_buf()).unwrap();
  let event_thread = spawn_event_loop(events.subscribe(), vec![Box::new(persistence)]);

  let states = build_worker_runtime_states(plan.clone(), service.clone(), &[], access.clone(), events.clone()).unwrap();
  let runtime = Arc::new(LiveRuntime::new(states, 0));
  let worker_info = runtime.worker_info("oversized-forwarding-fixture");
  let worker_dir = tempfile::tempdir().unwrap();
  std::fs::set_permissions(worker_dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
  let worker_socket = worker_dir.path().join("worker.sock");
  let (stop_worker, worker_stopped) = oneshot::channel();
  let worker_task = tokio::spawn(serve_worker(worker_socket.clone(), runtime, worker_info, async {
    let _ = worker_stopped.await;
  }));
  loop {
    if UnixStream::connect(&worker_socket).await.is_ok() {
      break;
    }
    tokio::task::yield_now().await;
  }
  let pool = Arc::new(
    WorkerPool::new(vec![WorkerEndpoint {
      worker_id: "fixture".into(),
      socket_path: worker_socket,
      weight: 1,
    }])
    .unwrap(),
  );
  pool.check_ready(&plan).await.unwrap();
  let frontend = Frontend::new_with_events(plan, service, access, pool, events.clone()).unwrap();
  drop(reserved_api);
  drop(reserved_proxy);
  let bound = frontend.bind().await.unwrap();
  let (stop_frontend, frontend_stopped) = oneshot::channel();
  let frontend_task = tokio::spawn(bound.serve(async {
    let _ = frontend_stopped.await;
  }));

  let body = Bytes::from(
    serde_json::to_vec(&serde_json::json!({
      "model": "gpt-test",
      "stream": false,
      "input": "x".repeat(CAPTURE_LIMIT + 1)
    }))
    .unwrap(),
  );
  assert_eq!(RequestPersistenceOptions::default().body_max_bytes, CAPTURE_LIMIT);
  assert!(body.len() > CAPTURE_LIMIT);

  let api_client = reqwest::Client::builder().no_proxy().build().unwrap();
  let proxy_client = reqwest::Client::builder()
    .no_proxy()
    .proxy(reqwest::Proxy::http(format!("http://{proxy_addr}")).unwrap())
    .build()
    .unwrap();
  for (marker, client, url) in [
    ("api", &api_client, format!("http://{api_addr}/v1/responses")),
    // Fixed-provider proxy relays append the original path to base_url's /v1.
    ("proxy", &proxy_client, "http://original.invalid/responses".into()),
  ] {
    let response = client
      .post(url)
      .header("x-request-id", format!("req-oversized-{marker}"))
      .header("x-oversized-probe", marker)
      .header("content-type", "application/json")
      .body(body.clone())
      .send()
      .await
      .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK, "{marker}");
    assert_eq!(response.text().await.unwrap(), RESPONSE);
    let captured = capture_rx.recv().await.unwrap();
    assert_eq!(captured.marker, marker);
    assert_eq!(captured.content_length, body.len());
    assert_eq!(captured.body.len(), body.len());
    assert!(captured.body == body, "{marker} changed the upstream request bytes");
  }
  drop(api_client);
  drop(proxy_client);
  stop_frontend.send(()).unwrap();
  frontend_task.await.unwrap().unwrap();
  stop_worker.send(()).unwrap();
  worker_task.await.unwrap().unwrap();
  events.shutdown().await;
  event_thread.join().unwrap();
  upstream_task.abort();

  for marker in ["api", "proxy"] {
    let row = read_request_row(requests_dir.path(), &format!("req-oversized-{marker}"))
      .unwrap()
      .expect("successful oversized request must remain in request history");
    assert_eq!(row["model"], "gpt-test");
    assert_eq!(row["provider_id"], "local");
    assert_eq!(row["status"], 200);
    assert_eq!(row["outbound_resp_status"], 200);
    assert_eq!(row["inbound_resp_status"], 200);
    assert!(row["request_error"].is_null());
    assert!(
      row["inbound_req_body"].is_null(),
      "{marker} saved an oversized inbound body"
    );
    assert!(
      row["outbound_req_body"].is_null(),
      "{marker} saved an oversized outbound body"
    );
    assert_eq!(row["inbound_req_headers"]["x-oversized-probe"], marker);
    assert_eq!(row["outbound_req_headers"]["x-oversized-probe"], marker);
    assert!(row["ctx_json"]["latency_ms"].as_i64().is_some());
    let expected_response: serde_json::Value = serde_json::from_str(RESPONSE).unwrap();
    assert_eq!(row["outbound_resp_body"], expected_response);
    assert_eq!(row["inbound_resp_body"], expected_response);
  }
}

fn relay_config(api: SocketAddr, proxy: SocketAddr, upstream: SocketAddr) -> tokn_config::v2::CompiledConfig {
  tokn_config::v2::parse_config(
    &format!(
      r#"
schema_version = 2
[listeners.api]
kind = "llm_api"
bind = "{api}"
client_auth = "none"
[listeners.proxy]
kind = "forward_proxy"
bind = "{proxy}"
client_auth = "none"
default_http_action = {{ kind = "route", profile = "relay" }}
default_connect = "reject"
[profiles.relay]
route = "relay"
binding = {{ path = "/v1" }}
[routes.relay]
kind = "relay"
destination = {{ kind = "fixed_provider", provider = "local" }}
credentials = {{ kind = "client" }}
[providers.local]
driver = "openai"
base_url = "http://{upstream}/v1"
"#
    ),
    Path::new("oversized-forwarding.toml"),
  )
  .unwrap()
}
