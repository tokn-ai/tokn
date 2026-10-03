use super::protocol::CONTEXT_HEADER;
use super::*;
use crate::dispatch::{DispatchContext, RequestDispatcher, RequestOrigin};
use crate::frontend::Frontend;
use crate::routing::{RoutingControl, WorkerState};
use crate::v2::{build_worker_runtime_states, LiveRuntime};
use anyhow::Result;
use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::Request;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use hyper_util::rt::TokioIo;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokn_access::AccessStore;
use tokn_access::{AccessContext, ProviderAccess};
use tokn_core::event::EventBus;
use tokn_policy::GatewayPlan;

struct Worker {
  _directory: tempfile::TempDir,
  path: PathBuf,
  stop: Option<oneshot::Sender<()>>,
  task: JoinHandle<Result<()>>,
}

impl Worker {
  async fn start(plan: GatewayPlan, dispatcher: Arc<dyn RequestDispatcher>) -> Self {
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.path().join("worker.sock");
    let (stop, stopped) = oneshot::channel();
    let info = WorkerInfo::new(&plan, "fixture").unwrap();
    let task = tokio::spawn(serve_worker(path.clone(), dispatcher, info, async {
      let _ = stopped.await;
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
      loop {
        if UnixStream::connect(&path).await.is_ok() {
          break;
        }
        tokio::task::yield_now().await;
      }
    })
    .await
    .unwrap();
    Self {
      _directory: directory,
      path,
      stop: Some(stop),
      task,
    }
  }

  fn endpoint(&self, id: &str, weight: u32) -> WorkerEndpoint {
    WorkerEndpoint {
      worker_id: id.into(),
      socket_path: self.path.clone(),
      weight,
    }
  }

  async fn stop(mut self) {
    self.stop.take().unwrap().send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut self.task)
      .await
      .unwrap()
      .unwrap()
      .unwrap();
    assert!(!self.path.exists(), "worker must remove its own socket");
  }
}

impl Drop for Worker {
  fn drop(&mut self) {
    self.task.abort();
  }
}

fn address() -> SocketAddr {
  std::net::TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
}

fn config(api: SocketAddr, proxy: SocketAddr, ca: &Path, upstream: SocketAddr) -> tokn_config::v2::CompiledConfig {
  tokn_config::v2::parse_config(
    &format!(
      r#"
schema_version = 2
[listeners.api]
kind = "llm_api"
bind = "{api}"
client_auth = "local_keys"
[listeners.proxy]
kind = "forward_proxy"
bind = "{proxy}"
client_auth = "local_keys"
request_body_max_bytes = 256
default_http_action = {{ kind = "route", profile = "relay" }}
default_connect = "intercept"
ca_dir = {ca:?}
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
    Path::new("ipc-test.toml"),
  )
  .unwrap()
}

async fn local_worker(compiled: tokn_config::v2::CompiledConfig) -> Worker {
  let (plan, service) = compiled.into_parts();
  let states = build_worker_runtime_states(
    plan.clone(),
    service,
    &[],
    Arc::new(AccessStore::disabled()),
    Arc::new(EventBus::noop()),
  )
  .unwrap();
  let live = LiveRuntime::new(states, 0);
  let actual = live.worker_info("fixture");
  let expected = WorkerInfo::new(&plan, "fixture").unwrap();
  assert_eq!(actual.api_admission, expected.api_admission);
  Worker::start(plan, Arc::new(live)).await
}

async fn upstream(marker: &'static str) -> (SocketAddr, JoinHandle<()>) {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();
  let app = axum::Router::new().fallback(move |request: axum::extract::Request| async move {
    assert!(request.headers().get(CONTEXT_HEADER).is_none());
    assert!(request.headers().get("x-tokn-ipc-forged").is_none());
    assert!(request.headers().get("proxy-authorization").is_none());
    assert!(matches!(request.uri().path(), "/v1/responses" | "/v1/v1/responses"));
    assert_eq!(request.headers()["authorization"], "Bearer client-token");
    assert_eq!(
      to_bytes(request.into_body(), 1024).await.unwrap(),
      "{\"model\":\"test\"}"
    );
    marker
  });
  let task = tokio::spawn(async move {
    axum::serve(listener, app).await.unwrap();
  });
  (addr, task)
}

async fn wait_for_frontend(addr: SocketAddr) {
  tokio::time::timeout(Duration::from_secs(5), async {
    loop {
      if TcpStream::connect(addr).await.is_ok() {
        break;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
}

async fn intercepted(proxy: SocketAddr, cert: &Path, token: &str) -> tokio_rustls::client::TlsStream<TcpStream> {
  let mut stream = TcpStream::connect(proxy).await.unwrap();
  stream.write_all(format!("CONNECT api.example.test:443 HTTP/1.1\r\nHost: api.example.test:443\r\nProxy-Authorization: Bearer {token}\r\n\r\n").as_bytes()).await.unwrap();
  let mut head = Vec::new();
  while !head.ends_with(b"\r\n\r\n") {
    head.push(stream.read_u8().await.unwrap());
  }
  assert!(head.starts_with(b"HTTP/1.1 200"), "{}", String::from_utf8_lossy(&head));
  let mut reader = std::io::BufReader::new(std::fs::File::open(cert).unwrap());
  let mut roots = rustls::RootCertStore::empty();
  for cert in rustls_pemfile::certs(&mut reader) {
    roots.add(cert.unwrap()).unwrap();
  }
  let tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
  tokio_rustls::TlsConnector::from(Arc::new(tls))
    .connect(
      rustls::pki_types::ServerName::try_from("api.example.test").unwrap(),
      stream,
    )
    .await
    .unwrap()
}

fn request() -> axum::extract::Request {
  Request::post("/v1/responses?source=ipc")
    .header("host", "api.example.test")
    .header("authorization", "Bearer client-token")
    .header("x-tokn-ipc-context", "forged")
    .header("x-tokn-ipc-forged", "strip-me")
    .body(Body::from("{\"model\":\"test\"}"))
    .unwrap()
}

#[tokio::test]
async fn successive_requests_on_one_connect_connection_reach_different_real_workers() {
  let (old_upstream, old_server) = upstream("old").await;
  let (new_upstream, new_server) = upstream("new").await;
  let ca = tempfile::tempdir().unwrap();
  let api = address();
  let proxy = address();
  let old_config = config(api, proxy, ca.path(), old_upstream);
  let new_config = config(api, proxy, ca.path(), new_upstream);
  let old = local_worker(old_config.clone()).await;
  let new = local_worker(new_config).await;
  assert!(!ca.path().join("ca.crt").exists(), "workers must not create CA files");
  let pool = Arc::new(WorkerPool::new(vec![old.endpoint("old", 1), new.endpoint("new", 1)]).unwrap());
  pool.check_ready(old_config.gateway()).await.unwrap();
  let access = Arc::new(AccessStore::disabled());
  let key = access.create_key("proxy", vec!["local".into()]).unwrap();
  let (plan, service) = old_config.into_parts();
  let frontend = Frontend::new(plan, service, access, pool.clone())
    .unwrap()
    .with_routing_control(pool.clone());
  let (stop, stopped) = oneshot::channel();
  let frontend = tokio::spawn(frontend.serve(async {
    let _ = stopped.await;
  }));
  wait_for_frontend(proxy).await;
  let tls = intercepted(proxy, &ca.path().join("ca.crt"), &key.token).await;
  let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tls)).await.unwrap();
  let connection = tokio::spawn(connection);
  for marker in ["old", "new", "old"] {
    let response = sender.send_request(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-request-id"));
    assert_eq!(
      response.headers().get("connection"),
      None,
      "IPC closure must not close the client tunnel"
    );
    assert_eq!(to_bytes(Body::new(response.into_body()), 1024).await.unwrap(), marker);
  }
  // The API listener uses the same worker pool; relay requests preserve client credentials.
  let response = reqwest::Client::new()
    .post(format!("http://{api}/v1/responses"))
    .bearer_auth("client-token")
    .body("{\"model\":\"test\"}")
    .send()
    .await
    .unwrap();
  assert_eq!(response.text().await.unwrap(), "new");
  let oversized = Request::post("/v1/responses")
    .header("host", "api.example.test")
    .body(Body::from(vec![b'x'; 257]))
    .unwrap();
  let response = sender.send_request(oversized).await.unwrap();
  assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
  to_bytes(Body::new(response.into_body()), 1024).await.unwrap();
  let client = reqwest::Client::new();
  let control_url = format!("http://{api}/admin/workers");
  assert_eq!(
    client.get(&control_url).send().await.unwrap().status(),
    StatusCode::FORBIDDEN
  );
  assert_eq!(
    client
      .get(format!("http://{api}/v1/models"))
      .send()
      .await
      .unwrap()
      .status(),
    StatusCode::UNAUTHORIZED
  );
  let invalid = client
    .post(&control_url)
    .header("x-tokn-admin", "workers")
    .json(&serde_json::json!({"weights": {"old": 0, "new": 0}}))
    .send()
    .await
    .unwrap();
  assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
  assert_eq!(
    pool.status().generation,
    1,
    "rejected updates must retain the routing table"
  );
  for (weights, marker) in [
    (serde_json::json!({"old":0,"new":1}), "new"),
    (serde_json::json!({"old":1,"new":0}), "old"),
  ] {
    let updated = client
      .post(&control_url)
      .header("x-tokn-admin", "workers")
      .json(&serde_json::json!({"weights": weights}))
      .send()
      .await
      .unwrap();
    assert_eq!(updated.status(), StatusCode::OK);
    let response = sender.send_request(request()).await.unwrap();
    assert_eq!(to_bytes(Body::new(response.into_body()), 1024).await.unwrap(), marker);
  }
  drop(sender);
  connection.abort();
  stop.send(()).unwrap();
  tokio::time::timeout(Duration::from_secs(5), frontend)
    .await
    .unwrap()
    .unwrap()
    .unwrap();
  old.stop().await;
  new.stop().await;
  old_server.abort();
  new_server.abort();
}

struct FixtureDispatcher {
  stream: parking_lot::Mutex<Option<tokio::sync::mpsc::Receiver<Result<bytes::Bytes, std::io::Error>>>>,
  contexts: parking_lot::Mutex<Vec<DispatchContext>>,
}

#[async_trait]
impl RequestDispatcher for FixtureDispatcher {
  async fn dispatch(&self, context: DispatchContext, request: axum::extract::Request) -> Result<Response> {
    assert!(request.headers().get(CONTEXT_HEADER).is_none());
    assert!(request.headers().get("x-tokn-ipc-forged").is_none());
    self.contexts.lock().push(context);
    let stream = self.stream.lock().take();
    Ok(if let Some(receiver) = stream {
      let stream = futures_util::stream::unfold(receiver, |mut receiver| async {
        receiver.recv().await.map(|item| (item, receiver))
      });
      Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(stream))
        .unwrap()
    } else {
      Response::new(Body::from("fixture"))
    })
  }
}

fn context() -> DispatchContext {
  DispatchContext {
    listener_id: "proxy".into(),
    origin: RequestOrigin::Proxy {
      scheme: crate::dispatch::ProxyScheme::Https,
      authority: "api.example.test:443".into(),
      intercepted: true,
    },
    access: AccessContext {
      key_id: Some("key".into()),
      key_name: Some("test".into()),
      providers: ProviderAccess::Only(BTreeSet::new()),
    },
    local_addr: Some("127.0.0.1:4142".parse().unwrap()),
    peer_addr: Some("127.0.0.1:12345".parse().unwrap()),
  }
}

#[tokio::test]
async fn streams_are_pinned_and_dropping_response_cancels_ipc() {
  let ca = tempfile::tempdir().unwrap();
  let compiled = config(address(), address(), ca.path(), "127.0.0.1:1".parse().unwrap());
  let (tx, rx) = tokio::sync::mpsc::channel(1);
  let old_dispatcher = Arc::new(FixtureDispatcher {
    stream: parking_lot::Mutex::new(Some(rx)),
    contexts: parking_lot::Mutex::new(Vec::new()),
  });
  let new_dispatcher = Arc::new(FixtureDispatcher {
    stream: parking_lot::Mutex::new(None),
    contexts: parking_lot::Mutex::new(Vec::new()),
  });
  let old = Worker::start(compiled.gateway().clone(), old_dispatcher.clone()).await;
  let new = Worker::start(compiled.gateway().clone(), new_dispatcher.clone()).await;
  let pool = WorkerPool::new(vec![old.endpoint("old", 1), new.endpoint("new", 1)]).unwrap();
  pool.check_ready(compiled.gateway()).await.unwrap();
  let first = tokio::time::timeout(Duration::from_secs(2), pool.dispatch(context(), request()))
    .await
    .unwrap()
    .unwrap();
  let mut body = first.into_body().into_data_stream();
  tx.send(Ok(bytes::Bytes::from_static(b"data: first\n\n")))
    .await
    .unwrap();
  assert_eq!(
    tokio::time::timeout(Duration::from_secs(2), body.next())
      .await
      .unwrap()
      .unwrap()
      .unwrap(),
    "data: first\n\n"
  );
  let report = pool
    .update_weights(BTreeMap::from([("old".into(), 0), ("new".into(), 1)]))
    .await
    .unwrap();
  assert_eq!(
    report.workers[0].in_flight, 1,
    "a streaming response remains active during promotion"
  );
  let second = pool.dispatch(context(), request()).await.unwrap();
  assert_eq!(to_bytes(second.into_body(), 1024).await.unwrap(), "fixture");
  assert_eq!(new_dispatcher.contexts.lock().len(), 1);
  {
    let contexts = old_dispatcher.contexts.lock();
    assert_eq!(contexts[0].access.providers, ProviderAccess::Only(BTreeSet::new()));
    assert_eq!(contexts[0].peer_addr, context().peer_addr);
  }
  tx.send(Ok(bytes::Bytes::from_static(b"data: second\n\n")))
    .await
    .unwrap();
  assert_eq!(body.next().await.unwrap().unwrap(), "data: second\n\n");
  drop(body);
  assert_eq!(pool.status().workers[0].in_flight, 0);
  tokio::time::timeout(Duration::from_secs(2), tx.closed())
    .await
    .expect("dropping the client body must close the worker body");
  old.stop().await;
  new.stop().await;
}

#[tokio::test]
async fn readiness_rejects_incompatible_protocol_and_failures_are_not_replayed() {
  let ca = tempfile::tempdir().unwrap();
  let compiled = config(address(), address(), ca.path(), "127.0.0.1:1".parse().unwrap());
  let dispatcher = Arc::new(FixtureDispatcher {
    stream: parking_lot::Mutex::new(None),
    contexts: parking_lot::Mutex::new(Vec::new()),
  });
  let worker = Worker::start(compiled.gateway().clone(), dispatcher.clone()).await;
  let directory = tempfile::tempdir().unwrap();
  let pool = WorkerPool::new(vec![
    WorkerEndpoint {
      worker_id: "missing".into(),
      socket_path: directory.path().join("absent"),
      weight: 1,
    },
    worker.endpoint("available", 1),
  ])
  .unwrap();
  assert!(pool.dispatch(context(), request()).await.is_err());
  assert!(
    dispatcher.contexts.lock().is_empty(),
    "failed requests cannot be replayed on the other worker"
  );
  let incompatible = tokio::net::UnixListener::bind(directory.path().join("incompatible")).unwrap();
  let path = directory.path().join("incompatible");
  let mut info = WorkerInfo::new(compiled.gateway(), "incompatible").unwrap();
  info.protocol_version = PROTOCOL_VERSION + 1;
  let server = tokio::spawn(async move {
    let (stream, _) = incompatible.accept().await.unwrap();
    let service = hyper::service::service_fn(move |_| {
      let info = info.clone();
      async move { Ok::<_, std::convert::Infallible>(axum::Json(info).into_response()) }
    });
    let _ = hyper::server::conn::http1::Builder::new()
      .serve_connection(TokioIo::new(stream), service)
      .await;
  });
  let pool = WorkerPool::new(vec![WorkerEndpoint {
    worker_id: "incompatible".into(),
    socket_path: path,
    weight: 1,
  }])
  .unwrap();
  assert!(pool
    .check_ready(compiled.gateway())
    .await
    .unwrap_err()
    .to_string()
    .contains("incompatible IPC protocol"));
  server.await.unwrap();
  worker.stop().await;
}

#[tokio::test]
async fn socket_binding_refuses_public_directories_and_existing_sockets() {
  let directory = tempfile::tempdir().unwrap();
  std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
  let ca = tempfile::tempdir().unwrap();
  let plan = config(address(), address(), ca.path(), "127.0.0.1:1".parse().unwrap())
    .gateway()
    .clone();
  let dispatcher = Arc::new(FixtureDispatcher {
    stream: parking_lot::Mutex::new(None),
    contexts: parking_lot::Mutex::new(Vec::new()),
  });
  assert!(serve_worker(
    directory.path().join("public"),
    dispatcher.clone(),
    WorkerInfo::new(&plan, "fixture").unwrap(),
    std::future::pending()
  )
  .await
  .unwrap_err()
  .to_string()
  .contains("private"));
  let worker = Worker::start(plan.clone(), dispatcher.clone()).await;
  assert!(serve_worker(
    worker.path.clone(),
    dispatcher,
    WorkerInfo::new(&plan, "fixture").unwrap(),
    std::future::pending()
  )
  .await
  .is_err());
  assert!(
    UnixStream::connect(&worker.path).await.is_ok(),
    "failed bind must preserve the existing worker socket"
  );
  worker.stop().await;
}

struct PendingDispatcher {
  started: Arc<tokio::sync::Notify>,
  cancelled: Arc<tokio::sync::Notify>,
}

struct Cancelled(Arc<tokio::sync::Notify>);
impl Drop for Cancelled {
  fn drop(&mut self) {
    self.0.notify_one();
  }
}

#[async_trait]
impl RequestDispatcher for PendingDispatcher {
  async fn dispatch(&self, _context: DispatchContext, request: axum::extract::Request) -> Result<Response> {
    to_bytes(request.into_body(), 1024).await?;
    let _cancelled = Cancelled(self.cancelled.clone());
    self.started.notify_one();
    std::future::pending().await
  }
}

#[tokio::test]
async fn cancellation_before_response_headers_reaches_the_worker() {
  let ca = tempfile::tempdir().unwrap();
  let compiled = config(address(), address(), ca.path(), "127.0.0.1:1".parse().unwrap());
  let started = Arc::new(tokio::sync::Notify::new());
  let cancelled = Arc::new(tokio::sync::Notify::new());
  let worker = Worker::start(
    compiled.gateway().clone(),
    Arc::new(PendingDispatcher {
      started: started.clone(),
      cancelled: cancelled.clone(),
    }),
  )
  .await;
  let pool = Arc::new(WorkerPool::new(vec![worker.endpoint("pending", 1)]).unwrap());
  let task = tokio::spawn(async move { pool.dispatch(context(), request()).await });
  tokio::time::timeout(Duration::from_secs(2), started.notified())
    .await
    .unwrap();
  task.abort();
  let _ = task.await;
  tokio::time::timeout(Duration::from_secs(2), cancelled.notified())
    .await
    .expect("cancelling a pending IPC exchange must drop worker execution");
  worker.stop().await;
}

#[tokio::test]
async fn dynamic_registration_promotes_atomically_and_waits_for_stream_drain() {
  let ca = tempfile::tempdir().unwrap();
  let compiled = config(address(), address(), ca.path(), "127.0.0.1:1".parse().unwrap());
  let (tx, rx) = tokio::sync::mpsc::channel(1);
  let old = Worker::start(
    compiled.gateway().clone(),
    Arc::new(FixtureDispatcher {
      stream: parking_lot::Mutex::new(Some(rx)),
      contexts: parking_lot::Mutex::new(Vec::new()),
    }),
  )
  .await;
  let new = Worker::start(
    compiled.gateway().clone(),
    Arc::new(FixtureDispatcher {
      stream: parking_lot::Mutex::new(None),
      contexts: parking_lot::Mutex::new(Vec::new()),
    }),
  )
  .await;
  let pool = WorkerPool::empty(compiled.gateway()).unwrap();
  assert!(pool.dispatch(context(), request()).await.is_err());
  pool.register(old.endpoint("old", 1)).await.unwrap();
  pool.register_candidate(new.endpoint("new", 0)).await.unwrap();
  assert_eq!(pool.status().workers[0].weight, 1);
  assert_eq!(pool.status().workers[1].weight, 0);
  assert!(
    tokio::time::timeout(Duration::from_millis(20), pool.wait_retired("old"))
      .await
      .is_err(),
    "candidate registration must preserve the current worker"
  );
  pool.disconnect("new");

  let response = pool.dispatch(context(), request()).await.unwrap();
  let mut body = response.into_body().into_data_stream();
  tx.send(Ok(bytes::Bytes::from_static(b"data: old\n\n"))).await.unwrap();
  assert!(body.next().await.unwrap().is_ok());
  pool.register(new.endpoint("new", 1)).await.unwrap();
  assert_eq!(pool.status().workers[0].weight, 0);
  assert_eq!(pool.status().workers[1].weight, 1);
  assert!(
    tokio::time::timeout(Duration::from_millis(20), pool.wait_retired("old"))
      .await
      .is_err()
  );
  for _ in 0..3 {
    let response = pool.dispatch(context(), request()).await.unwrap();
    assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "fixture");
  }
  drop(tx);
  assert!(body.next().await.is_none());
  tokio::time::timeout(Duration::from_secs(2), pool.wait_retired("old"))
    .await
    .unwrap()
    .unwrap();
  assert_eq!(pool.status().workers[0].state, WorkerState::Exiting);
  pool.retire("new").unwrap();
  assert!(
    pool.status().main_worker_id.is_none(),
    "automatic exit must not be reversed"
  );
  pool.disconnect("old");
  assert_eq!(pool.status().workers.len(), 1);
  pool.disconnect("new");
  assert!(pool.dispatch(context(), request()).await.is_err());
  old.stop().await;
  new.stop().await;
}

#[tokio::test]
async fn retiring_current_promotes_busy_stale_without_interrupting_either_stream() {
  let ca = tempfile::tempdir().unwrap();
  let compiled = config(address(), address(), ca.path(), "127.0.0.1:1".parse().unwrap());
  let (old_tx, old_rx) = tokio::sync::mpsc::channel(1);
  let (new_tx, new_rx) = tokio::sync::mpsc::channel(1);
  let old = Worker::start(
    compiled.gateway().clone(),
    Arc::new(FixtureDispatcher {
      stream: parking_lot::Mutex::new(Some(old_rx)),
      contexts: parking_lot::Mutex::new(Vec::new()),
    }),
  )
  .await;
  let new = Worker::start(
    compiled.gateway().clone(),
    Arc::new(FixtureDispatcher {
      stream: parking_lot::Mutex::new(Some(new_rx)),
      contexts: parking_lot::Mutex::new(Vec::new()),
    }),
  )
  .await;
  let pool = WorkerPool::empty(compiled.gateway()).unwrap();
  pool.register(old.endpoint("old", 1)).await.unwrap();
  let mut old_body = pool
    .dispatch(context(), request())
    .await
    .unwrap()
    .into_body()
    .into_data_stream();
  old_tx.send(Ok(bytes::Bytes::from_static(b"old"))).await.unwrap();
  assert!(old_body.next().await.unwrap().is_ok());
  pool.register(new.endpoint("new", 1)).await.unwrap();
  let mut new_body = pool
    .dispatch(context(), request())
    .await
    .unwrap()
    .into_body()
    .into_data_stream();
  new_tx.send(Ok(bytes::Bytes::from_static(b"new"))).await.unwrap();
  assert!(new_body.next().await.unwrap().is_ok());
  pool.retire("new").unwrap();
  let report = pool.status();
  assert_eq!(report.main_worker_id.as_deref(), Some("old"));
  assert_eq!(report.workers[0].state, WorkerState::Current);
  assert_eq!(report.workers[0].weight, 1);
  assert_eq!(report.workers[1].state, WorkerState::Exiting);
  assert_eq!(report.workers[1].weight, 0);
  assert!(report.workers.iter().all(|worker| worker.in_flight == 1));
  assert!(pool
    .update_weights(BTreeMap::from([("old".into(), 0), ("new".into(), 1)]))
    .await
    .is_err());
  assert!(
    tokio::time::timeout(Duration::from_millis(20), pool.wait_retired("new"))
      .await
      .is_err()
  );
  drop(new_tx);
  assert!(new_body.next().await.is_none());
  pool.wait_retired("new").await.unwrap();
  pool.disconnect("new");
  let response = pool.dispatch(context(), request()).await.unwrap();
  assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "fixture");
  drop(old_tx);
  assert!(old_body.next().await.is_none());
  assert!(
    tokio::time::timeout(Duration::from_millis(20), pool.wait_retired("old"))
      .await
      .is_err()
  );
  pool.retire("old").unwrap();
  assert!(pool.status().main_worker_id.is_none());
  pool.wait_retired("old").await.unwrap();
  pool.disconnect("old");
  old.stop().await;
  new.stop().await;
}

#[tokio::test]
async fn disconnect_during_weight_readiness_does_not_publish_stale_assignments() {
  let ca = tempfile::tempdir().unwrap();
  let compiled = config(address(), address(), ca.path(), "127.0.0.1:1".parse().unwrap());
  let worker = Worker::start(
    compiled.gateway().clone(),
    Arc::new(FixtureDispatcher {
      stream: parking_lot::Mutex::new(None),
      contexts: parking_lot::Mutex::new(Vec::new()),
    }),
  )
  .await;
  let pool = WorkerPool::empty(compiled.gateway()).unwrap();
  pool.register(worker.endpoint("worker", 1)).await.unwrap();
  let update = pool.update_weights(BTreeMap::from([("worker".into(), 1)]));
  tokio::pin!(update);
  assert!(futures_util::poll!(update.as_mut()).is_pending());
  pool.disconnect("worker");
  assert!(update.await.unwrap_err().to_string().contains("workers changed"));
  assert!(pool.status().workers.is_empty());
  assert!(pool.dispatch(context(), request()).await.is_err());
  worker.stop().await;
}
