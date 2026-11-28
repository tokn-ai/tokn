use super::protocol::CONTEXT_HEADER;
use super::*;
use crate::dispatch::{DispatchContext, RequestDispatcher, RequestOrigin};
use crate::frontend::Frontend;
use crate::routing::{RolloutPolicy, RolloutStage, RoutingControl, WorkerState};
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
    let info = WorkerInfo::new(&plan, "fixture").unwrap();
    Self::start_with(|path, stopped| {
      serve_worker(path, dispatcher, info, async {
        let _ = stopped.await;
      })
    })
    .await
  }

  async fn start_with<F, Fut>(serve: F) -> Self
  where
    F: FnOnce(PathBuf, oneshot::Receiver<()>) -> Fut,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
  {
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.path().join("worker.sock");
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(serve(path.clone(), stopped));
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
  tokn_config::v2::compile_config(&raw_config(api, proxy, ca, upstream), Path::new("ipc-test.toml")).unwrap()
}

fn raw_config(api: SocketAddr, proxy: SocketAddr, ca: &Path, upstream: SocketAddr) -> tokn_config::v2::RawConfig {
  tokn_config::v2::decode(
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
  let stream = open_connect(proxy, "api.example.test:443", token).await;
  connect_intercepted_tls(stream, cert, "api.example.test").await
}

async fn open_connect(proxy: SocketAddr, authority: &str, token: &str) -> TcpStream {
  let mut stream = TcpStream::connect(proxy).await.unwrap();
  stream
    .write_all(
      format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Authorization: Bearer {token}\r\n\r\n")
        .as_bytes(),
    )
    .await
    .unwrap();
  let mut head = Vec::new();
  tokio::time::timeout(Duration::from_secs(5), async {
    while !head.ends_with(b"\r\n\r\n") {
      head.push(stream.read_u8().await.unwrap());
    }
  })
  .await
  .unwrap();
  assert!(head.starts_with(b"HTTP/1.1 200"), "{}", String::from_utf8_lossy(&head));
  stream
}

async fn connect_intercepted_tls(
  stream: TcpStream,
  cert: &Path,
  host: &'static str,
) -> tokio_rustls::client::TlsStream<TcpStream> {
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
    .connect(rustls::pki_types::ServerName::try_from(host).unwrap(), stream)
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
async fn ab_test_registration_shares_traffic_and_manual_weights_cancel_the_ramp() {
  let ca = tempfile::tempdir().unwrap();
  let compiled = config(address(), address(), ca.path(), "127.0.0.1:1".parse().unwrap());
  let old = Worker::start(
    compiled.gateway().clone(),
    Arc::new(FixtureDispatcher {
      stream: parking_lot::Mutex::new(None),
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
  let policy = RolloutPolicy {
    initial_percent: 10,
    stages: vec![RolloutStage {
      duration_seconds: 24 * 3600,
      traffic_percent: 90,
    }],
    completion_percent: 100,
  };
  assert!(pool
    .register_rollout(new.endpoint("new", 1), policy.clone())
    .await
    .is_err());
  assert!(pool.status().workers.is_empty());
  pool.register(old.endpoint("old", 1)).await.unwrap();
  let mut invalid = policy.clone();
  invalid.initial_percent = 0;
  assert!(pool.register_rollout(new.endpoint("new", 1), invalid).await.is_err());
  let unchanged = pool.status();
  assert_eq!(unchanged.workers.len(), 1);
  assert_eq!(unchanged.main_worker_id.as_deref(), Some("old"));
  assert!(unchanged.ab_test.is_none());
  pool
    .register_rollout(new.endpoint("new", 1), policy.clone())
    .await
    .unwrap();
  let report = pool.status();
  assert_eq!(report.main_worker_id.as_deref(), Some("new"));
  assert_eq!(report.workers[0].state, WorkerState::Stale);
  assert_eq!(report.workers[0].weight, 90);
  assert_eq!(report.workers[1].state, WorkerState::Current);
  assert_eq!(report.workers[1].weight, 10);
  let ramp = report.ab_test.unwrap();
  assert_eq!(ramp.baseline_worker_id, "old");
  assert_eq!(ramp.worker_id, "new");
  assert_eq!(ramp.duration_seconds, 24 * 60 * 60);
  assert_eq!(ramp.traffic_percent, 10);
  assert!(
    tokio::time::timeout(Duration::from_millis(20), pool.wait_retired("old"))
      .await
      .is_err()
  );
  for _ in 0..100 {
    let response = pool.dispatch(context(), request()).await.unwrap();
    assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "fixture");
  }
  assert_eq!(pool.status().workers[0].requests, 90);
  assert_eq!(pool.status().workers[1].requests, 10);
  pool
    .update_weights(BTreeMap::from([("old".into(), 1), ("new".into(), 0)]))
    .await
    .unwrap();
  pool.advance_ab_test();
  assert!(pool.status().ab_test.is_none());
  assert_eq!(pool.status().main_worker_id.as_deref(), Some("old"));
  assert_eq!(pool.status().workers[1].weight, 0);
  pool.disconnect("new");
  pool.register_rollout(new.endpoint("new", 1), policy).await.unwrap();
  pool.retire("old").unwrap();
  pool.advance_ab_test();
  assert!(pool.status().ab_test.is_none());
  assert_eq!(pool.status().main_worker_id.as_deref(), Some("new"));
  let response = pool.dispatch(context(), request()).await.unwrap();
  assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "fixture");
  pool.disconnect("new");
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

fn mount_config(api: SocketAddr, path: &str) -> tokn_config::v2::CompiledConfig {
  tokn_config::v2::parse_config(
    &format!(
      r#"
schema_version = 2
[listeners.api]
kind = "llm_api"
bind = "{api}"
client_auth = "none"
[profiles.work]
route = "shared"
binding = {{ path = "{path}" }}
[routes.shared]
kind = "managed"
provider = {{ kind = "any" }}
providers = ["openai"]
model = {{ kind = "capability" }}
operation = "translate_compatible"
"#
    ),
    Path::new("reload-test.toml"),
  )
  .unwrap()
}

async fn begin_admitted_request(api: SocketAddr, path: &str) -> TcpStream {
  let mut stream = TcpStream::connect(api).await.unwrap();
  stream.write_all(format!("GET {path}/models HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
  // Hyper only sends 100 Continue when the body is first polled. At that point
  // admission has selected an ingress generation, but execution has yet to start.
  let mut response = Vec::new();
  tokio::time::timeout(Duration::from_secs(5), async {
    while !response.ends_with(b"\r\n\r\n") {
      response.push(stream.read_u8().await.unwrap());
    }
  })
  .await
  .unwrap();
  assert!(response.starts_with(b"HTTP/1.1 100 Continue"));
  stream
}

async fn finish_admitted_request(mut stream: TcpStream) {
  stream.write_all(b"x").await.unwrap();
  let mut response = String::new();
  tokio::time::timeout(Duration::from_secs(5), stream.read_to_string(&mut response))
    .await
    .unwrap()
    .unwrap();
  assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
}

async fn assert_untagged_worker_mount(path: &Path, current: &str, previous: &str) {
  use base64::Engine;
  let ready = Request::get(super::protocol::READY_PATH).body(Body::empty()).unwrap();
  let response = super::transport::exchange(path, ready).await.unwrap();
  let info: WorkerInfo = serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
  for (mount, expected) in [(current, StatusCode::OK), (previous, StatusCode::NOT_FOUND)] {
    let uri = format!("{mount}/models");
    assert_eq!(
      info.api_admission.iter().any(|rule| rule.path == uri),
      expected == StatusCode::OK
    );
    let mut context = context();
    context.origin = RequestOrigin::Api;
    context.listener_id = "api".into();
    let wire = super::protocol::WireContext::new(context, uri);
    let metadata = base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&wire).unwrap());
    let request = Request::get("/_tokn/dispatch")
      .header(CONTEXT_HEADER, metadata)
      .body(Body::empty())
      .unwrap();
    let response = super::transport::exchange(path, request).await.unwrap();
    assert_eq!(response.status(), expected);
  }
}

#[tokio::test]
async fn split_reload_uses_the_worker_connect_policy_for_new_tunnels() {
  use tokn_config::v2::{self, RawBinding, RawBindingAction, RawConnectAction, RawConnectRule, RawListener};

  const REVISION: &str = "shared-source-revision";
  const HOST: &str = "127.0.0.1";
  const GREETING: &[u8] = b"opaque tunnel fixture";
  let tunnel = TcpListener::bind((HOST, 0)).await.unwrap();
  let tunnel_addr = tunnel.local_addr().unwrap();
  let tunnel_task = tokio::spawn(async move {
    let (mut connection, _) = tunnel.accept().await.unwrap();
    connection.write_all(GREETING).await.unwrap();
    let probe = connection.read_u8().await.unwrap();
    connection.write_u8(probe).await.unwrap();
  });
  let (upstream_addr, upstream_task) = upstream("reloaded relay").await;
  let ca = tempfile::tempdir().unwrap();
  let api = address();
  let proxy = address();
  let source = Path::new("worker-connect-reload.toml");
  let mut frontend_projection = raw_config(api, proxy, ca.path(), upstream_addr);
  let RawListener::ForwardProxy {
    default_connect,
    default_http_action,
    ..
  } = frontend_projection.listeners.get_mut("proxy").unwrap()
  else {
    panic!("fixture requires a proxy listener");
  };
  *default_connect = RawConnectAction::Tunnel;
  *default_http_action = RawBindingAction::Reject {};
  let mut worker_projection = frontend_projection.clone();
  worker_projection.connect_rules.push(RawConnectRule {
    id: "intercept-fixture".into(),
    listener: "proxy".into(),
    action: RawConnectAction::Intercept,
    hosts: vec![HOST.into()],
    ports: vec![tunnel_addr.port()],
  });
  worker_projection.bindings.push(RawBinding {
    id: "relay-fixture".into(),
    listener: "proxy".into(),
    action: RawBindingAction::Route {
      profile: "relay".into(),
    },
    hosts: vec![HOST.into()],
    path_prefixes: vec!["/v1".into()],
    methods: vec!["POST".into()],
    operations: vec![],
  });
  let frontend_config = v2::compile_config(&frontend_projection, source).unwrap();
  let worker_config = v2::compile_config(&worker_projection, source).unwrap();
  let build = |compiled: v2::CompiledConfig| {
    let (plan, service) = compiled.into_parts();
    build_worker_runtime_states(
      plan,
      service,
      &[],
      Arc::new(AccessStore::disabled()),
      Arc::new(EventBus::noop()),
    )
    .unwrap()
  };
  let live = Arc::new(LiveRuntime::new(build(frontend_config.clone()), 0));
  let info = live.worker_info("fixture");
  let prepare = WorkerReload::new(move |revision| {
    let worker_config = worker_config.clone();
    async move {
      assert_eq!(revision, REVISION);
      Ok((build(worker_config), 0))
    }
  });
  let worker = Worker::start_with(move |path, stopped| {
    serve_reloadable_worker(path, live, info, prepare, async {
      let _ = stopped.await;
    })
  })
  .await;
  let (plan, service) = frontend_config.into_parts();
  let pool = Arc::new(WorkerPool::empty(&plan).unwrap());
  pool.register(worker.endpoint("worker", 1)).await.unwrap();
  let access = Arc::new(AccessStore::disabled());
  let key = access.create_key("proxy", vec!["local".into()]).unwrap();
  let frontend = Frontend::new(plan.clone(), service.clone(), access, pool.dispatcher()).unwrap();
  let handle = frontend.reload_handle();
  let (stop, stopped) = oneshot::channel();
  let server = tokio::spawn(frontend.serve(async {
    let _ = stopped.await;
  }));
  wait_for_frontend(proxy).await;

  let authority = tunnel_addr.to_string();
  let mut opaque = open_connect(proxy, &authority, &key.token).await;
  let mut greeting = vec![0; GREETING.len()];
  tokio::time::timeout(Duration::from_secs(5), opaque.read_exact(&mut greeting))
    .await
    .unwrap()
    .unwrap();
  assert_eq!(greeting, GREETING);

  // The two versions can compile the same source revision differently. Reload
  // submits the old frontend projection, leaving the prepared worker to
  // advertise the policy matching its new execution rules.
  assert_eq!(
    pool.reload(&handle, plan, &service, REVISION).await.unwrap().generation,
    2
  );
  tokio::time::timeout(Duration::from_secs(5), async {
    opaque.write_u8(42).await.unwrap();
    assert_eq!(opaque.read_u8().await.unwrap(), 42);
  })
  .await
  .unwrap();
  drop(opaque);
  tunnel_task.await.unwrap();

  let stream = open_connect(proxy, &authority, &key.token).await;
  let tls = connect_intercepted_tls(stream, &ca.path().join("ca.crt"), HOST).await;
  let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tls)).await.unwrap();
  let connection = tokio::spawn(connection);
  let mut request = request();
  request.headers_mut().insert("host", authority.parse().unwrap());
  let response = sender.send_request(request).await.unwrap();
  assert_eq!(response.status(), StatusCode::OK);
  assert_eq!(
    to_bytes(Body::new(response.into_body()), 1024).await.unwrap(),
    "reloaded relay",
  );
  drop(sender);
  connection.abort();
  stop.send(()).unwrap();
  tokio::time::timeout(Duration::from_secs(5), server)
    .await
    .unwrap()
    .unwrap()
    .unwrap();
  worker.stop().await;
  upstream_task.abort();
}

#[tokio::test]
async fn split_reload_preserves_admitted_generations_and_resumes_worker_promotion() {
  use crate::v2::ReloadError;
  let api = address();
  let original = mount_config(api, "/before");
  let build = |config: tokn_config::v2::CompiledConfig| {
    let (plan, service) = config.into_parts();
    build_worker_runtime_states(
      plan,
      service,
      &[],
      Arc::new(AccessStore::disabled()),
      Arc::new(EventBus::noop()),
    )
    .unwrap()
  };
  let live = Arc::new(LiveRuntime::new(build(original.clone()), 0));
  let prepare = WorkerReload::new(move |revision| async move {
    match revision.as_str() {
      "/after" | "/next" => Ok((build(mount_config(api, &revision)), 0)),
      "invalid" => Err(ReloadError::Invalid("invalid worker settings".into())),
      "stale" => Err(ReloadError::Conflict("worker configuration revision differs".into())),
      _ => Err(ReloadError::RestartRequired(
        "worker configuration requires restart".into(),
      )),
    }
  });
  let initial = Arc::downgrade(&live.current_worker_generation());
  let observed = live.clone();
  let info = live.worker_info("fixture");
  let worker = Worker::start_with(move |path, stopped| {
    serve_reloadable_worker(path, live, info, prepare, async {
      let _ = stopped.await;
    })
  })
  .await;
  let (plan, service) = original.into_parts();
  let pool = Arc::new(WorkerPool::empty(&plan).unwrap());
  pool.register(worker.endpoint("original", 1)).await.unwrap();
  // Even a rejection after readiness has pinned a worker must release its lease.
  assert!(pool.register_candidate(worker.endpoint("duplicate", 0)).await.is_err());
  let frontend = Frontend::new(
    plan,
    service.clone(),
    Arc::new(AccessStore::disabled()),
    pool.dispatcher(),
  )
  .unwrap();
  let handle = frontend.reload_handle();
  let (stop, stopped) = oneshot::channel();
  let server = tokio::spawn(frontend.serve(async {
    let _ = stopped.await;
  }));
  wait_for_frontend(api).await;
  let client = reqwest::Client::new();
  let get = |path: &str| client.get(format!("http://{api}{path}/models")).send();
  let reload = |mount: &str, revision: &'static str| {
    pool.reload(&handle, mount_config(api, mount).into_parts().0, &service, revision)
  };
  for revision in ["stale", "invalid", "restart"] {
    let error = reload("/after", revision).await.unwrap_err();
    assert!(matches!(error, ReloadError::Invalid(_)) == (revision == "invalid"));
    assert!(matches!(error, ReloadError::RestartRequired(_)) == (revision == "restart"));
    if revision == "stale" {
      assert_eq!(
        error,
        ReloadError::Conflict("worker configuration revision differs".into())
      );
    }
    assert_eq!(get("/before").await.unwrap().status(), StatusCode::OK);
    assert_eq!(get("/after").await.unwrap().status(), StatusCode::NOT_FOUND);
  }
  // The worker can prepare successfully while the frontend rejects a bind change.
  // No activation message is sent and ordinary requests keep their existing state.
  let wrong_bind = mount_config(address(), "/after").into_parts().0;
  assert!(matches!(
    pool.reload(&handle, wrong_bind, &service, "/after").await,
    Err(ReloadError::RestartRequired(_))
  ));
  assert_eq!(observed.generation(), 1);
  assert_eq!(get("/before").await.unwrap().status(), StatusCode::OK);
  let slow_initial_upload = begin_admitted_request(api, "/before").await;
  for (generation, before, after) in [(2, "/before", "/after"), (3, "/after", "/next")] {
    let admitted = begin_admitted_request(api, before).await;
    assert_eq!(reload(after, after).await.unwrap().generation, generation);
    assert_untagged_worker_mount(&worker.path, after, before).await;
    assert!(initial.upgrade().is_some());
    assert_eq!(get(before).await.unwrap().status(), StatusCode::NOT_FOUND);
    assert_eq!(get(after).await.unwrap().status(), StatusCode::OK);
    finish_admitted_request(admitted).await;
  }
  pool
    .update_weights(BTreeMap::from([("original".into(), 1)]))
    .await
    .unwrap();
  let candidate = Worker::start(
    mount_config(api, "/next").into_parts().0,
    Arc::new(FixtureDispatcher {
      stream: parking_lot::Mutex::new(None),
      contexts: parking_lot::Mutex::new(vec![]),
    }),
  )
  .await;
  pool
    .register_rollout(
      candidate.endpoint("candidate", 0),
      RolloutPolicy {
        initial_percent: 10,
        stages: vec![RolloutStage {
          duration_seconds: 60,
          traffic_percent: 90,
        }],
        completion_percent: 100,
      },
    )
    .await
    .unwrap();
  assert!(matches!(
    reload("/after", "/after").await,
    Err(ReloadError::Conflict(_))
  ));
  pool.retire("original").unwrap();
  let mut retired = Box::pin(pool.wait_retired("original"));
  assert!(futures_util::poll!(&mut retired).is_pending());
  finish_admitted_request(slow_initial_upload).await;
  // No registration, default dispatcher, or rejected candidate keeps the startup
  // providers/accounts alive after the last request captured by that ingress ends.
  tokio::time::timeout(Duration::from_secs(5), async {
    while initial.upgrade().is_some() {
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  tokio::time::timeout(Duration::from_secs(5), retired)
    .await
    .unwrap()
    .unwrap();
  pool.disconnect("original");
  assert_eq!(get("/next").await.unwrap().text().await.unwrap(), "fixture");
  assert!(matches!(
    reload("/after", "/after").await,
    Err(ReloadError::RestartRequired(_))
  ));
  stop.send(()).unwrap();
  server.await.unwrap().unwrap();
  candidate.stop().await;
  worker.stop().await;
}

#[tokio::test]
async fn lost_worker_activation_acknowledgement_reports_a_published_generation() {
  use super::reload::{GENERATION_HEADER, LEASE_CAPABILITY, PIN_PATH, PREPARE_PATH};
  use crate::v2::{AdminReloader, ReloadError};
  use axum::http::HeaderMap;
  let api = address();
  let startup = mount_config(api, "/old");
  let updated = mount_config(api, "/new");
  let initial_info = Arc::new(WorkerInfo::new(startup.gateway(), "fixture").unwrap());
  let updated_info = Arc::new(WorkerInfo::new(updated.gateway(), "fixture").unwrap());
  let old_id = uuid::Uuid::new_v4();
  let new_id = uuid::Uuid::new_v4();

  // This peer answers readiness and creates both lease connections. It exits the
  // prepared lease when it receives activation, before acknowledging that byte.
  let worker = Worker::start_with(move |path, mut stop| async move {
    let listener = tokio::net::UnixListener::bind(path)?;
    let mut sessions = tokio::task::JoinSet::new();
    loop {
      let stream = tokio::select! {
        _ = &mut stop => break,
        next = listener.accept() => next?.0,
      };
      let initial_info = initial_info.clone();
      let updated_info = updated_info.clone();
      sessions.spawn(async move {
        let service = hyper::service::service_fn(move |mut req: hyper::Request<hyper::body::Incoming>| {
          let info = if req
            .headers()
            .get(GENERATION_HEADER)
            .is_some_and(|id| id.as_bytes() == new_id.to_string().as_bytes())
          {
            updated_info.clone()
          } else {
            initial_info.clone()
          };
          async move {
            let response = match req.uri().path() {
              super::protocol::READY_PATH => {
                let mut response = axum::Json(&*info).into_response();
                response.headers_mut().insert(LEASE_CAPABILITY, "1".parse().unwrap());
                response
              }
              route @ (PIN_PATH | PREPARE_PATH) => {
                let id = if route == PIN_PATH { old_id } else { new_id };
                let upgrade = hyper::upgrade::on(&mut req);
                tokio::spawn(async move {
                  if let Ok(upgraded) = upgrade.await {
                    let _ = TokioIo::new(upgraded).read_u8().await;
                  }
                });
                Response::builder()
                  .status(StatusCode::SWITCHING_PROTOCOLS)
                  .header("connection", "upgrade")
                  .header("upgrade", "tokn-reload-v1")
                  .header(GENERATION_HEADER, id.to_string())
                  .header("x-tokn-ipc-accounts", "0")
                  .body(Body::empty())
                  .unwrap()
              }
              _ => StatusCode::NOT_FOUND.into_response(),
            };
            Ok::<_, std::convert::Infallible>(response)
          }
        });
        let _ = hyper::server::conn::http1::Builder::new()
          .serve_connection(TokioIo::new(stream), service)
          .with_upgrades()
          .await;
      });
    }
    std::fs::remove_file(listener.local_addr()?.as_pathname().unwrap())?;
    Ok(())
  })
  .await;
  let (plan, service) = startup.into_parts();
  let pool = Arc::new(WorkerPool::empty(&plan).unwrap());
  pool.register(worker.endpoint("worker", 1)).await.unwrap();
  let frontend = Frontend::new(
    plan,
    service.clone(),
    Arc::new(AccessStore::disabled()),
    pool.dispatcher(),
  )
  .unwrap();
  let before = pool.status().generation;
  let error = pool
    .reload(
      &frontend.reload_handle(),
      updated.gateway().clone(),
      &service,
      "revision",
    )
    .await
    .unwrap_err();
  assert_eq!(error, ReloadError::ActivationUnconfirmed { generation: 2 });
  assert_eq!(pool.status().generation, before + 1);
  // The pool's published selector and registration manifest both describe the new mounts.
  pool.check_ready(updated.gateway()).await.unwrap();
  let mut headers = HeaderMap::new();
  headers.insert("x-tokn-admin", "reload".parse().unwrap());
  let reloader = AdminReloader::new(move || std::future::ready(Err(error.clone())));
  let response = crate::v2::handle_admin_reload(headers, Some(&reloader)).await;
  assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
  let body: serde_json::Value = serde_json::from_slice(&to_bytes(response.into_body(), 2048).await.unwrap()).unwrap();
  assert_eq!(body["error"]["type"], "activation_unconfirmed");
  assert!(body["error"]["message"]
    .as_str()
    .unwrap()
    .contains("generation 2 was published"));
  worker.stop().await;
}
