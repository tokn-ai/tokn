//! Owned Unix connections, streaming bodies, and exclusive socket lifecycle.

use super::protocol::{WireContext, CONNECT_TIMEOUT, CONTEXT_HEADER, MAX_CONTEXT_BYTES, PROTOCOL_VERSION, READY_PATH};
use super::WorkerInfo;
use crate::dispatch::{DispatchContext, RequestDispatcher, RequestOrigin};
use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use http_body::{Frame, SizeHint};
use hyper::body::Incoming;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::future::Future;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::{JoinHandle, JoinSet};

pub(super) fn strip_internal_headers(headers: &mut axum::http::HeaderMap) {
  let names = headers
    .keys()
    .filter(|name| name.as_str().starts_with("x-tokn-ipc-"))
    .cloned()
    .collect::<Vec<_>>();
  for name in names {
    headers.remove(name);
  }
}

pub(super) fn strip_connection_headers(headers: &mut axum::http::HeaderMap) -> Result<()> {
  let mut names = Vec::new();
  for value in headers.get_all("connection") {
    for name in value
      .to_str()?
      .split(',')
      .map(str::trim)
      .filter(|name| !name.is_empty())
    {
      names.push(axum::http::HeaderName::from_bytes(name.as_bytes())?);
    }
  }
  for name in names {
    headers.remove(name);
  }
  for name in [
    "connection",
    "proxy-connection",
    "keep-alive",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
  ] {
    headers.remove(name);
  }
  Ok(())
}

struct ConnectionTask(JoinHandle<()>);
impl Drop for ConnectionTask {
  fn drop(&mut self) {
    self.0.abort();
  }
}

pub(super) async fn exchange(path: &Path, mut request: Request) -> Result<Response> {
  let stream = tokio::time::timeout(CONNECT_TIMEOUT, UnixStream::connect(path))
    .await
    .context("connect worker timed out")?
    .with_context(|| format!("connect worker socket {}", path.display()))?;
  let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
  let task = ConnectionTask(tokio::spawn(async move {
    if let Err(error) = connection.await {
      tracing::debug!(%error, "worker IPC connection closed");
    }
  }));
  if !request.headers().contains_key("host") {
    request
      .headers_mut()
      .insert("host", HeaderValue::from_static("localhost"));
  }
  let response = sender.send_request(request).await?;
  let (parts, body) = response.into_parts();
  Ok(Response::from_parts(
    parts,
    Body::new(IpcBody {
      body,
      _connection: task,
    }),
  ))
}

struct IpcBody {
  body: Incoming,
  _connection: ConnectionTask,
}

impl http_body::Body for IpcBody {
  type Data = bytes::Bytes;
  type Error = hyper::Error;
  fn poll_frame(
    mut self: Pin<&mut Self>,
    cx: &mut TaskContext<'_>,
  ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
    Pin::new(&mut self.body).poll_frame(cx)
  }
  fn is_end_stream(&self) -> bool {
    self.body.is_end_stream()
  }
  fn size_hint(&self) -> SizeHint {
    self.body.size_hint()
  }
}

struct SocketFile {
  path: PathBuf,
  device: u64,
  inode: u64,
}
impl Drop for SocketFile {
  fn drop(&mut self) {
    if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
      if metadata.dev() == self.device && metadata.ino() == self.inode {
        let _ = std::fs::remove_file(&self.path);
      }
    }
  }
}

/// Only bind in a pre-existing private directory. Refuse stale sockets rather
/// than unlinking a socket that may still belong to a running worker.
pub async fn serve_worker<F>(
  path: PathBuf,
  dispatcher: Arc<dyn RequestDispatcher>,
  info: WorkerInfo,
  shutdown: F,
) -> Result<()>
where
  F: Future<Output = ()> + Send,
{
  let parent = path
    .parent()
    .filter(|path| !path.as_os_str().is_empty())
    .context("worker socket requires a parent directory")?;
  let metadata = std::fs::metadata(parent).context("worker socket directory must already exist")?;
  anyhow::ensure!(
    metadata.is_dir() && metadata.permissions().mode() & 0o077 == 0,
    "worker socket directory must be private (mode 0700)"
  );
  let listener = UnixListener::bind(&path).with_context(|| format!("bind worker socket {}", path.display()))?;
  let metadata = std::fs::symlink_metadata(&path)?;
  let socket = SocketFile {
    path,
    device: metadata.dev(),
    inode: metadata.ino(),
  };
  std::fs::set_permissions(&socket.path, std::fs::Permissions::from_mode(0o600))?;
  tracing::info!(socket_path = %socket.path.display(), protocol_version = info.protocol_version, "gateway worker listening");
  let info = Arc::new(info);
  let mut connections = JoinSet::new();
  let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
  tokio::pin!(shutdown);
  let result = loop {
    tokio::select! {
      biased;
      _ = &mut shutdown => break Ok(()),
      Some(joined) = connections.join_next(), if !connections.is_empty() => {
        if let Err(error) = joined { tracing::warn!(%error, "worker connection task failed"); }
      }
      accepted = listener.accept() => {
        let (stream, _) = match accepted { Ok(value) => value, Err(error) => break Err(error.into()) };
        let dispatcher = dispatcher.clone();
        let info = info.clone();
        let mut stopped = shutdown_rx.clone();
        connections.spawn(async move {
          let service = hyper::service::service_fn(move |request| {
            let dispatcher = dispatcher.clone();
            let info = info.clone();
            async move { Ok::<_, std::convert::Infallible>(worker_request(dispatcher, info, request).await) }
          });
          let mut builder = hyper::server::conn::http1::Builder::new();
          builder.keep_alive(false).timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(30));
          let connection = builder.serve_connection(TokioIo::new(stream), service);
          tokio::pin!(connection);
          let result = tokio::select! {
            result = &mut connection => result,
            _ = stopped.changed() => { connection.as_mut().graceful_shutdown(); connection.await }
          };
          if let Err(error) = result { tracing::debug!(%error, "worker request connection closed"); }
        });
      }
    }
  };
  drop(listener);
  drop(socket);
  let _ = shutdown_tx.send(true);
  result.and(crate::server::drain_connections(&mut connections, crate::server::SHUTDOWN_GRACE_PERIOD).await)
}

async fn worker_request(
  dispatcher: Arc<dyn RequestDispatcher>,
  info: Arc<WorkerInfo>,
  request: hyper::Request<Incoming>,
) -> Response {
  if request.method() == Method::GET
    && request.uri().path() == READY_PATH
    && !request.headers().contains_key(CONTEXT_HEADER)
  {
    return axum::Json(&*info).into_response();
  }
  match decode_request(request, &info) {
    Ok((context, request)) => match dispatcher.dispatch(context, request).await {
      Ok(response) => response,
      Err(error) => {
        tracing::warn!(%error, "worker execution failed");
        crate::api::error::ApiError::bad_gateway("worker execution failed").into_response()
      }
    },
    Err(error) => {
      tracing::debug!(%error, "invalid worker IPC request");
      (StatusCode::BAD_REQUEST, "invalid IPC request").into_response()
    }
  }
}

fn decode_request(request: hyper::Request<Incoming>, info: &WorkerInfo) -> Result<(DispatchContext, Request)> {
  let (mut parts, body) = request.into_parts();
  let mut values = parts.headers.get_all(CONTEXT_HEADER).iter();
  let value = values.next().context("missing IPC context")?;
  anyhow::ensure!(values.next().is_none(), "duplicate IPC context");
  anyhow::ensure!(value.as_bytes().len() <= MAX_CONTEXT_BYTES, "IPC context exceeds limit");
  let wire: WireContext = serde_json::from_slice(&STANDARD.decode(value.as_bytes())?)?;
  anyhow::ensure!(
    wire.protocol_version == info.protocol_version && wire.protocol_version == PROTOCOL_VERSION,
    "incompatible IPC protocol"
  );
  let kind = match wire.origin {
    RequestOrigin::Api => "api",
    RequestOrigin::Proxy { .. } => "proxy",
  };
  anyhow::ensure!(
    info
      .listeners
      .iter()
      .any(|listener| listener.listener_id == wire.listener_id && listener.kind == kind),
    "unknown worker listener"
  );
  wire.origin.ingress()?;
  parts.uri = wire.uri.parse()?;
  strip_internal_headers(&mut parts.headers);
  Ok((wire.into_context(), Request::from_parts(parts, Body::new(body))))
}
