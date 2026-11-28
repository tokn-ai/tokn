//! A prepared worker generation lives as long as its frontend ingress snapshot.
//! The upgraded Unix connection is its lease; a dropped or cancelled preparation
//! releases the candidate without publishing it.

use super::transport::ConnectionTask;
use super::WorkerInfo;
use crate::frontend::connect_policy::ConnectPolicy;
use crate::v2::{LiveRuntime, ReloadError, RuntimeGeneration, RuntimeStates};
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{watch, Mutex};
use uuid::Uuid;

pub(super) const GENERATION_HEADER: &str = "x-tokn-ipc-generation";
pub(super) const PIN_PATH: &str = "/_tokn/pin-current";
pub(super) const LEASE_CAPABILITY: &str = "x-tokn-ipc-generation-leases";
pub(super) const PREPARE_PATH: &str = "/_tokn/prepare-reload";
const UPGRADE: &str = "tokn-reload-v1";
const REVISION: &str = "x-tokn-ipc-config-revision";
const ACCOUNTS: &str = "x-tokn-ipc-accounts";
const FRONTEND_CONNECT_POLICY: &str = "x-tokn-ipc-frontend-connect-policy";
const FRONTEND_CONNECT_POLICY_VERSION: &str = "1";
const MAX_FRONTEND_CONNECT_POLICY_BYTES: usize = 64 * 1024;

type WorkerFuture = Pin<Box<dyn Future<Output = Result<(RuntimeStates, usize), ReloadError>> + Send>>;

/// Build a candidate from this worker's config, checking its revision against the frontend.
pub struct WorkerReload(Arc<dyn Fn(String) -> WorkerFuture + Send + Sync>);

impl WorkerReload {
  pub fn new<F, Fut>(prepare: F) -> Self
  where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(RuntimeStates, usize), ReloadError>> + Send + 'static,
  {
    Self(Arc::new(move |revision| Box::pin(prepare(revision))))
  }
}

#[derive(Clone)]
pub(super) struct PreparedWorker {
  pub generation: Arc<RuntimeGeneration>,
  pub info: Arc<WorkerInfo>,
}

pub(super) struct WorkerGenerations {
  live: Arc<LiveRuntime>,
  version: String,
  prepare: WorkerReload,
  next: AtomicU64,
  states: parking_lot::Mutex<BTreeMap<Uuid, PreparedWorker>>,
}

impl WorkerGenerations {
  pub fn new(live: Arc<LiveRuntime>, info: WorkerInfo, prepare: WorkerReload) -> Self {
    Self {
      next: AtomicU64::new(live.generation() + 1),
      live,
      version: info.version,
      prepare,
      states: parking_lot::Mutex::new(BTreeMap::new()),
    }
  }

  pub fn current(&self) -> PreparedWorker {
    let generation = self.live.current_worker_generation();
    let info = Arc::new(generation.worker_info(&self.version));
    PreparedWorker { generation, info }
  }

  pub fn get(&self, id: &str) -> Option<PreparedWorker> {
    self.states.lock().get(&Uuid::parse_str(id).ok()?).cloned()
  }

  pub async fn prepare(self: Arc<Self>, mut request: Request<Incoming>, mut stop: watch::Receiver<bool>) -> Response {
    if request.headers().get(header::UPGRADE).and_then(|h| h.to_str().ok()) != Some(UPGRADE) {
      return StatusCode::BAD_REQUEST.into_response();
    }
    let result = if request.uri().path() == PIN_PATH {
      Ok(self.current())
    } else {
      let Some(revision) = request.headers().get(REVISION).and_then(|h| h.to_str().ok()) else {
        return StatusCode::BAD_REQUEST.into_response();
      };
      (self.prepare.0)(revision.to_owned())
        .await
        .and_then(|(states, accounts)| {
          let number = self.next.fetch_add(1, Ordering::Relaxed);
          let generation = self.live.prepare_worker_generation(number, states, accounts)?;
          let info = Arc::new(generation.worker_info(&self.version));
          Ok(PreparedWorker { generation, info })
        })
    };
    let prepared = match result {
      Ok(value) => value,
      Err(ReloadError::Conflict(reason)) => return (StatusCode::PRECONDITION_FAILED, reason).into_response(),
      Err(ReloadError::RestartRequired(reason)) => return (StatusCode::CONFLICT, reason).into_response(),
      Err(error) => {
        tracing::warn!(%error, "cannot prepare worker configuration");
        return StatusCode::UNPROCESSABLE_ENTITY.into_response();
      }
    };
    let frontend_connect_policy = if request.uri().path() == PREPARE_PATH
      && request
        .headers()
        .get(FRONTEND_CONNECT_POLICY)
        .is_some_and(|value| value == FRONTEND_CONNECT_POLICY_VERSION)
    {
      let policy = prepared.generation.frontend_connect_policy();
      let Ok(bytes) = serde_json::to_vec(&policy) else {
        return StatusCode::UNPROCESSABLE_ENTITY.into_response();
      };
      if bytes.len() > MAX_FRONTEND_CONNECT_POLICY_BYTES {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
      }
      Some(bytes)
    } else {
      None
    };
    let id = Uuid::new_v4();
    let accounts = prepared.generation.accounts;
    let generation = prepared.generation.clone();
    let upgrade = hyper::upgrade::on(&mut request);
    self.states.lock().insert(id, prepared);
    let mut response = Response::builder()
      .status(StatusCode::SWITCHING_PROTOCOLS)
      .header(header::CONNECTION, "upgrade")
      .header(header::UPGRADE, UPGRADE)
      .header(GENERATION_HEADER, id.to_string())
      .header(ACCOUNTS, accounts);
    if frontend_connect_policy.is_some() {
      response = response.header(FRONTEND_CONNECT_POLICY, FRONTEND_CONNECT_POLICY_VERSION);
    }
    tokio::spawn(async move {
      let lease = async {
        let mut socket = TokioIo::new(upgrade.await?);
        // Only negotiated peers exchange the candidate generation's policy
        // before the existing one-byte activation acknowledgement protocol.
        if let Some(bytes) = frontend_connect_policy {
          socket.write_u32(bytes.len() as u32).await?;
          socket.write_all(&bytes).await?;
        }
        // The frontend sends one byte after swapping its admission state. Request
        // dispatch already addresses the prepared runtime by its opaque id.
        if socket.read_u8().await? == 1 {
          self.live.activate_worker_generation(&generation);
          socket.write_u8(1).await?;
          let _ = socket.read_u8().await;
        }
        Ok::<_, anyhow::Error>(())
      };
      tokio::select! { _ = lease => {}, _ = stop.changed() => {} }
      self.states.lock().remove(&id);
    });
    response
      .body(Body::empty())
      .expect("internal worker response uses validated headers")
  }
}

pub(super) struct GenerationLease {
  pub id: String,
  pub accounts: usize,
  pub frontend_connect_policy: Option<ConnectPolicy>,
  socket: Mutex<TokioIo<hyper::upgrade::Upgraded>>,
}

impl GenerationLease {
  pub async fn prepare(path: &Path, revision: &str) -> Result<Arc<Self>, ReloadError> {
    Self::request(path, Some(revision)).await
  }

  pub async fn pin_current(path: &Path) -> Result<Arc<Self>, ReloadError> {
    Self::request(path, None).await
  }

  async fn request(path: &Path, revision: Option<&str>) -> Result<Arc<Self>, ReloadError> {
    tokio::time::timeout(std::time::Duration::from_secs(15), Self::open(path, revision))
      .await
      .map_err(|_| ReloadError::Invalid("worker configuration preparation timed out".into()))?
  }

  async fn open(path: &Path, revision: Option<&str>) -> Result<Arc<Self>, ReloadError> {
    let stream = UnixStream::connect(path).await.map_err(invalid)?;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
      .await
      .map_err(invalid)?;
    let _connection = ConnectionTask(tokio::spawn(async move {
      let _ = connection.with_upgrades().await;
    }));
    let request = match revision {
      Some(revision) => Request::post(PREPARE_PATH)
        .header(REVISION, revision)
        .header(FRONTEND_CONNECT_POLICY, FRONTEND_CONNECT_POLICY_VERSION),
      None => Request::post(PIN_PATH),
    }
    .header(header::HOST, "localhost")
    .header(header::CONNECTION, "upgrade")
    .header(header::UPGRADE, UPGRADE)
    .body(Body::empty())
    .map_err(invalid)?;
    let mut response = sender.send_request(request).await.map_err(invalid)?;
    match response.status() {
      StatusCode::SWITCHING_PROTOCOLS => {}
      status @ (StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED) => {
        let bytes = axum::body::to_bytes(Body::new(response.into_body()), 4096)
          .await
          .map_err(invalid)?;
        let reason = String::from_utf8_lossy(&bytes).into_owned();
        return Err(if status == StatusCode::PRECONDITION_FAILED {
          ReloadError::Conflict(reason)
        } else {
          ReloadError::RestartRequired(reason)
        });
      }
      StatusCode::UNPROCESSABLE_ENTITY => return Err(ReloadError::Invalid("worker configuration is invalid".into())),
      StatusCode::PAYLOAD_TOO_LARGE => return Err(invalid("worker CONNECT policy exceeds the IPC size limit")),
      _ => {
        return Err(ReloadError::RestartRequired(
          "the active worker does not support configuration reload".into(),
        ))
      }
    }
    let field = |name: &str| {
      response
        .headers()
        .get(name)
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| ReloadError::Invalid("invalid worker reload response".into()))
    };
    let id = Uuid::parse_str(field(GENERATION_HEADER)?).map_err(invalid)?.to_string();
    let accounts = field(ACCOUNTS)?.parse().map_err(invalid)?;
    let has_frontend_connect_policy = match response.headers().get(FRONTEND_CONNECT_POLICY) {
      Some(value) if revision.is_some() && value == FRONTEND_CONNECT_POLICY_VERSION => true,
      Some(_) => return Err(invalid("invalid worker CONNECT policy negotiation")),
      None => false,
    };
    let mut socket = TokioIo::new(hyper::upgrade::on(&mut response).await.map_err(invalid)?);
    let frontend_connect_policy = if has_frontend_connect_policy {
      Some(read_frontend_connect_policy(&mut socket).await?)
    } else {
      None
    };
    Ok(Arc::new(Self {
      id,
      accounts,
      frontend_connect_policy,
      socket: Mutex::new(socket),
    }))
  }

  /// Complete publication even if the public HTTP client disconnects while waiting
  /// for the worker to acknowledge its new default for untagged IPC clients.
  pub async fn activate(self: Arc<Self>) -> Result<(), ReloadError> {
    tokio::spawn(async move {
      let confirm = async {
        let mut socket = self.socket.lock().await;
        socket.write_u8(1).await?;
        anyhow::ensure!(
          socket.read_u8().await? == 1,
          "invalid worker activation acknowledgement"
        );
        Ok::<(), anyhow::Error>(())
      };
      tokio::time::timeout(super::protocol::CONNECT_TIMEOUT, confirm)
        .await
        .map_err(invalid)?
        .map_err(invalid)
    })
    .await
    .map_err(invalid)?
  }
}

async fn read_frontend_connect_policy(socket: &mut (impl AsyncRead + Unpin)) -> Result<ConnectPolicy, ReloadError> {
  let size = socket.read_u32().await.map_err(invalid)? as usize;
  if size == 0 || size > MAX_FRONTEND_CONNECT_POLICY_BYTES {
    return Err(invalid("invalid worker CONNECT policy size"));
  }
  let mut bytes = vec![0; size];
  socket.read_exact(&mut bytes).await.map_err(invalid)?;
  serde_json::from_slice(&bytes).map_err(|_| invalid("invalid worker CONNECT policy"))
}

pub(super) fn invalid(error: impl std::fmt::Display) -> ReloadError {
  ReloadError::Invalid(error.to_string())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn connect_policy_frame_leaves_activation_bytes_unread() {
    let expected = ConnectPolicy::from_listeners(std::iter::empty());
    let payload = serde_json::to_vec(&expected).unwrap();
    let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&payload);
    frame.push(1);
    let mut input = frame.as_slice();

    let actual = read_frontend_connect_policy(&mut input).await.unwrap();
    assert_eq!(serde_json::to_vec(&actual).unwrap(), payload);
    assert_eq!(input, &[1]);
  }

  #[tokio::test]
  async fn connect_policy_frame_rejects_empty_oversized_and_invalid_payloads() {
    for size in [0, MAX_FRONTEND_CONNECT_POLICY_BYTES as u32 + 1] {
      let frame = size.to_be_bytes();
      assert_eq!(
        read_frontend_connect_policy(&mut frame.as_slice()).await.unwrap_err(),
        invalid("invalid worker CONNECT policy size")
      );
    }
    let mut invalid_frame = &[0, 0, 0, 1, b'{'][..];
    assert_eq!(
      read_frontend_connect_policy(&mut invalid_frame).await.unwrap_err(),
      invalid("invalid worker CONNECT policy")
    );
  }
}
