//! Frontend discovery and the private, versioned worker control protocol.

mod discovery;
mod worker;

pub use discovery::{control_socket_path, discover_or_launch, inspect, FrontendLaunch};
pub use worker::WorkerAttachment;

use crate::supervision::FrontendOwner;
use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokn_router::ipc::{WorkerEndpoint, WorkerPool};
use tokn_router::routing::{AbTestStatus, RolloutPolicy, RoutingControl};

// Optional capabilities extend control v2 without excluding older worker binaries.
const CONTROL_PROTOCOL_VERSION: u32 = 2;
const START_TIMEOUT: Duration = Duration::from_secs(15);
const MESSAGE_LIMIT: u64 = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrontendInfo<S> {
  protocol_version: u32,
  #[serde(default)]
  supports_ab_test: bool,
  #[serde(default)]
  supports_ab_test_status: bool,
  #[serde(default)]
  capabilities: Vec<String>,
  pub frontend_pid: u32,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub maintenance_requests_dir: Option<PathBuf>,
  pub args: S,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum ControlRequest {
  Inspect,
  AbTestStatus,
  Retire,
  Register {
    worker_id: String,
    socket_path: PathBuf,
    candidate: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    ab_test: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rollout_policy: Option<RolloutPolicy>,
  },
}

fn is_false(value: &bool) -> bool {
  !*value
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum ControlReply {
  Info { info: serde_json::Value },
  AbTest { ab_test: Option<AbTestStatus> },
  Registered,
  Retired,
  Stopping,
  Error { message: String },
}

async fn write_message<T: Serialize>(stream: &mut UnixStream, message: &T) -> Result<()> {
  let mut bytes = serde_json::to_vec(message)?;
  anyhow::ensure!(bytes.len() < MESSAGE_LIMIT as usize, "control message exceeds limit");
  bytes.push(b'\n');
  stream.write_all(&bytes).await?;
  Ok(())
}

async fn read_message<T: DeserializeOwned>(stream: &mut BufReader<UnixStream>) -> Result<T> {
  read_buffered_message(stream, &mut Vec::new()).await
}

async fn read_buffered_message<T: DeserializeOwned>(
  stream: &mut BufReader<UnixStream>,
  bytes: &mut Vec<u8>,
) -> Result<T> {
  let remaining = MESSAGE_LIMIT.saturating_sub(bytes.len() as u64);
  let read = stream.take(remaining + 1).read_until(b'\n', bytes).await?;
  anyhow::ensure!(read > 0, "frontend control connection closed");
  anyhow::ensure!(
    bytes.len() as u64 <= MESSAGE_LIMIT && bytes.last() == Some(&b'\n'),
    "invalid control message length"
  );
  let message = serde_json::from_slice(bytes)?;
  bytes.clear();
  Ok(message)
}

async fn read_ab_test_status(path: &Path) -> Result<Option<AbTestStatus>> {
  tokio::time::timeout(Duration::from_secs(1), async {
    let mut stream = UnixStream::connect(path).await?;
    write_message(&mut stream, &ControlRequest::AbTestStatus).await?;
    match read_message(&mut BufReader::new(stream)).await? {
      ControlReply::AbTest { ab_test } => Ok(ab_test),
      _ => anyhow::bail!("invalid frontend A/B status response"),
    }
  })
  .await
  .context("frontend A/B status timed out")?
}

pub(super) struct ControlHost {
  info: serde_json::Value,
  pool: Arc<WorkerPool>,
  owner: Option<Arc<FrontendOwner>>,
  default_rollout: RolloutPolicy,
}

impl ControlHost {
  pub(super) fn new(
    settings: impl Serialize,
    pool: Arc<WorkerPool>,
    owner: Option<Arc<FrontendOwner>>,
    default_rollout: RolloutPolicy,
    maintenance_requests_dir: Option<PathBuf>,
  ) -> Result<Self> {
    Ok(Self {
      info: serde_json::to_value(FrontendInfo {
        protocol_version: CONTROL_PROTOCOL_VERSION,
        supports_ab_test: true,
        supports_ab_test_status: true,
        capabilities: vec!["rollout_policy_v1".into()],
        frontend_pid: std::process::id(),
        args: settings,
        maintenance_requests_dir,
      })?,
      pool,
      owner,
      default_rollout,
    })
  }
}

struct Registration {
  pool: Arc<WorkerPool>,
  worker_id: String,
}
impl Drop for Registration {
  fn drop(&mut self) {
    self.pool.disconnect(&self.worker_id);
  }
}

impl ControlHost {
  async fn connection(&self, stream: UnixStream, mut stop: watch::Receiver<bool>) -> Result<()> {
    let pool = &self.pool;
    let mut stream = BufReader::new(stream);
    let request = tokio::time::timeout(Duration::from_secs(5), read_message(&mut stream)).await??;
    match request {
      ControlRequest::Retire => anyhow::bail!("register before retiring a worker"),
      ControlRequest::AbTestStatus => {
        let ab_test = pool.status().ab_test;
        write_message(stream.get_mut(), &ControlReply::AbTest { ab_test }).await
      }
      ControlRequest::Inspect => {
        write_message(
          stream.get_mut(),
          &ControlReply::Info {
            info: self.info.clone(),
          },
        )
        .await
      }
      ControlRequest::Register {
        worker_id,
        socket_path,
        candidate,
        ab_test,
        rollout_policy,
      } => {
        let endpoint = WorkerEndpoint {
          worker_id: worker_id.clone(),
          socket_path,
          weight: 1,
        };
        anyhow::ensure!(
          !(candidate && (ab_test || rollout_policy.is_some())),
          "--candidate and --ab-test are mutually exclusive"
        );
        let register = async {
          if ab_test || rollout_policy.is_some() {
            pool
              .register_rollout(endpoint, rollout_policy.unwrap_or_else(|| self.default_rollout.clone()))
              .await
          } else if candidate {
            pool.register_candidate(endpoint).await
          } else {
            pool.register(endpoint).await
          }
        };
        let result = match &self.owner {
          Some(owner) => owner.register(stream.get_ref().peer_cred()?.pid(), register).await,
          None => register.await,
        };
        if let Err(error) = result {
          return write_message(
            stream.get_mut(),
            &ControlReply::Error {
              message: format!("{error:#}"),
            },
          )
          .await;
        }
        let _registration = Registration {
          pool: pool.clone(),
          worker_id: worker_id.clone(),
        };
        write_message(stream.get_mut(), &ControlReply::Registered).await?;
        tracing::info!(%worker_id, candidate, "worker registered with frontend");
        loop {
          let reply = tokio::select! {
            result = pool.wait_retired(&worker_id) => { result?; ControlReply::Retired }
            _ = stop.changed() => ControlReply::Stopping,
            request = read_message::<ControlRequest>(&mut stream) => {
              match request {
                Ok(ControlRequest::Retire) => {
                  pool.retire(&worker_id)?;
                  tracing::info!(%worker_id, main_worker_id = ?pool.status().main_worker_id, "worker exiting; main worker reassigned");
                  continue;
                }
                Err(_) => return Ok(()),
                _ => anyhow::bail!("unexpected registered-worker command"),
              }
            }
          };
          return write_message(stream.get_mut(), &reply).await;
        }
      }
    }
  }

  pub(super) async fn serve(self, listener: UnixListener, mut stop: watch::Receiver<bool>) -> Result<()> {
    let host = Arc::new(self);
    let mut tasks = JoinSet::new();
    let mut ramp_tick = tokio::time::interval(Duration::from_secs(1));
    ramp_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
      tokio::select! {
        biased;
        _ = stop.changed() => break,
        _ = ramp_tick.tick() => host.pool.advance_ab_test(),
        result = tasks.join_next(), if !tasks.is_empty() => {
          if let Some(Err(error)) = result { tracing::warn!(%error, "frontend control task failed"); }
        }
        accepted = listener.accept() => {
          let (stream, _) = accepted?;
          let host = host.clone();
          let stop = stop.clone();
          tasks.spawn(async move {
            if let Err(error) = host.connection(stream, stop).await {
              tracing::debug!(%error, "frontend control connection ended");
            }
          });
        }
      }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
  }
}
#[cfg(test)]
mod tests;
