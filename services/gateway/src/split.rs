//! Unix startup, worker attachment, and orderly retirement.

use crate::control::{self, FrontendInfo};
use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use tokio::process::Command;
use tokio::sync::{oneshot, watch};
use tokn_core::util::shutdown::ShutdownSignal;
use tokn_router::routing::{AbTestStatus, RolloutPolicy};

pub struct WorkerRegistration {
  pub candidate: bool,
  pub rollout_policy: Option<RolloutPolicy>,
  pub default_rollout: RolloutPolicy,
}

pub struct WorkerSettings<S> {
  pub frontend: S,
  pub socket_path: PathBuf,
  pub frontend_maintenance_dir: Option<PathBuf>,
}

/// The process owner may exit immediately after a second shutdown signal.
#[derive(Debug)]
pub struct ForceShutdown;

impl Display for ForceShutdown {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.write_str("Second shutdown signal received; exiting immediately.")
  }
}

impl std::error::Error for ForceShutdown {}

/// Discover or start a frontend, then serve requests through the registered worker.
pub async fn serve<S: DeserializeOwned>(
  resolve_runtime_dir: impl FnOnce() -> Result<PathBuf>,
  require_existing: bool,
  command: impl FnOnce() -> Result<Command>,
  registration: impl FnOnce(&S) -> Result<WorkerRegistration>,
  worker: impl AsyncFnOnce(WorkerSettings<S>, watch::Receiver<bool>) -> Result<()>,
  set_status: impl Fn(Option<AbTestStatus>),
) -> Result<()> {
  let mut shutdown = ShutdownSignal::new()?;
  let dir = resolve_runtime_dir()?;
  let Some(control::FrontendLaunch { info, owned }) =
    control::discover_or_launch(&dir, require_existing, shutdown.recv(), command).await?
  else {
    return Ok(());
  };
  let registration = registration(&info.args)?;
  tracing::info!(frontend_pid = info.frontend_pid, "using frontend");
  if let Some((frontend, parent)) = owned {
    let stop = ShutdownSignal::new()?;
    return crate::supervision::serve_owned(
      frontend,
      parent,
      |release| run_worker(&dir, info, registration, shutdown, Some(release), worker, set_status),
      stop.wait(),
    )
    .await;
  }
  run_worker(&dir, info, registration, shutdown, None, worker, set_status).await
}

/// Join a previously started frontend from an independent worker process.
pub async fn join<S: DeserializeOwned>(
  resolve_runtime_dir: impl FnOnce() -> Result<PathBuf>,
  registration: impl FnOnce() -> Result<WorkerRegistration>,
  worker: impl AsyncFnOnce(WorkerSettings<S>, watch::Receiver<bool>) -> Result<()>,
  set_status: impl Fn(Option<AbTestStatus>),
) -> Result<()> {
  let mut shutdown = ShutdownSignal::new()?;
  let dir = resolve_runtime_dir()?;
  let control_path = control::control_socket_path(&dir);
  let info = tokio::select! {
    biased;
    result = shutdown.recv() => return result.map_err(Into::into),
    result = control::inspect(&control_path) => result.context(
      "no compatible frontend; run `tokn-gateway frontend --with-proxy` or `tokn-gateway serve --with-proxy` first",
    )?,
  };
  run_worker(&dir, info, registration()?, shutdown, None, worker, set_status).await
}

async fn run_worker<S>(
  dir: &Path,
  info: FrontendInfo<S>,
  registration: WorkerRegistration,
  mut shutdown: ShutdownSignal,
  supervisor: Option<oneshot::Receiver<()>>,
  worker: impl AsyncFnOnce(WorkerSettings<S>, watch::Receiver<bool>) -> Result<()>,
  set_status: impl Fn(Option<AbTestStatus>),
) -> Result<()> {
  let supervised = supervisor.is_some();
  let attachment = info.worker_attachment(
    dir,
    registration.candidate,
    registration.rollout_policy,
    &registration.default_rollout,
    supervised,
  )?;
  let worker_id = attachment.worker_id().to_owned();
  let settings = WorkerSettings {
    frontend: info.args,
    socket_path: attachment.socket_path().to_owned(),
    frontend_maintenance_dir: info.maintenance_requests_dir,
  };
  let (stop_tx, stop_rx) = watch::channel(false);
  let server = worker(settings, stop_rx);
  tokio::pin!(server);
  let (retire_tx, retire_rx) = watch::channel(false);
  let attach = attachment.attach(retire_rx, &set_status);
  tokio::pin!(attach);
  let mut attachment_done = false;
  let mut attachment_result = Ok(());
  let mut interrupted = false;
  let released = async {
    match supervisor {
      Some(lease) => {
        let _ = lease.await;
      }
      None => std::future::pending().await,
    }
  };
  tokio::pin!(released);
  loop {
    tokio::select! {
      biased;
      // The supervisor coordinates termination with frontend drain. An
      // independently launched worker handles its own shutdown signals.
      result = shutdown.recv(), if !supervised => {
        result?;
        if interrupted {
          return Err(ForceShutdown.into());
        }
        interrupted = true;
        tracing::info!(%worker_id, "retiring worker; waiting for existing requests (Ctrl+C again forces exit)");
        let _ = retire_tx.send(true);
      }
      _ = &mut released, if !interrupted => {
        interrupted = true;
        let _ = retire_tx.send(true);
      }
      result = &mut server => {
        set_status(None);
        return attachment_result.and(result);
      }
      result = &mut attach, if !attachment_done => {
        attachment_result = result;
        attachment_done = true;
        set_status(None);
        let _ = stop_tx.send(true);
      }
    }
  }
}
