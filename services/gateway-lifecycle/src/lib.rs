//! Supervise a gateway over a private pipe and release its frontend when another worker takes ownership.
#![cfg(unix)]

use anyhow::{Context, Result};
use std::future::Future;
use std::io::{self, Write};
use std::os::fd::AsFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::unix::pipe::Receiver;
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, Mutex};

const SUPERVISED: &str = "TOKN_GATEWAY_SUPERVISED";
const LAUNCHER_PID: &str = "TOKN_GATEWAY_LAUNCHER_PID";
const HANDOFF: &[u8] = b"tokn-gateway transferred\n";
const READY: &[u8] = b"tokn-gateway ready\n";
// Allow the frontend's 30s HTTP grace and 5s persistence flush before
// terminating the child. The launcher has additional time to reap it.
const FRONTEND_GRACE: Duration = Duration::from_secs(37);
const LAUNCHER_GRACE: Duration = Duration::from_secs(40);

/// Use a short path for macOS Unix sockets. Configuration identity is stable
/// across binary versions, and explicit config paths keep test homes isolated.
pub fn runtime_dir(config_path: &Path) -> Result<PathBuf> {
  let config_path = std::fs::canonicalize(config_path)?;
  let mut hash = 0xcbf29ce484222325_u64;
  for byte in config_path.as_os_str().as_encoded_bytes() {
    hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
  }
  // SAFETY: geteuid has no preconditions and does not modify process state.
  let uid = unsafe { libc::geteuid() };
  let path = PathBuf::from(format!("/tmp/tokn-gateway-{uid}-{hash:016x}"));
  match std::fs::DirBuilder::new().mode(0o700).create(&path) {
    Ok(()) => {}
    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
    Err(error) => return Err(error.into()),
  }
  let metadata = std::fs::symlink_metadata(&path)?;
  anyhow::ensure!(
    metadata.is_dir() && metadata.uid() == uid && metadata.mode() & 0o077 == 0,
    "frontend runtime directory {} must be owned by this user and private (0700)",
    path.display()
  );
  Ok(path)
}

/// Keep the child's stdin open until shutdown. Its stdout acknowledges worker registration.
pub fn supervise(command: &mut Command) {
  command
    .env(SUPERVISED, "1")
    .stdin(Stdio::piped())
    .stdout(Stdio::piped());
}

/// Release the launch pipe and reap the original child process, including on timeout.
pub async fn stop(child: &mut Child) -> io::Result<ExitStatus> {
  drop(child.stdin.take());
  match tokio::time::timeout(LAUNCHER_GRACE, child.wait()).await {
    Ok(result) => result,
    Err(_) => {
      child.kill().await?;
      child.wait().await
    }
  }
}

pub async fn wait_ready(stdout: &mut (impl AsyncRead + Unpin)) -> io::Result<()> {
  let mut message = [0; READY.len()];
  stdout.read_exact(&mut message).await?;
  if message != READY {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      "invalid gateway readiness message",
    ));
  }
  Ok(())
}

pub fn notify_ready() -> io::Result<()> {
  let mut stdout = io::stdout().lock();
  stdout.write_all(READY)?;
  stdout.flush()
}

/// The write end belongs exclusively to the process that launched this gateway.
pub struct ParentLease(Receiver);

impl ParentLease {
  pub fn from_stdin() -> io::Result<Option<Self>> {
    if std::env::var_os(SUPERVISED).as_deref() != Some(std::ffi::OsStr::new("1")) {
      return Ok(None);
    }
    Receiver::from_owned_fd(io::stdin().as_fd().try_clone_to_owned()?).map(|pipe| Some(Self(pipe)))
  }

  pub async fn closed(&mut self) -> io::Result<()> {
    match self.0.read(&mut [0]).await? {
      0 => Ok(()),
      _ => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "unexpected data on gateway supervision pipe",
      )),
    }
  }
}

/// Prepare a frontend child with a private lifetime pipe and ownership notification.
pub fn supervise_frontend(command: &mut Command) {
  supervise(command);
  command.env(LAUNCHER_PID, std::process::id().to_string());
}

#[derive(PartialEq)]
enum Ownership {
  Starting,
  Owned,
  Released,
  Transferred,
}

/// Serialize the last ownership decision with external worker admission.
pub struct FrontendOwner {
  launcher_pid: i32,
  state: Mutex<Ownership>,
}

impl FrontendOwner {
  pub fn for_launch() -> Result<Self> {
    Ok(Self {
      launcher_pid: std::env::var(LAUNCHER_PID)?.parse()?,
      state: Mutex::new(Ownership::Starting),
    })
  }

  pub async fn register<T>(&self, peer_pid: Option<i32>, register: impl Future<Output = Result<T>>) -> Result<T> {
    let mut state = self.state.lock().await;
    anyhow::ensure!(*state != Ownership::Released, "frontend owner has requested shutdown");
    anyhow::ensure!(
      *state != Ownership::Starting || peer_pid == Some(self.launcher_pid),
      "waiting for the supervising worker to register"
    );
    let registered = register.await?;
    if *state == Ownership::Starting {
      *state = Ownership::Owned;
    }
    if *state == Ownership::Owned && peer_pid != Some(self.launcher_pid) {
      *state = Ownership::Transferred;
      // A vanished launcher needs no acknowledgement. Admission has already committed.
      let mut stdout = io::stdout().lock();
      let _ = stdout.write_all(HANDOFF).and_then(|()| stdout.flush());
    }
    Ok(registered)
  }

  pub async fn released(&self, mut parent: ParentLease) -> io::Result<()> {
    let closed = parent.closed().await;
    let mut state = self.state.lock().await;
    if *state == Ownership::Transferred {
      drop(state);
      return std::future::pending().await;
    }
    *state = Ownership::Released;
    closed
  }
}

fn exited(status: ExitStatus) -> Result<()> {
  anyhow::ensure!(status.success(), "supervised frontend exited ({status})");
  Ok(())
}

/// The worker may retire after another process adopts the frontend. While
/// owned, wait for its exact child handle; after transfer, retire only our worker.
pub async fn serve_owned<F>(
  mut frontend: Child,
  mut parent: ParentLease,
  start_worker: impl FnOnce(oneshot::Receiver<()>) -> F,
  shutdown: impl Future<Output = io::Result<()>>,
) -> Result<()>
where
  F: Future<Output = Result<()>>,
{
  let lease = frontend.stdin.take().context("missing frontend supervision pipe")?;
  let mut stdout = frontend.stdout.take().context("missing frontend ownership channel")?;
  let handoff = async move {
    let mut message = [0; HANDOFF.len()];
    stdout.read_exact(&mut message).await.is_ok() && message == HANDOFF
  };
  let (retire, stop_worker) = oneshot::channel();
  let worker = start_worker(stop_worker);
  tokio::pin!(worker, shutdown, handoff);
  let mut worker_result = None;
  let mut frontend_status = None;
  let mut transferred = None;
  let stop = loop {
    tokio::select! {
      biased;
      result = &mut handoff, if transferred.is_none() => {
        transferred = Some(result);
        if result && worker_result.is_some() { break Ok(()); }
      }
      result = parent.closed() => break result,
      result = &mut shutdown => break result,
      result = frontend.wait() => {
        frontend_status = Some(result?);
        break Ok(());
      }
      result = &mut worker, if worker_result.is_none() => {
        let done = result.is_err() || transferred == Some(true);
        worker_result = Some(result);
        if done { break Ok(()); }
      }
    }
  };
  drop(lease);
  let _ = retire.send(());
  let wait_frontend = async {
    if let Some(status) = frontend_status {
      return exited(status);
    }
    if transferred == Some(true) {
      return Ok(());
    }
    let finish = async {
      if transferred.is_none() && handoff.await {
        return Ok(());
      }
      exited(frontend.wait().await?)
    };
    match tokio::time::timeout(FRONTEND_GRACE, finish).await {
      Ok(result) => result,
      Err(_) => {
        frontend.kill().await?;
        anyhow::bail!("supervised frontend exceeded its shutdown deadline");
      }
    }
  };
  let wait_worker = async {
    match worker_result {
      Some(result) => result,
      None => worker.await,
    }
  };
  let (frontend, worker) = tokio::join!(wait_frontend, wait_worker);
  stop?;
  frontend.and(worker)
}
