use super::{
  read_message, write_message, ControlReply, ControlRequest, FrontendInfo, CONTROL_PROTOCOL_VERSION, START_TIMEOUT,
};
use crate::supervision::{self, ParentLease};
use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use std::fs::OpenOptions;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};

pub async fn inspect<S: DeserializeOwned>(path: &Path) -> Result<FrontendInfo<S>> {
  tokio::time::timeout(Duration::from_secs(2), async {
    let mut stream = UnixStream::connect(path).await?;
    write_message(&mut stream, &ControlRequest::Inspect).await?;
    match read_message(&mut BufReader::new(stream)).await? {
      ControlReply::Info { info } => {
        let info: FrontendInfo<S> = serde_json::from_value(info)?;
        anyhow::ensure!(
          info.protocol_version == CONTROL_PROTOCOL_VERSION,
          "incompatible frontend control protocol; restart the frontend with this gateway version"
        );
        Ok(info)
      }
      _ => anyhow::bail!("frontend did not return discovery information"),
    }
  })
  .await
  .context("frontend discovery timed out")?
}

/// Start a frontend only when discovery finds none. A supervised caller can own only its original child.
pub struct FrontendLaunch<S> {
  pub info: FrontendInfo<S>,
  pub owned: Option<(Child, ParentLease)>,
}

pub fn control_socket_path(dir: &Path) -> PathBuf {
  dir.join("frontend.sock")
}

pub async fn discover_or_launch<S: DeserializeOwned>(
  dir: &Path,
  require_existing: bool,
  shutdown: impl Future<Output = std::io::Result<()>>,
  command: impl FnOnce() -> Result<Command>,
) -> Result<Option<FrontendLaunch<S>>> {
  let mut parent = ParentLease::from_stdin()?;
  let mut owned = None;
  let control_path = control_socket_path(dir);
  let info = if control_path.exists() {
    anyhow::ensure!(
      parent.is_none(),
      "a frontend already owns this configuration; supervised startup will not replace it"
    );
    tokio::select! {
      biased;
      result = shutdown => { result?; return Ok(None); },
      result = inspect::<S>(&control_path) => result.context(
        "frontend socket exists but is unavailable; stop its owner or remove the stale socket before restarting",
      )?,
    }
  } else {
    anyhow::ensure!(
      !require_existing,
      "--ab-test requires an existing frontend with one active baseline worker"
    );
    let mut command = command()?;
    let log = OpenOptions::new()
      .create(true)
      .append(true)
      .open(dir.join("frontend.log"))?;
    if parent.is_some() {
      supervision::supervise_frontend(&mut command);
    } else {
      command.stdin(Stdio::null()).stdout(log.try_clone()?);
    }
    command.stderr(log).process_group(0);
    let mut child = command.spawn().context("start frontend process")?;
    let ready = {
      let startup = async {
        tokio::time::timeout(START_TIMEOUT, async {
          loop {
            if let Ok(info) = inspect::<S>(&control_path).await {
              anyhow::ensure!(
                Some(info.frontend_pid) == child.id(),
                "another frontend claimed the configuration during startup"
              );
              return Ok(info);
            }
            if let Some(status) = child.try_wait()? {
              anyhow::bail!("frontend exited ({status}); see {}", dir.join("frontend.log").display());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
          }
        })
        .await
        .context("frontend startup timed out")
        .and_then(|result| result)
      };
      tokio::select! {
        biased;
        _ = shutdown => Err(anyhow::anyhow!("frontend startup interrupted")),
        _ = async { match parent.as_mut() {
          Some(lease) => lease.closed().await,
          None => std::future::pending().await,
        }} => Err(anyhow::anyhow!("frontend supervisor closed during startup")),
        result = startup => result,
      }
    };
    match ready {
      Ok(info) => {
        tracing::info!(frontend_pid = info.frontend_pid, log = %dir.join("frontend.log").display(), "started independent frontend");
        if parent.is_some() {
          owned = Some(child);
        }
        info
      }
      Err(error) => {
        if parent.is_some() {
          // No worker from this launcher has registered yet. The frontend
          // rejects external registration until that first admission succeeds.
          drop(child.stdin.take());
          if tokio::time::timeout(START_TIMEOUT, child.wait()).await.is_err() {
            let _ = child.kill().await;
          }
        } else {
          let _ = child.kill().await;
        }
        return Err(error);
      }
    }
  };
  Ok(Some(FrontendLaunch {
    info,
    owned: owned.zip(parent),
  }))
}
