use super::{
  control_socket_path, read_ab_test_status, read_buffered_message, read_message, write_message, ControlReply,
  ControlRequest, FrontendInfo, START_TIMEOUT,
};
use crate::supervision;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::sync::watch;
use tokn_router::routing::{AbTestStatus, RolloutPolicy};

/// A worker registers once and retains its control connection until retirement.
pub struct WorkerAttachment {
  worker_id: String,
  socket_path: PathBuf,
  control_path: PathBuf,
  expected_frontend: Option<u32>,
  supports_ab_test_status: bool,
  candidate: bool,
  ab_test: bool,
  rollout_policy: Option<RolloutPolicy>,
}

impl<S> FrontendInfo<S> {
  pub fn worker_attachment(
    &self,
    dir: &Path,
    candidate: bool,
    rollout_policy: Option<RolloutPolicy>,
    default_rollout: &RolloutPolicy,
    supervised: bool,
  ) -> Result<WorkerAttachment> {
    let ab_test = rollout_policy.is_some();
    let rollout_policy = if self
      .capabilities
      .iter()
      .any(|capability| capability == "rollout_policy_v1")
    {
      rollout_policy
    } else {
      anyhow::ensure!(
        rollout_policy.as_ref().is_none_or(|policy| policy == default_rollout),
        "this frontend cannot execute custom rollout policies; restart it with a version supporting rollout_policy_v1"
      );
      None
    };
    anyhow::ensure!(
      !ab_test || self.supports_ab_test,
      "this frontend does not support --ab-test; restart it with a gateway version supporting automatic A/B ramps"
    );
    let worker_id = uuid::Uuid::new_v4().simple().to_string();
    let socket_path = dir.join(format!("w-{worker_id}.sock"));
    let expected_frontend = supervised.then_some(self.frontend_pid);
    let supports_ab_test_status = self.supports_ab_test_status;
    Ok(WorkerAttachment {
      worker_id,
      socket_path,
      control_path: control_socket_path(dir),
      expected_frontend,
      supports_ab_test_status,
      candidate,
      ab_test,
      rollout_policy,
    })
  }
}

impl WorkerAttachment {
  pub fn worker_id(&self) -> &str {
    &self.worker_id
  }

  pub fn socket_path(&self) -> &Path {
    &self.socket_path
  }

  pub async fn attach(
    self,
    mut retire_rx: watch::Receiver<bool>,
    set_status: impl Fn(Option<AbTestStatus>),
  ) -> Result<()> {
    let Self {
      worker_id,
      socket_path,
      control_path,
      expected_frontend,
      supports_ab_test_status,
      candidate,
      ab_test,
      rollout_policy,
    } = self;
    tokio::time::timeout(START_TIMEOUT, async {
      loop {
        if *retire_rx.borrow() {
          return Ok::<(), anyhow::Error>(());
        }
        match UnixStream::connect(&socket_path).await {
          Ok(stream) => {
            drop(stream);
            return Ok::<(), anyhow::Error>(());
          }
          Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::time::sleep(Duration::from_millis(10)).await
          }
          Err(error) => return Err(error.into()),
        }
      }
    })
    .await
    .context("worker socket startup timed out")??;
    if *retire_rx.borrow() {
      return Ok(());
    }
    let mut stream = UnixStream::connect(&control_path).await?;
    if let Some(pid) = expected_frontend {
      anyhow::ensure!(
        stream.peer_cred()?.pid().and_then(|peer| u32::try_from(peer).ok()) == Some(pid),
        "frontend was replaced during supervised startup"
      );
    }
    write_message(
      &mut stream,
      &ControlRequest::Register {
        worker_id: worker_id.clone(),
        socket_path,
        candidate,
        ab_test,
        rollout_policy,
      },
    )
    .await?;
    let mut stream = BufReader::new(stream);
    match tokio::time::timeout(START_TIMEOUT, read_message(&mut stream)).await?? {
      ControlReply::Registered => {}
      ControlReply::Error { message } => anyhow::bail!(message),
      _ => anyhow::bail!("invalid frontend registration response"),
    }
    if expected_frontend.is_some() {
      supervision::notify_ready()?;
    }
    tracing::info!(%worker_id, "worker started; frontend owns traffic assignment");
    let mut retirement_sent = false;
    let mut reply_buffer = Vec::new();
    let mut status_tick = tokio::time::interval(Duration::from_secs(5));
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
      if !retirement_sent && *retire_rx.borrow() {
        write_message(stream.get_mut(), &ControlRequest::Retire).await?;
        retirement_sent = true;
      }
      let status_update = async {
        status_tick.tick().await;
        read_ab_test_status(&control_path).await
      };
      let reply = tokio::select! {
        status = status_update, if supports_ab_test_status => {
          match status {
            Ok(status) => set_status(status),
            Err(error) => {
              set_status(None);
              tracing::debug!(%error, "frontend A/B banner status unavailable");
            }
          }
          continue;
        }
        reply = read_buffered_message(&mut stream, &mut reply_buffer) => reply,
        result = retire_rx.changed(), if !retirement_sent => { result?; continue; }
      };
      match reply {
        Ok(ControlReply::Retired) => {
          tracing::info!(%worker_id, "worker drained; exiting");
          return Ok(());
        }
        Ok(ControlReply::Stopping) => return Ok(()),
        Ok(_) => anyhow::bail!("unexpected frontend lifecycle message"),
        Err(error) => {
          tracing::warn!(%error, "frontend disconnected; stopping worker");
          return Ok(());
        }
      }
    }
  }
}
