use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::{
  collections::VecDeque,
  net::SocketAddr,
  path::PathBuf,
  process::{ExitStatus, Stdio},
  time::Duration,
};
use tauri::{Emitter, Manager};
use tokio::{
  io::AsyncReadExt,
  process::{Child, Command},
  sync::Mutex,
  task::JoinHandle,
};

#[derive(Default)]
pub struct Gateway {
  child: Mutex<Option<ManagedGateway>>,
  last_error: Mutex<Option<String>>,
}

struct ManagedGateway {
  process: Child,
  stderr_tail: JoinHandle<Vec<u8>>,
}

impl ManagedGateway {
  fn new(mut process: Child) -> Result<Self> {
    let mut stderr = process.stderr.take().context("Missing bundled gateway stderr")?;
    let stderr_tail = tokio::spawn(async move {
      let mut buffer = [0u8; 1024];
      let mut tail = VecDeque::new();
      while let Ok(count) = stderr.read(&mut buffer).await {
        if count == 0 {
          break;
        }
        tail.extend(&buffer[..count]);
        tail.drain(..tail.len().saturating_sub(8192));
      }
      tail.into()
    });
    Ok(Self { process, stderr_tail })
  }

  async fn exit_error(&mut self, exit: ExitStatus) -> String {
    let bytes = (&mut self.stderr_tail).await.unwrap_or_default();
    format!("Gateway exited ({exit}). {}", String::from_utf8_lossy(&bytes))
  }
}

#[derive(Clone, Serialize)]
pub struct GatewayStatus {
  pub state: String,
  pub ownership: String,
  pub address: String,
  pub pid: Option<u32>,
  pub last_error: Option<String>,
}

fn client() -> Result<reqwest::Client> {
  Ok(
    reqwest::Client::builder()
      .no_proxy()
      .redirect(reqwest::redirect::Policy::none())
      .timeout(Duration::from_secs(2))
      .build()?,
  )
}

async fn healthy(address: SocketAddr) -> bool {
  let Ok(client) = client() else {
    return false;
  };
  match client.get(format!("http://{address}/healthz")).send().await {
    Ok(response) if response.status().is_success() => response.text().await.is_ok_and(|body| body == "ok"),
    _ => false,
  }
}

impl Gateway {
  pub async fn status(&self, address: SocketAddr) -> Result<GatewayStatus> {
    let mut child = self.child.lock().await;
    if let Some(managed) = child.as_mut() {
      if let Some(exit) = managed.process.try_wait()? {
        *self.last_error.lock().await = if exit.success() {
          None
        } else {
          Some(managed.exit_error(exit).await)
        };
        *child = None;
      }
    }
    let running = healthy(address).await;
    Ok(GatewayStatus {
      state: if running {
        "running"
      } else if child.is_some() {
        "starting"
      } else {
        "stopped"
      }
      .into(),
      ownership: if child.is_some() {
        "managed"
      } else if running {
        "external"
      } else {
        "none"
      }
      .into(),
      address: address.to_string(),
      pid: child.as_ref().and_then(|managed| managed.process.id()),
      last_error: self.last_error.lock().await.clone(),
    })
  }

  pub async fn start(&self, mut command: Command, config_path: PathBuf, address: SocketAddr) -> Result<()> {
    let mut child = self.child.lock().await;
    if let Some(managed) = child.as_mut() {
      if managed.process.try_wait()?.is_none() {
        bail!("An app-managed gateway is already running");
      }
      *child = None;
    }
    // Refuse any occupied port, including an unrelated service.
    let guard = tokio::net::TcpListener::bind(address)
      .await
      .context("Listener is already in use; the desktop app will not replace it")?;
    drop(guard);
    tokn_gateway::supervision::supervise(&mut command);
    let mut process = command
      .arg("--config")
      .arg(config_path)
      .arg("serve")
      .stderr(Stdio::piped())
      .kill_on_drop(true)
      .spawn()
      .context("Starting the bundled gateway")?;
    let mut stdout = process.stdout.take().context("Missing gateway readiness pipe")?;
    *self.last_error.lock().await = None;
    let managed = child.insert(ManagedGateway::new(process)?);
    let ready = async {
      tokn_gateway::supervision::wait_ready(&mut stdout)
        .await
        .context("Waiting for the bundled worker to register")?;
      anyhow::ensure!(healthy(address).await, "The bundled gateway failed its health check");
      Ok::<_, anyhow::Error>(())
    };
    if let Err(error) = tokio::time::timeout(Duration::from_secs(20), ready)
      .await
      .context("Gateway startup timed out")
      .and_then(|result| result)
    {
      let exit = tokn_gateway::supervision::stop(&mut managed.process).await?;
      let message = format!("{error:#}. {}", managed.exit_error(exit).await);
      *child = None;
      *self.last_error.lock().await = Some(message.clone());
      bail!(message);
    }
    Ok(())
  }

  pub async fn stop(&self) -> Result<()> {
    let mut child = self.child.lock().await;
    if let Some(managed) = child.as_mut() {
      tokn_gateway::supervision::stop(&mut managed.process).await?;
      *child = None;
    }
    Ok(())
  }
}

pub fn executable() -> Result<PathBuf> {
  let path = std::env::current_exe()?
    .parent()
    .context("Missing application directory")?
    .join("tokn-gateway");
  if !path.is_file() {
    bail!("Bundled gateway missing. Run pnpm prepare:gateway before building the desktop app.");
  }
  Ok(path)
}

pub async fn reload(address: SocketAddr) -> Result<String> {
  let response = client()?
    .post(format!("http://{address}/admin/config/reload"))
    .header("x-tokn-admin", "reload")
    .timeout(Duration::from_secs(30))
    .send()
    .await?;
  let status = response.status();
  if status == reqwest::StatusCode::NOT_FOUND {
    bail!(
      "Saved on disk. Local reload is unavailable on this gateway (404). Use its process manager to apply the changes."
    );
  }
  let body = response.text().await?;
  if status != reqwest::StatusCode::OK {
    bail!("Saved on disk, but gateway reload returned {status}: {body}");
  }
  confirmed_reload(&body)
}

fn confirmed_reload(body: &str) -> Result<String> {
  #[derive(serde::Deserialize)]
  struct ReloadReport {
    status: String,
    generation: std::num::NonZeroU64,
  }

  let report = serde_json::from_str::<ReloadReport>(body)
    .ok()
    .filter(|report| report.status == "reloaded")
    .context("Saved on disk. The gateway did not confirm an active configuration generation.")?;
  Ok(format!(
    "Gateway reloaded. Configuration generation {} is active.",
    report.generation
  ))
}

pub fn monitor(app: tauri::AppHandle) {
  tauri::async_runtime::spawn(async move {
    let mut previous = String::new();
    loop {
      // Config loading includes disk I/O and compilation; keep it off the
      // runtime worker that drives process supervision.
      if let Ok(Ok(address)) = tokio::task::spawn_blocking(crate::config::address).await {
        if let Ok(status) = app.state::<Gateway>().status(address).await {
          if let Ok(encoded) = serde_json::to_string(&status) {
            if encoded != previous {
              previous = encoded;
              let _ = app.emit("gateway-status", status);
            }
          }
        }
      }
      tokio::time::sleep(Duration::from_secs(2)).await;
    }
  });
}

#[cfg(test)]
mod tests;
