//! Stable listener discovery, worker promotion, and process ownership.
use super::serve::{self, ServeArgs};
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::OpenOptions;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokn_core::util::shutdown::ShutdownSignal;
use tokn_router::ipc::{WorkerEndpoint, WorkerPool, PROTOCOL_VERSION};

const START_TIMEOUT: Duration = Duration::from_secs(15);
const MESSAGE_LIMIT: u64 = 64 * 1024;

#[derive(Subcommand, Debug)]
pub enum WorkerCmd {
  /// Start a worker, take all new requests, and drain the previous worker.
  Start(WorkerArgs),
}

#[derive(Args, Debug, Default)]
pub struct WorkerArgs {
  /// Discover a frontend started with a different configuration file.
  #[arg(long)]
  frontend_config: Option<PathBuf>,
  /// Register without promoting, for an A/B experiment using /admin/workers.
  #[arg(long)]
  candidate: bool,
  /// Skip outbound proxy for this worker.
  #[arg(long)]
  no_proxy: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FrontendInfo {
  protocol_version: u32,
  frontend_pid: u32,
  args: ServeArgs,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum ControlRequest {
  Inspect,
  Register {
    worker_id: String,
    socket_path: PathBuf,
    candidate: bool,
  },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum ControlReply {
  Info { info: FrontendInfo },
  Registered,
  Retired,
  Stopping,
  Error { message: String },
}

/// Use a short path for macOS Unix sockets. Configuration identity is stable
/// across binary versions, and explicit config paths keep test homes isolated.
fn runtime_dir(config: Option<&Path>) -> Result<PathBuf> {
  let loaded = tokn_config::load_config(config)?;
  let config_path = std::fs::canonicalize(loaded.path())?;
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

async fn write_message<T: Serialize>(stream: &mut UnixStream, message: &T) -> Result<()> {
  let mut bytes = serde_json::to_vec(message)?;
  anyhow::ensure!(bytes.len() < MESSAGE_LIMIT as usize, "control message exceeds limit");
  bytes.push(b'\n');
  stream.write_all(&bytes).await?;
  Ok(())
}

async fn read_message<T: DeserializeOwned>(stream: &mut BufReader<UnixStream>) -> Result<T> {
  let mut bytes = Vec::new();
  let read = stream.take(MESSAGE_LIMIT + 1).read_until(b'\n', &mut bytes).await?;
  anyhow::ensure!(read > 0, "frontend control connection closed");
  anyhow::ensure!(
    read as u64 <= MESSAGE_LIMIT && bytes.last() == Some(&b'\n'),
    "invalid control message length"
  );
  Ok(serde_json::from_slice(&bytes)?)
}

async fn inspect(path: &Path) -> Result<FrontendInfo> {
  tokio::time::timeout(Duration::from_secs(2), async {
    let mut stream = UnixStream::connect(path).await?;
    write_message(&mut stream, &ControlRequest::Inspect).await?;
    match read_message(&mut BufReader::new(stream)).await? {
      ControlReply::Info { info } => {
        anyhow::ensure!(
          info.protocol_version == PROTOCOL_VERSION,
          "incompatible frontend control protocol"
        );
        Ok(info)
      }
      _ => anyhow::bail!("frontend did not return discovery information"),
    }
  })
  .await
  .context("frontend discovery timed out")?
}

struct OwnedSocket {
  path: PathBuf,
  device: u64,
  inode: u64,
}
impl Drop for OwnedSocket {
  fn drop(&mut self) {
    if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
      if metadata.dev() == self.device && metadata.ino() == self.inode {
        let _ = std::fs::remove_file(&self.path);
      }
    }
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

async fn connection(
  stream: UnixStream,
  info: Arc<FrontendInfo>,
  pool: Arc<WorkerPool>,
  mut stop: watch::Receiver<bool>,
) -> Result<()> {
  let mut stream = BufReader::new(stream);
  let request = tokio::time::timeout(Duration::from_secs(5), read_message(&mut stream)).await??;
  match request {
    ControlRequest::Inspect => write_message(stream.get_mut(), &ControlReply::Info { info: (*info).clone() }).await,
    ControlRequest::Register {
      worker_id,
      socket_path,
      candidate,
    } => {
      let endpoint = WorkerEndpoint {
        worker_id: worker_id.clone(),
        socket_path,
        weight: 1,
      };
      let result = if candidate {
        pool.register_candidate(endpoint).await
      } else {
        pool.register(endpoint).await
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
      let reply = tokio::select! {
        result = pool.wait_retired(&worker_id) => { result?; ControlReply::Retired }
        _ = stop.changed() => ControlReply::Stopping,
        _ = read_message::<ControlRequest>(&mut stream) => return Ok(()),
      };
      write_message(stream.get_mut(), &reply).await
    }
  }
}

async fn serve_control(
  listener: UnixListener,
  info: FrontendInfo,
  pool: Arc<WorkerPool>,
  mut stop: watch::Receiver<bool>,
) -> Result<()> {
  let info = Arc::new(info);
  let mut tasks = JoinSet::new();
  loop {
    tokio::select! {
      biased;
      _ = stop.changed() => break,
      result = tasks.join_next(), if !tasks.is_empty() => {
        if let Some(Err(error)) = result { tracing::warn!(%error, "frontend control task failed"); }
      }
      accepted = listener.accept() => {
        let (stream, _) = accepted?;
        let info = info.clone();
        let pool = pool.clone();
        let stop = stop.clone();
        tasks.spawn(async move {
          if let Err(error) = connection(stream, info, pool, stop).await {
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

pub async fn frontend(config: Option<PathBuf>, args: ServeArgs) -> Result<()> {
  anyhow::ensure!(args.worker_socket.is_none(), "frontend does not accept --worker-socket");
  let dir = runtime_dir(config.as_deref())?;
  let path = dir.join("frontend.sock");
  let (_, loaded) = serve::load_runtime(config, args.clone())?;
  let (plan, service) = loaded.compiled.into_parts();
  let pool = Arc::new(WorkerPool::empty(&plan)?);
  let needs_access = plan
    .listeners()
    .values()
    .any(|listener| listener.client_auth() == tokn_policy::ClientAuthPlan::LocalKeys);
  let access = crate::server_runtime::load_access_store(needs_access)?;
  let bind_override = if loaded.args.host.is_some() || loaded.args.port.is_some() {
    anyhow::ensure!(
      plan.listeners().len() == 1,
      "--host and --port can only override a config with exactly one listener"
    );
    let listener = plan.listeners().values().next().context("missing listener")?;
    Some(serve::listener_bind(
      listener.bind(),
      listener.client_auth(),
      &loaded.args,
    )?)
  } else {
    None
  };
  let mut frontend = tokn_router::frontend::Frontend::new(plan, service, access, pool.clone())?;
  if let Some(bind) = bind_override {
    frontend = frontend.with_bind_override(bind)?;
  }
  let frontend = frontend.with_routing_control(pool.clone());
  let frontend = frontend.bind().await?;
  let listener = UnixListener::bind(&path).with_context(|| {
    format!(
      "bind frontend control socket {}; another frontend may already be running",
      path.display()
    )
  })?;
  let metadata = std::fs::symlink_metadata(&path)?;
  let _socket = OwnedSocket {
    path: path.clone(),
    device: metadata.dev(),
    inode: metadata.ino(),
  };
  std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
  let shutdown = ShutdownSignal::new()?;
  let (stop_tx, mut stop_rx) = watch::channel(false);
  let control = serve_control(
    listener,
    FrontendInfo {
      protocol_version: PROTOCOL_VERSION,
      frontend_pid: std::process::id(),
      args,
    },
    pool,
    stop_rx.clone(),
  );
  let public = frontend.serve(async move {
    let _ = stop_rx.changed().await;
  });
  tokio::pin!(control, public);
  tracing::info!(control_socket = %path.display(), "frontend starting; waiting for workers");
  let result = tokio::select! {
    result = &mut public => { let _ = stop_tx.send(true); result.and(control.await) }
    result = &mut control => { let _ = stop_tx.send(true); result.and(public.await) }
    result = shutdown.wait() => { let _ = stop_tx.send(true); result?; public.await.and(control.await) }
  };
  result
}

pub async fn worker(config: Option<PathBuf>, command: WorkerCmd) -> Result<()> {
  let WorkerCmd::Start(args) = command;
  let discovery = args.frontend_config.as_deref().or(config.as_deref());
  let dir = runtime_dir(discovery)?;
  let info = inspect(&dir.join("frontend.sock")).await.context(
    "no compatible frontend; run `tokn-gateway frontend --with-proxy` or `tokn-gateway serve --with-proxy` first",
  )?;
  start_worker(config, dir, info, args.candidate, args.no_proxy).await
}

async fn start_worker(
  config: Option<PathBuf>,
  dir: PathBuf,
  info: FrontendInfo,
  candidate: bool,
  no_proxy: bool,
) -> Result<()> {
  let worker_id = uuid::Uuid::new_v4().simple().to_string();
  let socket_path = dir.join(format!("w-{worker_id}.sock"));
  let mut args = info.args;
  args.worker_socket = Some(socket_path.clone());
  args.no_proxy |= no_proxy;
  let (source, mut loaded) = serve::load_runtime(config, args)?;
  // Public binding consent is consumed during projection; this process only binds IPC.
  loaded.args.insecure_allow_remote = false;
  loaded.args.host = None;
  loaded.args.port = None;
  let shutdown = ShutdownSignal::new()?;
  let (stop_tx, mut stop_rx) = watch::channel(false);
  let server = serve::run_loaded(source, loaded, async move {
    let _ = stop_rx.changed().await;
    Ok(())
  });
  tokio::pin!(server);
  // Poll only during startup. A long-lived control connection owns registration
  // and delivers retirement without polling or observing idle CONNECT tunnels.
  let result = {
    let attach = async {
      tokio::time::timeout(START_TIMEOUT, async {
        loop {
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
      let mut stream = UnixStream::connect(dir.join("frontend.sock")).await?;
      write_message(
        &mut stream,
        &ControlRequest::Register {
          worker_id: worker_id.clone(),
          socket_path,
          candidate,
        },
      )
      .await?;
      let mut stream = BufReader::new(stream);
      match tokio::time::timeout(START_TIMEOUT, read_message(&mut stream)).await?? {
        ControlReply::Registered => {}
        ControlReply::Error { message } => anyhow::bail!(message),
        _ => anyhow::bail!("invalid frontend registration response"),
      }
      tracing::info!(%worker_id, "worker started; frontend owns traffic assignment");
      match read_message(&mut stream).await {
        Ok(ControlReply::Retired) => {
          tracing::info!(%worker_id, "worker drained; exiting");
          Ok(())
        }
        Ok(ControlReply::Stopping) => Ok(()),
        Ok(_) => anyhow::bail!("unexpected frontend lifecycle message"),
        Err(error) => {
          tracing::warn!(%error, "frontend disconnected; stopping worker");
          Ok(())
        }
      }
    };
    tokio::pin!(attach);
    tokio::select! {
      result = &mut server => return result,
      result = &mut attach => result,
      result = shutdown.wait() => result.map_err(Into::into),
    }
  };
  let _ = stop_tx.send(true);
  result.and(server.await)
}

pub async fn serve(config: Option<PathBuf>, args: ServeArgs) -> Result<()> {
  let dir = runtime_dir(config.as_deref())?;
  let control_path = dir.join("frontend.sock");
  let info = if control_path.exists() {
    inspect(&control_path).await.context(
      "frontend socket exists but is unavailable; stop its owner or remove the stale socket before restarting",
    )?
  } else {
    // Validate all configuration before spawning an independently owned process.
    let _ = serve::load_runtime(config.clone(), args.clone())?;
    let mut command = Command::new(std::env::current_exe()?);
    let loaded = tokn_config::load_config(config.as_deref())?;
    command
      .arg("--config")
      .arg(std::fs::canonicalize(loaded.path())?)
      .arg("frontend");
    if args.with_proxy {
      command.arg("--with-proxy");
    }
    if let Some(host) = &args.host {
      command.arg("--host").arg(host);
    }
    if let Some(port) = args.port {
      command.arg("--port").arg(port.to_string());
    }
    if let Some(mode) = args.proxy_route_mode {
      use clap::ValueEnum;
      command
        .arg("--proxy-route-mode")
        .arg(mode.to_possible_value().context("invalid route mode")?.get_name());
    }
    if args.insecure_allow_remote {
      command.arg("--insecure-allow-remote");
    }
    if args.no_proxy {
      command.arg("--no-proxy");
    }
    let log = OpenOptions::new()
      .create(true)
      .append(true)
      .open(dir.join("frontend.log"))?;
    command
      .stdin(Stdio::null())
      .stdout(log.try_clone()?)
      .stderr(log)
      .process_group(0);
    let mut child = command.spawn().context("start frontend process")?;
    let ready = tokio::time::timeout(START_TIMEOUT, async {
      loop {
        if let Ok(info) = inspect(&control_path).await {
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
    .and_then(|result| result);
    match ready {
      Ok(info) => {
        tracing::info!(frontend_pid = info.frontend_pid, log = %dir.join("frontend.log").display(), "started independent frontend");
        info
      }
      Err(error) => {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
      }
    }
  };
  anyhow::ensure!(
    !args.with_proxy || info.args.with_proxy,
    "the existing frontend was started without --with-proxy; restart the frontend to enable its legacy proxy listener"
  );
  tracing::info!(frontend_pid = info.frontend_pid, "using frontend");
  start_worker(config, dir, info, false, args.no_proxy).await
}
