//! In-process listener serving and request execution for split-mode workers.

use crate::events::{EventObservers, RuntimeEvents};
use anyhow::{Context, Result};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use std::future::Future;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;
use tokn_access::AccessStore;
use tokn_config::v2::{CompiledConfig, ServiceSettings};
use tokn_core::account::AccountConfig;
use tokn_core::event::EventBus;
use tokn_policy::ClientAuthPlan;
use tokn_router::v2::{self, AdminReloader, LiveRuntime, ReloadError, RuntimeStates};

/// Compiled configuration and credentials for one request-runtime generation.
pub struct RuntimeConfig {
  pub compiled: CompiledConfig,
  pub accounts: Vec<AccountConfig>,
}

/// Optional overrides for a config with exactly one public listener.
#[derive(Default)]
pub struct BindOptions {
  pub host: Option<String>,
  pub port: Option<u16>,
  pub insecure_allow_remote: bool,
}

pub enum RuntimeMode {
  InProcess(BindOptions),
  #[cfg(unix)]
  Worker {
    socket_path: PathBuf,
    frontend_maintenance_dir: Option<PathBuf>,
  },
}

impl RuntimeMode {
  fn is_worker(&self) -> bool {
    match self {
      Self::InProcess(_) => false,
      #[cfg(unix)]
      Self::Worker { .. } => true,
    }
  }
}

/// Coordinated reloads need a revision from the same input that was compiled.
#[derive(Clone, Copy)]
pub enum ReloadRequest {
  InProcess,
  #[cfg(unix)]
  Coordinated,
}

pub struct ReloadCandidate {
  pub config: RuntimeConfig,
  pub revision: Option<String>,
}

type CandidateLoader = dyn Fn(ReloadRequest) -> Result<ReloadCandidate, ReloadError> + Send + Sync;

/// Serve a prepared config and drain its connections before flushing persistence.
pub async fn serve(
  config: RuntimeConfig,
  mode: RuntimeMode,
  load_candidate: impl Fn(ReloadRequest) -> Result<ReloadCandidate, ReloadError> + Send + Sync + 'static,
  observers: impl FnOnce() -> EventObservers + Send,
  shutdown: impl Future<Output = Result<()>> + Send,
) -> Result<()> {
  let needs_access = config
    .compiled
    .gateway()
    .listeners()
    .values()
    .any(|listener| listener.client_auth() == ClientAuthPlan::LocalKeys);
  let access = load_access_store(needs_access)?;
  let frontend_maintenance_dir = match &mode {
    RuntimeMode::InProcess(_) => None,
    #[cfg(unix)]
    RuntimeMode::Worker {
      frontend_maintenance_dir,
      ..
    } => frontend_maintenance_dir.as_deref(),
  };
  let events = RuntimeEvents::start(
    config.compiled.service().persistence(),
    frontend_maintenance_dir,
    observers,
  )?;
  let result = serve_runtime(
    config,
    mode,
    Arc::new(load_candidate),
    access,
    events.events(),
    shutdown,
  )
  .await;
  let cleanup = events.finish().await;
  result.and(cleanup)
}

async fn serve_runtime(
  config: RuntimeConfig,
  mode: RuntimeMode,
  load_candidate: Arc<CandidateLoader>,
  access: Arc<AccessStore>,
  events: Arc<EventBus>,
  shutdown: impl Future<Output = Result<()>> + Send,
) -> Result<()> {
  let service = config.compiled.service().clone();
  let worker = mode.is_worker();
  let (states, accounts) = build_states(config, worker, access.clone(), events.clone())?;
  let live = Arc::new(LiveRuntime::new(states, accounts));
  let model_refresh = live.start_model_refresh(service.models());
  let reload = RuntimeReload {
    load_candidate,
    initial_service: service,
    worker,
    access,
    events,
  };
  let result = async {
    match mode {
      RuntimeMode::InProcess(bind) => {
        reload.install_admin_reloader(&live)?;
        serve_listeners(&live, bind, shutdown).await
      }
      #[cfg(unix)]
      RuntimeMode::Worker { socket_path, .. } => serve_worker(live, socket_path, reload, shutdown).await,
    }
  }
  .await;
  model_refresh.shutdown().await;
  result
}

fn build_states(
  config: RuntimeConfig,
  worker: bool,
  access: Arc<AccessStore>,
  events: Arc<EventBus>,
) -> Result<(RuntimeStates, usize)> {
  let (plan, service) = config.compiled.into_parts();
  let build = if worker {
    v2::build_worker_runtime_states
  } else {
    v2::build_runtime_states_with_service
  };
  let states = build(plan, service, &config.accounts, access, events)?;
  Ok((states, config.accounts.len()))
}

#[derive(Clone)]
struct RuntimeReload {
  load_candidate: Arc<CandidateLoader>,
  initial_service: ServiceSettings,
  worker: bool,
  access: Arc<AccessStore>,
  events: Arc<EventBus>,
}

impl RuntimeReload {
  async fn prepare(
    self,
    live: &LiveRuntime,
    request: ReloadRequest,
    revision: Option<String>,
  ) -> Result<(RuntimeStates, usize), ReloadError> {
    let candidate = tokio::task::spawn_blocking(move || (self.load_candidate)(request))
      .await
      .map_err(invalid_reload)??;
    if revision.is_some() && revision != candidate.revision {
      return Err(ReloadError::Conflict(
        "worker and frontend have different configuration sources or inputs".into(),
      ));
    }
    v2::ensure_service_reload_compatible(&self.initial_service, candidate.config.compiled.service())?;
    live.validate_reload(candidate.config.compiled.gateway())?;
    build_states(candidate.config, self.worker, self.access, self.events).map_err(invalid_reload)
  }

  fn install_admin_reloader(self, live: &Arc<LiveRuntime>) -> Result<()> {
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let runtime = Arc::downgrade(live);
    live
      .set_admin_reloader(AdminReloader::new(move || {
        let candidate = self.clone();
        let runtime = runtime.clone();
        let lock = lock.clone();
        async move {
          let _guard = lock.lock().await;
          // The runtime stores this callback. A weak reference lets the
          // runtime and its state drop once the listener tasks have drained.
          let live = runtime
            .upgrade()
            .ok_or_else(|| ReloadError::Conflict("gateway is shutting down".into()))?;
          let (states, accounts) = candidate.prepare(&live, ReloadRequest::InProcess, None).await?;
          let report = live.replace(states, accounts)?;
          tracing::info!(
            generation = report.generation,
            accounts = report.accounts,
            "v2 runtime configuration reloaded"
          );
          Ok(report)
        }
      }))
      .map_err(|_| anyhow::anyhow!("admin config reloader is already installed"))
  }
}

fn invalid_reload(error: impl std::fmt::Display) -> ReloadError {
  ReloadError::Invalid(format!("{error:#}"))
}

#[cfg(unix)]
async fn serve_worker(
  live: Arc<LiveRuntime>,
  socket_path: PathBuf,
  reload: RuntimeReload,
  shutdown: impl Future<Output = Result<()>> + Send,
) -> Result<()> {
  let info = live.worker_info(tokn_core::util::version::full());
  let runtime = live.clone();
  let prepare = tokn_router::ipc::WorkerReload::new(move |revision| {
    let reload = reload.clone();
    let runtime = runtime.clone();
    async move {
      reload
        .prepare(&runtime, ReloadRequest::Coordinated, Some(revision))
        .await
    }
  });
  let (stop_tx, stop_rx) = watch::channel(false);
  let server: BoxFuture<'static, _> = Box::pin(tokn_router::ipc::serve_reloadable_worker(
    socket_path,
    live,
    info,
    prepare,
    wait_for_shutdown(stop_rx),
  ));
  supervise_listeners(vec![server], stop_tx, shutdown).await
}

async fn serve_listeners(
  live: &LiveRuntime,
  bind: BindOptions,
  shutdown: impl Future<Output = Result<()>> + Send,
) -> Result<()> {
  let states = live.llm_api_listeners();
  let proxy_states = live.forward_proxy_listeners();
  let listener_count = states.len() + proxy_states.len();
  if listener_count != 1 && (bind.host.is_some() || bind.port.is_some()) {
    anyhow::bail!("--host and --port can only override a v2 config with exactly one listener");
  }

  let mut servers = Vec::<BoxFuture<'static, Result<()>>>::with_capacity(listener_count);
  let (shutdown_tx, shutdown_rx) = watch::channel(false);
  for state in states {
    let addr = listener_bind(state.bind(), state.client_auth(), &bind)?;
    tracing::info!(listener = %state.listener_id(), %addr, "tokn-router v2 listener starting");
    let shutdown = shutdown_rx.clone();
    servers.push(Box::pin(v2::serve_live_http(state, addr, wait_for_shutdown(shutdown))));
  }
  for state in proxy_states {
    let addr = listener_bind(state.bind(), state.client_auth(), &bind)?;
    tracing::info!(listener = %state.listener_id(), %addr, "tokn-router v2 forward proxy starting");
    let shutdown = shutdown_rx.clone();
    servers.push(Box::pin(async move {
      v2::serve_live_forward_proxy(state, addr, wait_for_shutdown(shutdown)).await
    }));
  }
  supervise_listeners(servers, shutdown_tx, shutdown).await
}

pub fn load_access_store(enabled: bool) -> Result<Arc<AccessStore>> {
  if enabled {
    Ok(Arc::new(AccessStore::open_default()?))
  } else {
    Ok(Arc::new(AccessStore::disabled()))
  }
}

pub fn resolve_bind_addr(host: &str, port: u16, insecure_allow_remote: bool) -> Result<SocketAddr> {
  ensure_bind_host(host, insecure_allow_remote)?;
  Ok(format!("{host}:{port}").parse()?)
}

pub fn ensure_bind_host(host: &str, insecure_allow_remote: bool) -> Result<()> {
  let loopback = matches!(host, "127.0.0.1" | "::1" | "localhost")
    || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback());
  if !insecure_allow_remote && !loopback {
    anyhow::bail!(
      "refusing to bind to non-loopback host '{host}' without --insecure-allow-remote; API-key auth does not cover tunnels, passthrough traffic, or helper routes"
    );
  }
  Ok(())
}

pub(crate) fn listener_bind(
  configured: SocketAddr,
  client_auth: ClientAuthPlan,
  bind: &BindOptions,
) -> Result<SocketAddr> {
  if bind.host.is_none() && bind.port.is_none() {
    return Ok(configured);
  }
  let host = bind.host.clone().unwrap_or_else(|| configured.ip().to_string());
  let port = bind.port.unwrap_or(configured.port());
  let addr = resolve_bind_addr(&host, port, bind.insecure_allow_remote)
    .with_context(|| format!("parse v2 bind address {host}:{port}"))?;
  // Compilation validated the authored address. Evaluate overrides separately.
  if !addr.ip().is_loopback() && client_auth == ClientAuthPlan::None {
    anyhow::bail!("non-loopback listener overrides require client_auth = \"local_keys\"");
  }
  Ok(addr)
}

async fn supervise_listeners(
  servers: Vec<BoxFuture<'static, Result<()>>>,
  shutdown_tx: watch::Sender<bool>,
  shutdown: impl Future<Output = Result<()>>,
) -> Result<()> {
  let mut servers = servers.into_iter().collect::<FuturesUnordered<_>>();
  let mut stopping = false;
  let mut outcome = Ok(());
  tokio::pin!(shutdown);
  while !servers.is_empty() {
    let result = tokio::select! {
      biased;
      result = &mut shutdown, if !stopping => result,
      Some(result) = servers.next() => result,
    };
    if let Err(error) = result {
      tracing::error!(%error, "listener runtime stopping after an error");
      if outcome.is_ok() {
        outcome = Err(error);
      }
    }
    if !stopping {
      stopping = true;
      tracing::info!("stopping listeners; draining active connections");
      let _ = shutdown_tx.send(true);
    }
  }
  outcome
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
  if *shutdown.borrow() {
    return;
  }
  let _ = shutdown.changed().await;
}

#[cfg(test)]
mod tests;
