//! Long-lived frontend coordinator for public listeners, worker control and live configuration.
use crate::control::{control_socket_path, ControlHost};
use crate::runtime::{listener_bind, load_access_store, BindOptions};
use crate::supervision::{FrontendOwner, ParentLease};
use anyhow::{Context, Result};
use serde::Serialize;
use std::future::Future;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::sync::watch;
use tokn_config::v2::{CompiledConfig, PersistencePlan};
use tokn_core::event::{EventBus, EventHandler};
use tokn_maintenance::ArchiveEventHandler;
use tokn_router::frontend::BoundFrontend;
use tokn_router::ipc::WorkerPool;
use tokn_router::routing::RolloutPolicy;
use tokn_router::v2::ReloadError;

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

/// Owns public listeners and the private control socket in one process.
pub struct Gateway {
  frontend: BoundFrontend,
  pool: Arc<WorkerPool>,
  listener: UnixListener,
  socket: OwnedSocket,
  owner: Option<Arc<FrontendOwner>>,
  lease: Option<ParentLease>,
  persistence: PersistencePlan,
  events: Arc<EventBus>,
  handlers: Vec<Box<dyn EventHandler>>,
}

impl Gateway {
  /// Configuration and credentials are resolved by the caller; the gateway coordinates replacement with the worker.
  pub async fn bind(
    directory: &Path,
    compiled: CompiledConfig,
    bind: BindOptions,
    candidate: impl Fn() -> Result<(CompiledConfig, String), ReloadError> + Send + Sync + 'static,
  ) -> Result<Self> {
    let (plan, service) = compiled.into_parts();
    let needs_access = plan
      .listeners()
      .values()
      .any(|listener| listener.client_auth() == tokn_policy::ClientAuthPlan::LocalKeys);
    let access = load_access_store(needs_access)?;
    let bind_override = if bind.host.is_some() || bind.port.is_some() {
      anyhow::ensure!(
        plan.listeners().len() == 1,
        "--host and --port can only override a config with exactly one listener"
      );
      let listener = plan.listeners().values().next().context("missing listener")?;
      Some(listener_bind(listener.bind(), listener.client_auth(), &bind)?)
    } else {
      None
    };
    let candidate = Arc::new(candidate);
    let path = control_socket_path(directory);
    let lease = ParentLease::from_stdin()?;
    let owner = lease
      .as_ref()
      .map(|_| FrontendOwner::for_launch().map(Arc::new))
      .transpose()?;
    let persistence = service.persistence().clone();
    let handlers = crate::ingress::handlers(&persistence)?;
    let events = Arc::new(EventBus::new(persistence.write_queue_capacity().max(256)));
    let pool = Arc::new(WorkerPool::empty(&plan)?);
    let mut frontend =
      tokn_router::frontend::Frontend::new_with_events(plan, service, access, pool.dispatcher(), events.clone())?;
    if let Some(bind) = bind_override {
      frontend = frontend.with_bind_override(bind)?;
    }
    let handle = frontend.reload_handle();
    let reload_pool = pool.clone();
    let reloader = tokn_router::v2::AdminReloader::new(move || {
      let candidate = candidate.clone();
      let handle = handle.clone();
      let pool = reload_pool.clone();
      async move {
        let (compiled, revision) = tokio::task::spawn_blocking(move || candidate())
          .await
          .map_err(|error| tokn_router::v2::ReloadError::Invalid(error.to_string()))??;
        let (plan, service) = compiled.into_parts();
        pool.reload(&handle, plan, &service, &revision).await
      }
    });
    let frontend = frontend
      .with_routing_control(pool.clone())
      .with_admin_reloader(reloader);
    let frontend = frontend.bind().await?;
    let listener = UnixListener::bind(&path).with_context(|| {
      format!(
        "bind frontend control socket {}; another frontend may already be running",
        path.display()
      )
    })?;
    let metadata = std::fs::symlink_metadata(&path)?;
    let socket = OwnedSocket {
      path: path.clone(),
      device: metadata.dev(),
      inode: metadata.ino(),
    };
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    Ok(Self {
      frontend,
      pool,
      listener,
      socket,
      owner,
      lease,
      persistence,
      events,
      handlers,
    })
  }

  /// Start retention only after binding succeeds. Advertise the actual maintained directory to workers.
  pub async fn serve(
    mut self,
    settings: impl Serialize,
    default_rollout: RolloutPolicy,
    maintenance_handlers: Vec<Box<dyn ArchiveEventHandler>>,
    shutdown: impl Future<Output = std::io::Result<()>> + Send,
  ) -> Result<()> {
    let (directory, maintenance) =
      tokn_maintenance::start_v2_maintenance(&self.persistence, maintenance_handlers)?.unzip();
    let events = self.events.clone();
    let _event_thread = tokn_core::event::spawn_event_loop(events.subscribe(), std::mem::take(&mut self.handlers));
    let result = self.serve_bound(settings, default_rollout, directory, shutdown).await;
    let cleanup = crate::finish_events(&events, maintenance).await;
    result.and(cleanup)
  }

  /// Advertise the original serving settings for replacement workers, and drain both listeners on shutdown.
  async fn serve_bound(
    self,
    settings: impl Serialize,
    default_rollout: RolloutPolicy,
    maintenance_requests_dir: Option<PathBuf>,
    shutdown: impl Future<Output = std::io::Result<()>> + Send,
  ) -> Result<()> {
    let (stop_public, mut public_shutdown) = watch::channel(false);
    let (stop_workers, worker_shutdown) = watch::channel(false);
    let host = ControlHost::new(
      settings,
      self.pool,
      self.owner.clone(),
      default_rollout,
      maintenance_requests_dir,
    )?;
    let control = host.serve(self.listener, worker_shutdown);
    let public = self.frontend.serve(async move {
      let _ = public_shutdown.changed().await;
    });
    let released = async {
      match (self.owner, self.lease) {
        (Some(owner), Some(lease)) => owner.released(lease).await,
        _ => std::future::pending().await,
      }
    };
    let stopping = async {
      tokio::select! { result = shutdown => result, result = released => result }
    };
    tokio::pin!(control, public);
    tracing::info!(control_socket = %self.socket.path.display(), "frontend starting; waiting for workers");
    tokio::select! {
      result = &mut public => { let _ = stop_workers.send(true); result.and(control.await) }
      result = &mut control => { let _ = stop_public.send(true); result.and(public.await) }
      result = stopping => {
        // An admitted upload may still need its first worker assignment while
        // public listeners drain. Keep registered workers available until then.
        let _ = stop_public.send(true);
        let drained = public.await;
        let _ = stop_workers.send(true);
        result.map_err(anyhow::Error::from).and(drained).and(control.await)
      }
    }
  }
}
