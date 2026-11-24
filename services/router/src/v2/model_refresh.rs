//! Serving-owned model refresh. State construction and one-shot smoke stay offline.

use super::{discovery::DiscoveryRuntime, LiveRuntime, RuntimeGeneration};
use arc_swap::ArcSwap;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokn_config::v2::ModelRefreshPlan;

/// Owns background model refresh tasks. Dropping it cancels outstanding work;
/// `shutdown` also waits for cancellation before serving resources are released.
#[derive(Default)]
pub struct ModelRefreshGuard {
  tasks: Vec<JoinHandle<()>>,
}

impl ModelRefreshGuard {
  pub async fn shutdown(mut self) {
    for task in self.tasks.drain(..) {
      task.abort();
      let _ = task.await;
    }
  }
}

impl Drop for ModelRefreshGuard {
  fn drop(&mut self) {
    for task in &self.tasks {
      task.abort();
    }
  }
}

impl LiveRuntime {
  /// Start periodic discovery for this serving runtime. Call once and retain
  /// the returned guard for the lifetime of the server. Refresh uses the current
  /// generation after reload, including forward-proxy-only configurations.
  /// Synchronous builders and smoke requests never start background network work.
  pub fn start_model_refresh(&self, settings: ModelRefreshPlan) -> ModelRefreshGuard {
    if !settings.enabled() {
      return ModelRefreshGuard::default();
    }
    let current = Arc::downgrade(&self.current);
    ModelRefreshGuard {
      tasks: vec![
        tokio::spawn(refresh_loop(
          current.clone(),
          self.refresh_changed.subscribe(),
          settings.upstream_interval(),
          settings.request_timeout(),
          Source::Upstream,
        )),
        tokio::spawn(refresh_loop(
          current,
          self.refresh_changed.subscribe(),
          settings.catalogue_interval(),
          settings.request_timeout(),
          Source::Catalogue,
        )),
      ],
    }
  }
}

#[derive(Clone, Copy)]
enum Source {
  Upstream,
  Catalogue,
}

fn discovery(current: &Weak<ArcSwap<RuntimeGeneration>>) -> Option<Arc<DiscoveryRuntime>> {
  let current = current.upgrade()?;
  let generation = current.load();
  generation
    .llm_api
    .values()
    .map(|state| &state.discovery)
    .chain(generation.forward_proxy.values().map(|state| &state.discovery))
    .next()
    .cloned()
}

async fn refresh_loop(
  current: Weak<ArcSwap<RuntimeGeneration>>,
  mut generation_changed: watch::Receiver<u64>,
  interval: Duration,
  timeout: Duration,
  source: Source,
) {
  let mut timer = tokio::time::interval(interval);
  timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
  loop {
    tokio::select! {
      _ = timer.tick() => {}
      changed = generation_changed.changed(), if matches!(source, Source::Upstream) => {
        if changed.is_err() {
          break;
        }
      }
    }
    let Some(discovery) = discovery(&current) else {
      break;
    };
    match source {
      Source::Upstream => discovery.refresh_upstream(timeout).await,
      Source::Catalogue => {
        discovery.refresh_catalogue(timeout).await;
        // A reload may have replaced the destination caches during the fetch.
        if let Some(latest) = self::discovery(&current) {
          latest.apply_catalogue();
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use axum::{routing::get, Json, Router};
  use serde_json::json;
  use std::net::SocketAddr;
  use tokn_core::event::EventBus;

  fn runtime_states(upstream: SocketAddr, proxy_only: bool) -> super::super::RuntimeStates {
    let listener = if proxy_only {
      r#"[listeners.proxy]
kind = "forward_proxy"
bind = "127.0.0.1:4142"
client_auth = "none"
default_http_action = { kind = "route", profile = "default" }
default_connect = "reject"
"#
    } else {
      r#"[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"
"#
    };
    let config = format!(
      r#"schema_version = 2
{listener}
[defaults]
providers = ["local"]
provider = {{ kind = "fixed", provider = "local" }}
[providers.local]
driver = "openai"
base_url = "http://{upstream}/v1"
"#
    );
    let plan = tokn_config::v2::parse(&config, std::path::Path::new("refresh.toml")).unwrap();
    let account = toml::from_str("id = 'acct'\nprovider = 'local'\napi_key = 'test-key'").unwrap();
    super::super::build_runtime_states(
      plan,
      &[account],
      Arc::new(tokn_access::AccessStore::disabled()),
      Arc::new(EventBus::noop()),
    )
    .unwrap()
  }

  async fn upstream() -> (SocketAddr, tokio::sync::mpsc::UnboundedReceiver<()>, JoinHandle<()>) {
    let (sent, received) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new().route(
      "/v1/models",
      get(move || {
        let sent = sent.clone();
        async move {
          let _ = sent.send(());
          Json(json!({"data": [{"id": "new-live-model"}]}))
        }
      }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (address, received, task)
  }

  async fn received(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<()>) {
    assert!(tokio::time::timeout(Duration::from_secs(5), receiver.recv())
      .await
      .unwrap()
      .is_some());
  }

  #[tokio::test]
  async fn periodic_refresh_follows_reload_and_shutdown_cancels_it() {
    for proxy_only in [false, true] {
      let (first, mut first_requests, first_server) = upstream().await;
      let (second, mut second_requests, second_server) = upstream().await;
      let live = LiveRuntime::new(runtime_states(first, proxy_only), 1);
      let task = tokio::spawn(refresh_loop(
        Arc::downgrade(&live.current),
        live.refresh_changed.subscribe(),
        Duration::from_millis(100),
        Duration::from_secs(1),
        Source::Upstream,
      ));
      let guard = ModelRefreshGuard { tasks: vec![task] };
      received(&mut first_requests).await;
      received(&mut first_requests).await;
      live.replace(runtime_states(second, proxy_only), 1).unwrap();
      received(&mut second_requests).await;
      guard.shutdown().await;
      while second_requests.try_recv().is_ok() {}
      assert!(tokio::time::timeout(Duration::from_millis(200), second_requests.recv())
        .await
        .is_err());
      first_server.abort();
      second_server.abort();
    }
  }

  #[test]
  fn disabled_refresh_does_not_require_a_tokio_runtime() {
    let settings = tokn_config::v2::parse_config(
      "schema_version = 2\n[defaults]\n[listeners.api]\nkind = 'llm_api'\nbind = '127.0.0.1:4141'\nclient_auth = 'none'\n[service.models]\nenabled = false",
      std::path::Path::new("disabled.toml"),
    )
    .unwrap();
    let live = LiveRuntime::new(
      super::super::RuntimeStates {
        llm_api: vec![],
        forward_proxy: vec![],
      },
      0,
    );
    assert!(live.start_model_refresh(settings.service().models()).tasks.is_empty());
  }
}
