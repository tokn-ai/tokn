use super::*;
use reqwest::{Client, StatusCode};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokn_gateway::events::EventObservers;

pub(super) async fn serve_fixture(
  compiled: tokn_config::v2::CompiledConfig,
  accounts: Vec<tokn_core::account::AccountConfig>,
  args: ServeArgs,
  source: Option<RuntimeSource>,
  mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
  assert!(!compiled.service().persistence().enabled());
  runtime::serve(
    RuntimeConfig { compiled, accounts },
    runtime_mode(args, None)?,
    move |request| {
      source
        .as_ref()
        .expect("fixture reload requires a config source")
        .reload_candidate(request)
    },
    EventObservers::default,
    async move {
      if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
      }
      Ok(())
    },
  )
  .await
}

pub(super) async fn reload_config(client: &Client, address: std::net::SocketAddr) -> (StatusCode, serde_json::Value) {
  let response = client
    .post(format!("http://{address}/admin/config/reload"))
    .header("x-tokn-admin", "reload")
    .send()
    .await
    .unwrap();
  let status = response.status();
  let body = response.json().await.unwrap();
  (status, body)
}

pub(super) fn unused_loopback_addr() -> std::net::SocketAddr {
  let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  listener.local_addr().unwrap()
}

// Legacy projection enables model refresh. Keep those requests on loopback
// while allowing the passthrough test to reach its local upstream.
pub(super) async fn isolate_legacy_outbound(legacy: &mut Config) -> TcpListener {
  let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
  legacy.proxy.url = Some(format!("http://{}", proxy.local_addr().unwrap()));
  legacy.proxy.no_proxy = vec!["127.0.0.1".into()];
  proxy
}

pub(super) async fn wait_for_listener(addr: std::net::SocketAddr) {
  for _ in 0..50 {
    if TcpStream::connect(addr).await.is_ok() {
      return;
    }
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
  }
  panic!("listener did not start at {addr}");
}
