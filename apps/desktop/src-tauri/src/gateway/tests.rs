use super::*;
use nix::{
  sys::signal::{kill, Signal},
  unistd::Pid,
};
use std::path::Path;

fn bundled_gateway(home: &Path) -> Command {
  let executable = std::env::var_os("TOKN_DESKTOP_TEST_GATEWAY").expect("gateway binary path");
  let mut command = Command::new(executable);
  command.env("HOME", home);
  command
}

async fn free_address() -> SocketAddr {
  tokio::net::TcpListener::bind("127.0.0.1:0")
    .await
    .unwrap()
    .local_addr()
    .unwrap()
}

fn write_config(home: &Path, address: SocketAddr) -> PathBuf {
  let config = home.join("config.toml");
  let source = format!(
    r#"schema_version = 2
[defaults]
[listeners.local]
kind = "llm_api"
bind = "{address}"
client_auth = "none"
[service.persistence]
enabled = false
usage_db_path = "{}/usage.db"
sessions_db_path = "{}/sessions.db"
requests_dir = "{}/requests"
[service.logging]
target = "stderr"
[service.models]
enabled = false
"#,
    home.display(),
    home.display(),
    home.display()
  );
  std::fs::write(&config, source).unwrap();
  config
}

async fn worker_ids(address: SocketAddr) -> Vec<String> {
  let response = client()
    .unwrap()
    .get(format!("http://{address}/admin/workers"))
    .header("x-tokn-admin", "workers")
    .send()
    .await
    .unwrap()
    .error_for_status()
    .unwrap();
  let status: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
  status["workers"]
    .as_array()
    .unwrap()
    .iter()
    .map(|worker| worker["worker_id"].as_str().unwrap().to_owned())
    .collect()
}

async fn worker_id(address: SocketAddr) -> String {
  let mut workers = worker_ids(address).await;
  assert_eq!(workers.len(), 1);
  workers.pop().unwrap()
}

async fn wait_port_released(address: SocketAddr) {
  tokio::time::timeout(Duration::from_secs(10), async {
    while tokio::net::TcpListener::bind(address).await.is_err() {
      tokio::time::sleep(Duration::from_millis(25)).await;
    }
  })
  .await
  .expect("frontend released its API listener");
}

/// The test relinquishes its real frontend during takeover. Keep its identity
/// for teardown, including on panic. Only signal a live peer on this fixture's
/// private control socket if the kernel still identifies the original process.
struct TestFrontend {
  socket: PathBuf,
  pid: Pid,
}

impl TestFrontend {
  fn capture(config: &Path) -> Self {
    let socket = tokn_gateway::supervision::runtime_dir(config)
      .unwrap()
      .join("frontend.sock");
    let (_, pid) = Self::peer(&socket).unwrap();
    Self { socket, pid }
  }

  fn peer(path: &Path) -> std::io::Result<(tokio::net::UnixStream, Pid)> {
    let stream = std::os::unix::net::UnixStream::connect(path)?;
    stream.set_nonblocking(true)?;
    let stream = tokio::net::UnixStream::from_std(stream)?;
    let pid = stream
      .peer_cred()?
      .pid()
      .filter(|pid| *pid > 1)
      .ok_or_else(|| std::io::Error::other("missing frontend peer PID"))?;
    Ok((stream, Pid::from_raw(pid)))
  }
}

impl Drop for TestFrontend {
  fn drop(&mut self) {
    if let Ok((_connection, pid)) = Self::peer(&self.socket) {
      if pid == self.pid {
        let _ = kill(self.pid, Signal::SIGTERM);
      }
    }
  }
}

#[tokio::test]
#[ignore = "requires TOKN_DESKTOP_TEST_GATEWAY and loopback sockets"]
async fn bundled_gateway_start_reload_stop() {
  let dir = tempfile::tempdir().unwrap();
  let address = free_address().await;
  let config = write_config(dir.path(), address);
  let gateway = Gateway::default();
  gateway
    .start(bundled_gateway(dir.path()), config.clone(), address)
    .await
    .unwrap();
  assert_eq!(gateway.status(address).await.unwrap().ownership, "managed");
  let original_worker = worker_id(address).await;
  let document = crate::config::read_at(&config).unwrap();
  crate::config::save_at(
    &config,
    &document.revision,
    "[defaults]\n[profiles.applied]\nroute = 'default'\n",
  )
  .unwrap();
  let client = client().unwrap();
  let providers = format!("http://{address}/applied/v1/providers");
  assert_eq!(
    client.get(&providers).send().await.unwrap().status(),
    reqwest::StatusCode::NOT_FOUND
  );
  reload(address).await.unwrap();
  let response = client.get(&providers).send().await.unwrap();
  assert_eq!(response.status(), reqwest::StatusCode::OK);
  let discovery: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
  assert_eq!(discovery["object"], "list");
  assert_eq!(worker_id(address).await, original_worker);
  drop(client);
  gateway.stop().await.unwrap();
  assert_eq!(gateway.status(address).await.unwrap().ownership, "none");
  tokio::net::TcpListener::bind(address)
    .await
    .expect("stop releases the API port");
}

#[tokio::test]
#[ignore = "requires TOKN_DESKTOP_TEST_GATEWAY and loopback sockets"]
async fn bundled_gateway_preserves_an_existing_frontend_and_worker() {
  let dir = tempfile::tempdir().unwrap();
  let original = free_address().await;
  let config = write_config(dir.path(), original);
  let owner = Gateway::default();
  owner
    .start(bundled_gateway(dir.path()), config, original)
    .await
    .unwrap();
  let original_worker = worker_id(original).await;

  // Editing the bind leaves the old frontend on its original port. The new
  // port passes Desktop's TCP preflight, but the control socket is occupied.
  let next = free_address().await;
  let config = write_config(dir.path(), next);
  let desktop = Gateway::default();
  assert!(desktop.start(bundled_gateway(dir.path()), config, next).await.is_err());
  desktop.stop().await.unwrap();
  assert_eq!(desktop.status(original).await.unwrap().ownership, "external");
  assert_eq!(worker_id(original).await, original_worker);
  assert!(tokio::net::TcpListener::bind(next).await.is_ok());
  owner.stop().await.unwrap();
  assert!(tokio::net::TcpListener::bind(original).await.is_ok());
}

#[tokio::test]
#[ignore = "requires TOKN_DESKTOP_TEST_GATEWAY and loopback sockets"]
async fn bundled_gateway_releases_the_frontend_if_the_managed_process_is_killed() {
  let home = tempfile::tempdir().unwrap();
  let address = free_address().await;
  let config = write_config(home.path(), address);
  let gateway = Gateway::default();
  gateway
    .start(bundled_gateway(home.path()), config.clone(), address)
    .await
    .unwrap();
  let _cleanup = TestFrontend::capture(&config);
  gateway
    .child
    .lock()
    .await
    .as_mut()
    .unwrap()
    .process
    .kill()
    .await
    .unwrap();
  wait_port_released(address).await;
  assert_eq!(gateway.status(address).await.unwrap().ownership, "none");
}

#[tokio::test]
#[ignore = "requires TOKN_DESKTOP_TEST_GATEWAY and loopback sockets"]
async fn bundled_gateway_hands_off_to_an_external_worker() {
  exercise_external_worker(false).await;
}

#[tokio::test]
#[ignore = "requires TOKN_DESKTOP_TEST_GATEWAY and loopback sockets"]
async fn bundled_gateway_stop_with_an_external_candidate_preserves_service() {
  exercise_external_worker(true).await;
}

async fn exercise_external_worker(candidate: bool) {
  let home = tempfile::tempdir().unwrap();
  let address = free_address().await;
  let config = write_config(home.path(), address);
  let desktop = Gateway::default();
  desktop
    .start(bundled_gateway(home.path()), config.clone(), address)
    .await
    .unwrap();
  let frontend = TestFrontend::capture(&config);
  let original = worker_id(address).await;
  let mut command = bundled_gateway(home.path());
  command.arg("--config").arg(&config).args(["worker", "start"]);
  if candidate {
    command.arg("--candidate");
  }
  let mut external = command
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .kill_on_drop(true)
    .spawn()
    .unwrap();
  let external_id = tokio::time::timeout(Duration::from_secs(10), async {
    loop {
      assert!(external.try_wait().unwrap().is_none(), "external worker exited early");
      let status = desktop.status(address).await.unwrap();
      let ids = worker_ids(address).await;
      if let Some(id) = ids.iter().find(|id| **id != original) {
        if candidate {
          assert!(ids.contains(&original), "the desktop worker must still be registered");
          assert_eq!(status.ownership, "managed");
          break id.clone();
        } else if status.ownership == "external" {
          assert!(status.last_error.is_none(), "expected a clean handoff");
          break id.clone();
        }
      }
      tokio::time::sleep(Duration::from_millis(25)).await;
    }
  })
  .await
  .expect("external worker registration completed");
  if candidate {
    let conflict = reload(address).await.unwrap_err().to_string();
    assert!(conflict.contains("409 Conflict"), "{conflict}");
  }
  desktop.stop().await.unwrap();
  assert_eq!(desktop.status(address).await.unwrap().ownership, "external");
  assert_eq!(worker_id(address).await, external_id);
  let discovery = client()
    .unwrap()
    .get(format!("http://{address}/v1/providers"))
    .send()
    .await
    .unwrap();
  assert_eq!(discovery.status(), reqwest::StatusCode::OK);
  drop(discovery);

  // The external worker accepts a compatible reload and reports a restart-only
  // edit as a conflict. Both outcomes must reach the user initiating Apply.
  assert!(reload(address).await.unwrap().contains("generation"));
  write_config(home.path(), free_address().await);
  let conflict = reload(address).await.unwrap_err().to_string();
  assert!(conflict.contains("409 Conflict"), "{conflict}");
  assert!(healthy(address).await);
  drop(frontend);
  tokio::time::timeout(Duration::from_secs(10), external.wait())
    .await
    .unwrap()
    .unwrap();
  wait_port_released(address).await;
}

#[test]
fn requires_a_reload_confirmation_with_a_positive_generation() {
  for body in [
    "ok",
    r#"{"status":"reloaded"}"#,
    r#"{"status":"reloaded","generation":0}"#,
    r#"{"status":"pending","generation":2}"#,
  ] {
    assert!(confirmed_reload(body).is_err(), "{body}");
  }
  let result = confirmed_reload(r#"{"status":"reloaded","generation":2,"accounts":3}"#).unwrap();
  assert!(result.contains("generation 2"));
}

#[tokio::test]
async fn refuses_an_external_listener_and_stop_does_not_close_it() {
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let address = listener.local_addr().unwrap();
  let gateway = Gateway::default();
  assert!(gateway
    .start(Command::new("/nonexistent"), "/nonexistent".into(), address)
    .await
    .is_err());
  gateway.stop().await.unwrap();
  assert!(tokio::net::TcpStream::connect(address).await.is_ok());
}
#[tokio::test]
async fn stops_and_reaps_owned_process() {
  let process = Command::new("/bin/cat")
    .stdin(Stdio::piped())
    .stdout(Stdio::null())
    .stderr(Stdio::piped())
    .kill_on_drop(true)
    .spawn()
    .unwrap();
  let gateway = Gateway {
    child: Mutex::new(Some(ManagedGateway::new(process).unwrap())),
    ..Gateway::default()
  };
  gateway.stop().await.unwrap();
  assert!(gateway.child.lock().await.is_none());
}
