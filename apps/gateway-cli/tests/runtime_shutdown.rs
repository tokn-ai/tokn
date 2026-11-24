//! Real process signals, isolated config/auth homes, and real persistence.
#![cfg(unix)]

use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokn_config::v2;

const WAIT: Duration = Duration::from_secs(10);

fn free_address() -> SocketAddr {
  std::net::TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
}

fn start_command(home: &Path, config: &Path, command: &[&str], log: &str) -> Child {
  let stderr = fs::File::create(home.join(log)).unwrap();
  Command::new(env!("CARGO_BIN_EXE_tokn-gateway"))
    .arg("--config")
    .arg(config)
    .args(command)
    // Only the child gets an isolated home; never read or mutate user auth.
    .env("HOME", home)
    .env_remove("RUST_LOG")
    .env_remove("HTTP_PROXY")
    .env_remove("HTTPS_PROXY")
    .env_remove("ALL_PROXY")
    .stdout(Stdio::null())
    .stderr(stderr)
    .kill_on_drop(true)
    .spawn()
    .unwrap()
}

fn start(home: &Path, config: &Path) -> Child {
  start_command(home, config, &["worker", "start"], "stderr.log")
}

fn frontend(home: &Path, config: &Path) -> Child {
  start_command(home, config, &["frontend"], "frontend-stderr.log")
}

async fn ready_frontend(child: &mut Child, address: SocketAddr, home: &Path) {
  tokio::time::timeout(WAIT, async {
    while TcpStream::connect(address).await.is_err() {
      assert!(
        child.try_wait().unwrap().is_none(),
        "{}",
        fs::read_to_string(home.join("frontend-stderr.log")).unwrap()
      );
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();
}

async fn workers(address: SocketAddr) -> serde_json::Value {
  reqwest::Client::builder()
    .no_proxy()
    .build()
    .unwrap()
    .get(format!("http://{address}/admin/workers"))
    .header("x-tokn-admin", "workers")
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap()
}

async fn wait_for_exiting_worker(address: SocketAddr) {
  tokio::time::timeout(WAIT, async {
    while workers(address).await["workers"][0]["state"] != "exiting" {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();
}

async fn ready(child: &mut Child, address: SocketAddr, home: &Path) {
  let client = reqwest::Client::builder().no_proxy().build().unwrap();
  tokio::time::timeout(WAIT, async {
    loop {
      assert!(
        child.try_wait().unwrap().is_none(),
        "gateway exited: {}",
        fs::read_to_string(home.join("stderr.log")).unwrap()
      );
      if client.get(format!("http://{address}/health")).send().await.is_ok()
        && workers(address).await["workers"]
          .as_array()
          .is_some_and(|values| values.iter().any(|value| value["weight"].as_u64().unwrap() > 0))
      {
        return;
      }
      tokio::time::sleep(Duration::from_millis(20)).await;
    }
  })
  .await
  .expect("gateway ready deadline");
}

fn signal(child: &Child, signal: &str) {
  assert!(std::process::Command::new("kill")
    .arg(signal)
    .arg(child.id().expect("running child").to_string())
    .status()
    .unwrap()
    .success());
}

async fn wait_for_closed_listener(address: SocketAddr) {
  tokio::time::timeout(WAIT, async {
    while TcpStream::connect(address).await.is_ok() {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("accept socket must close before streams finish");
}

async fn assert_clean_exit(child: &mut Child, home: &Path) {
  let status = tokio::time::timeout(WAIT, child.wait())
    .await
    .expect("shutdown deadline")
    .unwrap();
  let log = fs::read_to_string(home.join("stderr.log")).unwrap();
  assert!(status.success(), "{status}: {log}");
  assert!(log.contains("shutdown persistence cleanup complete"), "{log}");
}

#[tokio::test]
async fn sigint_and_sigterm_drain_streams_and_flush_request_records() {
  for (signal_name, force) in [("-INT", false), ("-TERM", false), ("-INT", true)] {
    let home = tempfile::tempdir().unwrap();
    let config_path = home.path().join("config.toml");
    let requests_dir = home.path().join("requests");
    let address = free_address();
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let (release, released) = oneshot::channel();
    let upstream_task = tokio::spawn(async move {
      let (mut stream, _) = upstream.accept().await.unwrap();
      let mut header = Vec::new();
      while !header.ends_with(b"\r\n\r\n") {
        header.push(stream.read_u8().await.unwrap());
      }
      let header = String::from_utf8(header).unwrap();
      let length = header
        .lines()
        .find_map(|line| {
          let (name, value) = line.split_once(':')?;
          name
            .eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
      stream.read_exact(&mut vec![0; length]).await.unwrap();
      let first = "data: {\"id\":\"fixture\",\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n";
      let last = "data: [DONE]\n\n";
      stream
        .write_all(
          format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{first}",
        first.len() + last.len()
      )
          .as_bytes(),
        )
        .await
        .unwrap();
      released.await.unwrap();
      stream.write_all(last.as_bytes()).await.unwrap();
    });
    let source = format!(
      r#"
schema_version = 2
[listeners.api]
kind = "llm_api"
bind = "{address}"
client_auth = "none"
[profiles.default]
route = "relay"
[routes.relay]
kind = "relay"
destination = {{ kind = "fixed_provider", provider = "local" }}
credentials = {{ kind = "client" }}
[providers.local]
driver = "openai"
base_url = "http://{upstream_address}/v1"
"#
    );
    let mut raw = v2::decode(&source, &config_path).unwrap();
    raw.service.logging.target = tokn_config::LogTarget::Stderr;
    raw.service.persistence.requests_dir = Some(requests_dir.clone());
    raw.service.persistence.usage_db_path = Some(home.path().join("usage.db"));
    raw.service.persistence.sessions_db_path = Some(home.path().join("sessions.db"));
    fs::write(&config_path, toml::to_string(&raw).unwrap()).unwrap();
    let mut frontend = frontend(home.path(), &config_path);
    ready_frontend(&mut frontend, address, home.path()).await;
    let mut child = start(home.path(), &config_path);
    ready(&mut child, address, home.path()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut response = tokio::time::timeout(
      WAIT,
      client
        .post(format!("http://{address}/v1/chat/completions"))
        .header("x-request-id", "shutdown-fixture")
        .json(&serde_json::json!({"model": "fixture", "messages": [], "stream": true}))
        .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), 200);
    assert!(tokio::time::timeout(WAIT, response.chunk())
      .await
      .unwrap()
      .unwrap()
      .is_some());
    signal(&child, signal_name);
    wait_for_exiting_worker(address).await;
    assert!(
      TcpStream::connect(address).await.is_ok(),
      "worker signals must preserve the frontend listener"
    );
    assert!(child.try_wait().unwrap().is_none(), "must wait for the admitted stream");
    if force {
      signal(&child, "-INT");
      let status = tokio::time::timeout(Duration::from_secs(2), child.wait())
        .await
        .unwrap()
        .unwrap();
      assert_eq!(status.code(), Some(130));
      drop(response);
      upstream_task.abort();
      let _ = upstream_task.await;
      signal(&frontend, "-TERM");
      assert!(tokio::time::timeout(WAIT, frontend.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
      continue;
    }
    release.send(()).unwrap();
    let remaining = tokio::time::timeout(WAIT, response.bytes()).await.unwrap().unwrap();
    assert!(remaining.ends_with(b"data: [DONE]\n\n"));
    upstream_task.await.unwrap();
    assert_clean_exit(&mut child, home.path()).await;
    signal(&frontend, "-TERM");
    assert!(tokio::time::timeout(WAIT, frontend.wait())
      .await
      .unwrap()
      .unwrap()
      .success());
    wait_for_closed_listener(address).await;
    let row = tokn_persistence::read_request_row(&requests_dir, "shutdown-fixture")
      .unwrap()
      .expect("flushed request row");
    assert_eq!(row["inbound_req_method"], "POST");
    assert!(row["ctx_json"]["latency_ms"].is_number(), "{row:?}");
  }
}

#[tokio::test]
async fn projected_v1_also_shuts_down_cleanly_on_sigterm() {
  let home = tempfile::tempdir().unwrap();
  let path = home.path().join("config.toml");
  let address = free_address();
  let source = format!(
    r#"
[server]
host = "127.0.0.1"
port = {}
[logging]
target = "stderr"
[db]
enabled = false
"#,
    address.port()
  );
  fs::write(&path, source).unwrap();
  let auth_path = home.path().join(".tokn/router/auth.yaml");
  let mut auth = tokn_auth::AuthStore::load(Some(&auth_path), None).unwrap();
  auth.upsert(toml::from_str("id = 'fixture'\nprovider = 'openai'\napi_key = 'not-a-real-key'").unwrap());
  auth.save().unwrap();
  let mut frontend = frontend(home.path(), &path);
  ready_frontend(&mut frontend, address, home.path()).await;
  let mut child = start(home.path(), &path);
  ready(&mut child, address, home.path()).await;
  signal(&child, "-TERM");
  assert_clean_exit(&mut child, home.path()).await;
  signal(&frontend, "-TERM");
  assert!(tokio::time::timeout(WAIT, frontend.wait())
    .await
    .unwrap()
    .unwrap()
    .success());
}

#[tokio::test]
async fn frontend_bind_failure_closes_sibling_listener_before_exiting() {
  let home = tempfile::tempdir().unwrap();
  let path = home.path().join("config.toml");
  let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let occupied_address = occupied.local_addr().unwrap();
  let sibling = free_address();
  let source = format!(
    r#"
schema_version = 2
[service.logging]
target = "stderr"
[service.persistence]
enabled = false
[listeners.a]
kind = "llm_api"
bind = "{sibling}"
client_auth = "none"
[listeners.z]
kind = "llm_api"
bind = "{occupied_address}"
client_auth = "none"
"#
  );
  fs::write(&path, source).unwrap();
  let mut child = frontend(home.path(), &path);
  let status = tokio::time::timeout(WAIT, child.wait()).await.unwrap().unwrap();
  assert!(!status.success());
  assert!(TcpStream::connect(sibling).await.is_err());
}

#[tokio::test]
async fn serve_interrupt_promotes_busy_stale_worker_and_preserves_its_stream() {
  let home = tempfile::tempdir().unwrap();
  let path = home.path().join("config.toml");
  let address = free_address();
  let old_upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let old_address = old_upstream.local_addr().unwrap();
  let new_upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let new_address = new_upstream.local_addr().unwrap();
  let (release, released) = oneshot::channel();
  let old_task = tokio::spawn(async move {
    let (mut stream, _) = old_upstream.accept().await.unwrap();
    read_request(&mut stream).await;
    let first = "data: {\"id\":\"old\",\"choices\":[{\"delta\":{\"content\":\"old\"}}]}\n\n";
    let last = "data: [DONE]\n\n";
    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{first}", first.len() + last.len()).as_bytes()).await.unwrap();
    let recovery = tokio::spawn(async move {
      let (mut stream, _) = old_upstream.accept().await.unwrap();
      read_request(&mut stream).await;
      let body = r#"{"id":"old-takeover","choices":[]}"#;
      stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    });
    released.await.unwrap();
    stream.write_all(last.as_bytes()).await.unwrap();
    recovery.await.unwrap();
  });
  let new_task = tokio::spawn(async move {
    let (mut stream, _) = new_upstream.accept().await.unwrap();
    read_request(&mut stream).await;
    let body = r#"{"id":"new","choices":[]}"#;
    stream
      .write_all(
        format!(
          "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
          body.len()
        )
        .as_bytes(),
      )
      .await
      .unwrap();
  });
  let source = format!(
    r#"
schema_version = 2
[service.logging]
target = "stderr"
[service.persistence]
enabled = false
[listeners.api]
kind = "llm_api"
bind = "{address}"
client_auth = "none"
[profiles.default]
route = "relay"
[routes.relay]
kind = "relay"
destination = {{ kind = "fixed_provider", provider = "local" }}
credentials = {{ kind = "client" }}
[providers.local]
driver = "openai"
base_url = "http://{old_address}/v1"
"#
  );
  fs::write(&path, &source).unwrap();
  let mut frontend = frontend(home.path(), &path);
  ready_frontend(&mut frontend, address, home.path()).await;
  let mut old = start(home.path(), &path);
  ready(&mut old, address, home.path()).await;
  let client = reqwest::Client::builder().no_proxy().build().unwrap();
  let mut response = client
    .post(format!("http://{address}/v1/chat/completions"))
    .json(&serde_json::json!({"model":"fixture", "messages":[], "stream":true}))
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 200);
  let first = response.chunk().await.unwrap().unwrap();
  assert!(std::str::from_utf8(&first).unwrap().contains("old"));
  fs::write(
    &path,
    source.replace(&old_address.to_string(), &new_address.to_string()),
  )
  .unwrap();
  let mut new = start_command(home.path(), &path, &["serve"], "new-stderr.log");
  tokio::time::timeout(WAIT, async {
    while workers(address).await["workers"].as_array().unwrap().len() != 2 {
      assert!(
        new.try_wait().unwrap().is_none(),
        "{}",
        fs::read_to_string(home.path().join("new-stderr.log")).unwrap()
      );
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();
  assert!(frontend.try_wait().unwrap().is_none());
  assert!(
    old.try_wait().unwrap().is_none(),
    "retired worker must retain its stream"
  );
  let report = workers(address).await;
  assert_eq!(report["workers"][0]["weight"], 0);
  assert_eq!(report["workers"][0]["in_flight"], 1);
  assert_eq!(report["workers"][1]["weight"], 1);
  let next = client
    .post(format!("http://{address}/v1/chat/completions"))
    .json(&serde_json::json!({"model":"fixture", "messages":[]}))
    .send()
    .await
    .unwrap();
  assert_eq!(next.status(), 200);
  assert_eq!(next.json::<serde_json::Value>().await.unwrap()["id"], "new");
  assert_eq!(report["workers"][0]["state"], "stale");
  assert_eq!(report["workers"][1]["state"], "current");
  signal(&new, "-INT");
  assert!(tokio::time::timeout(WAIT, new.wait()).await.unwrap().unwrap().success());
  let promoted = workers(address).await;
  assert_eq!(promoted["main_worker_id"], report["workers"][0]["worker_id"]);
  assert_eq!(promoted["workers"][0]["state"], "current");
  assert_eq!(promoted["workers"][0]["weight"], 1);
  let next = client
    .post(format!("http://{address}/v1/chat/completions"))
    .json(&serde_json::json!({"model":"fixture", "messages":[]}))
    .send()
    .await
    .unwrap();
  assert_eq!(next.json::<serde_json::Value>().await.unwrap()["id"], "old-takeover");
  release.send(()).unwrap();
  assert!(response.bytes().await.unwrap().ends_with(b"data: [DONE]\n\n"));
  assert!(
    old.try_wait().unwrap().is_none(),
    "promoted worker must remain available after draining"
  );
  signal(&old, "-INT");
  assert_clean_exit(&mut old, home.path()).await;
  assert!(frontend.try_wait().unwrap().is_none());
  signal(&frontend, "-TERM");
  assert!(tokio::time::timeout(WAIT, frontend.wait())
    .await
    .unwrap()
    .unwrap()
    .success());
  old_task.await.unwrap();
  new_task.await.unwrap();
}

#[tokio::test]
async fn ab_test_flags_keep_idle_baseline_available_and_interrupt_cancels_the_ramp() {
  for custom_policy in [false, true] {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.toml");
    let policy_path = home.path().join("rollout.toml");
    let policy_source =
      "initial_percent = 10\ncompletion_percent = 100\n[[stages]]\nduration_seconds = 259200\ntraffic_percent = 90\n";
    fs::write(&policy_path, policy_source).unwrap();
    let command = if custom_policy {
      vec![
        "worker",
        "start",
        "--ab-test",
        "--ab-test-policy",
        policy_path.to_str().unwrap(),
      ]
    } else {
      vec!["serve", "--ab-test", "--ab-test-duration", "72h"]
    };
    let address = free_address();
    fs::write(
      &path,
      format!(
        r#"
schema_version = 2
[service.logging]
target = "stderr"
[service.persistence]
enabled = false
[listeners.api]
kind = "llm_api"
bind = "{address}"
client_auth = "none"
"#
      ),
    )
    .unwrap();
    let mut frontend = frontend(home.path(), &path);
    ready_frontend(&mut frontend, address, home.path()).await;
    let mut baseline = start(home.path(), &path);
    ready(&mut baseline, address, home.path()).await;
    let baseline_id = workers(address).await["main_worker_id"].clone();
    let mut candidate = start_command(home.path(), &path, &command, "ab-stderr.log");
    tokio::time::timeout(WAIT, async {
      while !workers(address).await["ab_test"].is_object() {
        assert!(
          candidate.try_wait().unwrap().is_none(),
          "{}",
          fs::read_to_string(home.path().join("ab-stderr.log")).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .unwrap();
    let report = workers(address).await;
    assert_eq!(report["ab_test"]["baseline_worker_id"], baseline_id);
    assert_eq!(report["ab_test"]["traffic_percent"], 10);
    assert_eq!(report["ab_test"]["duration_seconds"], 72 * 3600);
    assert_eq!(
      report["ab_test"]["rollout_policy"]["stages"][0]["duration_seconds"],
      72 * 3600
    );
    assert_eq!(report["workers"][0]["weight"], 90);
    assert_eq!(report["workers"][1]["weight"], 10);
    if custom_policy {
      fs::write(&policy_path, policy_source.replace("259200", "129600")).unwrap();
      assert_eq!(
        workers(address).await["ab_test"]["rollout_policy"],
        report["ab_test"]["rollout_policy"]
      );
    }
    assert!(
      baseline.try_wait().unwrap().is_none(),
      "idle baseline must survive the experiment"
    );
    signal(&candidate, "-INT");
    assert!(tokio::time::timeout(WAIT, candidate.wait())
      .await
      .unwrap()
      .unwrap()
      .success());
    let report = workers(address).await;
    assert!(report["ab_test"].is_null());
    assert_eq!(report["main_worker_id"], baseline_id);
    signal(&baseline, "-INT");
    assert_clean_exit(&mut baseline, home.path()).await;
    signal(&frontend, "-TERM");
    assert!(tokio::time::timeout(WAIT, frontend.wait())
      .await
      .unwrap()
      .unwrap()
      .success());
  }
}

async fn read_request(stream: &mut TcpStream) {
  let mut header = Vec::new();
  while !header.ends_with(b"\r\n\r\n") {
    header.push(stream.read_u8().await.unwrap());
  }
  let header = String::from_utf8(header).unwrap();
  let length = header
    .lines()
    .find_map(|line| {
      let (name, value) = line.split_once(':')?;
      name
        .eq_ignore_ascii_case("content-length")
        .then(|| value.trim().parse::<usize>().unwrap())
    })
    .unwrap_or(0);
  stream.read_exact(&mut vec![0; length]).await.unwrap();
}

struct IndependentFrontend(u32);
impl Drop for IndependentFrontend {
  fn drop(&mut self) {
    let _ = std::process::Command::new("kill")
      .arg("-TERM")
      .arg(self.0.to_string())
      .stdout(Stdio::null())
      .stderr(Stdio::null())
      .status();
  }
}

#[tokio::test]
async fn serve_bootstraps_proxy_frontend_and_worker_start_inherits_proxy_settings() {
  let home = tempfile::tempdir().unwrap();
  let path = home.path().join("config.toml");
  let address = free_address();
  let proxy = free_address();
  fs::write(
    &path,
    format!(
      r#"
[server]
host = "127.0.0.1"
port = {}
[proxy_mode]
host = "127.0.0.1"
port = {}
[logging]
target = "stderr"
ansi = false
[db]
enabled = false
"#,
      address.port(),
      proxy.port()
    ),
  )
  .unwrap();
  let auth_path = home.path().join(".tokn/router/auth.yaml");
  let mut auth = tokn_auth::AuthStore::load(Some(&auth_path), None).unwrap();
  auth.upsert(toml::from_str("id = 'fixture'\nprovider = 'openai'\napi_key = 'not-a-real-key'").unwrap());
  auth.save().unwrap();
  let mut old = start_command(home.path(), &path, &["serve", "--with-proxy"], "stderr.log");
  let frontend_pid = tokio::time::timeout(WAIT, async {
    loop {
      let log = fs::read_to_string(home.path().join("stderr.log")).unwrap();
      if let Some(pid) = log
        .lines()
        .find(|line| line.contains("started independent frontend"))
        .and_then(|line| line.split("frontend_pid=").nth(1))
        .and_then(|field| field.split_whitespace().next())
        .and_then(|pid| pid.parse::<u32>().ok())
      {
        break pid;
      }
      assert!(old.try_wait().unwrap().is_none(), "{log}");
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .unwrap();
  let frontend = IndependentFrontend(frontend_pid);
  ready(&mut old, address, home.path()).await;
  assert!(TcpStream::connect(proxy).await.is_ok());
  let old_id = workers(address).await["workers"][0]["worker_id"]
    .as_str()
    .unwrap()
    .to_string();
  let mut new = start_command(home.path(), &path, &["worker", "start"], "new-stderr.log");
  assert_clean_exit(&mut old, home.path()).await;
  let report = workers(address).await;
  assert_eq!(report["workers"].as_array().unwrap().len(), 1);
  assert_ne!(report["workers"][0]["worker_id"], old_id);
  assert!(new.try_wait().unwrap().is_none());
  assert!(
    TcpStream::connect(proxy).await.is_ok(),
    "worker replacement must preserve proxy listener"
  );
  drop(frontend);
  assert!(tokio::time::timeout(WAIT, new.wait()).await.unwrap().unwrap().success());
  wait_for_closed_listener(address).await;
  wait_for_closed_listener(proxy).await;
}
