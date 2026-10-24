//! Exercise the configured SDK as a standalone consumer of its default features.
//!
//! The loopback server uses only the standard library. Depending on the
//! workspace mock server or other internal crates here could hide missing SDK
//! dependency features through Cargo feature unification.

#[cfg(test)]
mod tests {
  use std::{
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
      atomic::{AtomicBool, Ordering},
      Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
  };

  use serde_json::{json, Value};
  use tokn_sdk::{chat_completions::ChatRequest, Client, Event, RequestEventPayload, RequestOptions, StageEvent};

  fn assert_send_sync<T: Send + Sync>() {}

  fn assert_send<T: Send>(value: T) -> T {
    value
  }

  const BUFFERED: &str = r#"{"id":"chatcmpl-std","object":"chat.completion","model":"test-model","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}"#;
  const STREAM_PARTS: [&str; 2] = [
    "data: {\"id\":\"chatcmpl-std\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n",
    "data: {\"id\":\"chatcmpl-std\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
  ];

  struct Fixture {
    root: PathBuf,
    stopped: Arc<AtomicBool>,
    server: Option<JoinHandle<io::Result<Vec<Value>>>>,
  }

  impl Fixture {
    fn start() -> Self {
      Self::start_with_requests(2)
    }

    fn start_with_requests(expected_requests: usize) -> Self {
      let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind loopback server");
      listener.set_nonblocking(true).expect("nonblocking listener");
      let address = listener.local_addr().unwrap();
      let stopped = Arc::new(AtomicBool::new(false));
      let stop = stopped.clone();
      let server = thread::spawn(move || serve(listener, &stop, expected_requests));
      let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
      let root = std::env::temp_dir().join(format!("tokn-sdk-std-{}-{unique}", std::process::id()));
      fs::create_dir(&root).expect("create isolated config directory");
      fs::write(root.join("config.toml"), format!(
        "schema_version = 2\n[defaults]\nprovider = {{ kind = 'fixed', provider = 'llama-cpp' }}\nmodel = {{ kind = 'qualified', namespace = 'provider' }}\nwire_identity = 'none'\n[providers.llama-cpp]\ndriver = 'llama-cpp'\nbase_url = 'http://{address}'\n"
      )).unwrap();
      fs::write(
        root.join("auth.yaml"),
        "version: 1\naccounts:\n  - id: local\n    provider: llama-cpp\n",
      )
      .unwrap();
      Self {
        root,
        stopped,
        server: Some(server),
      }
    }

    fn client(&self) -> Client {
      Client::builder()
        .config_path(self.root.join("config.toml"))
        .auth_path(self.root.join("auth.yaml"))
        .build()
        .expect("build default-feature SDK client")
    }

    fn requests(&mut self) -> Vec<Value> {
      self
        .server
        .take()
        .expect("server still running")
        .join()
        .expect("server did not panic")
        .expect("server completed all requests")
    }
  }

  impl Drop for Fixture {
    fn drop(&mut self) {
      self.stopped.store(true, Ordering::Relaxed);
      if let Some(server) = self.server.take() {
        let _ = server.join();
      }
      let _ = fs::remove_dir_all(&self.root);
    }
  }

  fn serve(listener: TcpListener, stopped: &AtomicBool, expected_requests: usize) -> io::Result<Vec<Value>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut requests = Vec::new();
    while requests.len() < expected_requests {
      if stopped.load(Ordering::Relaxed) {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "fixture stopped"));
      }
      if Instant::now() >= deadline {
        return Err(io::Error::new(io::ErrorKind::TimedOut, "waiting for SDK request"));
      }
      let mut socket = match listener.accept() {
        Ok((socket, _)) => socket,
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
          thread::sleep(Duration::from_millis(5));
          continue;
        }
        Err(error) => return Err(error),
      };
      socket.set_nonblocking(false)?;
      socket.set_read_timeout(Some(Duration::from_secs(5)))?;
      socket.set_write_timeout(Some(Duration::from_secs(5)))?;
      let request = read_request(&mut socket)?;
      if request["model"] == "test-retry" && requests.is_empty() {
        let body = r#"{"error":{"message":"retry on another account"}}"#;
        write!(
          socket,
          "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
          body.len()
        )?;
      } else if request["stream"] == true {
        socket.write_all(
          b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        )?;
        let parts = if request["model"] == "test-truncated" {
          &STREAM_PARTS[..1]
        } else {
          &STREAM_PARTS[..]
        };
        for part in parts {
          write!(socket, "{:x}\r\n{part}\r\n", part.len())?;
          socket.flush()?;
        }
        // Closing an unterminated chunked HTTP body would produce a transport
        // error if the SDK incorrectly read beyond the protocol terminal.
        if request["model"] != "test-no-eof" {
          socket.write_all(b"0\r\n\r\n")?;
        }
      } else {
        write!(
          socket,
          "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{BUFFERED}",
          BUFFERED.len()
        )?;
      }
      requests.push(request);
    }
    Ok(requests)
  }

  fn read_request(socket: &mut TcpStream) -> io::Result<Value> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 1024];
    let (body_start, content_length) = loop {
      let count = socket.read(&mut buffer)?;
      if count == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "request headers"));
      }
      bytes.extend_from_slice(&buffer[..count]);
      if bytes.len() > 64 * 1024 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "request too large"));
      }
      if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
        let headers = std::str::from_utf8(&bytes[..end]).map_err(io::Error::other)?;
        assert_eq!(headers.lines().next(), Some("POST /chat/completions HTTP/1.1"));
        let content_length = headers
          .lines()
          .filter_map(|line| line.split_once(':'))
          .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
          .expect("SDK sends Content-Length")
          .1
          .trim()
          .parse::<usize>()
          .map_err(io::Error::other)?;
        if content_length > 64 * 1024 {
          return Err(io::Error::new(io::ErrorKind::InvalidData, "request too large"));
        }
        break (end + 4, content_length);
      }
    };
    while bytes.len() - body_start < content_length {
      let count = socket.read(&mut buffer)?;
      if count == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "request body"));
      }
      bytes.extend_from_slice(&buffer[..count]);
    }
    serde_json::from_slice(&bytes[body_start..body_start + content_length]).map_err(io::Error::other)
  }

  #[test]
  fn default_client_sends_buffered_and_streaming_requests() {
    assert_send_sync::<Client>();
    let mut fixture = Fixture::start();
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .expect("build runtime");
    runtime.block_on(async {
      tokio::time::timeout(Duration::from_secs(10), async {
        let client = fixture.client();
        assert_eq!(client.config_path(), fixture.root.join("config.toml"));
        assert_eq!(client.auth_path(), fixture.root.join("auth.yaml"));
        client.reload().expect("reload explicit config");
        let request: ChatRequest = serde_json::from_value(json!({
          "model": "llama-cpp/test-model",
          "messages": [{"role": "user", "content": "hello"}]
        }))
        .unwrap();
        let endpoint = client.chat_completions();
        let response = assert_send(endpoint.create_with(
          &request,
          RequestOptions::default().with_request_id("isolated-std-buffered"),
        ))
        .await
        .expect("buffered SDK call");
        assert_eq!(response.status, 200);
        assert_eq!(response.data.id.as_deref(), Some("chatcmpl-std"));
        assert_eq!(
          serde_json::to_value(&response.data).unwrap()["choices"][0]["message"]["content"],
          "hello"
        );

        let response = assert_send(endpoint.stream(&request))
          .await
          .expect("streaming SDK call");
        assert_eq!(response.status, 200);
        let bytes = response.bytes().await.expect("read streaming response");
        let body = std::str::from_utf8(&bytes).expect("UTF-8 SSE body");
        let events: Vec<_> = body.lines().filter_map(|line| line.strip_prefix("data: ")).collect();
        assert_eq!(events.len(), 3);
        let delta: Value = serde_json::from_str(events[0]).unwrap();
        assert_eq!(delta["id"], "chatcmpl-std");
        assert_eq!(delta["choices"][0]["delta"]["content"], "hello");
        let finished: Value = serde_json::from_str(events[1]).unwrap();
        assert_eq!(finished["choices"][0]["finish_reason"], "stop");
        assert_eq!(events[2], "[DONE]");
      })
      .await
      .expect("SDK calls completed before timeout");
    });
    let requests = fixture.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["model"], "test-model");
    assert_eq!(requests[0]["stream"], false);
    assert_eq!(requests[1]["model"], "test-model");
    assert_eq!(requests[1]["stream"], true);
  }

  #[test]
  fn default_stream_lifecycle_stops_at_terminal_and_rejects_truncation() {
    let mut fixture = Fixture::start_with_requests(4);
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .expect("build runtime");
    runtime.block_on(async {
      tokio::time::timeout(Duration::from_secs(10), async {
        let client = fixture.client();
        let endpoint = client.chat_completions();
        let request = |model| -> ChatRequest {
          serde_json::from_value(json!({
            "model": model,
            "messages": [{"role": "user", "content": "hello"}]
          }))
          .unwrap()
        };
        let complete = endpoint.stream(&request("llama-cpp/test-no-eof")).await.unwrap();
        let bytes = complete
          .bytes()
          .await
          .expect("terminal ends stream before the invalid HTTP EOF");
        assert!(std::str::from_utf8(&bytes).unwrap().ends_with("data: [DONE]\n\n"));

        let truncated = endpoint.stream(&request("llama-cpp/test-truncated")).await.unwrap();
        let error = truncated
          .bytes()
          .await
          .expect_err("clean HTTP EOF is not protocol completion");
        assert!(error.to_string().contains("[DONE]"));

        let cancelled = endpoint.stream(&request("llama-cpp/test-drop")).await.unwrap();
        drop(cancelled);
        let response = endpoint.create(&request("llama-cpp/test-model")).await.unwrap();
        assert_eq!(response.data.id.as_deref(), Some("chatcmpl-std"));
      })
      .await
      .expect("stream lifecycle calls completed before timeout");
    });
    let requests = fixture.requests();
    assert_eq!(requests.len(), 4);
    assert!(requests[..3].iter().all(|request| request["stream"] == true));
    assert_eq!(requests[3]["stream"], false);
  }

  #[test]
  fn live_std_streams_keep_independent_ownership_and_complete_once() {
    let mut fixture = Fixture::start_with_requests(3);
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .expect("build runtime");
    runtime.block_on(async {
      tokio::time::timeout(Duration::from_secs(10), async {
        let client = fixture.client();
        let mut events = client.subscribe_events();
        let endpoint = client.chat_completions();
        let request: ChatRequest = serde_json::from_value(json!({
          "model": "llama-cpp/test-model",
          "messages": [{"role": "user", "content": "hello"}]
        }))
        .unwrap();
        let cancelled = assert_send(endpoint.stream_with(
          &request,
          RequestOptions::default().with_request_id("isolated-cancelled"),
        ))
        .await
        .expect("open first live stream");
        // Keep the first response alive while opening a second exchange. A
        // shared exclusive transport borrow would either fail or deadlock here.
        let surviving = assert_send(endpoint.stream_with(
          &request,
          RequestOptions::default().with_request_id("isolated-surviving"),
        ))
        .await
        .expect("a live std response must not lock the client");
        while let Ok(event) = events.try_recv() {
          if let Event::Requests(event) = event.as_ref() {
            assert!(
              !matches!(event.payload, RequestEventPayload::Stage(StageEvent::Completed { .. })),
              "returning a response must not finish an unread stream"
            );
          }
        }
        drop(cancelled);
        let bytes = assert_send(surviving.bytes())
          .await
          .expect("cancelling one exchange must not close its sibling");
        assert!(std::str::from_utf8(&bytes).unwrap().ends_with("data: [DONE]\n\n"));
        endpoint
          .create_with(
            &request,
            RequestOptions::default().with_request_id("isolated-after-streams"),
          )
          .await
          .expect("client remains usable after both streams finish");
        let mut outcomes = Vec::new();
        let mut failures = Vec::new();
        while let Ok(event) = events.try_recv() {
          if let Event::Requests(event) = event.as_ref() {
            match event.payload {
              RequestEventPayload::Stage(StageEvent::Completed { success, attempts }) => {
                outcomes.push((event.request_id.to_string(), success, attempts));
              }
              RequestEventPayload::Stage(StageEvent::Error { .. }) => {
                failures.push(event.request_id.to_string());
              }
              _ => {}
            }
          }
        }
        assert_eq!(
          outcomes,
          [
            ("isolated-cancelled".into(), false, 1),
            ("isolated-surviving".into(), true, 1),
            ("isolated-after-streams".into(), true, 1),
          ]
        );
        assert_eq!(failures, ["isolated-cancelled"]);
      })
      .await
      .expect("independent std exchanges completed before timeout");
    });
    let requests = fixture.requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[..2].iter().all(|request| request["stream"] == true));
    assert_eq!(requests[2]["stream"], false);
  }

  #[test]
  fn retry_reselects_an_account_and_attributes_each_attempt() {
    let mut fixture = Fixture::start_with_requests(2);
    let config_path = fixture.root.join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap().replace(
      "[defaults]",
      "[defaults]\nretry = { kind = 'recoverable', policy = 'retry' }",
    );
    fs::write(
      config_path,
      format!("{config}\n[retry_policies.retry]\nmax_retries = 1\ninitial_backoff_ms = 1\n"),
    )
    .unwrap();
    fs::write(
      fixture.root.join("auth.yaml"),
      "version: 1\naccounts:\n  - id: primary\n    provider: llama-cpp\n  - id: backup\n    provider: llama-cpp\n    tier: fallback\n",
    )
    .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .expect("build runtime");
    runtime.block_on(async {
      tokio::time::timeout(Duration::from_secs(10), async {
        let client = fixture.client();
        let mut events = client.subscribe_events();
        let request: ChatRequest = serde_json::from_value(json!({
          "model": "llama-cpp/test-retry",
          "messages": [{"role": "user", "content": "hello"}]
        }))
        .unwrap();
        let response = client
          .chat_completions()
          .create_with(&request, RequestOptions::default().with_request_id("isolated-retry"))
          .await
          .expect("recoverable send failure retries with the fallback account");
        assert_eq!(response.data.id.as_deref(), Some("chatcmpl-std"));
        let mut accounts = Vec::new();
        let mut outcomes = Vec::new();
        while let Ok(event) = events.try_recv() {
          if let Event::Requests(event) = event.as_ref() {
            assert_eq!(event.request_id, "isolated-retry");
            match &event.payload {
              RequestEventPayload::Stage(StageEvent::Resolve(summary)) => {
                accounts.push((event.attempt, summary.account_id.to_string()));
              }
              RequestEventPayload::Stage(StageEvent::Completed { success, attempts }) => {
                outcomes.push((event.attempt, *success, *attempts));
              }
              _ => {}
            }
          }
        }
        assert_eq!(accounts, [(0, "primary".into()), (1, "backup".into())]);
        assert_eq!(outcomes, [(0, false, 1), (1, true, 2)]);
      })
      .await
      .expect("retried request completed before timeout");
    });
    let requests = fixture.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
  }
}
