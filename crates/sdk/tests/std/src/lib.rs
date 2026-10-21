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
  use tokn_sdk::{chat_completions::ChatRequest, Client, RequestOptions};

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
      let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind loopback server");
      listener.set_nonblocking(true).expect("nonblocking listener");
      let address = listener.local_addr().unwrap();
      let stopped = Arc::new(AtomicBool::new(false));
      let stop = stopped.clone();
      let server = thread::spawn(move || serve(listener, &stop));
      let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
      let root = std::env::temp_dir().join(format!("tokn-sdk-std-{}-{unique}", std::process::id()));
      fs::create_dir(&root).expect("create isolated config directory");
      fs::write(root.join("config.toml"), "[defaults]\nmode = \"exact\"\n").unwrap();
      fs::write(
        root.join("auth.yaml"),
        format!("version: 1\naccounts:\n  - id: local\n    provider: llama-cpp\n    base_url: http://{address}\n"),
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
        .expect("server completed both requests")
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

  fn serve(listener: TcpListener, stopped: &AtomicBool) -> io::Result<Vec<Value>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut requests = Vec::new();
    while requests.len() < 2 {
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
      if request["stream"] == true {
        socket.write_all(
          b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        )?;
        for part in STREAM_PARTS {
          write!(socket, "{:x}\r\n{part}\r\n", part.len())?;
          socket.flush()?;
        }
        socket.write_all(b"0\r\n\r\n")?;
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
        let response = client
          .chat_completions()
          .create_with(
            &request,
            RequestOptions::default().with_request_id("isolated-std-buffered"),
          )
          .await
          .expect("buffered SDK call");
        assert_eq!(response.status, 200);
        assert_eq!(response.data.id.as_deref(), Some("chatcmpl-std"));
        assert_eq!(
          serde_json::to_value(&response.data).unwrap()["choices"][0]["message"]["content"],
          "hello"
        );

        let response = client
          .chat_completions()
          .stream(&request)
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
}
