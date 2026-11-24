use super::*;
use bytes::{Buf, BytesMut};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, Sleep};

const TEST_PING_INTERVAL: Duration = Duration::from_millis(75);
const TEST_POOL_IDLE_TIMEOUT: Duration = Duration::from_millis(250);
const PEER_IDLE_TIMEOUT: Duration = Duration::from_millis(500);
const SILENT_BODY_DELAY: Duration = Duration::from_millis(1200);
const TEST_DEADLINE: Duration = Duration::from_secs(5);

fn client_builder(kind: ClientKind) -> reqwest::ClientBuilder {
  match kind {
    ClientKind::Control => control_plane_client_builder(),
    ClientKind::Managed => managed_client_builder(),
    ClientKind::Opaque => opaque_client_builder(),
  }
}

#[tokio::test]
async fn all_client_kinds_keep_silent_http2_bodies_alive_after_pool_expiry() {
  let mut probes = JoinSet::new();
  for kind in [ClientKind::Control, ClientKind::Managed, ClientKind::Opaque] {
    probes.spawn(assert_silent_body_survives_pool_expiry(kind));
  }
  while let Some(result) = probes.join_next().await {
    result.expect("HTTP/2 keepalive probe failed");
  }
}

async fn assert_silent_body_survives_pool_expiry(kind: ClientKind) {
  let fixture = H2Fixture::spawn(SILENT_BODY_DELAY).await;
  let client = client_builder(kind)
    .no_proxy()
    // Cleartext prior knowledge is only for this local fixture. Production
    // clients negotiate HTTP/2 through ALPN and retain HTTP/1 fallback.
    .http2_prior_knowledge()
    .http2_keep_alive_interval(TEST_PING_INTERVAL)
    .pool_idle_timeout(Some(TEST_POOL_IDLE_TIMEOUT))
    // Inherit while_idle from the production builder: setting it false must
    // break this test when pool eviction drops the request sender.
    .build()
    .expect("build HTTP/2 test client");
  let response = tokio::time::timeout(TEST_DEADLINE, client.get(fixture.url()).send())
    .await
    .expect("HTTP/2 response headers timed out")
    .expect("send HTTP/2 request");
  assert_eq!(response.status(), reqwest::StatusCode::OK);
  assert_eq!(response.version(), http::Version::HTTP_2);

  let body = tokio::time::timeout(TEST_DEADLINE, response.bytes())
    .await
    .expect("silent HTTP/2 response timed out")
    .unwrap_or_else(|error| panic!("{kind:?} closed the silent response before END_STREAM: {error:?}"));
  assert_eq!(body.as_ref(), b"*");

  let response_started = fixture
    .response_started
    .lock()
    .expect("response timestamp lock")
    .expect("peer sent response data");
  assert!(
    Instant::now().saturating_duration_since(response_started) >= SILENT_BODY_DELAY,
    "{kind:?} must wait for the peer's delayed END_STREAM"
  );
  // Allow two pool sweep intervals before checking. This covers a sweep
  // landing exactly on the expiration boundary rather than just past it.
  let after_pool_expiry = TEST_POOL_IDLE_TIMEOUT + TEST_POOL_IDLE_TIMEOUT + TEST_PING_INTERVAL;
  let pings = fixture.pings.lock().expect("ping observation lock");
  assert!(
    pings
      .iter()
      .any(|at| at.saturating_duration_since(response_started) >= after_pool_expiry),
    "{kind:?} must keep sending PING after its pooled sender expires"
  );
}

#[tokio::test]
async fn native_http2_wire_preserves_duplicate_and_raw_values_and_strips_connection_headers() {
  let mut fixture = H2Fixture::spawn(Duration::ZERO).await;
  let client = opaque_client_builder()
    .no_proxy()
    .http2_prior_knowledge()
    .build()
    .expect("build opaque HTTP/2 client");
  let mut headers = NativeHeaderMap::new();
  headers.append("x-duplicate", http::HeaderValue::from_static("first"));
  headers.append("x-duplicate", http::HeaderValue::from_static("second"));
  headers.insert("x-raw", http::HeaderValue::from_bytes(&[0x80, 0xff]).unwrap());
  headers.insert(
    http::header::CONNECTION,
    http::HeaderValue::from_static("keep-alive, upgrade, x-connection-only"),
  );
  headers.insert("keep-alive", http::HeaderValue::from_static("timeout=600"));
  headers.insert("x-connection-only", http::HeaderValue::from_static("local-value"));
  headers.insert(http::header::UPGRADE, http::HeaderValue::from_static("websocket"));
  headers.insert(http::header::HOST, http::HeaderValue::from_static("wrong.invalid"));
  headers.insert(http::header::CONTENT_LENGTH, http::HeaderValue::from_static("999"));

  let response = tokio::time::timeout(
    TEST_DEADLINE,
    send_native(
      &client,
      Method::GET,
      &fixture.url(),
      headers,
      None,
      "HTTP/2 header wire test",
    ),
  )
  .await
  .expect("HTTP/2 header request timed out")
  .expect("send native HTTP/2 request");
  assert_eq!(response.version(), http::Version::HTTP_2);
  assert_eq!(response.bytes().await.unwrap().as_ref(), b"*");

  let received = fixture.request_headers().await;
  assert_eq!(
    received
      .get_all("x-duplicate")
      .iter()
      .map(|value| value.as_bytes())
      .collect::<Vec<_>>(),
    [b"first".as_slice(), b"second".as_slice()]
  );
  assert_eq!(received["x-raw"].as_bytes(), [0x80, 0xff]);
  for name in [
    "connection",
    "keep-alive",
    "upgrade",
    "x-connection-only",
    "content-length",
  ] {
    assert!(!received.contains_key(name), "{name} must not reach the HTTP/2 peer");
  }
  assert_ne!(
    received.get(http::header::HOST).map(|value| value.as_bytes()),
    Some(b"wrong.invalid".as_slice())
  );
}

struct H2Fixture {
  address: SocketAddr,
  headers: Option<oneshot::Receiver<NativeHeaderMap>>,
  pings: Arc<Mutex<Vec<Instant>>>,
  response_started: Arc<Mutex<Option<Instant>>>,
  task: JoinHandle<()>,
}

impl H2Fixture {
  async fn spawn(body_delay: Duration) -> Self {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (headers_tx, headers_rx) = oneshot::channel();
    let pings = Arc::new(Mutex::new(Vec::new()));
    let response_started = Arc::new(Mutex::new(None));
    let observed_pings = pings.clone();
    let observed_start = response_started.clone();
    let task = tokio::spawn(async move {
      let (socket, _) = listener.accept().await.unwrap();
      let peer = IdleTimeoutIo::new(socket, observed_pings);
      let mut connection = h2::server::handshake(peer).await.unwrap();
      let (request, mut respond) = connection.accept().await.unwrap().unwrap();
      let _ = headers_tx.send(request.headers().clone());
      let response = http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .body(())
        .unwrap();
      let mut stream = respond.send_response(response, false).unwrap();
      stream.send_data(Bytes::from_static(b"*"), false).unwrap();
      *observed_start.lock().expect("response timestamp lock") = Some(Instant::now());
      let finish = tokio::time::sleep(body_delay);
      tokio::pin!(finish);
      let mut body_pending = true;
      loop {
        tokio::select! {
          _ = &mut finish, if body_pending => {
            stream.send_data(Bytes::new(), true).unwrap();
            body_pending = false;
          }
          accepted = connection.accept() => {
            match accepted {
              Some(Ok(_)) => panic!("fixture expects one HTTP/2 request"),
              Some(Err(_)) | None => break,
            }
          }
        }
      }
    });
    Self {
      address,
      headers: Some(headers_rx),
      pings,
      response_started,
      task,
    }
  }

  fn url(&self) -> String {
    format!("http://{}/silent", self.address)
  }

  async fn request_headers(&mut self) -> NativeHeaderMap {
    tokio::time::timeout(TEST_DEADLINE, self.headers.take().expect("read request headers once"))
      .await
      .expect("peer request headers timed out")
      .expect("peer received request headers")
  }
}

impl Drop for H2Fixture {
  fn drop(&mut self) {
    self.task.abort();
  }
}

/// Model a tunnel that closes when it receives no application bytes. The h2
/// server handles and acknowledges PING normally; this wrapper only observes
/// incoming frames and installs the transport idle cutoff.
struct IdleTimeoutIo {
  socket: TcpStream,
  deadline: Pin<Box<Sleep>>,
  observer: FrameObserver,
}

impl IdleTimeoutIo {
  fn new(socket: TcpStream, pings: Arc<Mutex<Vec<Instant>>>) -> Self {
    Self {
      socket,
      deadline: Box::pin(tokio::time::sleep(PEER_IDLE_TIMEOUT)),
      observer: FrameObserver {
        preface_remaining: 24,
        pending: BytesMut::new(),
        pings,
      },
    }
  }
}

impl AsyncRead for IdleTimeoutIo {
  fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
    let before = buffer.filled().len();
    match Pin::new(&mut self.socket).poll_read(cx, buffer) {
      Poll::Ready(Ok(())) => {
        let received = &buffer.filled()[before..];
        if !received.is_empty() {
          self.deadline.as_mut().reset(Instant::now() + PEER_IDLE_TIMEOUT);
          self.observer.observe(received);
        }
        Poll::Ready(Ok(()))
      }
      Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
      Poll::Pending => match self.deadline.as_mut().poll(cx) {
        Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
          io::ErrorKind::TimedOut,
          "fixture transport idle timeout",
        ))),
        Poll::Pending => Poll::Pending,
      },
    }
  }
}

impl AsyncWrite for IdleTimeoutIo {
  fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
    Pin::new(&mut self.socket).poll_write(cx, bytes)
  }

  fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Pin::new(&mut self.socket).poll_flush(cx)
  }

  fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Pin::new(&mut self.socket).poll_shutdown(cx)
  }
}

struct FrameObserver {
  preface_remaining: usize,
  pending: BytesMut,
  pings: Arc<Mutex<Vec<Instant>>>,
}

impl FrameObserver {
  fn observe(&mut self, mut bytes: &[u8]) {
    let preface = self.preface_remaining.min(bytes.len());
    self.preface_remaining -= preface;
    bytes = &bytes[preface..];
    self.pending.extend_from_slice(bytes);
    while self.pending.len() >= 9 {
      let payload_length =
        (usize::from(self.pending[0]) << 16) | (usize::from(self.pending[1]) << 8) | usize::from(self.pending[2]);
      let frame_length = 9 + payload_length;
      if self.pending.len() < frame_length {
        break;
      }
      // PING (type 6) uses stream 0, has an eight-byte payload, and requests
      // an ACK when flag bit 0 is clear. Other frames are handled by h2.
      if self.pending[3] == 6 && self.pending[4] & 1 == 0 && payload_length == 8 && self.pending[5..9] == [0, 0, 0, 0] {
        self.pings.lock().expect("ping observation lock").push(Instant::now());
      }
      self.pending.advance(frame_length);
    }
  }
}
