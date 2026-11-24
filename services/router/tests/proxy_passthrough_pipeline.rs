//! Integration test for the MITM proxy passthrough pipeline.
//!
//! This test drives [`proxy_passthrough_via_pipeline_inner`] (the
//! pre-body-read inner core) directly with a mock TCP upstream. Going
//! through the inner fn lets us bypass the full proxy TCP/CONNECT
//! machinery while still exercising:
//!
//! * `ProxyResolve` (reads `proxy.host` / `proxy.provider_id` /
//!   `proxy.account_id` from `RunConfig`).
//! * `PassthroughExtract` → `PassthroughBuildHeaders` (router-owned
//!   header stripping + client-auth preservation).
//! * `PassthroughConvertRequest` (verbatim body bytes).
//! * `ProxySend` (dispatch to `{scheme}://{host}{path}`).
//! * `PassthroughConvertResponse` (buffered response forwarding).
//! * `AccountIdentityResolver` integration (provider_id falls back to
//!   the intercepted host when no fingerprint match).
//! * `RecordEvent::UpstreamReq` emission with the right url + headers.

use axum::body::to_bytes;
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokn_access::{AccessContext, ProviderAccess};
use tokn_config::RouteMode;
use tokn_core::account::{AccountTier, AuthType};
use tokn_core::event::{Event, EventBus};
use tokn_core::request_event::{RecordEvent, RequestEventPayload};
use tokn_router::api::build_proxy_state;
use tokn_router::config::Config;
use tokn_router::proxy::passthrough_pipeline::{proxy_passthrough_via_pipeline_inner, proxy_switch_via_pipeline_inner};
use tokn_router::util::secret::Secret;

fn openai_account() -> tokn_router::config::Account {
  tokn_router::config::Account {
    id: "openai-acct".into(),
    provider: "openai".into(),
    enabled: true,
    tier: AccountTier::Active,
    tags: Vec::new(),
    label: None,
    base_url: None,
    headers: Default::default(),
    auth_type: Some(AuthType::Bearer),
    username: None,
    api_key: Some(Secret::new("sk-router".into())),
    api_key_expires_at: None,
    access_token: None,
    access_token_expires_at: None,
    id_token: None,
    refresh_token: None,
    provider_account_id: None,
    extra: Default::default(),
    refresh_url: None,
    last_refresh: None,
    settings: Default::default(),
  }
}

fn llama_cpp_account() -> tokn_router::config::Account {
  tokn_router::config::Account {
    id: "llama-acct".into(),
    provider: "llama-cpp".into(),
    enabled: true,
    tier: AccountTier::Active,
    tags: Vec::new(),
    label: None,
    base_url: None,
    headers: Default::default(),
    auth_type: Some(AuthType::Bearer),
    username: None,
    api_key: Some(Secret::new("sk-router".into())),
    api_key_expires_at: None,
    access_token: None,
    access_token_expires_at: None,
    id_token: None,
    refresh_token: None,
    provider_account_id: None,
    extra: Default::default(),
    refresh_url: None,
    last_refresh: None,
    settings: Default::default(),
  }
}

#[tokio::test]
async fn proxy_passthrough_pipeline_forwards_request_and_preserves_client_auth() {
  use tokn_core::event::Event as CoreEvent;
  use tokn_core::request_event::{RecordEvent, RequestEventPayload};

  // Mock TCP upstream — captures the request, returns a known JSON.
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();

  let upstream_body = br#"{"id":"resp-proxy","ok":true}"#;
  let (req_tx, req_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();

  let server = tokio::spawn(async move {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut buf = vec![0_u8; 16384];
    let n = stream.read(&mut buf).await.unwrap();
    buf.truncate(n);
    let resp = format!(
      "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
      upstream_body.len()
    );
    stream.write_all(resp.as_bytes()).await.unwrap();
    stream.write_all(upstream_body).await.unwrap();
    stream.flush().await.unwrap();
    let _ = req_tx.send(buf);
  });

  // Build router state in passthrough mode with zero accounts. The
  // proxy passthrough pipeline does no account resolution; identity
  // fallback for provider_id will be the intercepted host.
  let mut cfg = Config::default();
  cfg.server.route_mode = RouteMode::Passthrough;
  let events = Arc::new(EventBus::new(256));
  let mut rx = events.subscribe();
  let state = build_proxy_state(&cfg, &[], events.clone()).unwrap();

  let inbound_body =
    Bytes::from_static(br#"{"stream":false,"model":"glm-4.6","messages":[{"role":"user","content":"hi proxy"}]}"#);

  // Use a non-default port + http scheme so the test exercises the
  // port-preservation path. The mock listener already bound to an
  // arbitrary high port; we pass the bare host and that port through
  // explicitly.
  let intercepted_host = addr.ip().to_string();
  let intercepted_port = addr.port();
  let expected_authority = format!("{intercepted_host}:{intercepted_port}");

  let req = Request::builder()
    .method(Method::POST)
    .uri("/v1/chat/completions")
    .header("content-type", "application/json")
    .header("authorization", "Bearer client-bearer-should-reach-upstream")
    .header("x-tokn-router-local-addr", "127.0.0.1:9999")
    .body(())
    .unwrap();
  let (parts, ()) = req.into_parts();

  let resp = proxy_passthrough_via_pipeline_inner(
    &state,
    &intercepted_host,
    intercepted_port,
    "http",
    None,
    Some(addr.to_string()),
    parts,
    inbound_body.clone(),
  )
  .await;
  assert_eq!(resp.status(), StatusCode::OK, "proxy passthrough should succeed");

  let resp_body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
  assert_eq!(
    resp_body.as_ref(),
    upstream_body,
    "downstream body must be upstream body verbatim"
  );

  server.await.unwrap();
  let raw_req = req_rx.await.unwrap();
  let raw_req_str = String::from_utf8_lossy(&raw_req);
  let lower = raw_req_str.to_ascii_lowercase();

  // Inbound body bytes reach upstream verbatim.
  assert!(
    raw_req_str.contains(std::str::from_utf8(&inbound_body).unwrap()),
    "upstream must receive inbound body verbatim, got:\n{raw_req_str}"
  );

  // Client's own Authorization is preserved (no provider injection in
  // the proxy variant).
  assert!(
    lower.contains("authorization: bearer client-bearer-should-reach-upstream"),
    "client auth must reach upstream untouched, got:\n{raw_req_str}"
  );

  // Router-owned headers are stripped.
  assert!(
    !lower.contains("x-tokn-router-local-addr"),
    "x-tokn-router-* headers must be stripped before upstream send, got:\n{raw_req_str}"
  );

  // Upstream Host header is the resolved authority with the non-default
  // port preserved (since scheme=http and port != 80).
  assert!(
    lower.contains(&format!("host: {}", expected_authority.to_ascii_lowercase())),
    "Host header must be {expected_authority}, got:\n{raw_req_str}"
  );

  // Drain the full pipeline event stream (StageEvent + RecordEvent) so
  // we can assert ordering, content, and absence in one pass. The drain
  // stops as soon as we see `StageEvent::Completed` (the runner emits
  // it exactly once at the very end) or after a hard 2s budget.
  let mut events: Vec<tokn_core::request_event::RequestEvent> = Vec::new();
  let drain_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
  loop {
    let remaining = drain_deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
      break;
    }
    let Ok(Ok(ev)) = tokio::time::timeout(remaining, rx.recv()).await else {
      break;
    };
    let CoreEvent::Requests(req) = &*ev else { continue };
    let req = req.clone();
    let done = matches!(
      &req.payload,
      RequestEventPayload::Stage(tokn_core::request_event::StageEvent::Completed { .. })
    );
    events.push(req);
    if done {
      break;
    }
  }

  // Helper: locate the first event matching a predicate, or panic with
  // a debug-dump of the whole stream.
  let kinds: Vec<String> = events
    .iter()
    .map(|e| match &e.payload {
      RequestEventPayload::Stage(s) => format!("Stage::{s:?}").chars().take(40).collect(),
      RequestEventPayload::Record(r) => format!("Record::{r:?}").chars().take(40).collect(),
      RequestEventPayload::Custom(c) => format!("Custom::{}", c.kind),
    })
    .collect();
  let debug_dump = || format!("event stream was:\n  {}", kinds.join("\n  "));

  // --- Stage stream: presence + ordering ---
  use tokn_core::request_event::StageEvent;
  let pos =
    |pred: &dyn Fn(&RequestEventPayload) -> bool| -> Option<usize> { events.iter().position(|e| pred(&e.payload)) };

  let p_started = pos(&|p| matches!(p, RequestEventPayload::Stage(StageEvent::Started { .. })))
    .unwrap_or_else(|| panic!("missing StageEvent::Started; {}", debug_dump()));
  let p_extract = pos(&|p| matches!(p, RequestEventPayload::Stage(StageEvent::Extract(_))))
    .unwrap_or_else(|| panic!("missing StageEvent::Extract; {}", debug_dump()));
  let p_resolve = pos(&|p| matches!(p, RequestEventPayload::Stage(StageEvent::Resolve(_))))
    .unwrap_or_else(|| panic!("missing StageEvent::Resolve; {}", debug_dump()));
  let p_build = pos(&|p| matches!(p, RequestEventPayload::Stage(StageEvent::BuildHeaders(_))))
    .unwrap_or_else(|| panic!("missing StageEvent::BuildHeaders; {}", debug_dump()));
  let p_convreq = pos(&|p| matches!(p, RequestEventPayload::Stage(StageEvent::ConvertRequest(_))))
    .unwrap_or_else(|| panic!("missing StageEvent::ConvertRequest; {}", debug_dump()));
  let p_send = pos(&|p| matches!(p, RequestEventPayload::Stage(StageEvent::Send(_))))
    .unwrap_or_else(|| panic!("missing StageEvent::Send; {}", debug_dump()));
  let p_convresp = pos(&|p| matches!(p, RequestEventPayload::Stage(StageEvent::ConvertResponse(_))))
    .unwrap_or_else(|| panic!("missing StageEvent::ConvertResponse; {}", debug_dump()));
  let p_completed = pos(&|p| matches!(p, RequestEventPayload::Stage(StageEvent::Completed { .. })))
    .unwrap_or_else(|| panic!("missing StageEvent::Completed; {}", debug_dump()));

  assert!(
    p_started < p_extract
      && p_extract < p_resolve
      && p_resolve < p_build
      && p_build < p_convreq
      && p_convreq < p_send
      && p_send < p_convresp
      && p_convresp < p_completed,
    "stage events out of order; {}",
    debug_dump()
  );

  // StageEvent::Started carries the endpoint inferred from the path.
  if let RequestEventPayload::Stage(StageEvent::Started { request_endpoint }) = &events[p_started].payload {
    assert_eq!(
      *request_endpoint,
      tokn_core::request_event::RequestEndpoint::Known(tokn_core::provider::Endpoint::ChatCompletions)
    );
  }

  // StageEvent::Resolve: provider_id falls back to bare intercepted
  // host (no accounts configured -> no fingerprint match). account_id
  // is the bearer-token fingerprint synthesised by
  // `AccountIdentityResolver` for the long `Authorization` header in
  // this test (≥32 chars triggers the `account_fp_<suffix>` fallback).
  if let RequestEventPayload::Stage(StageEvent::Resolve(r)) = &events[p_resolve].payload {
    assert_eq!(r.provider_id.as_str(), intercepted_host);
    assert!(
      r.account_id.as_str().starts_with("account_fp_"),
      "expected synthetic fingerprint account_id, got {:?}",
      r.account_id
    );
  } else {
    unreachable!()
  }

  // StageEvent::Send summary carries the upstream status.
  if let RequestEventPayload::Stage(StageEvent::Send(s)) = &events[p_send].payload {
    assert_eq!(s.status, 200, "send summary status; {}", debug_dump());
    assert!(!s.stream, "non-streaming");
  } else {
    unreachable!()
  }

  // StageEvent::Completed signals success.
  if let RequestEventPayload::Stage(StageEvent::Completed { success, attempts }) = &events[p_completed].payload {
    assert!(*success, "pipeline should report success; {}", debug_dump());
    assert!(*attempts >= 1, "at least one attempt");
  } else {
    unreachable!()
  }

  // --- Record stream: transport-truth captures ---
  let p_upreq = pos(&|p| matches!(p, RequestEventPayload::Record(RecordEvent::UpstreamReq { .. })))
    .unwrap_or_else(|| panic!("missing RecordEvent::UpstreamReq; {}", debug_dump()));
  let p_upresp = pos(&|p| matches!(p, RequestEventPayload::Record(RecordEvent::UpstreamResp { .. })))
    .unwrap_or_else(|| panic!("missing RecordEvent::UpstreamResp; {}", debug_dump()));
  let p_upbody = pos(&|p| matches!(p, RequestEventPayload::Record(RecordEvent::UpstreamBody { .. })))
    .unwrap_or_else(|| panic!("missing RecordEvent::UpstreamBody; {}", debug_dump()));

  // Wire-truth ordering: req → resp → body, all within the
  // Send/ConvertResponse window of the stage stream.
  assert!(
    p_upreq < p_upresp && p_upresp < p_upbody,
    "record events out of order; {}",
    debug_dump()
  );

  if let RequestEventPayload::Record(RecordEvent::UpstreamReq {
    method,
    url,
    headers,
    body,
  }) = &events[p_upreq].payload
  {
    assert_eq!(method.as_str(), "POST");
    assert_eq!(
      url.as_str(),
      &format!("http://{expected_authority}/v1/chat/completions"),
      "upstream url; {}",
      debug_dump()
    );
    assert_eq!(body.as_ref(), inbound_body.as_ref(), "upstream request body verbatim");
    // Client-auth preserved on the wire-truth capture too.
    let auth = headers
      .get("authorization")
      .map(|v| v.as_str().to_string())
      .unwrap_or_default();
    assert!(
      auth.contains("Bearer client-bearer-should-reach-upstream"),
      "wire-truth authorization header preserved, got {auth:?}"
    );
    // Host is transport-owned in ProxySend: the copied inbound value is
    // stripped from the event header map before reqwest derives/sends the
    // authoritative Host from the URL. The raw TCP capture above asserts
    // the actual wire Host header.
    assert!(
      headers.get("host").is_none(),
      "upstream request event headers should not include copied Host"
    );
    // Router-owned headers stripped before send.
    assert!(
      headers.get("x-tokn-router-local-addr").is_none(),
      "wire-truth must not include x-tokn-router-* headers"
    );
  } else {
    unreachable!()
  }

  if let RequestEventPayload::Record(RecordEvent::UpstreamResp { status, headers }) = &events[p_upresp].payload {
    assert_eq!(*status, 200);
    let ct = headers
      .get("content-type")
      .map(|v| v.as_str().to_string())
      .unwrap_or_default();
    assert!(ct.starts_with("application/json"), "content-type; got {ct:?}");
  } else {
    unreachable!()
  }

  if let RequestEventPayload::Record(RecordEvent::UpstreamBody { body, error }) = &events[p_upbody].payload {
    assert!(error.is_none(), "no upstream body error");
    assert_eq!(body.as_ref(), upstream_body, "upstream body bytes");
  } else {
    unreachable!()
  }

  // Buffered (non-streaming) passthrough: ConvertedBody MUST be
  // emitted (verbatim copy of upstream bytes) so the persistence
  // layer fills `inbound_resp_body` with the real wire bytes rather
  // than the `null` placeholder that `StageEvent::ConvertResponse`
  // carries for passthrough (where body_json is intentionally Null).
  let p_convbody = pos(&|p| matches!(p, RequestEventPayload::Record(RecordEvent::ConvertedBody { .. })))
    .unwrap_or_else(|| {
      panic!(
        "missing RecordEvent::ConvertedBody for buffered passthrough; {}",
        debug_dump()
      )
    });
  if let RequestEventPayload::Record(RecordEvent::ConvertedBody { body, error }) = &events[p_convbody].payload {
    assert!(error.is_none(), "no converted body error");
    assert_eq!(
      body.as_ref(),
      upstream_body,
      "buffered passthrough ConvertedBody must equal upstream bytes verbatim"
    );
  } else {
    unreachable!()
  }

  // InboundConnection MUST be emitted with the inbound transport facts
  // so persistence populates `local_addr`/`peer_addr`/`mode`/`method`/
  // `inbound_req_method`/`inbound_req_url`.
  let p_inbound = pos(&|p| matches!(p, RequestEventPayload::Record(RecordEvent::InboundConnection { .. })))
    .unwrap_or_else(|| panic!("missing RecordEvent::InboundConnection; {}", debug_dump()));
  if let RequestEventPayload::Record(RecordEvent::InboundConnection {
    local_addr,
    peer_addr,
    mode,
    method,
    inbound_method,
    url,
    ..
  }) = &events[p_inbound].payload
  {
    assert_eq!(local_addr.as_deref(), Some(addr.to_string().as_str()));
    assert_eq!(peer_addr.as_deref(), None);
    assert_eq!(mode.as_str(), "passthrough");
    assert_eq!(method.as_str(), "proxy");
    assert_eq!(inbound_method.as_str(), "POST");
    assert_eq!(
      url.as_deref(),
      Some(format!("http://{expected_authority}/v1/chat/completions").as_str())
    );
  } else {
    unreachable!()
  }

  // Stage::Error must not appear on a successful run.
  assert!(
    !events
      .iter()
      .any(|e| matches!(&e.payload, RequestEventPayload::Stage(StageEvent::Error { .. }))),
    "no StageEvent::Error on a successful run; {}",
    debug_dump()
  );
}

#[tokio::test]
async fn proxy_passthrough_pipeline_decodes_zstd_for_model_peek_and_forwards_raw_body() {
  use tokn_core::event::Event as CoreEvent;
  use tokn_core::request_event::{RecordEvent, RequestEventPayload, StageEvent};

  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();
  let upstream_body = br#"{"id":"resp-zstd","ok":true}"#;
  let (req_tx, req_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();

  let server = tokio::spawn(async move {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut buf = vec![0_u8; 16384];
    let n = stream.read(&mut buf).await.unwrap();
    buf.truncate(n);
    let resp = format!(
      "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
      upstream_body.len()
    );
    stream.write_all(resp.as_bytes()).await.unwrap();
    stream.write_all(upstream_body).await.unwrap();
    stream.flush().await.unwrap();
    let _ = req_tx.send(buf);
  });

  let mut cfg = Config::default();
  cfg.server.route_mode = RouteMode::Passthrough;
  let events = Arc::new(EventBus::new(256));
  let mut rx = events.subscribe();
  let state = build_proxy_state(&cfg, &[], events).unwrap();

  let decoded_body =
    Bytes::from_static(br#"{"stream":false,"model":"glm-4.6","messages":[{"role":"user","content":"hi"}]}"#);
  let raw_body = Bytes::from(zstd::stream::encode_all(decoded_body.as_ref(), 0).unwrap());

  let intercepted_host = addr.ip().to_string();
  let intercepted_port = addr.port();
  let req = Request::builder()
    .method(Method::POST)
    .uri("/v1/chat/completions")
    .header("content-type", "application/json")
    .header("content-encoding", "zstd")
    .header("authorization", "Bearer client-bearer-zstd")
    .body(())
    .unwrap();
  let (parts, ()) = req.into_parts();

  let resp = proxy_passthrough_via_pipeline_inner(
    &state,
    &intercepted_host,
    intercepted_port,
    "http",
    None,
    Some(addr.to_string()),
    parts,
    raw_body.clone(),
  )
  .await;
  assert_eq!(resp.status(), StatusCode::OK);
  let resp_body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
  assert_eq!(resp_body.as_ref(), upstream_body);

  server.await.unwrap();
  let raw_req = req_rx.await.unwrap();
  assert!(
    raw_req
      .windows(raw_body.len())
      .any(|window| window == raw_body.as_ref()),
    "upstream request must contain the compressed inbound body verbatim"
  );
  assert!(
    !raw_req
      .windows(decoded_body.len())
      .any(|window| window == decoded_body.as_ref()),
    "decoded body must only be used for inspection, not forwarded"
  );

  let mut collected: Vec<tokn_core::request_event::RequestEvent> = Vec::new();
  let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
  loop {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
      break;
    }
    let Ok(Ok(ev)) = tokio::time::timeout(remaining, rx.recv()).await else {
      break;
    };
    let CoreEvent::Requests(req) = &*ev else { continue };
    let req = req.clone();
    let done = matches!(&req.payload, RequestEventPayload::Stage(StageEvent::Completed { .. }));
    collected.push(req);
    if done {
      break;
    }
  }

  let resolved_model = collected.iter().find_map(|event| match &event.payload {
    RequestEventPayload::Stage(StageEvent::Resolve(summary)) => Some(summary.model.as_str()),
    _ => None,
  });
  assert_eq!(
    resolved_model,
    Some("glm-4.6"),
    "proxy should decode zstd body for model peeking"
  );

  let upstream_req_body = collected.iter().find_map(|event| match &event.payload {
    RequestEventPayload::Record(RecordEvent::UpstreamReq { body, .. }) => Some(body.clone()),
    _ => None,
  });
  assert_eq!(
    upstream_req_body.as_deref(),
    Some(raw_body.as_ref()),
    "wire-truth event should keep the compressed upstream request body"
  );
}

/// Streaming SSE passthrough: the inner pipeline must spawn the
/// shared accumulator so `RecordEvent::UpstreamBody`,
/// `RecordEvent::ConvertedBody`, and `StageEvent::Completed` all fire
/// after the upstream byte stream drains. Without this wiring the
/// persistence layer would leave `outbound_resp_body`/`inbound_resp_body`
/// empty and the row would stay "in-flight" forever (no `Completed`
/// → no `latency_ms` write, no `clear_request`).
#[tokio::test]
async fn proxy_passthrough_pipeline_streams_emit_bodies_and_completed() {
  use tokn_core::event::Event as CoreEvent;
  use tokn_core::request_event::{RecordEvent, RequestEventPayload, StageEvent};

  // Mock TCP upstream that emits a 3-frame SSE response.
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();
  let frame1 = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n";
  let frame2 = "data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4}}\n\n";
  let frame3 = "data: [DONE]\n\n";
  let sse_body = format!("{frame1}{frame2}{frame3}");
  let sse_body_clone = sse_body.clone();

  let server = tokio::spawn(async move {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut buf = vec![0_u8; 16384];
    let _ = stream.read(&mut buf).await.unwrap();
    // Chunked SSE response.
    let header = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n";
    stream.write_all(header.as_bytes()).await.unwrap();
    for frame in [frame1, frame2, frame3] {
      let chunk = format!("{:x}\r\n{}\r\n", frame.len(), frame);
      stream.write_all(chunk.as_bytes()).await.unwrap();
      stream.flush().await.unwrap();
      tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    stream.write_all(b"0\r\n\r\n").await.unwrap();
    stream.flush().await.unwrap();
  });

  let mut cfg = Config::default();
  cfg.server.route_mode = RouteMode::Passthrough;
  let events = Arc::new(EventBus::new(512));
  let mut rx = events.subscribe();
  let state = build_proxy_state(&cfg, &[], events.clone()).unwrap();

  let inbound_body =
    Bytes::from_static(br#"{"stream":true,"model":"glm-4.6","messages":[{"role":"user","content":"hi"}]}"#);

  let intercepted_host = addr.ip().to_string();
  let intercepted_port = addr.port();

  let req = Request::builder()
    .method(Method::POST)
    .uri("/v1/chat/completions")
    .header("content-type", "application/json")
    .header("accept", "text/event-stream")
    .header("authorization", "Bearer client-bearer-sse")
    .body(())
    .unwrap();
  let (parts, ()) = req.into_parts();

  let resp = proxy_passthrough_via_pipeline_inner(
    &state,
    &intercepted_host,
    intercepted_port,
    "http",
    None,
    Some(addr.to_string()),
    parts,
    inbound_body,
  )
  .await;
  assert_eq!(resp.status(), StatusCode::OK);

  // Drain the body fully so the AccumHelper finishes and emits
  // UpstreamBody / ConvertedBody / Completed.
  let drained = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
  assert_eq!(
    std::str::from_utf8(drained.as_ref()).unwrap(),
    sse_body_clone.as_str(),
    "downstream SSE bytes must equal upstream verbatim"
  );

  server.await.unwrap();

  // Drain pipeline events until we see Completed (or hard timeout).
  let mut collected: Vec<tokn_core::request_event::RequestEvent> = Vec::new();
  let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
  loop {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
      break;
    }
    let Ok(Ok(ev)) = tokio::time::timeout(remaining, rx.recv()).await else {
      break;
    };
    let CoreEvent::Requests(req) = &*ev else { continue };
    let req = req.clone();
    let done = matches!(&req.payload, RequestEventPayload::Stage(StageEvent::Completed { .. }));
    collected.push(req);
    if done {
      break;
    }
  }

  let kinds: Vec<String> = collected
    .iter()
    .map(|e| match &e.payload {
      RequestEventPayload::Stage(s) => format!("Stage::{s:?}").chars().take(40).collect(),
      RequestEventPayload::Record(r) => format!("Record::{r:?}").chars().take(40).collect(),
      RequestEventPayload::Custom(c) => format!("Custom::{}", c.kind),
    })
    .collect();
  let dump = || format!("event stream was:\n  {}", kinds.join("\n  "));

  // Mandatory persistence-feeding events for streaming passthrough.
  let upstream_body = collected.iter().find_map(|e| match &e.payload {
    RequestEventPayload::Record(RecordEvent::UpstreamBody { body, error }) => Some((body.clone(), error.clone())),
    _ => None,
  });
  let (up_bytes, up_err) =
    upstream_body.unwrap_or_else(|| panic!("missing RecordEvent::UpstreamBody for SSE; {}", dump()));
  assert!(up_err.is_none(), "no upstream body error");
  assert_eq!(
    std::str::from_utf8(&up_bytes).unwrap(),
    sse_body.as_str(),
    "UpstreamBody must accumulate the full SSE stream verbatim"
  );

  let converted_body = collected.iter().find_map(|e| match &e.payload {
    RequestEventPayload::Record(RecordEvent::ConvertedBody { body, error }) => Some((body.clone(), error.clone())),
    _ => None,
  });
  let (conv_bytes, conv_err) =
    converted_body.unwrap_or_else(|| panic!("missing RecordEvent::ConvertedBody for SSE; {}", dump()));
  assert!(conv_err.is_none(), "no converted body error");
  assert_eq!(
    std::str::from_utf8(&conv_bytes).unwrap(),
    sse_body.as_str(),
    "ConvertedBody must equal UpstreamBody for passthrough"
  );

  // Completed MUST fire (otherwise the row stays in-flight and CLI
  // shows perpetual progress).
  let completed = collected.iter().any(|e| {
    matches!(
      &e.payload,
      RequestEventPayload::Stage(StageEvent::Completed { success: true, .. })
    )
  });
  assert!(
    completed,
    "StageEvent::Completed must fire after stream drains; {}",
    dump()
  );
}

#[tokio::test]
async fn proxy_passthrough_4xx_stream_request_completes_before_downstream_body_drain() {
  use tokn_core::event::Event as CoreEvent;
  use tokn_core::request_event::{RequestEventPayload, StageEvent};

  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();
  let upstream_body = br#"{"error":{"message":"forbidden"}}"#;

  let server = tokio::spawn(async move {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut buf = vec![0_u8; 16384];
    let _ = stream.read(&mut buf).await.unwrap();
    let header = format!(
      "HTTP/1.1 403 Forbidden\r\ncontent-length: {}\r\n\r\n",
      upstream_body.len()
    );
    stream.write_all(header.as_bytes()).await.unwrap();
    stream.write_all(upstream_body).await.unwrap();
    stream.flush().await.unwrap();
  });

  let mut cfg = Config::default();
  cfg.server.route_mode = RouteMode::Passthrough;
  let events = Arc::new(EventBus::new(256));
  let mut rx = events.subscribe();
  let state = build_proxy_state(&cfg, &[], events).unwrap();

  let inbound_body =
    Bytes::from_static(br#"{"stream":true,"model":"glm-4.6","messages":[{"role":"user","content":"hi"}]}"#);

  let intercepted_host = addr.ip().to_string();
  let intercepted_port = addr.port();
  let req = Request::builder()
    .method(Method::POST)
    .uri("/v1/chat/completions")
    .header("content-type", "application/json")
    .header("accept", "text/event-stream")
    .header("authorization", "Bearer client-bearer-stream-403")
    .body(())
    .unwrap();
  let (parts, ()) = req.into_parts();

  let resp = proxy_passthrough_via_pipeline_inner(
    &state,
    &intercepted_host,
    intercepted_port,
    "http",
    None,
    Some(addr.to_string()),
    parts,
    inbound_body,
  )
  .await;

  assert_eq!(resp.status(), StatusCode::FORBIDDEN);

  let mut saw_completed = false;
  for _ in 0..32 {
    let ev = tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await;
    let Ok(Ok(ev)) = ev else { break };
    let CoreEvent::Requests(req) = &*ev else { continue };
    if matches!(
      &req.payload,
      RequestEventPayload::Stage(StageEvent::Completed { success: true, .. })
    ) {
      saw_completed = true;
      break;
    }
  }
  assert!(
    saw_completed,
    "4xx proxy responses should be buffered so the pipeline completes before the client drains the body"
  );

  let resp_body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
  assert_eq!(resp_body.as_ref(), upstream_body);

  server.await.unwrap();
}

#[tokio::test]
async fn proxy_passthrough_preserves_unknown_request_endpoint() {
  use tokn_core::event::Event as CoreEvent;
  use tokn_core::request_event::{RequestEndpoint, RequestEventPayload, StageEvent};

  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();
  let upstream_body = br#"{"id":"resp-unknown","ok":true}"#;

  let server = tokio::spawn(async move {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut buf = vec![0_u8; 8192];
    let _ = stream.read(&mut buf).await.unwrap();
    let resp = format!(
      "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
      upstream_body.len()
    );
    stream.write_all(resp.as_bytes()).await.unwrap();
    stream.write_all(upstream_body).await.unwrap();
    stream.flush().await.unwrap();
  });

  let mut cfg = Config::default();
  cfg.server.route_mode = RouteMode::Passthrough;
  let events = Arc::new(EventBus::new(128));
  let mut rx = events.subscribe();
  let state = build_proxy_state(&cfg, &[], events).unwrap();

  let req = Request::builder()
    .method(Method::POST)
    .uri("/v1/experimental/agents")
    .header("content-type", "application/json")
    .body(())
    .unwrap();
  let (parts, ()) = req.into_parts();

  let resp = proxy_passthrough_via_pipeline_inner(
    &state,
    &addr.ip().to_string(),
    addr.port(),
    "http",
    None,
    Some(addr.to_string()),
    parts,
    Bytes::from_static(br#"{"model":"gpt-4.1"}"#),
  )
  .await;

  assert_eq!(resp.status(), StatusCode::OK);
  server.await.unwrap();

  let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
  loop {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    assert!(!remaining.is_zero(), "timed out waiting for Started event");
    let Ok(Ok(ev)) = tokio::time::timeout(remaining, rx.recv()).await else {
      continue;
    };
    let CoreEvent::Requests(req) = &*ev else { continue };
    if let RequestEventPayload::Stage(StageEvent::Started { request_endpoint }) = &req.payload {
      assert_eq!(
        request_endpoint,
        &RequestEndpoint::CustomPath("/v1/experimental/agents".into())
      );
      break;
    }
  }
}

#[tokio::test]
async fn proxy_switch_rejects_unrecognized_provider_url() {
  let mut cfg = Config::default();
  cfg.server.route_mode = RouteMode::Switch;
  let events = Arc::new(EventBus::new(64));
  let state = build_proxy_state(&cfg, &[openai_account()], events).unwrap();

  let req = Request::builder()
    .method(Method::POST)
    .uri("/not-a-provider")
    .header("content-type", "application/json")
    .body(())
    .unwrap();
  let (parts, ()) = req.into_parts();

  let resp = proxy_switch_via_pipeline_inner(
    &state,
    "127.0.0.1",
    8080,
    "http",
    None,
    None,
    parts,
    Bytes::from_static(br#"{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}"#),
  )
  .await;

  assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
  let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
  let body = String::from_utf8_lossy(&body);
  assert!(body.contains("recognized provider URL"), "unexpected body: {body}");
}

#[tokio::test]
async fn proxy_switch_rejects_a_provider_outside_the_api_key_allowlist() {
  let mut cfg = Config::default();
  cfg.server.route_mode = RouteMode::Switch;
  let events = Arc::new(EventBus::new(32));
  let mut receiver = events.subscribe();
  let state = build_proxy_state(&cfg, &[openai_account()], events).unwrap();

  let mut req = Request::builder()
    .method(Method::GET)
    .uri("/v1/models")
    .body(())
    .unwrap();
  req.extensions_mut().insert(AccessContext {
    key_id: Some("key-test".into()),
    key_name: Some("restricted-client".into()),
    providers: ProviderAccess::from_provider_ids(vec!["deepseek".into()]).unwrap(),
  });
  let (parts, ()) = req.into_parts();

  let resp =
    proxy_switch_via_pipeline_inner(&state, "api.openai.com", 443, "https", None, None, parts, Bytes::new()).await;

  assert_eq!(resp.status(), StatusCode::FORBIDDEN);
  let mut attribution = None;
  while let Ok(event) = receiver.try_recv() {
    if let Event::Requests(request) = event.as_ref() {
      if let RequestEventPayload::Record(RecordEvent::InboundConnection { user, api_key_id, .. }) = &request.payload {
        attribution = Some((user.clone(), api_key_id.clone()));
        break;
      }
    }
  }
  let (user, api_key_id) = attribution.expect("proxy switch inbound attribution");
  assert_eq!(user.as_deref(), Some("restricted-client"));
  assert_eq!(api_key_id.as_deref(), Some("key-test"));
}

#[tokio::test]
async fn proxy_switch_forwards_custom_path_with_router_auth() {
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();
  let (req_tx, req_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
  let server = tokio::spawn(async move {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut buf = vec![0_u8; 8192];
    let n = stream.read(&mut buf).await.unwrap();
    buf.truncate(n);
    let body = br#"{"object":"list","data":[]}"#;
    let resp = format!(
      "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
      body.len()
    );
    stream.write_all(resp.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream.flush().await.unwrap();
    let _ = req_tx.send(buf);
  });

  let mut cfg = Config::default();
  cfg.server.route_mode = RouteMode::Switch;
  let events = Arc::new(EventBus::noop());
  let state = build_proxy_state(&cfg, &[llama_cpp_account()], events).unwrap();

  let req = Request::builder()
    .method(Method::GET)
    .uri("/models")
    .header("authorization", "Bearer client-secret-must-not-leak")
    .body(())
    .unwrap();
  let (parts, ()) = req.into_parts();

  let resp = proxy_switch_via_pipeline_inner(
    &state,
    &addr.ip().to_string(),
    addr.port(),
    "http",
    None,
    None,
    parts,
    Bytes::new(),
  )
  .await;

  assert_eq!(resp.status(), StatusCode::OK);
  server.await.unwrap();
  let raw_req = String::from_utf8_lossy(&req_rx.await.unwrap()).to_ascii_lowercase();
  assert!(raw_req.starts_with("get /models "), "unexpected request:\n{raw_req}");
  assert!(raw_req.contains("authorization: bearer sk-router"));
  assert!(!raw_req.contains("client-secret-must-not-leak"));
}
