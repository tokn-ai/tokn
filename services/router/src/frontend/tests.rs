use super::*;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::StatusCode;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokn_core::event::Event;
use tokn_core::request_event::{IngressRejection, RecordEvent, RequestEventPayload};
use tower::ServiceExt;

struct Dispatcher {
  calls: Arc<AtomicUsize>,
  response: Option<StatusCode>,
}

#[async_trait]
impl RequestDispatcher for Dispatcher {
  async fn dispatch(&self, _: DispatchContext, _: Request) -> anyhow::Result<Response> {
    self.calls.fetch_add(1, Ordering::SeqCst);
    match self.response {
      Some(status) => Ok(status.into_response()),
      None => anyhow::bail!("fixture worker unavailable"),
    }
  }
}

fn frontend(
  auth: &str,
  response: Option<StatusCode>,
) -> (Frontend, tokio::sync::broadcast::Receiver<Arc<Event>>, Arc<AtomicUsize>) {
  let config = format!(
    r#"
schema_version = 2
[service.request_limits]
max_wire_bytes = 4
[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "{auth}"
[listeners.proxy]
kind = "forward_proxy"
bind = "127.0.0.1:8080"
client_auth = "none"
request_body_max_bytes = 4
default_http_action = {{ kind = "route", profile = "default" }}
default_connect = "reject"
[profiles.default]
route = "default"
[routes.default]
kind = "managed"
providers = ["openai"]
provider = {{ kind = "any" }}
model = {{ kind = "capability" }}
operation = "translate_compatible"
"#
  );
  let (plan, service) = tokn_config::v2::parse_config(&config, Path::new("frontend-tests.toml"))
    .unwrap()
    .into_parts();
  let events = Arc::new(EventBus::new(16));
  let receiver = events.subscribe();
  let calls = Arc::new(AtomicUsize::new(0));
  let frontend = Frontend::new_with_events(
    plan,
    service,
    Arc::new(AccessStore::disabled()),
    Arc::new(Dispatcher {
      calls: calls.clone(),
      response,
    }),
    events,
  )
  .unwrap();
  (frontend, receiver, calls)
}

fn app(
  auth: &str,
  response: Option<StatusCode>,
) -> (Router, tokio::sync::broadcast::Receiver<Arc<Event>>, Arc<AtomicUsize>) {
  let (mut frontend, receiver, calls) = frontend(auth, response);
  (frontend.api.pop().unwrap().1, receiver, calls)
}

fn rejected(receiver: &mut tokio::sync::broadcast::Receiver<Arc<Event>>, id: &str) -> IngressRejection {
  let event = receiver.try_recv().expect("frontend rejection record");
  let Event::Requests(event) = &*event else {
    panic!("expected request event");
  };
  assert_eq!(event.request_id, id);
  assert_eq!(event.attempt, 0);
  let RequestEventPayload::Record(RecordEvent::IngressRejected(rejection)) = &event.payload else {
    panic!("expected ingress rejection");
  };
  assert!(receiver.try_recv().is_err(), "only one rejection is emitted");
  rejection.clone()
}

#[tokio::test]
async fn local_rejections_keep_the_request_id_and_do_not_dispatch() {
  for (auth, method, path, body, status) in [
    ("none", Method::POST, "/unknown", "{}", StatusCode::NOT_FOUND),
    (
      "local_keys",
      Method::POST,
      "/v1/responses",
      "{}",
      StatusCode::UNAUTHORIZED,
    ),
    ("none", Method::CONNECT, "/v1/responses", "{}", StatusCode::BAD_REQUEST),
    (
      "none",
      Method::POST,
      "/v1/responses",
      "12345",
      StatusCode::PAYLOAD_TOO_LARGE,
    ),
  ] {
    let (app, mut receiver, calls) = app(auth, None);
    let response = app
      .oneshot(
        Request::builder()
          .method(method.clone())
          .uri(path)
          .header("x-request-id", "req-frontend-rejected")
          .header("authorization", "Bearer private-test-value")
          .body(Body::from(body))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), status, "{auth} {method} {path}");
    assert_eq!(response.headers()["x-request-id"], "req-frontend-rejected");
    let rejection = rejected(&mut receiver, "req-frontend-rejected");
    assert_eq!(rejection.status, status.as_u16());
    assert_eq!(rejection.method, method.as_str());
    assert_eq!(rejection.url.as_deref(), Some(path));
    assert_eq!(
      rejection.headers.get("authorization").unwrap().as_str(),
      "Bearer private-test-value"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
  }
}

#[tokio::test]
async fn body_read_errors_are_recorded_without_dispatch() {
  let (app, mut receiver, calls) = app("none", None);
  let body = Body::from_stream(futures_util::stream::once(async {
    Err::<bytes::Bytes, _>(std::io::Error::other("fixture body read failed"))
  }));
  let response = app
    .oneshot(Request::post("/v1/responses").body(body).unwrap())
    .await
    .unwrap();
  assert_eq!(response.status(), StatusCode::BAD_REQUEST);
  let request_id = response.headers()["x-request-id"].to_str().unwrap();
  let rejection = rejected(&mut receiver, request_id);
  assert_eq!(rejection.error, "could not read request body");
  assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_request_ids_use_one_generated_id_for_the_record_and_response() {
  let (app, mut receiver, calls) = app("none", None);
  let mut request = Request::post("/v1/responses").body(Body::from("12345")).unwrap();
  request
    .headers_mut()
    .insert("x-request-id", axum::http::HeaderValue::from_bytes(&[0xff]).unwrap());
  let response = app.oneshot(request).await.unwrap();
  assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
  let request_id = response.headers()["x-request-id"].to_str().unwrap();
  assert!(request_id.starts_with("req-"));
  let rejection = rejected(&mut receiver, request_id);
  assert_eq!(rejection.headers.get("x-request-id").unwrap().as_str(), request_id);
  assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unavailable_worker_is_recorded_but_worker_error_responses_are_not() {
  for worker_response in [None, Some(StatusCode::PAYLOAD_TOO_LARGE)] {
    let (app, mut receiver, calls) = app("none", worker_response);
    let response = app
      .oneshot(
        Request::post("/v1/responses")
          .header("x-request-id", "req-worker-result")
          .body(Body::from("{}"))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    if let Some(status) = worker_response {
      assert_eq!(response.status(), status);
      assert!(receiver.try_recv().is_err(), "workers own dispatched request records");
    } else {
      assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
      let rejection = rejected(&mut receiver, "req-worker-result");
      assert_eq!(rejection.error, "gateway worker unavailable");
    }
  }
}

#[tokio::test]
async fn proxy_rejection_retains_authority_and_authenticated_client_context() {
  let (mut frontend, mut receiver, calls) = frontend("none", None);
  let state = frontend.proxy.pop().unwrap().1;
  let ingress = IngressAuthority::from_http(
    tokn_policy::CanonicalAuthority::parse("api.example.test").unwrap(),
    std::num::NonZeroU16::new(80).unwrap(),
  );
  let response = state
    .dispatch_http(
      &ingress,
      "http",
      AccessContext {
        key_id: Some("key-123".into()),
        key_name: Some("fixture client".into()),
        providers: tokn_access::ProviderAccess::All,
      },
      InboundConnectionInfo {
        local_addr: Some("127.0.0.1:8080".parse().unwrap()),
        peer_addr: Some("127.0.0.1:1234".parse().unwrap()),
      },
      Request::post("/v1/responses?debug=1")
        .header("x-request-id", "req-proxy-rejected")
        .body(Body::from("12345"))
        .unwrap(),
    )
    .await;
  assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
  assert_eq!(response.headers()["x-request-id"], "req-proxy-rejected");
  assert_eq!(calls.load(Ordering::SeqCst), 0);
  let rejection = rejected(&mut receiver, "req-proxy-rejected");
  assert_eq!(rejection.pipeline_id, "proxy");
  assert_eq!(
    rejection.url.as_deref(),
    Some("http://api.example.test:80/v1/responses?debug=1")
  );
  assert_eq!(rejection.user.as_deref(), Some("fixture client"));
  assert_eq!(rejection.api_key_id.as_deref(), Some("key-123"));
  assert_eq!(rejection.local_addr.as_deref(), Some("127.0.0.1:8080"));
  assert_eq!(rejection.peer_addr.as_deref(), Some("127.0.0.1:1234"));
}
