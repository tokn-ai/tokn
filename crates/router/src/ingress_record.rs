//! Terminal records for requests that never enter a provider pipeline.

use crate::api::error::ApiError;
use crate::v2::InboundConnectionInfo;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use smol_str::SmolStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokn_access::AccessContext;
use tokn_core::event::{Event, EventBus};
use tokn_core::request_event::{IngressRejection, RecordEvent, RequestEndpoint, RequestEvent, RequestEventPayload};

/// Captures metadata only; reading or retaining rejected request bodies is unnecessary.
/// Clones share the handoff flag so an outer admission handler cannot duplicate
/// a failure already recorded by the pipeline.
#[derive(Clone)]
pub(crate) struct IngressRecorder {
  events: Arc<EventBus>,
  request_id: SmolStr,
  rejection: IngressRejection,
  handed_off: Arc<AtomicBool>,
}

impl IngressRecorder {
  pub(crate) fn new<B>(
    events: Arc<EventBus>,
    request: &Request<B>,
    connection: InboundConnectionInfo,
    pipeline_id: &str,
  ) -> Self {
    let request_id = request
      .headers()
      .get(crate::request_id::REQUEST_ID_HEADER)
      .and_then(|value| value.to_str().ok())
      .map(SmolStr::new)
      .unwrap_or_else(|| SmolStr::new(crate::request_id::new_request_id()));
    Self {
      events,
      request_id,
      rejection: IngressRejection {
        started_ts: tokn_core::util::now_unix_ms(),
        request_endpoint: RequestEndpoint::infer_from_path(request.uri().path()),
        status: 0,
        error: SmolStr::default(),
        mode: SmolStr::new(pipeline_id),
        pipeline_id: SmolStr::new(pipeline_id),
        method: SmolStr::new(request.method().as_str()),
        url: Some(SmolStr::new(request.uri().to_string())),
        headers: request.headers().into(),
        user: None,
        api_key_id: None,
        local_addr: connection.local_addr.map(|value| SmolStr::new(value.to_string())),
        peer_addr: connection.peer_addr.map(|value| SmolStr::new(value.to_string())),
      },
      handed_off: Arc::new(AtomicBool::new(false)),
    }
  }

  pub(crate) fn with_access(mut self, access: &AccessContext) -> Self {
    self.rejection.user = access.key_name.clone().map(SmolStr::from);
    self.rejection.api_key_id = access.key_id.clone().map(SmolStr::from);
    self
  }

  pub(crate) fn with_mode(mut self, mode: &str) -> Self {
    self.rejection.mode = SmolStr::new(mode);
    self
  }

  pub(crate) fn with_url(mut self, url: String) -> Self {
    self.rejection.url = Some(SmolStr::new(url));
    self
  }

  pub(crate) fn pipeline_started(&self) {
    self.handed_off.store(true, Ordering::Release);
  }

  pub(crate) fn record(&self, status: StatusCode, error: impl AsRef<str>) {
    if !status.is_client_error() && !status.is_server_error() {
      return;
    }
    if self.handed_off.swap(true, Ordering::AcqRel) {
      return;
    }
    let mut rejection = self.rejection.clone();
    rejection.status = status.as_u16();
    rejection.error = SmolStr::new(error.as_ref());
    self.events.emit(Event::Requests(RequestEvent {
      request_id: self.request_id.clone(),
      attempt: 0,
      ts: tokn_core::util::now_unix_ms(),
      payload: RequestEventPayload::Record(RecordEvent::IngressRejected(rejection)),
    }));
  }

  pub(crate) fn reject(&self, error: ApiError) -> Response {
    self.record(error.status(), error.to_string());
    let mut response = error.into_response();
    if let Ok(request_id) = axum::http::HeaderValue::from_str(&self.request_id) {
      crate::request_id::set_response_request_id(&mut response, request_id);
    }
    response
  }
}
