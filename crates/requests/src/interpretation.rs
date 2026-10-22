//! Platform-independent interpretation of inbound request metadata.
//!
//! Managed JSON requests share these rules across the standard and injected
//! executors. Transport decoding, routing, and provider policy remain separate.

use serde_json::Value;
use tokn_core::{
  endpoint::RequestEndpoint,
  request_classification::{classify_request, RequestClassification},
  types::SmolStr,
  util::initiator::classify_initiator_any,
};
use tokn_headers::{
  inbound::{first_present_smol, inbound_correlation, PROJECT_ID_HEADERS},
  HeaderMap,
};

/// Normalized input to routing and provider request preparation.
///
/// An absent model is represented as `unknown`, preserving the standard
/// pipeline's ability to resolve it through a configured default route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestInterpretation {
  pub model: SmolStr,
  pub stream: bool,
  pub session_id: Option<SmolStr>,
  pub project_id: Option<SmolStr>,
  pub initiator: Option<SmolStr>,
  /// Valid explicit initiator, before falling back to body classification.
  pub header_initiator: Option<SmolStr>,
  pub request_classification: Option<RequestClassification>,
  pub route_mode_hint: Option<SmolStr>,
}

impl RequestInterpretation {
  pub fn from_json(endpoint: &RequestEndpoint, headers: &HeaderMap, body: &Value) -> Self {
    let mut interpreted = Self::from_headers(headers).with_body_metadata(
      body.get("model").and_then(Value::as_str),
      body.get("stream").and_then(Value::as_bool),
    );
    interpreted.initiator = interpreted
      .header_initiator
      .clone()
      .or_else(|| classify_initiator_any(body).map(SmolStr::new));
    interpreted.request_classification = classify_request(endpoint, body);
    interpreted
  }

  // Passthrough retains its all-or-nothing typed metadata peek and never
  // infers an initiator from the body. It still uses the same header rules.
  pub(crate) fn from_headers(headers: &HeaderMap) -> Self {
    let header_initiator = headers
      .get("x-initiator")
      .map(|value| value.as_str().trim())
      .and_then(|value| {
        if value.eq_ignore_ascii_case("user") {
          Some(SmolStr::new("user"))
        } else if value.eq_ignore_ascii_case("agent") {
          Some(SmolStr::new("agent"))
        } else {
          None
        }
      });
    let stream = headers.get("accept").is_some_and(|value| {
      value
        .as_str()
        .split(',')
        .any(|part| part.split(';').next().map(str::trim) == Some("text/event-stream"))
    });
    Self {
      model: SmolStr::new("unknown"),
      stream,
      session_id: inbound_correlation(headers).session_id,
      project_id: first_present_smol(headers, PROJECT_ID_HEADERS),
      initiator: header_initiator.clone(),
      header_initiator,
      request_classification: None,
      route_mode_hint: headers
        .get("x-route-mode")
        .map(|value| value.as_str().trim())
        .filter(|value| !value.is_empty())
        .map(SmolStr::new),
    }
  }

  pub(crate) fn with_body_metadata(mut self, model: Option<&str>, stream: Option<bool>) -> Self {
    if let Some(model) = model.filter(|model| !model.is_empty()) {
      self.model = SmolStr::new(model);
    }
    if let Some(stream) = stream {
      self.stream = stream;
    }
    self
  }
}
