//! Conversions from requests's full stage-output structs into the cloneable
//! `*Summary` types defined in `tokn_core::request_event`. The runner uses
//! these `From` impls at emit time so subscribers receive tokn-core types
//! while stages keep operating on the richer requests-internal structs.

use crate::pipeline::stages::{
  BuiltHeaders, ConvertedBody, ConvertedRequest, ConvertedResponse, Extracted, Resolved, SentResponse,
};
use tokn_core::request_event::{
  BuiltHeadersSummary, ConvertedRequestSummary, ConvertedResponseSummary, ExtractedSummary, ResolvedSummary,
  SentSummary,
};

impl From<&Extracted> for ExtractedSummary {
  fn from(e: &Extracted) -> Self {
    Self {
      agent_id: e.agent_id.clone(),
      model: e.model.clone(),
      stream: e.stream,
      session_id: e.session_id.clone(),
      project_id: e.project_id.clone(),
      initiator: e.initiator.clone(),
      header_initiator: e.header_initiator.clone(),
      request_classification: e.request_classification,
      route_mode_hint: e.route_mode_hint.clone(),
      headers: e.headers.clone(),
      raw_body: e.raw_body.clone(),
      decoded_body: e.decoded_body.clone(),
      body_json: e.body_json.clone(),
    }
  }
}

impl From<&Resolved> for ResolvedSummary {
  fn from(r: &Resolved) -> Self {
    Self {
      agent_id: r.agent_id.clone(),
      model: r.model.clone(),
      resolved_endpoint: r.route.resolved_endpoint(),
      upstream_model: r.upstream_model.clone(),
      upstream_endpoint: r.route.upstream_endpoint(),
      account_id: r.account_id.clone(),
      provider_id: r.provider_id.clone(),
    }
  }
}

impl From<&BuiltHeaders> for BuiltHeadersSummary {
  fn from(h: &BuiltHeaders) -> Self {
    Self {
      headers: h.headers.clone(),
      vars: h.vars.clone(),
      agent_id: h.agent_id.clone(),
    }
  }
}

impl From<&ConvertedRequest> for ConvertedRequestSummary {
  fn from(c: &ConvertedRequest) -> Self {
    Self {
      upstream_body: c.upstream_body.clone(),
      upstream_wire_body: c.upstream_wire_body.clone(),
      debug_outbound_body: c.debug_outbound_body.clone(),
      content_encoding: c.content_encoding.map(|e| smol_str::SmolStr::new(e.as_str())),
    }
  }
}

impl From<&SentResponse> for SentSummary {
  fn from(s: &SentResponse) -> Self {
    Self {
      status: s.status,
      headers: s.headers.clone(),
      upstream_endpoint: s.upstream_endpoint,
      stream: s.stream,
    }
  }
}

impl From<&ConvertedResponse> for ConvertedResponseSummary {
  fn from(c: &ConvertedResponse) -> Self {
    Self {
      status: c.status(),
      headers: c.headers().clone(),
      body: match &c.body {
        ConvertedBody::Buffered { body_json, .. } => body_json.clone(),
        ConvertedBody::Stream { .. } => None,
      },
    }
  }
}
