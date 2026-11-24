//! No-op Send stage. Returns a `PipelineError::stop` so the runner
//! short-circuits without invoking the network. Used by
//! [`Profile::without_send`](crate::profile::Profile::without_send) for
//! dry-run / smoke flows: the runner emits every prior stage's event
//! (Extract/Resolve/BuildHeaders/ConvertRequest) and then a single Error
//! event tagged `stage = Send, stop = true`. Callers detect the stop flag
//! and render whatever partial state they captured from the bus.

use crate::event::Stage;
use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::error::PipelineError;
use crate::pipeline::stages::{BuiltHeaders, ConvertedRequest, Extracted, Resolved, SendStage, SentResponse};
use async_trait::async_trait;

pub mod default;
pub mod proxy;
pub use default::DefaultSend;
pub use proxy::ProxySend;

use crate::header_policy::remove_body_digests;

fn patch_compaction_routing_hint(
  ctx: &PipelineCtx,
  extracted: &Extracted,
  resolved: &Resolved,
  body: &ConvertedRequest,
  headers: &mut tokn_headers::HeaderMap,
) {
  super::convert_request::apply_compaction_priority_routing_hint(
    super::convert_request::compaction_provider_id(ctx, resolved),
    extracted.request_classification,
    &body.debug_outbound_body,
    headers,
  );
}

pub struct NoopSend;

#[async_trait]
impl SendStage for NoopSend {
  async fn send(
    &self,
    _ctx: &PipelineCtx,
    _extracted: &Extracted,
    _resolved: &Resolved,
    _headers: &BuiltHeaders,
    _body: &ConvertedRequest,
  ) -> Result<SentResponse, PipelineError> {
    Err(PipelineError::stop(Stage::Send))
  }
}
