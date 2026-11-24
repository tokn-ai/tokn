//! No-op ConvertRequest stage. Echoes the inbound body and bytes
//! unchanged; `content_encoding` is propagated from the [`Extracted`]
//! payload so a transitional Profile that uses [`NoopConvertRequest`]
//! still produces a well-formed [`ConvertedRequest`].

use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::error::PipelineError;
use crate::pipeline::stages::{ConvertRequestStage, ConvertedRequest, Extracted, Resolved};
use async_trait::async_trait;

pub struct NoopConvertRequest;

#[async_trait]
impl ConvertRequestStage for NoopConvertRequest {
  async fn convert_request(
    &self,
    _ctx: &PipelineCtx,
    extracted: &Extracted,
    _resolved: &Resolved,
  ) -> Result<ConvertedRequest, PipelineError> {
    Ok(ConvertedRequest {
      upstream_body: extracted.body_json.clone(),
      upstream_wire_body: extracted.raw_body.clone(),
      debug_outbound_body: extracted.decoded_body.clone(),
      content_encoding: extracted.content_encoding,
    })
  }
}
