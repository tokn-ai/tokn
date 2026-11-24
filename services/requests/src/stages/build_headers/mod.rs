//! BuildHeaders stage impls.
//!
//! - [`NoopBuildHeaders`] returns an empty header set; useful for tests and
//!   for the `without_send` profile when callers don't care about outbound
//!   headers.
//! - [`DefaultBuildHeaders`] composes the real outbound `HeaderMap` from the
//!   inbound request via the [`tokn_headers`] agent + overlay registry.

pub mod default;
pub mod passthrough;

use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::error::PipelineError;
use crate::pipeline::stages::{BuildHeadersStage, BuiltHeaders, Extracted, Resolved};
use async_trait::async_trait;

pub use default::DefaultBuildHeaders;
pub use passthrough::PassthroughBuildHeaders;

/// No-op BuildHeaders stage. Returns an empty header set. Available as a
/// placeholder for tests and profiles that short-circuit before Send.
pub struct NoopBuildHeaders;

#[async_trait]
impl BuildHeadersStage for NoopBuildHeaders {
  async fn build_headers(
    &self,
    _ctx: &PipelineCtx,
    _extracted: &Extracted,
    _resolved: &Resolved,
  ) -> Result<BuiltHeaders, PipelineError> {
    Ok(BuiltHeaders::default())
  }
}
