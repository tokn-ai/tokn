//! Standard request context adapter for portable compaction policy.

use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::stages::Resolved;
pub(crate) use crate::preparation::compaction::requires_compaction_priority;
pub use crate::preparation::{apply_compaction_priority, apply_compaction_priority_routing_hint};

pub(crate) fn compaction_provider_id<'a>(ctx: &'a PipelineCtx, resolved: &'a Resolved) -> &'a str {
  ctx
    .config
    .get_str(crate::stages::resolve::proxy::keys::PROVIDER_DRIVER_ID)
    .unwrap_or_else(|| resolved.account_handle.provider.info().id.as_str())
}
