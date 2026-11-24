//! Header normalizers rebuild client-derived maps into provider-owned wire
//! shapes after credentials have been injected.

use crate::{AgentId, HeaderMap, TemplateVars};

#[derive(Debug, Clone, Copy)]
pub struct HeaderNormalizeCtx<'a> {
  pub agent_id: &'a AgentId,
  pub stream: bool,
  pub content_encoding: Option<&'a str>,
  pub vars: &'a TemplateVars,
}

/// Rebuild a header map into a known provider/client wire shape.
pub trait HeaderNormalizer {
  fn normalize(&self, headers: &HeaderMap, ctx: &HeaderNormalizeCtx<'_>) -> HeaderMap;
}
