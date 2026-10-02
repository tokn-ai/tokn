//! Resolve stage — picks the upstream account and upstream model for an
//! extracted request.
//!
//! [`PoolResolve`] is the production wiring (PR2 onward) but in PR1 it depends
//! on a small [`AccountSelector`] trait rather than the legacy
//! `crates/router::accounts::AccountPool`. This keeps requests free of any
//! dependency on the legacy crate; PR2 will provide a real implementation of
//! [`AccountSelector`] backed by the existing pool (or its successor in a
//! shared crate).

use crate::event::Stage;
use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::error::{PipelineError, RequestsError};
use crate::pipeline::stages::{Extracted, ResolveStage, Resolved, ResolvedRoute};
use async_trait::async_trait;
use smol_str::SmolStr;
use std::sync::Arc;
use tokn_accounts::AccountHandle;
use tokn_core::provider::Endpoint;

/// Outcome of consulting an account pool for a given extracted request.
pub enum SelectorOutcome {
  /// An account was selected. The handle is typed (not `Arc<dyn Any>`)
  /// so back-half stages can reach the provider via
  /// `handle.provider.input_transformer()` without a downcast.
  Selected {
    account_id: SmolStr,
    provider_id: SmolStr,
    upstream_endpoint: Option<Endpoint>,
    upstream_model: SmolStr,
    account_handle: Arc<AccountHandle>,
  },
  /// A session-affinity binding existed but its account is no longer
  /// available; the caller's session has effectively expired.
  SessionExpired { session_id: SmolStr },
  /// No account supports this endpoint+model combination.
  NoAccount,
  /// A matching provider exists but is outside the caller's allowlist.
  ProviderAccessDenied,
}

#[async_trait]
pub trait AccountSelector: Send + Sync {
  async fn select(&self, ctx: &PipelineCtx, extracted: &Extracted) -> Result<SelectorOutcome, PipelineError>;
}

pub struct PoolResolve<S: AccountSelector> {
  pub selector: Arc<S>,
}

impl<S: AccountSelector> PoolResolve<S> {
  pub fn new(selector: Arc<S>) -> Self {
    Self { selector }
  }
}

#[async_trait]
impl<S: AccountSelector + 'static> ResolveStage for PoolResolve<S> {
  async fn resolve(&self, ctx: &PipelineCtx, extracted: &Extracted) -> Result<Resolved, PipelineError> {
    match self.selector.select(ctx, extracted).await? {
      SelectorOutcome::Selected {
        account_id,
        provider_id,
        upstream_endpoint,
        upstream_model,
        account_handle,
      } => Ok(Resolved {
        agent_id: extracted.agent_id.clone(),
        model: extracted.model.clone(),
        upstream_model,
        route: {
          let resolved_endpoint = ctx.request_endpoint.resolved().ok_or_else(|| {
            PipelineError::permanent(
              Stage::Resolve,
              RequestsError::MissingResolvedEndpoint {
                request_endpoint: SmolStr::new(ctx.request_endpoint.as_str()),
              },
            )
          })?;
          ResolvedRoute::operation(resolved_endpoint, upstream_endpoint.unwrap_or(resolved_endpoint))
        },
        account_id,
        provider_id,
        account_handle,
      }),
      SelectorOutcome::SessionExpired { session_id } => Err(PipelineError::permanent(
        Stage::Resolve,
        RequestsError::SessionExpired { session_id },
      )),
      SelectorOutcome::NoAccount => Err(PipelineError::permanent(
        Stage::Resolve,
        RequestsError::NoAccount {
          endpoint: ctx.request_endpoint.resolved().ok_or_else(|| {
            PipelineError::permanent(
              Stage::Resolve,
              RequestsError::MissingResolvedEndpoint {
                request_endpoint: SmolStr::new(ctx.request_endpoint.as_str()),
              },
            )
          })?,
          model: extracted.model.clone(),
        },
      )),
      SelectorOutcome::ProviderAccessDenied => Err(PipelineError::permanent(
        Stage::Resolve,
        RequestsError::ProviderAccessDenied,
      )),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::event::EventBus;
  use crate::pipeline::stages::Extracted;
  use crate::test_support::mock_handle;
  use bytes::Bytes;
  use tokn_headers::HeaderMap;

  struct FixedSelector(SelectorOutcomeKind);

  enum SelectorOutcomeKind {
    Ok,
    None,
  }

  #[async_trait]
  impl AccountSelector for FixedSelector {
    async fn select(&self, _ctx: &PipelineCtx, _ex: &Extracted) -> Result<SelectorOutcome, PipelineError> {
      Ok(match self.0 {
        SelectorOutcomeKind::Ok => SelectorOutcome::Selected {
          account_id: SmolStr::new("acct-1"),
          provider_id: SmolStr::new("zai-coding-plan"),
          upstream_endpoint: Some(Endpoint::ChatCompletions),
          upstream_model: SmolStr::new("glm-4"),
          account_handle: mock_handle("acct-1", "zai-coding-plan"),
        },
        SelectorOutcomeKind::None => SelectorOutcome::NoAccount,
      })
    }
  }

  fn fake_extracted() -> Extracted {
    Extracted {
      agent_id: None,
      model: SmolStr::new("input-model"),
      stream: false,
      session_id: None,
      project_id: None,
      initiator: None,
      header_initiator: None,
      request_classification: None,
      route_mode_hint: None,
      headers: HeaderMap::new(),
      raw_body: Bytes::new(),
      decoded_body: Bytes::new(),
      body_json: std::sync::Arc::new(serde_json::Value::Null),
      content_encoding: None,
    }
  }

  fn ctx() -> PipelineCtx {
    PipelineCtx::new("req-r", Endpoint::ChatCompletions.into(), Arc::new(EventBus::new(64)))
  }

  #[tokio::test]
  async fn happy_path_carries_upstream_model_and_provider() {
    let stage = PoolResolve::new(Arc::new(FixedSelector(SelectorOutcomeKind::Ok)));
    let res = stage.resolve(&ctx(), &fake_extracted()).await.unwrap();
    assert_eq!(res.upstream_model, "glm-4");
    assert_eq!(res.account_id, "acct-1");
    assert_eq!(res.provider_id, "zai-coding-plan");
    assert_eq!(res.model, "input-model");
  }

  #[tokio::test]
  async fn no_account_yields_permanent_resolve_error() {
    let stage = PoolResolve::new(Arc::new(FixedSelector(SelectorOutcomeKind::None)));
    let err = stage.resolve(&ctx(), &fake_extracted()).await.unwrap_err();
    assert_eq!(err.stage, Stage::Resolve);
    assert!(!err.recoverable);
    assert!(err.message().contains("no account"));
  }
}
