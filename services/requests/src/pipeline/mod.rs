//! Runner that drives a [`Profile`] through the 6-stage pipeline.
//!
//! Responsibilities:
//!
//! * Build a fresh [`PipelineCtx`](ctx::PipelineCtx) from the inbound [`RawInbound`].
//! * Emit [`StageEvent::Started`] before the first stage and
//!   [`StageEvent::Completed`] after the last (always).
//! * Run each stage; on success, emit the matching per-stage event carrying
//!   the stage's own output (cloned where the type permits); on failure,
//!   emit [`StageEvent::Error`] (with the stage / recoverable / stop flags
//!   pulled verbatim from [`PipelineError`]) followed by `Completed { success
//!   = !err.stop }` and short-circuit.
//! * Return `Result<ConvertedResponse, PipelineError>` to the caller. The
//!   runner does not retain partial state — subscribers that need the
//!   per-stage outputs read them off the [`EventBus`] events, which carry
//!   each stage's own output as the payload.
//! * A stage may halt the pipeline deliberately by returning
//!   `PipelineError::stop(...)`. This is the mechanism used by dry-run
//!   profiles (e.g. [`NoopSend`](crate::stages::NoopSend)): the runner
//!   still emits Error + Completed, but the caller can branch on
//!   `err.stop` to render a successful early-termination report instead
//!   of a failure.
//!
//! Hooks are intentionally absent from PR1.
//!
//! [`Profile`]: crate::profile::Profile
//! [`StageEvent::Started`]: crate::event::StageEvent::Started
//! [`StageEvent::Completed`]: crate::event::StageEvent::Completed
//! [`StageEvent::Error`]: crate::event::StageEvent::Error
//! [`EventBus`]: crate::event::EventBus

mod compatibility;
pub mod config;
pub mod ctx;
pub mod error;
pub mod stages;

use crate::event::EventBus;
use crate::profile::Profile;
pub use crate::retry::RetryPolicy;
pub use config::{RunConfig, RunConfigBuilder};
use error::PipelineError;
pub use stages::{
  BuildHeadersStage, BuiltHeaders, ConvertRequestStage, ConvertResponseStage, ConvertedRequest, ConvertedResponse,
  ExtractStage, Extracted, RawInbound, ResolveStage, Resolved, SendStage, SentResponse,
};
use std::sync::Arc;

/// Alias for clarity at call sites — the same type as [`PipelineRunner`].
pub type Pipeline = PipelineRunner;

pub struct PipelineRunner {
  pub profile: Arc<Profile>,
  pub events: Arc<EventBus>,
  retry_policy: RetryPolicy,
}

impl PipelineRunner {
  pub fn new(profile: Arc<Profile>, events: Arc<EventBus>) -> Self {
    Self::new_with_retry(profile, events, RetryPolicy::default())
  }

  pub fn new_with_retry(profile: Arc<Profile>, events: Arc<EventBus>, retry_policy: RetryPolicy) -> Self {
    Self {
      profile,
      events,
      retry_policy,
    }
  }

  /// Drive the pipeline through all six stages. Returns the final
  /// [`ConvertedResponse`] on success or the originating [`PipelineError`]
  /// on failure. Callers that need partial state (per-stage outputs)
  /// subscribe to the [`EventBus`] — each per-stage `StageEvent` variant
  /// carries that stage's own output.
  ///
  /// A `PipelineError` with `stop == true` indicates a deliberate early
  /// termination (e.g. dry-run); it is still returned as `Err` but should
  /// not be reported as a failure.
  pub async fn run(&self, raw: RawInbound) -> Result<ConvertedResponse, PipelineError> {
    self.run_with(raw, RunConfig::default()).await
  }

  /// Same as [`run`](Self::run) but with a caller-supplied [`RunConfig`]
  /// bag. The bag is attached to [`PipelineCtx::config`](ctx::PipelineCtx::config) (wrapped in an
  /// `Arc`) and is visible to every stage via `ctx.config`. Used by
  /// secondary pipeline variants (proxy passthrough) that thread
  /// transport-level hints down to custom Resolve / Send stages.
  pub async fn run_with(&self, raw: RawInbound, config: RunConfig) -> Result<ConvertedResponse, PipelineError> {
    compatibility::run(self, raw, config).await
  }
}
