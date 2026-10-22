//! Runner that drives a [`Profile`] through the 6-stage pipeline.
//!
//! Responsibilities:
//!
//! * Build a fresh [`PipelineCtx`] from the inbound [`RawInbound`].
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

pub mod config;
pub mod ctx;
pub mod error;
pub mod stages;

use crate::event::{EventBus, Stage, StageEvent};
use crate::profile::Profile;
pub use config::{RunConfig, RunConfigBuilder};
use ctx::PipelineCtx;
use error::PipelineError;
use smol_str::SmolStr;
pub use stages::{
  BuildHeadersStage, BuiltHeaders, ConvertRequestStage, ConvertResponseStage, ConvertedRequest, ConvertedResponse,
  ExtractStage, Extracted, RawInbound, ResolveStage, Resolved, SendStage, SentResponse,
};
use std::sync::Arc;
use std::time::Duration;
use tokn_core::event::Event as CoreEvent;
use tokn_core::request_event::{RequestEvent, RequestEventPayload};

/// Alias for clarity at call sites — the same type as [`PipelineRunner`].
pub type Pipeline = PipelineRunner;

pub struct PipelineRunner {
  pub profile: Arc<Profile>,
  pub events: Arc<EventBus>,
  retry_policy: RetryPolicy,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RetryPolicy {
  pub max_retries: u32,
  pub initial_backoff: Duration,
  replay_safety: ReplaySafety,
}

#[derive(Clone, Copy, Debug, Default)]
enum ReplaySafety {
  #[default]
  Buffered,
  SafeMethods,
}

impl RetryPolicy {
  pub const fn new(max_retries: u32, initial_backoff: Duration) -> Self {
    Self {
      max_retries,
      initial_backoff,
      replay_safety: ReplaySafety::Buffered,
    }
  }

  /// Retry only HTTP methods defined as safe by RFC 9110. Requests without a
  /// transport method are not retried under this policy.
  pub const fn safe_methods(max_retries: u32, initial_backoff: Duration) -> Self {
    Self {
      max_retries,
      initial_backoff,
      replay_safety: ReplaySafety::SafeMethods,
    }
  }

  fn should_retry(&self, attempt: u32, err: &PipelineError, config: &RunConfig) -> bool {
    attempt < self.max_retries
      && err.stage == crate::event::Stage::Send
      && err.recoverable
      && !err.stop
      && self.request_is_replayable(config)
  }

  fn request_is_replayable(&self, config: &RunConfig) -> bool {
    match self.replay_safety {
      ReplaySafety::Buffered => true,
      ReplaySafety::SafeMethods => config.request_method().is_some_and(|method| {
        method == http::Method::GET
          || method == http::Method::HEAD
          || method == http::Method::OPTIONS
          || method == http::Method::TRACE
      }),
    }
  }

  fn backoff_for(&self, attempt: u32) -> Duration {
    crate::driver::backoff(self.initial_backoff, attempt)
  }
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
  /// bag. The bag is attached to [`PipelineCtx::config`] (wrapped in an
  /// `Arc`) and is visible to every stage via `ctx.config`. Used by
  /// secondary pipeline variants (proxy passthrough) that thread
  /// transport-level hints down to custom Resolve / Send stages.
  pub async fn run_with(&self, raw: RawInbound, config: RunConfig) -> Result<ConvertedResponse, PipelineError> {
    let request_id = raw.request_id.clone().unwrap_or_else(|| SmolStr::new(uuid_like()));
    crate::driver::run(StandardSession {
      runner: self,
      raw,
      config: Arc::new(config),
      request_id,
      ctx: None,
      terminal: None,
    })
    .await
  }
}

/// Adapts the existing dynamic std stages to the common pipeline driver.
struct StandardSession<'a> {
  runner: &'a PipelineRunner,
  raw: RawInbound,
  config: Arc<RunConfig>,
  request_id: SmolStr,
  ctx: Option<PipelineCtx>,
  terminal: Option<AttemptTerminalGuard>,
}

impl StandardSession<'_> {
  fn stage(&mut self, stage: Stage) {
    self.terminal.as_mut().expect("attempt started").set_stage(stage);
  }

  fn ctx(&self) -> &PipelineCtx {
    self.ctx.as_ref().expect("attempt started")
  }
}

impl crate::driver::Session for StandardSession<'_> {
  type Extracted = Extracted;
  type Resolved = Resolved;
  type Headers = BuiltHeaders;
  type Converted = ConvertedRequest;
  type Sent = SentResponse;
  type Output = ConvertedResponse;
  type Error = PipelineError;

  fn start(&mut self, attempt: u32) {
    let ctx = PipelineCtx::new_with_attempt_and_config(
      self.request_id.clone(),
      attempt,
      self.raw.request_endpoint.clone(),
      self.runner.events.clone(),
      self.config.clone(),
    );
    ctx.emit_stage(StageEvent::Started {
      request_endpoint: self.raw.request_endpoint.clone(),
    });
    self.terminal = Some(AttemptTerminalGuard::new(&ctx));
    self.ctx = Some(ctx);
  }

  async fn extract(&mut self) -> Result<Extracted, PipelineError> {
    self.stage(Stage::Extract);
    let output = self
      .runner
      .profile
      .extract
      .extract(self.ctx(), self.raw.clone())
      .await?;
    self.ctx().emit_stage(StageEvent::Extract((&output).into()));
    Ok(output)
  }

  async fn resolve(&mut self, extracted: &Extracted) -> Result<Resolved, PipelineError> {
    self.stage(Stage::Resolve);
    let output = self.runner.profile.resolve.resolve(self.ctx(), extracted).await?;
    self.ctx().emit_stage(StageEvent::Resolve((&output).into()));
    Ok(output)
  }

  async fn build_headers(&mut self, extracted: &Extracted, resolved: &Resolved) -> Result<BuiltHeaders, PipelineError> {
    self.stage(Stage::BuildHeaders);
    let output = self
      .runner
      .profile
      .build_headers
      .build_headers(self.ctx(), extracted, resolved)
      .await?;
    self.ctx().emit_stage(StageEvent::BuildHeaders((&output).into()));
    Ok(output)
  }

  async fn convert_request(
    &mut self,
    extracted: &Extracted,
    resolved: &Resolved,
    _headers: &BuiltHeaders,
  ) -> Result<ConvertedRequest, PipelineError> {
    self.stage(Stage::ConvertRequest);
    let output = self
      .runner
      .profile
      .convert_request
      .convert_request(self.ctx(), extracted, resolved)
      .await?;
    self.ctx().emit_stage(StageEvent::ConvertRequest((&output).into()));
    Ok(output)
  }

  async fn send(
    &mut self,
    extracted: &Extracted,
    resolved: &Resolved,
    headers: &BuiltHeaders,
    converted: &ConvertedRequest,
  ) -> Result<SentResponse, PipelineError> {
    self.stage(Stage::Send);
    let output = self
      .runner
      .profile
      .send
      .send(self.ctx(), extracted, resolved, headers, converted)
      .await?;
    self.ctx().emit_stage(StageEvent::Send((&output).into()));
    Ok(output)
  }

  async fn convert_response(mut self, sent: SentResponse) -> Result<ConvertedResponse, PipelineError> {
    self.stage(Stage::ConvertResponse);
    let result = self
      .runner
      .profile
      .convert_response
      .convert_response(self.ctx(), sent)
      .await;
    match result {
      Ok(output) => {
        self.ctx().emit_stage(StageEvent::ConvertResponse((&output).into()));
        if let stages::ConvertedBody::Buffered { body_bytes, .. } = &output.body {
          self
            .ctx()
            .emit_record(tokn_core::request_event::RecordEvent::ConvertedBody {
              body: body_bytes.clone(),
              error: None,
            });
          self.ctx().emit_stage(StageEvent::Completed {
            success: true,
            attempts: self.ctx().attempt + 1,
          });
        }
        self.terminal.as_mut().expect("attempt started").mark_terminal();
        Ok(output)
      }
      Err(error) => {
        self.failed(self.ctx().attempt, &error);
        Err(error)
      }
    }
  }

  fn failed(&mut self, attempt: u32, error: &PipelineError) -> Option<Duration> {
    self.ctx().emit_stage(StageEvent::Error {
      stage: error.stage,
      message: SmolStr::new(error.message().as_ref()),
      recoverable: error.recoverable,
      stop: error.stop,
    });
    self.ctx().emit_stage(StageEvent::Completed {
      success: false,
      attempts: attempt + 1,
    });
    self.terminal.as_mut().expect("attempt started").mark_terminal();
    self
      .runner
      .retry_policy
      .should_retry(attempt, error, self.config.as_ref())
      .then(|| self.runner.retry_policy.backoff_for(attempt))
  }

  async fn sleep(&mut self, delay: Duration) {
    tokio::time::sleep(delay).await;
  }
}

struct AttemptTerminalGuard {
  request_id: SmolStr,
  attempt: u32,
  attempts: u32,
  events: Arc<EventBus>,
  stage: Stage,
  terminal: bool,
}

impl AttemptTerminalGuard {
  fn new(ctx: &PipelineCtx) -> Self {
    Self {
      request_id: ctx.request_id.clone(),
      attempt: ctx.attempt,
      attempts: ctx.attempt + 1,
      events: ctx.events.clone(),
      stage: Stage::Extract,
      terminal: false,
    }
  }

  fn set_stage(&mut self, stage: Stage) {
    self.stage = stage;
  }

  fn mark_terminal(&mut self) {
    self.terminal = true;
  }

  fn emit_stage(&self, payload: StageEvent) {
    self.events.emit(CoreEvent::Requests(RequestEvent {
      request_id: self.request_id.clone(),
      attempt: self.attempt,
      ts: tokn_core::util::now_unix_ms(),
      payload: RequestEventPayload::Stage(payload),
    }));
  }
}

impl Drop for AttemptTerminalGuard {
  fn drop(&mut self) {
    if self.terminal {
      return;
    }
    self.emit_stage(StageEvent::Error {
      stage: self.stage,
      message: SmolStr::new("pipeline attempt cancelled before completion"),
      recoverable: false,
      stop: false,
    });
    self.emit_stage(StageEvent::Completed {
      success: false,
      attempts: self.attempts,
    });
  }
}

/// Cheap unique-ish id without pulling in the `uuid` crate. The runner only
/// uses this when the caller did not supply a request id (tests, smoke
/// fixtures); production transports always populate `RawInbound.request_id`.
fn uuid_like() -> String {
  use std::sync::atomic::{AtomicU64, Ordering};
  static COUNTER: AtomicU64 = AtomicU64::new(0);
  let n = COUNTER.fetch_add(1, Ordering::Relaxed);
  let ts = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map(|d| d.as_nanos())
    .unwrap_or(0);
  format!("req-{ts:032x}-{n:08x}")
}

// `Stage` is re-exported at the crate root via `lib.rs`.
