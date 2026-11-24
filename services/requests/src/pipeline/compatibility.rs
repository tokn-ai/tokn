//! Compatibility executor for explicit dynamic `Profile` stage extensions.
//!
//! Configured v2 execution uses `crate::session::Session` on both platforms.
//! This facade retains custom stages, legacy gateway configurations, and
//! original-origin proxy stages while those older entry points remain public.

use super::{
  stages, BuiltHeaders, ConvertedRequest, ConvertedResponse, Extracted, PipelineRunner, RawInbound, Resolved,
  RunConfig, SentResponse,
};
use crate::event::{EventBus, Stage, StageEvent};
use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::error::PipelineError;
use crate::retry::RetryFailure;
use smol_str::SmolStr;
use std::{sync::Arc, time::Duration};
use tokn_core::event::Event as CoreEvent;
use tokn_core::request_event::{RequestEvent, RequestEventPayload};

pub(super) async fn run(
  runner: &PipelineRunner,
  raw: RawInbound,
  config: RunConfig,
) -> Result<ConvertedResponse, PipelineError> {
  let request_id = raw.request_id.clone().unwrap_or_else(|| SmolStr::new(uuid_like()));
  crate::driver::run(ProfileExecution {
    runner,
    raw,
    config: Arc::new(config),
    request_id,
    ctx: None,
    terminal: None,
  })
  .await
}

/// Adapts the existing dynamic std stages to the common pipeline driver.
struct ProfileExecution<'a> {
  runner: &'a PipelineRunner,
  raw: RawInbound,
  config: Arc<RunConfig>,
  request_id: SmolStr,
  ctx: Option<PipelineCtx>,
  terminal: Option<AttemptTerminalGuard>,
}

impl ProfileExecution<'_> {
  fn stage(&mut self, stage: Stage) {
    self.terminal.as_mut().expect("attempt started").set_stage(stage);
  }

  fn ctx(&self) -> &PipelineCtx {
    self.ctx.as_ref().expect("attempt started")
  }
}

impl crate::driver::Session for ProfileExecution<'_> {
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
    self.runner.retry_policy.next_delay(
      attempt,
      RetryFailure {
        send_stage: error.stage == Stage::Send,
        recoverable: error.recoverable,
        stop: error.stop,
      },
      self.config.request_method().map(http::Method::as_str),
    )
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
