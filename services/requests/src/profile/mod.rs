//! Composition of stages into a complete pipeline definition.
//!
//! A [`Profile`] holds an `Arc<dyn StageTrait>` for each of the six pipeline
//! slots. Profiles are immutable after construction; per-request behavior is
//! varied by the stage implementations themselves (e.g. a `RetrySend` that
//! wraps an inner `SendStage`). This composition-over-configuration approach
//! is the substitute for the per-stage hook abstraction discussed in
//! planning; see crate-level docs.
//!
//! Constructors:
//!
//! * [`Profile::full`] — all six slots provided. The intended production
//!   shape.
//! * [`Profile::without_send`] — convenience for dry-run / smoke flows: the
//!   Send slot is filled with [`NoopSend`](crate::stages::NoopSend), which
//!   halts the pipeline by returning `PipelineError::stop(...)`. The
//!   ConvertResponse slot is filled with [`NoopConvertResponse`] but is
//!   never invoked. Callers detect the stop via `err.stop` on the
//!   `PipelineError` returned by `PipelineRunner::run` and render the
//!   per-stage outputs captured from the [`EventBus`].
//!
//! [`EventBus`]: crate::event::EventBus

use crate::pipeline::stages::{
  BuildHeadersStage, ConvertRequestStage, ConvertResponseStage, ExtractStage, ResolveStage, SendStage,
};
use crate::stages::{NoopConvertResponse, NoopSend};
use smol_str::SmolStr;
use std::sync::Arc;

pub struct Profile {
  pub name: SmolStr,
  pub extract: Arc<dyn ExtractStage>,
  pub resolve: Arc<dyn ResolveStage>,
  pub build_headers: Arc<dyn BuildHeadersStage>,
  pub convert_request: Arc<dyn ConvertRequestStage>,
  pub send: Arc<dyn SendStage>,
  pub convert_response: Arc<dyn ConvertResponseStage>,
}

impl Profile {
  pub fn full(
    name: impl Into<SmolStr>,
    extract: Arc<dyn ExtractStage>,
    resolve: Arc<dyn ResolveStage>,
    build_headers: Arc<dyn BuildHeadersStage>,
    convert_request: Arc<dyn ConvertRequestStage>,
    send: Arc<dyn SendStage>,
    convert_response: Arc<dyn ConvertResponseStage>,
  ) -> Self {
    Self {
      name: name.into(),
      extract,
      resolve,
      build_headers,
      convert_request,
      send,
      convert_response,
    }
  }

  /// Convenience constructor for dry-run / smoke flows. Fills the Send
  /// slot with [`NoopSend`](crate::stages::NoopSend), which halts the
  /// pipeline by returning `PipelineError::stop(...)` after every prior
  /// stage has run. The ConvertResponse slot is filled with
  /// [`NoopConvertResponse`] but is never invoked. Callers branch on
  /// `err.stop` and read per-stage outputs from the event bus.
  pub fn without_send(
    name: impl Into<SmolStr>,
    extract: Arc<dyn ExtractStage>,
    resolve: Arc<dyn ResolveStage>,
    build_headers: Arc<dyn BuildHeadersStage>,
    convert_request: Arc<dyn ConvertRequestStage>,
  ) -> Self {
    Self {
      name: name.into(),
      extract,
      resolve,
      build_headers,
      convert_request,
      send: Arc::new(NoopSend),
      convert_response: Arc::new(NoopConvertResponse),
    }
  }
}
