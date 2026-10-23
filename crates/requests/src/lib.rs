//! Shared request execution for standard and embedded Tokn applications.
//!
//! [`driver`] owns the six-stage sequence and retry control flow. The standard
//! dynamic profiles and configured injected sessions adapt their platform services
//! to that driver; routing, selection, provider preparation, and conversion remain
//! in their domain crates. Standard-only modules retain Tower, HTTP gateway,
//! recording, and custom stage adapters.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod configured;
pub mod driver;
pub mod interpretation;
pub mod retry;
#[cfg(feature = "std")]
pub mod runtime;
pub mod send_policy;

#[cfg(feature = "std")]
pub mod event;
#[cfg(feature = "std")]
pub mod executor;
#[cfg(feature = "std")]
pub mod pipeline;
#[cfg(feature = "std")]
pub mod profile;
#[cfg(feature = "std")]
pub mod stages;
#[cfg(feature = "std")]
pub mod utils;

#[cfg(all(test, feature = "std"))]
pub(crate) mod test_support;

#[cfg(feature = "std")]
pub use event::{CustomEvent, Event, EventBus, EventPayload, Stage, StageEvent};
#[cfg(feature = "std")]
pub use executor::{ExecutionRequest, PipelineRequestContext, PipelineResponseKind, RequestError, RequestService};
#[cfg(feature = "std")]
pub use pipeline::{
  ctx::PipelineCtx, error::PipelineError, stages as stage_traits, Pipeline, PipelineRunner, RawInbound, RunConfig,
  RunConfigBuilder,
};
#[cfg(feature = "std")]
pub use profile::Profile;
pub use retry::{MissingRetryPolicy, RetryFailure, RetryPolicy};

mod header_policy;
