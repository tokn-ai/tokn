//! Re-export shim. The event payload types live in `tokn_core::request_event`
//! and the bus itself is `tokn_core::event::EventBus` (a tokio broadcast
//! channel). This module keeps the historical `crate::event::*` import paths
//! working by re-exporting the relocated types under their old names.
//!
//! Conversions between requests's full stage-output structs and the lossy
//! `*Summary` types in tokn-core live in [`stage`].

pub mod stage;

pub use tokn_core::event::EventBus;
pub use tokn_core::request_event::{
  BuiltHeadersSummary, ConvertedRequestSummary, ConvertedResponseSummary, CustomEvent, ExtractedSummary, RecordEvent,
  RequestEvent as Event, RequestEventPayload as EventPayload, ResolvedSummary, SentSummary, Stage, StageEvent,
};
