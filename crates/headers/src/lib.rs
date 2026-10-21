//! Header parsing, schemas, and personas built on shared core header values.
//!
//! This crate provides:
//!
//! * [`HeaderName`] — a case- and order-preserving header name backed by [`tokn_core::types::SmolStr`].
//! * [`HeaderValue`] — a header value stored as `Cow<'static, str>` for zero-cost
//!   static defaults.
//! * [`HeaderMap`] — a `Vec`-backed map that preserves insertion order and original
//!   case while supporting case-insensitive lookup and duplicate names.
//! * [`keys`] — a catalogue of static [`HeaderName`] constants for popular headers.
//! * [`TemplateVars`] — per-request correlation metadata extracted from inbound
//!   headers, shared between header rendering and provider header construction.
//! * [`HeaderSchema`] — a trait implemented by typed (provider, client) header
//!   structs to round-trip between their typed form and a [`HeaderMap`].
//! * [`HeaderNormalizer`] — a trait for rebuilding client-derived headers into
//!   a provider-owned wire shape.
//! * [`schemas`] — concrete client/overlay structs implementing [`HeaderSchema`].
//! * [`agent`] — agent-specific outbound header builders.
//! * [`registry`] — runtime lookup of (`AgentKind`, `OverlayKind`) for a given
//!   `(provider_id, agent_id)` pair.
//!
//! Header values and correlation primitives are re-exported from `tokn-core`,
//! keeping one representation across configuration, providers, and execution.
//! Reqwest compatibility is available with the default `std` feature.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;
#[cfg(test)]
extern crate std;

pub mod agent;
pub mod agent_id;
pub mod error;
pub mod inbound;
pub mod keys;
pub mod map;
pub mod name;
pub mod normalizer;
pub mod registry;
#[cfg(feature = "std")]
pub use tokn_core::headers::reqwest_compat;
pub mod schema;
pub mod schemas;
pub mod value;
pub mod vars;

pub use agent_id::AgentId;
pub use error::Error;
pub use map::HeaderMap;
pub use name::HeaderName;
pub use normalizer::{HeaderNormalizeCtx, HeaderNormalizer};
pub use schema::HeaderSchema;
pub use value::HeaderValue;
pub use vars::TemplateVars;
