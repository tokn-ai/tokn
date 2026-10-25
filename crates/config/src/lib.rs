//! Configuration models and compilation of shared execution rules.
//!
//! File loading and gateway service settings are available with `std`.
#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;

#[cfg(feature = "std")]
pub mod error;
#[cfg(feature = "std")]
mod file_identity;
#[cfg(feature = "std")]
pub mod paths;
#[cfg(feature = "std")]
mod schema;
#[cfg(feature = "std")]
mod standard;
pub mod v2;
#[cfg(feature = "std")]
pub use standard::*;

pub const DEFAULT_SESSION_TTL_SECS: u64 = 18_000;

pub(crate) fn model_score_pattern_error(pattern: &str) -> Option<&'static str> {
  if pattern.trim().is_empty() || pattern.trim() != pattern {
    return Some("model score patterns must be non-empty and have no surrounding whitespace");
  }
  let wildcard_count = pattern.bytes().filter(|byte| *byte == b'*').count();
  if wildcard_count > 1 || wildcard_count == 1 && !pattern.ends_with('*') {
    return Some("model score patterns may contain only one wildcard, at the end");
  }
  None
}
