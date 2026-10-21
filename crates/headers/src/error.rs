//! Errors produced by the headers crate.

use thiserror::Error;
use tokn_core::types::SmolStr;

/// All errors that can be produced when parsing or building headers via the
/// schema layer.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
  /// A required header was absent from the input map.
  #[error("required header missing: {name}")]
  MissingHeader { name: SmolStr },

  /// A header was present but its value did not match the expected shape.
  #[error("invalid value for header {name}: {value:?} ({message})")]
  InvalidValue {
    name: SmolStr,
    value: SmolStr,
    message: SmolStr,
  },
}
