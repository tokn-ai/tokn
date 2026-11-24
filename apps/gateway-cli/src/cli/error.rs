//! CLI error type.
//!
//! Subcommands currently still use [`anyhow::Result`] internally. This thin
//! wrapper exists so the top-level [`crate::Error`] can compose CLI failures
//! into the same enum as every other subsystem; the conversion eagerly
//! flattens the anyhow source chain into the displayed message.

use snafu::Snafu;
use std::error::Error as StdError;

#[allow(dead_code)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
  /// Catch-all for legacy anyhow-shaped errors emitted by subcommands.
  ///
  /// `message` is the joined `Display` chain; `root_cause_kind` records
  /// the type name of the deepest source for debugging.
  #[snafu(display("{message}"))]
  Message {
    message: String,
    root_cause_kind: Option<String>,
  },
}

impl Error {
  /// Build a [`Error::Message`] from a free-form string with no source.
  #[allow(dead_code)]
  pub fn msg(s: impl Into<String>) -> Self {
    Error::Message {
      message: s.into(),
      root_cause_kind: None,
    }
  }
}

impl From<anyhow::Error> for Error {
  fn from(e: anyhow::Error) -> Self {
    // Walk the source chain so the user sees `top: middle: leaf`.
    let mut parts = Vec::new();
    parts.push(e.to_string());
    let mut src: Option<&(dyn StdError + 'static)> = e.source();
    let mut last_kind: Option<String> = None;
    while let Some(s) = src {
      let msg = s.to_string();
      // Skip duplicates that anyhow sometimes produces when the
      // outer Display already includes the source.
      if !parts
        .last()
        .is_some_and(|prev| prev.contains(&msg) || msg.contains(prev))
      {
        parts.push(msg);
      }
      last_kind = Some(format!("{s:?}").split_whitespace().next().unwrap_or("").to_string());
      src = s.source();
    }
    Error::Message {
      message: parts.join(": "),
      root_cause_kind: last_kind,
    }
  }
}
