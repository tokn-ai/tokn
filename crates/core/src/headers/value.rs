//! Header value storage. Values are kept as `Cow<'static, str>` so static
//! defaults (e.g. `"application/json"`) cost zero allocations while dynamic
//! values (auth tokens, session IDs) heap-allocate on demand.

use crate::types::SmolStr;
use alloc::borrow::Cow;
use alloc::string::String;
use alloc::string::ToString;
use core::fmt;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A header value. Use [`HeaderValue::from_static`] for `'static str` literals
/// to avoid allocation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HeaderValue(Cow<'static, str>);

impl Serialize for HeaderValue {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&self.0)
  }
}

impl<'de> Deserialize<'de> for HeaderValue {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    let s = String::deserialize(deserializer)?;
    Ok(HeaderValue::from_string(s))
  }
}

impl HeaderValue {
  /// Construct from a `'static` string literal at compile time.
  pub const fn from_static(s: &'static str) -> Self {
    Self(Cow::Borrowed(s))
  }

  /// Construct from an owned [`String`]. Allocates only if the input is non-empty.
  pub fn from_string(s: String) -> Self {
    Self(Cow::Owned(s))
  }

  /// Borrow the value as a `&str`.
  pub fn as_str(&self) -> &str {
    &self.0
  }

  /// Length of the value in bytes.
  pub fn len(&self) -> usize {
    self.0.len()
  }

  /// Whether the value is empty.
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }
}

impl fmt::Display for HeaderValue {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(&self.0)
  }
}

impl From<&'static str> for HeaderValue {
  fn from(value: &'static str) -> Self {
    Self::from_static(value)
  }
}

impl From<String> for HeaderValue {
  fn from(value: String) -> Self {
    Self::from_string(value)
  }
}

impl From<SmolStr> for HeaderValue {
  fn from(value: SmolStr) -> Self {
    Self(Cow::Owned(value.to_string()))
  }
}

impl From<Cow<'static, str>> for HeaderValue {
  fn from(value: Cow<'static, str>) -> Self {
    Self(value)
  }
}

impl From<&HeaderValue> for HeaderValue {
  fn from(value: &HeaderValue) -> Self {
    value.clone()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn from_static_is_borrowed() {
    let v = HeaderValue::from_static("application/json");
    assert!(matches!(v.0, Cow::Borrowed(_)));
    assert_eq!(v.as_str(), "application/json");
  }

  #[test]
  fn from_string_is_owned() {
    let v: HeaderValue = String::from("Bearer abc").into();
    assert!(matches!(v.0, Cow::Owned(_)));
    assert_eq!(v.as_str(), "Bearer abc");
  }

  #[test]
  fn from_smol_str() {
    let v: HeaderValue = SmolStr::new("ses_42").into();
    assert_eq!(v.as_str(), "ses_42");
  }

  #[test]
  fn equality_ignores_storage_kind() {
    let a = HeaderValue::from_static("xyz");
    let b: HeaderValue = String::from("xyz").into();
    assert_eq!(a, b);
  }
}
