//! Case- and order-preserving HTTP header name.
//!
//! Internally stores both the original-cased `SmolStr` (for fidelity in logs,
//! golden snapshots, and outbound serialization) and a lowercase `SmolStr`
//! used for [`Eq`] / [`Hash`] comparisons. This lets us round-trip header
//! casing without paying for case-insensitive comparison at lookup time.
//!
//! Construct compile-time constants with [`HeaderName::new_static`]:
//!
//! ```
//! use tokn_core::headers::HeaderName;
//! const AUTH: HeaderName = HeaderName::new_static("Authorization", "authorization");
//! assert_eq!(AUTH.original(), "Authorization");
//! assert_eq!(AUTH.as_str(), "authorization");
//! ```

use crate::types::SmolStr;
use alloc::string::String;
use core::fmt;
use core::hash::{Hash, Hasher};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A header name that preserves its original ASCII casing while comparing
/// case-insensitively.
#[derive(Debug, Clone)]
pub struct HeaderName {
  original: SmolStr,
  lower: SmolStr,
}

impl Serialize for HeaderName {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&self.original)
  }
}

impl<'de> Deserialize<'de> for HeaderName {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    let s = SmolStr::deserialize(deserializer)?;
    Ok(HeaderName::new(s))
  }
}

impl HeaderName {
  /// Construct a header name from a `'static` original/lowercase pair.
  ///
  /// Both arguments must agree byte-for-byte modulo ASCII case. Use this for
  /// the entries in [`crate::headers::keys`].
  pub const fn new_static(original: &'static str, lower: &'static str) -> Self {
    Self {
      original: SmolStr::new_static(original),
      lower: SmolStr::new_static(lower),
    }
  }

  /// Construct a header name from an arbitrary string. The original casing is
  /// preserved; the lowercase form used for comparisons is computed eagerly.
  pub fn new(name: impl Into<SmolStr>) -> Self {
    let original: SmolStr = name.into();
    let lower = if original.bytes().all(|b| !b.is_ascii_uppercase()) {
      original.clone()
    } else {
      SmolStr::from(original.to_ascii_lowercase())
    };
    Self { original, lower }
  }

  /// The original-cased name as inserted by the caller.
  pub fn original(&self) -> &str {
    &self.original
  }

  /// The lowercase canonical form. Matches the on-the-wire HTTP/2 form.
  pub fn as_str(&self) -> &str {
    &self.lower
  }
}

impl PartialEq for HeaderName {
  fn eq(&self, other: &Self) -> bool {
    self.lower == other.lower
  }
}

impl Eq for HeaderName {}

impl Hash for HeaderName {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.lower.hash(state);
  }
}

impl fmt::Display for HeaderName {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(&self.original)
  }
}

impl From<&str> for HeaderName {
  fn from(value: &str) -> Self {
    Self::new(value)
  }
}

impl From<String> for HeaderName {
  fn from(value: String) -> Self {
    Self::new(SmolStr::from(value))
  }
}

impl From<SmolStr> for HeaderName {
  fn from(value: SmolStr) -> Self {
    Self::new(value)
  }
}

impl From<&HeaderName> for HeaderName {
  fn from(value: &HeaderName) -> Self {
    value.clone()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn case_insensitive_equality_preserves_original() {
    let a = HeaderName::new("Authorization");
    let b = HeaderName::new("authorization");
    let c = HeaderName::new("AUTHORIZATION");
    assert_eq!(a, b);
    assert_eq!(b, c);
    assert_eq!(a.original(), "Authorization");
    assert_eq!(b.original(), "authorization");
    assert_eq!(c.original(), "AUTHORIZATION");
  }

  #[test]
  fn lowercase_input_avoids_extra_allocation() {
    let n = HeaderName::new("content-type");
    assert_eq!(n.original(), "content-type");
    assert_eq!(n.as_str(), "content-type");
  }

  #[test]
  fn display_uses_original_case() {
    let n = HeaderName::new("X-Session-Id");
    assert_eq!(format!("{n}"), "X-Session-Id");
  }

  #[test]
  fn hash_matches_equality_under_case_change() {
    use std::collections::hash_map::DefaultHasher;
    fn h(n: &HeaderName) -> u64 {
      let mut s = DefaultHasher::new();
      n.hash(&mut s);
      s.finish()
    }
    assert_eq!(h(&HeaderName::new("Foo-Bar")), h(&HeaderName::new("foo-bar")));
  }

  #[test]
  fn new_static_round_trips_both_cases() {
    const N: HeaderName = HeaderName::new_static("X-Request-Id", "x-request-id");
    assert_eq!(N.original(), "X-Request-Id");
    assert_eq!(N.as_str(), "x-request-id");
    assert_eq!(N, HeaderName::new("X-REQUEST-ID"));
  }
}
