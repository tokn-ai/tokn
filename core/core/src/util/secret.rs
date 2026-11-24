//! `Secret<T>` — a transparent newtype that refuses to print its payload.
//!
//! Wrap any field that holds credentials, bearer tokens, or other material
//! that must not appear in logs, error messages, or `Debug` dumps.
//!
//! `Debug` and `Display` always render `***`; the only way to read the
//! contents is the explicit [`Secret::expose`] accessor.
//!
//! `Serialize` is **transparent** — it forwards to the inner type. We must
//! be able to round-trip credentials through TOML/JSON on disk; redaction
//! lives in the `Debug`/`Display` impls and at human-facing print sites.
//!
//! ```
//! use tokn_core::util::secret::Secret;
//! let s: Secret<String> = "hunter2".to_owned().into();
//! assert_eq!(format!("{s:?}"), "Secret(***)");
//! assert_eq!(s.expose(), "hunter2");
//! ```
//!
//! Equality, ordering, hashing intentionally aren't derived: comparing
//! secrets in the clear is itself a footgun (timing channels). Use
//! [`Secret::expose`] and a constant-time comparator if you really need it.

use alloc::string::String;
use core::fmt;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

#[derive(Clone, Default)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
  #[inline]
  pub const fn new(inner: T) -> Self {
    Self(inner)
  }

  #[inline]
  pub fn expose(&self) -> &T {
    &self.0
  }

  #[inline]
  #[allow(dead_code)]
  pub fn into_inner(self) -> T {
    self.0
  }
}

impl<T: AsRef<str>> Secret<T> {
  /// Return `sha256[..8]` of this secret as a lowercase hex string with a
  /// stable prefix so logs can correlate credentials without exposing them.
  pub fn fingerprint(&self) -> String {
    fingerprint_str(self.0.as_ref())
  }
}

pub(crate) fn fingerprint_str(secret: &str) -> String {
  if secret.is_empty() {
    return "fp:<empty>".into();
  }
  let mut h = Sha256::new();
  h.update(secret.as_bytes());
  let digest = h.finalize();
  let mut s = String::with_capacity(3 + 16);
  s.push_str("fp:");
  for b in digest.iter().take(8) {
    use core::fmt::Write as _;
    let _ = write!(s, "{b:02x}");
  }
  s
}

impl<T> From<T> for Secret<T> {
  #[inline]
  fn from(v: T) -> Self {
    Self(v)
  }
}

impl<T> fmt::Debug for Secret<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("Secret(***)")
  }
}

impl<T> fmt::Display for Secret<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("***")
  }
}

impl<T: Serialize> Serialize for Secret<T> {
  fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
    self.0.serialize(s)
  }
}

impl<'de, T> Deserialize<'de> for Secret<T>
where
  T: Deserialize<'de>,
{
  fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
    T::deserialize(d).map(Secret)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::string::ToString;

  #[test]
  fn debug_and_display_redact() {
    let s: Secret<String> = "tok_abc".to_string().into();
    assert_eq!(format!("{s:?}"), "Secret(***)");
    assert_eq!(format!("{s}"), "***");
  }

  #[test]
  fn expose_returns_payload() {
    let s = Secret::new(42_u32);
    assert_eq!(*s.expose(), 42);
  }

  #[test]
  fn serialize_is_transparent() {
    let s: Secret<String> = "tok_abc".to_string().into();
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(json, "\"tok_abc\"");
  }

  #[test]
  fn deserialize_passes_through() {
    let s: Secret<String> = serde_json::from_str("\"hello\"").unwrap();
    assert_eq!(s.expose(), "hello");
  }

  #[test]
  fn fingerprint_is_stable_and_short() {
    let a = Secret::new("hunter2".to_string()).fingerprint();
    let b = Secret::new("hunter2".to_string()).fingerprint();
    let c = Secret::new("hunter3".to_string()).fingerprint();
    assert_eq!(a, b);
    assert_ne!(a, c);
    assert!(a.starts_with("fp:"));
    assert_eq!(a.len(), 3 + 16);
  }

  #[test]
  fn fingerprint_tags_empty() {
    assert_eq!(Secret::new(String::new()).fingerprint(), "fp:<empty>");
  }
}
