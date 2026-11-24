//! Vec-backed, order- and case-preserving header map.
//!
//! Designed for typical HTTP request/response sizes (< 30 entries), where
//! linear scan beats hash-table overhead. Supports duplicate names (required
//! for headers like `Set-Cookie`) by storing all entries in insertion order.
//!
//! Lookup is case-insensitive via [`HeaderName`]'s `Eq` impl.

use crate::headers::name::HeaderName;
use crate::headers::value::HeaderValue;
use alloc::borrow::Cow;
use alloc::string::String;
use alloc::string::ToString;
use alloc::vec::Vec;
use core::fmt;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Deserializer, Serialize};

/// Trait for values that can be used as a lookup key against a [`HeaderMap`].
///
/// Implementations exist for [`HeaderName`] and `&HeaderName` (zero-allocation,
/// reusing the cached lowercase form), and for `&str` / `String` /
/// `&String` (lowercased on the fly when needed).
pub trait AsHeaderName {
  /// Return the ASCII-lowercase form of this name. Borrows from the value
  /// when already lowercase, allocates otherwise.
  fn as_lower(&self) -> Cow<'_, str>;
}

impl AsHeaderName for HeaderName {
  fn as_lower(&self) -> Cow<'_, str> {
    Cow::Borrowed(self.as_str())
  }
}

impl AsHeaderName for &HeaderName {
  fn as_lower(&self) -> Cow<'_, str> {
    Cow::Borrowed(self.as_str())
  }
}

impl AsHeaderName for str {
  fn as_lower(&self) -> Cow<'_, str> {
    if self.bytes().all(|b| !b.is_ascii_uppercase()) {
      Cow::Borrowed(self)
    } else {
      Cow::Owned(self.to_ascii_lowercase())
    }
  }
}

impl AsHeaderName for &str {
  fn as_lower(&self) -> Cow<'_, str> {
    <str as AsHeaderName>::as_lower(self)
  }
}

impl AsHeaderName for String {
  fn as_lower(&self) -> Cow<'_, str> {
    <str as AsHeaderName>::as_lower(self.as_str())
  }
}

impl AsHeaderName for &String {
  fn as_lower(&self) -> Cow<'_, str> {
    <str as AsHeaderName>::as_lower(self.as_str())
  }
}

/// A list of (name, value) pairs in insertion order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderMap {
  entries: Vec<(HeaderName, HeaderValue)>,
}

impl HeaderMap {
  /// Create an empty map.
  pub fn new() -> Self {
    Self { entries: Vec::new() }
  }

  /// Create a map with capacity for `n` entries (no allocations until exceeded).
  pub fn with_capacity(n: usize) -> Self {
    Self {
      entries: Vec::with_capacity(n),
    }
  }

  /// Append `(name, value)` to the map. Allows duplicate names — use this
  /// only for headers that are semantically multi-valued (`Set-Cookie`,
  /// `Via`, etc.). For the common case of "set this header to this value",
  /// use [`Self::insert`].
  pub fn append(&mut self, name: impl Into<HeaderName>, value: impl Into<HeaderValue>) {
    self.entries.push((name.into(), value.into()));
  }

  /// Set `name` to `value`, replacing any existing entries with the same
  /// name. Matches `http::HeaderMap::insert` / `reqwest::HeaderMap::insert`
  /// semantics: a single entry with this name will exist after the call.
  ///
  /// The new entry is placed at the position of the first existing match
  /// (preserving header order when overwriting), otherwise appended.
  pub fn insert(&mut self, name: impl Into<HeaderName>, value: impl Into<HeaderValue>) {
    let name = name.into();
    let value = value.into();
    let mut first = None;
    let mut i = 0;
    while i < self.entries.len() {
      if self.entries[i].0 == name {
        if first.is_none() {
          first = Some(i);
          self.entries[i].1 = value.clone();
          i += 1;
        } else {
          self.entries.remove(i);
        }
      } else {
        i += 1;
      }
    }
    if first.is_none() {
      self.entries.push((name, value));
    }
  }

  /// Remove all entries matching `name`. Returns the count removed.
  pub fn remove<N: AsHeaderName>(&mut self, name: N) -> usize {
    let lower = name.as_lower();
    let before = self.entries.len();
    self.entries.retain(|(n, _)| n.as_str() != lower.as_ref());
    before - self.entries.len()
  }

  /// First value matching `name`, if any.
  pub fn get<N: AsHeaderName>(&self, name: N) -> Option<&HeaderValue> {
    let lower = name.as_lower();
    self
      .entries
      .iter()
      .find(|(n, _)| n.as_str() == lower.as_ref())
      .map(|(_, v)| v)
  }

  /// All values matching `name`, in insertion order.
  pub fn get_all<N: AsHeaderName>(&self, name: N) -> impl Iterator<Item = &HeaderValue> {
    let lower = name.as_lower().into_owned();
    self
      .entries
      .iter()
      .filter(move |(n, _)| n.as_str() == lower.as_str())
      .map(|(_, v)| v)
  }

  /// Whether at least one entry matches `name`.
  pub fn contains_key<N: AsHeaderName>(&self, name: N) -> bool {
    let lower = name.as_lower();
    self.entries.iter().any(|(n, _)| n.as_str() == lower.as_ref())
  }

  /// Iterate entries in insertion order.
  pub fn iter(&self) -> impl Iterator<Item = (&HeaderName, &HeaderValue)> {
    self.entries.iter().map(|(n, v)| (n, v))
  }

  /// Number of entries (counts duplicates).
  pub fn len(&self) -> usize {
    self.entries.len()
  }

  /// Whether the map is empty.
  pub fn is_empty(&self) -> bool {
    self.entries.is_empty()
  }

  /// Append all entries from `other` in their original order. Duplicates are
  /// preserved (this does not deduplicate against `self`).
  pub fn extend(&mut self, other: HeaderMap) {
    self.entries.extend(other.entries);
  }

  /// Append all entries from `other`, with later names winning over earlier ones
  /// in `self`. Within `other`, all entries are kept (even duplicate names).
  pub fn merge_replacing(&mut self, other: HeaderMap) {
    for (name, value) in &other.entries {
      self.entries.retain(|(n, _)| n != name);
      let _ = value;
    }
    self.entries.extend(other.entries);
  }
}

impl FromIterator<(HeaderName, HeaderValue)> for HeaderMap {
  fn from_iter<I: IntoIterator<Item = (HeaderName, HeaderValue)>>(iter: I) -> Self {
    Self {
      entries: iter.into_iter().collect(),
    }
  }
}

impl IntoIterator for HeaderMap {
  type Item = (HeaderName, HeaderValue);
  type IntoIter = alloc::vec::IntoIter<Self::Item>;
  fn into_iter(self) -> Self::IntoIter {
    self.entries.into_iter()
  }
}

impl<'a> IntoIterator for &'a HeaderMap {
  type Item = (&'a HeaderName, &'a HeaderValue);
  type IntoIter = core::iter::Map<
    core::slice::Iter<'a, (HeaderName, HeaderValue)>,
    fn(&'a (HeaderName, HeaderValue)) -> (&'a HeaderName, &'a HeaderValue),
  >;
  fn into_iter(self) -> Self::IntoIter {
    self.entries.iter().map(|(n, v)| (n, v))
  }
}

// --- serde ---
//
// JSON shape: object keyed by the **original-cased** header name, with the
// value either a single string (one entry for that name) or an array of strings
// (multiple entries, in insertion order).
//
// Deserialization accepts both shapes (string-or-array-of-strings) and is
// also backward-compatible with legacy DB rows whose values are always single
// strings (as written by the previous `reqwest::header::HeaderMap` serde impl).

impl Serialize for HeaderMap {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    // Group consecutive duplicate names, preserving first-occurrence order.
    let mut grouped: Vec<(&HeaderName, Vec<&HeaderValue>)> = Vec::with_capacity(self.entries.len());
    'outer: for (n, v) in &self.entries {
      for (existing_name, vs) in grouped.iter_mut() {
        if *existing_name == n {
          vs.push(v);
          continue 'outer;
        }
      }
      grouped.push((n, vec![v]));
    }
    let mut map = serializer.serialize_map(Some(grouped.len()))?;
    for (name, vals) in grouped {
      if vals.len() == 1 {
        map.serialize_entry(name, vals[0])?;
      } else {
        map.serialize_entry(name, &vals)?;
      }
    }
    map.end()
  }
}

/// Visitor accepting either a single string or a sequence of strings as the
/// value side of a header-map entry.
struct StringOrSeq;

impl<'de> Visitor<'de> for StringOrSeq {
  type Value = Vec<String>;

  fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
    f.write_str("a string or array of strings")
  }

  fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
    Ok(vec![v.to_string()])
  }

  fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
    Ok(vec![v])
  }

  fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
    let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
    while let Some(s) = seq.next_element::<String>()? {
      out.push(s);
    }
    Ok(out)
  }
}

#[derive(Default)]
struct HeaderMapVisitor;

impl<'de> Visitor<'de> for HeaderMapVisitor {
  type Value = HeaderMap;

  fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
    f.write_str("a map of header name to string or array of strings")
  }

  fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
    let mut out = HeaderMap::with_capacity(access.size_hint().unwrap_or(0));
    while let Some(name) = access.next_key::<HeaderName>()? {
      let values = access.next_value_seed(StringOrSeqSeed)?;
      for v in values {
        out.append(name.clone(), HeaderValue::from_string(v));
      }
    }
    Ok(out)
  }
}

struct StringOrSeqSeed;

impl<'de> de::DeserializeSeed<'de> for StringOrSeqSeed {
  type Value = Vec<String>;
  fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
    deserializer.deserialize_any(StringOrSeq)
  }
}

impl<'de> Deserialize<'de> for HeaderMap {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_map(HeaderMapVisitor)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn name(s: &'static str) -> HeaderName {
    HeaderName::new(s)
  }

  #[test]
  fn insert_preserves_order_and_case() {
    let mut m = HeaderMap::new();
    m.insert(name("X-Aaa"), "1");
    m.insert(name("Authorization"), "Bearer x");
    m.insert(name("Content-Type"), "application/json");
    let collected: Vec<_> = m
      .iter()
      .map(|(n, v)| (n.original().to_string(), v.as_str().to_string()))
      .collect();
    assert_eq!(
      collected,
      vec![
        ("X-Aaa".into(), "1".into()),
        ("Authorization".into(), "Bearer x".into()),
        ("Content-Type".into(), "application/json".into()),
      ]
    );
  }

  #[test]
  fn get_is_case_insensitive() {
    let mut m = HeaderMap::new();
    m.insert(name("Authorization"), "Bearer x");
    assert_eq!(m.get(name("authorization")).map(|v| v.as_str()), Some("Bearer x"));
    assert_eq!(m.get(name("AUTHORIZATION")).map(|v| v.as_str()), Some("Bearer x"));
  }

  #[test]
  fn duplicates_preserved() {
    let mut m = HeaderMap::new();
    m.append(name("Set-Cookie"), "a=1");
    m.append(name("Set-Cookie"), "b=2");
    let all: Vec<_> = m.get_all(&name("set-cookie")).map(|v| v.as_str().to_string()).collect();
    assert_eq!(all, vec!["a=1", "b=2"]);
    assert_eq!(m.len(), 2);
  }

  #[test]
  fn insert_overrides_existing_value() {
    let mut m = HeaderMap::new();
    m.insert(name("Authorization"), "Bearer old");
    m.insert(name("authorization"), "Bearer new");
    assert_eq!(m.len(), 1);
    assert_eq!(m.get(name("authorization")).map(|v| v.as_str()), Some("Bearer new"));
  }

  #[test]
  fn insert_removes_duplicates_keeps_position() {
    let mut m = HeaderMap::new();
    m.insert(name("A"), "1");
    m.append(name("Set-Cookie"), "old1");
    m.insert(name("B"), "2");
    m.append(name("Set-Cookie"), "old2");
    m.insert(name("set-cookie"), "new");
    let collected: Vec<_> = m
      .iter()
      .map(|(n, v)| (n.original().to_string(), v.as_str().to_string()))
      .collect();
    assert_eq!(
      collected,
      vec![
        ("A".into(), "1".into()),
        ("Set-Cookie".into(), "new".into()),
        ("B".into(), "2".into()),
      ]
    );
  }

  #[test]
  fn remove_returns_count() {
    let mut m = HeaderMap::new();
    m.append(name("X"), "1");
    m.append(name("X"), "2");
    m.insert(name("Y"), "3");
    assert_eq!(m.remove(name("x")), 2);
    assert_eq!(m.len(), 1);
  }

  #[test]
  fn extend_keeps_duplicates() {
    let mut a = HeaderMap::new();
    a.append(name("X"), "1");
    let mut b = HeaderMap::new();
    b.append(name("X"), "2");
    a.extend(b);
    assert_eq!(a.len(), 2);
  }

  #[test]
  fn merge_replacing_overrides_existing() {
    let mut a = HeaderMap::new();
    a.insert(name("X"), "old");
    a.insert(name("Y"), "keep");
    let mut b = HeaderMap::new();
    b.insert(name("x"), "new");
    a.merge_replacing(b);
    let collected: Vec<_> = a
      .iter()
      .map(|(n, v)| (n.original().to_string(), v.as_str().to_string()))
      .collect();
    assert_eq!(collected, vec![("Y".into(), "keep".into()), ("x".into(), "new".into())]);
  }

  #[test]
  fn ref_into_iter_yields_borrowed_pairs() {
    let mut m = HeaderMap::new();
    m.insert(name("A"), "1");
    m.insert(name("B"), "2");
    let collected: Vec<_> = (&m).into_iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
    assert_eq!(collected, vec![("a", "1"), ("b", "2")]);
  }

  #[test]
  fn serde_round_trip_preserves_case_and_duplicates() {
    let mut m = HeaderMap::new();
    m.insert(name("Authorization"), "Bearer x");
    m.append(name("Set-Cookie"), "a=1");
    m.append(name("Set-Cookie"), "b=2");
    m.insert(name("Content-Type"), "application/json");
    let json = serde_json::to_string(&m).unwrap();
    assert!(json.contains("\"Authorization\":\"Bearer x\""), "got {json}");
    assert!(json.contains("\"Set-Cookie\":[\"a=1\",\"b=2\"]"), "got {json}");
    let back: HeaderMap = serde_json::from_str(&json).unwrap();
    assert_eq!(back.len(), 4);
    assert_eq!(back.get(name("authorization")).unwrap().as_str(), "Bearer x");
    let cookie_name = name("set-cookie");
    let cookies: Vec<_> = back.get_all(&cookie_name).map(|v| v.as_str()).collect();
    assert_eq!(cookies, vec!["a=1", "b=2"]);
  }

  #[test]
  fn serde_deserialize_accepts_legacy_string_only_object() {
    // Legacy DB shape: every value a single string, no arrays.
    let json = r#"{"authorization":"Bearer x","content-type":"application/json"}"#;
    let m: HeaderMap = serde_json::from_str(json).unwrap();
    assert_eq!(m.len(), 2);
    assert_eq!(m.get(name("Authorization")).unwrap().as_str(), "Bearer x");
  }

  #[test]
  fn serde_deserialize_accepts_array_value() {
    let json = r#"{"X-Foo":["a","b","c"]}"#;
    let m: HeaderMap = serde_json::from_str(json).unwrap();
    assert_eq!(m.len(), 3);
    let foo = name("x-foo");
    let vals: Vec<_> = m.get_all(&foo).map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["a", "b", "c"]);
  }
}
