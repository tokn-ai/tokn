use alloc::{boxed::Box, rc::Rc, string::String, vec::Vec};
use core::{
  borrow::Borrow,
  cmp::Ordering,
  fmt,
  hash::{Hash, Hasher},
  ops::{Bound, Deref, RangeBounds},
};
use serde::{
  de::{Error, SeqAccess, Visitor},
  Deserialize, Deserializer, Serialize, Serializer,
};

/// An immutable shared byte view for local alloc-only execution.
///
/// Cloning and slicing are constant-time and share backing storage without
/// copying bytes. A nonempty view retains the full backing allocation until
/// its final owner is dropped. Static and empty buffers require no allocation.
/// This type is neither `Send` nor `Sync`.
///
/// ```compile_fail
/// fn require_thread_safe<T: Send + Sync>() {}
/// require_thread_safe::<tokn_core::types::Bytes>();
/// ```
#[derive(Clone)]
pub struct Bytes {
  storage: Storage,
  start: usize,
  length: usize,
}

#[derive(Clone)]
enum Storage {
  Static(&'static [u8]),
  Shared(Rc<Vec<u8>>),
}

impl Bytes {
  pub const fn new() -> Self {
    Self::from_static(&[])
  }

  pub const fn from_static(bytes: &'static [u8]) -> Self {
    Self {
      storage: Storage::Static(bytes),
      start: 0,
      length: bytes.len(),
    }
  }

  pub fn copy_from_slice(bytes: &[u8]) -> Self {
    Self::from(bytes.to_vec())
  }

  pub fn len(&self) -> usize {
    self.length
  }

  pub fn is_empty(&self) -> bool {
    self.length == 0
  }

  /// Share the selected range. Panics if the bounds are out of range or
  /// reversed. An empty view does not retain the backing allocation.
  pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
    let start = match range.start_bound() {
      Bound::Included(&index) => index,
      Bound::Excluded(&index) => index.checked_add(1).expect("byte range start overflows"),
      Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
      Bound::Included(&index) => index.checked_add(1).expect("byte range end overflows"),
      Bound::Excluded(&index) => index,
      Bound::Unbounded => self.length,
    };
    assert!(start <= end && end <= self.length, "byte range is outside the buffer");
    if start == end {
      return Self::new();
    }
    Self {
      storage: self.storage.clone(),
      start: self.start + start,
      length: end - start,
    }
  }

  /// Keep the prefix and return the suffix, sharing storage. Panics when
  /// `at > self.len()`.
  #[must_use]
  pub fn split_off(&mut self, at: usize) -> Self {
    assert!(at <= self.length, "split point is outside the buffer");
    if at == 0 {
      return core::mem::take(self);
    }
    let suffix = self.slice(at..);
    self.length = at;
    suffix
  }

  /// Return the prefix and keep the suffix, sharing storage. Panics when
  /// `at > self.len()`.
  #[must_use]
  pub fn split_to(&mut self, at: usize) -> Self {
    assert!(at <= self.length, "split point is outside the buffer");
    if at == self.length {
      return core::mem::take(self);
    }
    let prefix = self.slice(..at);
    self.start += at;
    self.length -= at;
    prefix
  }

  /// Keep at most `length` bytes. Larger lengths leave this view unchanged.
  pub fn truncate(&mut self, length: usize) {
    if length == 0 {
      self.clear();
    } else if length < self.length {
      self.length = length;
    }
  }

  /// Release this view's ownership and replace it with an empty static buffer.
  pub fn clear(&mut self) {
    *self = Self::new();
  }

  fn as_slice(&self) -> &[u8] {
    let bytes: &[u8] = match &self.storage {
      Storage::Static(bytes) => bytes,
      Storage::Shared(bytes) => bytes,
    };
    &bytes[self.start..self.start + self.length]
  }
}

impl Default for Bytes {
  fn default() -> Self {
    Self::new()
  }
}

impl Deref for Bytes {
  type Target = [u8];

  fn deref(&self) -> &[u8] {
    self.as_slice()
  }
}

impl AsRef<[u8]> for Bytes {
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}

impl Borrow<[u8]> for Bytes {
  fn borrow(&self) -> &[u8] {
    self.as_slice()
  }
}

impl From<Vec<u8>> for Bytes {
  fn from(bytes: Vec<u8>) -> Self {
    let length = bytes.len();
    if length == 0 {
      Self::new()
    } else {
      // Moving the Vec into Rc allocates a control block, not another payload.
      Self {
        storage: Storage::Shared(Rc::new(bytes)),
        start: 0,
        length,
      }
    }
  }
}

impl From<String> for Bytes {
  fn from(text: String) -> Self {
    Self::from(text.into_bytes())
  }
}

impl From<Box<[u8]>> for Bytes {
  fn from(bytes: Box<[u8]>) -> Self {
    Self::from(bytes.into_vec())
  }
}

impl From<&'static [u8]> for Bytes {
  fn from(bytes: &'static [u8]) -> Self {
    Self::from_static(bytes)
  }
}

impl From<&'static str> for Bytes {
  fn from(text: &'static str) -> Self {
    Self::from_static(text.as_bytes())
  }
}

impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for Bytes {
  fn eq(&self, other: &T) -> bool {
    self.as_slice() == other.as_ref()
  }
}

impl Eq for Bytes {}

impl PartialOrd for Bytes {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Bytes {
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_slice().cmp(other.as_slice())
  }
}

impl Hash for Bytes {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_slice().hash(state);
  }
}

impl fmt::Debug for Bytes {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter.write_str("b\"")?;
    for &byte in self.as_slice() {
      match byte {
        b'\n' => formatter.write_str("\\n")?,
        b'\r' => formatter.write_str("\\r")?,
        b'\t' => formatter.write_str("\\t")?,
        b'\0' => formatter.write_str("\\0")?,
        b'"' | b'\\' => write!(formatter, "\\{}", char::from(byte))?,
        0x20..=0x7e => write!(formatter, "{}", char::from(byte))?,
        _ => write!(formatter, "\\x{byte:02x}")?,
      }
    }
    formatter.write_str("\"")
  }
}

impl Serialize for Bytes {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_bytes(self.as_slice())
  }
}

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
  type Value = Bytes;

  fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter.write_str("a byte array")
  }

  fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
    let mut bytes = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(4096));
    while let Some(byte) = sequence.next_element()? {
      bytes.push(byte);
    }
    Ok(Bytes::from(bytes))
  }

  fn visit_bytes<E: Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
    Ok(Bytes::copy_from_slice(bytes))
  }

  fn visit_byte_buf<E: Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
    Ok(Bytes::from(bytes))
  }

  fn visit_str<E: Error>(self, text: &str) -> Result<Self::Value, E> {
    Ok(Bytes::copy_from_slice(text.as_bytes()))
  }

  fn visit_string<E: Error>(self, text: String) -> Result<Self::Value, E> {
    Ok(Bytes::from(text))
  }
}

impl<'de> Deserialize<'de> for Bytes {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_byte_buf(BytesVisitor)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn last_view_releases_shared_storage() {
    let original = Bytes::from(Vec::from(&b"request body"[..]));
    let Storage::Shared(storage) = &original.storage else {
      panic!("owned bytes must use shared storage");
    };
    let weak = Rc::downgrade(storage);
    let view = original.slice(2..5);
    drop(original);
    assert_eq!(view.as_ref(), b"que");
    assert!(weak.upgrade().is_some());
    drop(view);
    assert!(weak.upgrade().is_none());
  }

  #[test]
  fn empty_views_do_not_retain_a_large_buffer() {
    let mut original = Bytes::from(Vec::from(&b"request body"[..]));
    let Storage::Shared(storage) = &original.storage else {
      panic!("owned bytes must use shared storage");
    };
    let weak = Rc::downgrade(storage);
    let empty = original.slice(4..4);
    original.clear();
    assert!(empty.is_empty());
    assert!(weak.upgrade().is_none());
  }
}
