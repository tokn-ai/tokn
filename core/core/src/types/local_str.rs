use alloc::{boxed::Box, rc::Rc, string::String, vec::Vec};
use core::{
  borrow::Borrow,
  cmp::Ordering,
  fmt,
  hash::{Hash, Hasher},
  ops::Deref,
};
use serde::{
  de::{Error, Unexpected, Visitor},
  Deserialize, Deserializer, Serialize, Serializer,
};

const INLINE_CAPACITY: usize = 23;

/// A compact, immutable string for local alloc-only execution.
///
/// Short strings are stored inline, static strings borrow static storage,
/// and longer strings share an `Rc<str>`. Cloning a long string does not copy
/// its bytes. The `Rc` representation makes this type neither `Send` nor `Sync`.
///
/// ```compile_fail
/// fn require_thread_safe<T: Send + Sync>() {}
/// require_thread_safe::<tokn_core::types::SmolStr>();
/// ```
#[derive(Clone)]
pub struct SmolStr(Storage);

#[derive(Clone)]
enum Storage {
  Inline { bytes: [u8; INLINE_CAPACITY], length: u8 },
  Static(&'static str),
  Shared(Rc<str>),
}

impl SmolStr {
  pub fn new(text: impl AsRef<str>) -> Self {
    let text = text.as_ref();
    if text.len() <= INLINE_CAPACITY {
      Self::new_inline(text)
    } else {
      Self(Storage::Shared(Rc::from(text)))
    }
  }

  /// Store a string inline without allocation. Panics if its UTF-8 encoding
  /// exceeds 23 bytes.
  pub const fn new_inline(text: &str) -> Self {
    assert!(text.len() <= INLINE_CAPACITY, "string exceeds inline capacity");
    let mut bytes = [0; INLINE_CAPACITY];
    let mut index = 0;
    while index < text.len() {
      bytes[index] = text.as_bytes()[index];
      index += 1;
    }
    Self(Storage::Inline {
      bytes,
      length: text.len() as u8,
    })
  }

  /// Borrow a static string without allocating, including strings longer than
  /// the inline capacity.
  pub const fn new_static(text: &'static str) -> Self {
    Self(Storage::Static(text))
  }

  pub fn as_str(&self) -> &str {
    match &self.0 {
      Storage::Inline { bytes, length } => {
        // Inline storage is populated only from a complete, valid UTF-8 string.
        core::str::from_utf8(&bytes[..usize::from(*length)]).expect("inline string contains valid UTF-8")
      }
      Storage::Static(text) => text,
      Storage::Shared(text) => text,
    }
  }

  pub fn len(&self) -> usize {
    self.as_str().len()
  }

  pub fn is_empty(&self) -> bool {
    self.as_str().is_empty()
  }
}

impl Default for SmolStr {
  fn default() -> Self {
    Self::new_static("")
  }
}

impl Deref for SmolStr {
  type Target = str;

  fn deref(&self) -> &str {
    self.as_str()
  }
}

impl AsRef<str> for SmolStr {
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl Borrow<str> for SmolStr {
  fn borrow(&self) -> &str {
    self.as_str()
  }
}

impl From<&str> for SmolStr {
  fn from(text: &str) -> Self {
    Self::new(text)
  }
}

impl From<&String> for SmolStr {
  fn from(text: &String) -> Self {
    Self::new(text)
  }
}

impl From<String> for SmolStr {
  fn from(text: String) -> Self {
    if text.len() <= INLINE_CAPACITY {
      Self::new(text)
    } else {
      Self(Storage::Shared(Rc::from(text)))
    }
  }
}

impl From<Box<str>> for SmolStr {
  fn from(text: Box<str>) -> Self {
    if text.len() <= INLINE_CAPACITY {
      Self::new(text)
    } else {
      Self(Storage::Shared(Rc::from(text)))
    }
  }
}

impl<T: AsRef<str> + ?Sized> PartialEq<T> for SmolStr {
  fn eq(&self, other: &T) -> bool {
    self.as_str() == other.as_ref()
  }
}

impl Eq for SmolStr {}

impl PartialEq<SmolStr> for str {
  fn eq(&self, other: &SmolStr) -> bool {
    self == other.as_str()
  }
}

impl PartialEq<SmolStr> for &str {
  fn eq(&self, other: &SmolStr) -> bool {
    *self == other.as_str()
  }
}

impl PartialEq<SmolStr> for String {
  fn eq(&self, other: &SmolStr) -> bool {
    self.as_str() == other.as_str()
  }
}

impl PartialEq<SmolStr> for &String {
  fn eq(&self, other: &SmolStr) -> bool {
    self.as_str() == other.as_str()
  }
}

impl PartialOrd for SmolStr {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for SmolStr {
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_str().cmp(other.as_str())
  }
}

impl Hash for SmolStr {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_str().hash(state);
  }
}

impl fmt::Display for SmolStr {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Display::fmt(self.as_str(), f)
  }
}

impl fmt::Debug for SmolStr {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Debug::fmt(self.as_str(), f)
  }
}

impl Serialize for SmolStr {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(self.as_str())
  }
}

struct StringVisitor;

impl<'de> Visitor<'de> for StringVisitor {
  type Value = SmolStr;

  fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter.write_str("a string")
  }

  fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
    Ok(SmolStr::new(value))
  }

  fn visit_string<E: Error>(self, value: String) -> Result<Self::Value, E> {
    Ok(SmolStr::from(value))
  }

  fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Self::Value, E> {
    let text = core::str::from_utf8(value).map_err(|_| E::invalid_value(Unexpected::Bytes(value), &self))?;
    Ok(SmolStr::new(text))
  }

  fn visit_byte_buf<E: Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
    let text =
      String::from_utf8(value).map_err(|error| E::invalid_value(Unexpected::Bytes(&error.into_bytes()), &self))?;
    Ok(SmolStr::from(text))
  }
}

impl<'de> Deserialize<'de> for SmolStr {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_str(StringVisitor)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn last_shared_owner_releases_the_string() {
    let original = SmolStr::new("a string longer than the inline storage capacity");
    let Storage::Shared(shared) = &original.0 else {
      panic!("long string must use shared storage");
    };
    let weak = Rc::downgrade(shared);
    let clone = original.clone();
    drop(original);
    assert!(weak.upgrade().is_some());
    drop(clone);
    assert!(weak.upgrade().is_none());
  }
}
