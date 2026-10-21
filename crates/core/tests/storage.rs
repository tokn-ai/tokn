//! The same value/ownership contract runs against upstream standard types and
//! the local alloc-only implementations, without relying on their layouts.

use std::{
  collections::{hash_map::DefaultHasher, BTreeMap, HashMap},
  hash::{Hash, Hasher},
  ops::Bound,
};

use serde::Deserialize;
use tokn_core::types::{Bytes, SmolStr};

fn hash(value: impl Hash) -> u64 {
  let mut hasher = DefaultHasher::new();
  value.hash(&mut hasher);
  hasher.finish()
}

#[test]
fn string_values_preserve_unicode_boundaries_ordering_and_borrowed_lookup() {
  for text in [
    String::new(),
    "a".repeat(23),
    "é".repeat(11),
    "é".repeat(12),
    "🌍".repeat(16),
  ] {
    let compact = SmolStr::new(&text);
    assert_eq!(compact.as_str(), text);
    assert_eq!(compact.len(), text.len());
    assert_eq!(compact.is_empty(), text.is_empty());
    assert_eq!(compact, text);
    assert_eq!(text, compact);
    assert_eq!(hash(&compact), hash(compact.as_str()));
    assert_eq!(
      serde_json::to_string(&compact).unwrap(),
      serde_json::to_string(&text).unwrap()
    );
    assert_eq!(
      serde_json::from_str::<SmolStr>(&serde_json::to_string(&text).unwrap()).unwrap(),
      compact
    );
    assert_eq!(SmolStr::from(text.clone().into_boxed_str()), compact);
    assert_eq!(SmolStr::from(text.clone()), compact);
  }

  let values = BTreeMap::from([(SmolStr::new("second"), 2), (SmolStr::new("first"), 1)]);
  assert_eq!(values.get("first"), Some(&1));
  assert_eq!(
    values.keys().map(SmolStr::as_str).collect::<Vec<_>>(),
    ["first", "second"]
  );
  let values = HashMap::from([(SmolStr::new("lookup"), 3)]);
  assert_eq!(values.get("lookup"), Some(&3));
}

#[test]
fn string_clones_share_long_storage_and_static_construction_borrows() {
  let text = "long model identifier that exceeds inline string capacity";
  let original = SmolStr::new(String::from(text));
  let clone = original.clone();
  assert_eq!(original.as_str().as_ptr(), clone.as_str().as_ptr());
  drop(original);
  assert_eq!(clone, text);

  const STATIC: SmolStr = SmolStr::new_static("static provider identifier");
  const INLINE: SmolStr = SmolStr::new_inline("héllo");
  assert_eq!(STATIC.as_str().as_ptr(), "static provider identifier".as_ptr());
  assert_eq!(INLINE, "héllo");
  assert_eq!(SmolStr::default(), "");
}

#[test]
fn string_deserialization_accepts_utf8_and_rejects_invalid_byte_data() {
  let valid = serde::de::value::BorrowedBytesDeserializer::<serde::de::value::Error>::new("héllo".as_bytes());
  assert_eq!(SmolStr::deserialize(valid).unwrap(), "héllo");
  let invalid = serde::de::value::BorrowedBytesDeserializer::<serde::de::value::Error>::new(&[0xff]);
  assert!(SmolStr::deserialize(invalid).is_err());
  assert!(serde_json::from_str::<SmolStr>("123").is_err());
}

#[test]
fn byte_views_share_payload_and_survive_the_original_owner() {
  let mut payload = Vec::with_capacity(64);
  payload.extend_from_slice(b"prefix:payload:suffix");
  let pointer = payload.as_ptr();
  let original = Bytes::from(payload);
  assert_eq!(original.as_ptr(), pointer);
  let clone = original.clone();
  let view = clone.slice(7..14);
  assert_eq!(clone.as_ptr(), pointer);
  assert_eq!(view.as_ptr(), original[7..].as_ptr());
  drop(original);
  drop(clone);
  assert_eq!(view.as_ref(), b"payload");
  assert_eq!(view.slice(1..=4).as_ref(), b"aylo");
  assert_eq!(view.slice((Bound::Excluded(0), Bound::Excluded(4))).as_ref(), b"ayl");
  assert_eq!(view.slice(..).as_ref(), b"payload");
  assert!(view.slice(3..3).is_empty());
}

#[test]
fn byte_splitting_and_truncation_preserve_the_wire_transcript() {
  let mut body = Bytes::from(String::from("header|body|tail"));
  let header = body.split_to(7);
  let tail = body.split_off(4);
  assert_eq!(header.as_ref(), b"header|");
  assert_eq!(body.as_ref(), b"body");
  assert_eq!(tail.as_ref(), b"|tail");
  body.truncate(100);
  assert_eq!(body.as_ref(), b"body");
  body.truncate(2);
  assert_eq!(body.as_ref(), b"bo");
  assert!(body.split_to(0).is_empty());
  let whole = body.split_off(0);
  assert_eq!(whole.as_ref(), b"bo");
  assert!(body.is_empty());
  let mut whole = whole;
  let length = whole.len();
  assert!(whole.split_off(length).is_empty());
  assert_eq!(whole.split_to(length).as_ref(), b"bo");
  assert!(whole.is_empty());
  body = tail;
  body.clear();
  assert!(body.is_empty());
  assert_eq!(header.as_ref(), b"header|");
}

#[test]
fn byte_construction_serialization_and_ordering_are_backend_independent() {
  const STATIC: Bytes = Bytes::from_static(b"static");
  assert_eq!(STATIC.as_ptr(), b"static".as_ptr());
  assert_eq!(Bytes::new().as_ref(), b"");
  let source = Box::<[u8]>::from(&b"boxed"[..]);
  let pointer = source.as_ptr();
  let owned = Bytes::from(source);
  assert_eq!(owned.as_ptr(), pointer);
  assert_eq!(owned.as_ref(), b"boxed");

  let bytes = Bytes::copy_from_slice(&[0, 10, 255]);
  assert_eq!(serde_json::to_string(&bytes).unwrap(), "[0,10,255]");
  assert_eq!(serde_json::from_str::<Bytes>("[0,10,255]").unwrap(), bytes);
  assert_eq!(
    serde_json::from_str::<Bytes>(r#""héllo""#).unwrap().as_ref(),
    "héllo".as_bytes()
  );
  assert!(serde_json::from_str::<Bytes>("[256]").is_err());
  assert_eq!(format!("{bytes:?}"), r#"b"\0\n\xff""#);
  assert_eq!(hash(&bytes), hash(bytes.as_ref()));
  let values = HashMap::from([(Bytes::from_static(b"lookup"), 7)]);
  assert_eq!(values.get(&b"lookup"[..]), Some(&7));
  assert!(Bytes::from_static(b"a") < Bytes::from_static(b"b"));
}

#[test]
fn invalid_byte_ranges_and_splits_fail_without_modifying_the_original() {
  let bytes = Bytes::from_static(b"body");
  for (start, end) in [(3, 2), (0, 5), (5, 5)] {
    assert!(std::panic::catch_unwind(|| bytes.slice(start..end)).is_err());
  }
  assert!(std::panic::catch_unwind(|| bytes.slice(..=usize::MAX)).is_err());
  assert!(std::panic::catch_unwind(|| bytes.slice((Bound::Excluded(usize::MAX), Bound::Unbounded))).is_err());
  let mut mutable = bytes.clone();
  assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| mutable.split_to(5))).is_err());
  assert_eq!(mutable, bytes);
  assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| mutable.split_off(5))).is_err());
  assert_eq!(mutable, bytes);
}

#[cfg(feature = "std")]
#[test]
fn standard_storage_keeps_the_existing_upstream_type_identity() {
  let bytes: bytes::Bytes = Bytes::from_static(b"wire");
  let text: smol_str::SmolStr = SmolStr::new("provider");
  let _: Bytes = bytes;
  let _: SmolStr = text;
}
