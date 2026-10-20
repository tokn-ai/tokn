use alloc::{
  format,
  string::{String, ToString},
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};

use crate::extras::Extras;

/// Compile-time list of JSON keys belonging to a struct.
///
/// Implemented by `#[derive(tokn_endpoint_macros::LenientFields)]` on
/// each `*RequestParameters` / `*ExtraParameters` struct. Used by
/// [`peel_lenient`] to know which keys to pluck out of a raw JSON map
/// before attempting strict deserialization.
pub trait LenientFields {
  const FIELDS: &'static [&'static str];
}

/// Try to deserialize a `T` from the keys in `root` that are declared
/// in `T::FIELDS`, falling back per-field if the strict deserialize
/// fails.
///
/// Behavior:
/// 1. Remove every key in `T::FIELDS` from `root` into a candidate map.
/// 2. Try `serde_json::from_value::<T>(candidate)` whole. If it
///    succeeds, return that `T`.
/// 3. On failure, start from `T::default()` and try one field at a
///    time: for each `(key, value)` in the candidate, build a probe
///    object combining the current best with that single field
///    overlaid, and re-parse. If the field parses, commit it; if not,
///    push the raw `(key, value)` into `leftovers` so the caller can
///    fold it into the parent's `extras`.
///
/// Requires `T: Default + DeserializeOwned + Serialize + LenientFields`.
/// The `Serialize + Default` bounds are used to construct the
/// per-field probe object by round-tripping the running best `T`
/// through JSON.
pub fn peel_lenient<T>(root: &mut Map<String, Value>, leftovers: &mut Extras) -> T
where
  T: Default + DeserializeOwned + Serialize + LenientFields,
{
  // 1. Pull out declared keys.
  let mut candidate = Map::new();
  for k in T::FIELDS {
    if let Some(v) = root.remove(*k) {
      candidate.insert((*k).to_string(), v);
    }
  }
  if candidate.is_empty() {
    return T::default();
  }

  // 2. Whole-object attempt.
  if let Ok(t) = serde_json::from_value::<T>(Value::Object(candidate.clone())) {
    return t;
  }

  // 3. Per-field fallback.
  let mut best = T::default();
  for (k, v) in candidate {
    let mut probe_map = match serde_json::to_value(&best) {
      Ok(Value::Object(m)) => m,
      _ => Map::new(),
    };
    probe_map.insert(k.clone(), v.clone());
    match serde_json::from_value::<T>(Value::Object(probe_map)) {
      Ok(next) => best = next,
      Err(_) => {
        leftovers.insert(k, v);
      }
    }
  }
  best
}

/// Pull a strictly-typed required field out of a JSON object map.
///
/// Used by hand-written `Deserialize` impls on request structs to
/// extract structured top-level fields that should fail-fast on
/// missing/mismatched values, rather than fall through to
/// [`peel_lenient`].
pub fn take_required<T, E>(root: &mut Map<String, Value>, key: &str) -> Result<T, E>
where
  T: DeserializeOwned,
  E: serde::de::Error,
{
  let v = root
    .remove(key)
    .ok_or_else(|| E::custom(format!("missing field `{key}`")))?;
  serde_json::from_value(v).map_err(|e| E::custom(format!("invalid `{key}`: {e}")))
}

/// Pull a strictly-typed optional field out of a JSON object map.
///
/// `null` is treated as absent. Type mismatches still fail.
pub fn take_optional<T, E>(root: &mut Map<String, Value>, key: &str) -> Result<Option<T>, E>
where
  T: DeserializeOwned,
  E: serde::de::Error,
{
  match root.remove(key) {
    None | Some(Value::Null) => Ok(None),
    Some(v) => serde_json::from_value(v)
      .map(Some)
      .map_err(|e| E::custom(format!("invalid `{key}`: {e}"))),
  }
}

/// Pull a strictly-typed optional field, defaulting to
/// `T::default()` when absent or `null`.
pub fn take_optional_default<T, E>(root: &mut Map<String, Value>, key: &str) -> Result<T, E>
where
  T: DeserializeOwned + Default,
  E: serde::de::Error,
{
  match root.remove(key) {
    None | Some(Value::Null) => Ok(T::default()),
    Some(v) => serde_json::from_value(v).map_err(|e| E::custom(format!("invalid `{key}`: {e}"))),
  }
}

/// Drain remaining keys from `root` into `extras`. Call after
/// [`peel_lenient`] has consumed all declared parameter keys.
pub fn drain_into_extras(root: &mut Map<String, Value>, extras: &mut Extras) {
  for (k, v) in core::mem::take(root) {
    extras.insert(k, v);
  }
}
