//! The `HeaderSchema` trait + helper functions used by per-(persona, overlay)
//! schema structs to round-trip between their typed Rust form and the
//! generic [`HeaderMap`].
//!
//! Each schema struct hand-implements [`HeaderSchema`] in Phase 1; a
//! proc-macro derive may follow in a later phase.

use crate::error::Error;
use crate::map::HeaderMap;
use crate::name::HeaderName;
use crate::value::HeaderValue;
use smol_str::SmolStr;

/// A typed view over a subset of HTTP headers belonging to a particular
/// persona or provider overlay.
pub trait HeaderSchema: Sized {
  /// Build the typed struct from a [`HeaderMap`]. Returns [`Error::MissingHeader`]
  /// when a required header is absent and [`Error::InvalidValue`] when a value
  /// fails domain-specific validation.
  fn parse(map: &HeaderMap) -> Result<Self, Error>;

  /// Render the typed struct back into a [`HeaderMap`]. Inverse of [`parse`];
  /// optional fields that are `None` are omitted from the output.
  ///
  /// The verb `dump` is deliberately distinct from `build`: a future `build`
  /// constructor (or `build_from_vars`) will *construct* a populated schema
  /// from a [`crate::TemplateVars`] instance. `dump` only round-trips an
  /// already-populated schema.
  fn dump(&self) -> HeaderMap;

  /// All header names that this schema may emit. Useful for golden-test
  /// allowlists and schema documentation.
  fn known_names() -> &'static [&'static HeaderName];
}

/// Read a required header value as a [`SmolStr`].
pub fn required(map: &HeaderMap, name: &HeaderName) -> Result<SmolStr, Error> {
  map
    .get(name)
    .map(|v| SmolStr::new(v.as_str()))
    .ok_or_else(|| Error::MissingHeader {
      name: SmolStr::new(name.as_str()),
    })
}

/// Read an optional header value as `Option<SmolStr>`.
pub fn optional(map: &HeaderMap, name: &HeaderName) -> Option<SmolStr> {
  map.get(name).map(|v| SmolStr::new(v.as_str()))
}

/// Combine every value of an optional header using its protocol's delimiter.
pub fn optional_joined(map: &HeaderMap, name: &HeaderName, separator: &str) -> Option<SmolStr> {
  let mut values = map.get_all(name);
  let mut joined = values.next()?.as_str().to_string();
  for value in values {
    joined.push_str(separator);
    joined.push_str(value.as_str());
  }
  Some(joined.into())
}

/// Insert a `SmolStr` value into the map under `name`.
pub fn put(map: &mut HeaderMap, name: &HeaderName, value: &SmolStr) {
  map.insert(name.clone(), HeaderValue::from_string(value.to_string()));
}

/// Insert a `SmolStr` value into the map under `name` only if `Some`.
pub fn put_opt(map: &mut HeaderMap, name: &HeaderName, value: &Option<SmolStr>) {
  if let Some(v) = value {
    put(map, name, v);
  }
}

/// Read a header value from `inbound` if present, otherwise compute the
/// supplied default. Used by `build` constructors on persona/overlay structs
/// to populate required fields with persona-specific fallbacks.
pub fn from_inbound_or<F: FnOnce() -> SmolStr>(inbound: &HeaderMap, key: &HeaderName, default: F) -> SmolStr {
  inbound
    .get(key)
    .map(|v| SmolStr::from(v.as_str()))
    .unwrap_or_else(default)
}

/// Read an optional header value from `inbound`. Returns `None` if absent.
/// Sister to [`from_inbound_or`] for non-required `build` fields.
pub fn opt_from_inbound(inbound: &HeaderMap, key: &HeaderName) -> Option<SmolStr> {
  inbound.get(key).map(|v| SmolStr::from(v.as_str()))
}
