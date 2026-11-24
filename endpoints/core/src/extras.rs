use alloc::string::String;
#[cfg(debug_assertions)]
use alloc::{boxed::Box, format, string::ToString, vec::Vec};
use serde_json::{Map, Value};

/// Catch-all map for unknown JSON fields preserved during deserialization.
///
/// Endpoint structs typically embed this with
/// `#[serde(default, flatten)] pub extras: Extras` to remain forward
/// compatible with provider-specific or unreleased fields.
pub type Extras = Map<String, Value>;

/// Debug-only trait that walks a value tree and reports which `extras`
/// keys were captured (i.e. fields the typed schemas don't yet model).
///
/// Implementations push fully-qualified key paths into `out`. The path
/// format is `prefix.key` with `[]` denoting a list index. Only compiled
/// in `debug_assertions` builds — release builds keep the wire types lean.
#[cfg(debug_assertions)]
pub trait ExtraKeys {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str);

  fn extra_keys(&self) -> Vec<String> {
    let mut v = Vec::new();
    self.extra_keys_into(&mut v, "");
    v
  }
}

#[cfg(debug_assertions)]
pub fn push_extras(extras: &Extras, prefix: &str, out: &mut Vec<String>) {
  for k in extras.keys() {
    if prefix.is_empty() {
      out.push(k.clone());
    } else {
      out.push(format!("{prefix}.{k}"));
    }
  }
}

#[cfg(debug_assertions)]
pub fn join_path(prefix: &str, segment: &str) -> String {
  if prefix.is_empty() {
    segment.to_string()
  } else {
    format!("{prefix}.{segment}")
  }
}

#[cfg(debug_assertions)]
impl<T: ExtraKeys> ExtraKeys for Option<T> {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    if let Some(v) = self {
      v.extra_keys_into(out, prefix);
    }
  }
}

#[cfg(debug_assertions)]
impl<T: ExtraKeys> ExtraKeys for Vec<T> {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    let elem_prefix = format!("{prefix}[]");
    for v in self {
      v.extra_keys_into(out, &elem_prefix);
    }
  }
}

#[cfg(debug_assertions)]
impl<T: ExtraKeys> ExtraKeys for Box<T> {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    (**self).extra_keys_into(out, prefix);
  }
}
