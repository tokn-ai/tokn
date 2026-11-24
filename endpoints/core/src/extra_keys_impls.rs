//! `ExtraKeys` implementations for `endpoint-core` types. Debug-only.

#![cfg(debug_assertions)]

use crate::extras::{push_extras, ExtraKeys};
use crate::tool::{ToolCall, ToolDef};
use alloc::{string::String, vec::Vec};

impl ExtraKeys for ToolCall {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for ToolDef {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}
