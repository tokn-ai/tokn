//! `ExtraKeys` implementations for chat-completions types. Debug-only.

#![cfg(debug_assertions)]

use alloc::{string::String, vec::Vec};
use tokn_endpoint_core::{join_path, push_extras, ExtraKeys};

use crate::content::{ChatContent, ContentPart};
use crate::event::{ChatChunk, ChatDelta, ChatEvent, ChunkChoice};
use crate::message::{ChatMessage, ChatToolCall, ChatToolFunction};
use crate::request::{ChatRequest, ChatToolDef};
use crate::response::{ChatChoice, ChatResponse};
use crate::usage::ChatUsage;

impl ExtraKeys for ChatUsage {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for ChatRequest {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.messages.extra_keys_into(out, &join_path(prefix, "messages"));
    self.tools.extra_keys_into(out, &join_path(prefix, "tools"));
  }
}

impl ExtraKeys for ChatToolDef {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for ChatMessage {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.content.extra_keys_into(out, &join_path(prefix, "content"));
    self.tool_calls.extra_keys_into(out, &join_path(prefix, "tool_calls"));
  }
}

impl ExtraKeys for ChatContent {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    if let ChatContent::Parts(parts) = self {
      parts.extra_keys_into(out, prefix);
    }
  }
}

impl ExtraKeys for ContentPart {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    match self {
      ContentPart::Text { extras, .. }
      | ContentPart::ImageUrl { extras, .. }
      | ContentPart::InputAudio { extras, .. } => push_extras(extras, prefix, out),
      ContentPart::Other => {}
    }
  }
}

impl ExtraKeys for ChatToolCall {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.function.extra_keys_into(out, &join_path(prefix, "function"));
  }
}

impl ExtraKeys for ChatToolFunction {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for ChatResponse {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.choices.extra_keys_into(out, &join_path(prefix, "choices"));
    self.usage.extra_keys_into(out, &join_path(prefix, "usage"));
  }
}

impl ExtraKeys for ChatChoice {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.message.extra_keys_into(out, &join_path(prefix, "message"));
  }
}

impl ExtraKeys for ChatChunk {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.choices.extra_keys_into(out, &join_path(prefix, "choices"));
    self.usage.extra_keys_into(out, &join_path(prefix, "usage"));
  }
}

impl ExtraKeys for ChunkChoice {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.delta.extra_keys_into(out, &join_path(prefix, "delta"));
  }
}

impl ExtraKeys for ChatDelta {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.tool_calls.extra_keys_into(out, &join_path(prefix, "tool_calls"));
  }
}

impl ExtraKeys for ChatEvent {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    if let ChatEvent::Chunk(c) = self {
      c.extra_keys_into(out, prefix);
    }
  }
}
