//! `ExtraKeys` implementations for messages types. Debug-only.

#![cfg(debug_assertions)]

use alloc::{string::String, vec::Vec};
use tokn_endpoint_core::{join_path, push_extras, ExtraKeys};

use crate::content::{ContentBlock, ContentBlockDelta};
use crate::event::MessagesEvent;
use crate::message::{Message, MessageContent};
use crate::request::{MessagesRequest, MessagesToolDef, SystemPrompt};
use crate::response::MessagesResponse;
use crate::usage::MessagesUsage;

impl ExtraKeys for MessagesUsage {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for MessagesRequest {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.messages.extra_keys_into(out, &join_path(prefix, "messages"));
    self.system.extra_keys_into(out, &join_path(prefix, "system"));
    self.tools.extra_keys_into(out, &join_path(prefix, "tools"));
  }
}

impl ExtraKeys for SystemPrompt {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    if let SystemPrompt::Blocks(blocks) = self {
      blocks.extra_keys_into(out, prefix);
    }
  }
}

impl ExtraKeys for MessagesToolDef {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for Message {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.content.extra_keys_into(out, &join_path(prefix, "content"));
  }
}

impl ExtraKeys for MessageContent {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    if let MessageContent::Blocks(blocks) = self {
      blocks.extra_keys_into(out, prefix);
    }
  }
}

impl ExtraKeys for ContentBlock {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    match self {
      ContentBlock::Text { extras, .. }
      | ContentBlock::Thinking { extras, .. }
      | ContentBlock::ToolUse { extras, .. }
      | ContentBlock::ToolResult { extras, .. }
      | ContentBlock::Image { extras, .. }
      | ContentBlock::Document { extras, .. } => push_extras(extras, prefix, out),
      ContentBlock::RedactedThinking { fields } => push_extras(fields, prefix, out),
      ContentBlock::Other => {}
    }
  }
}

impl ExtraKeys for ContentBlockDelta {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    match self {
      ContentBlockDelta::TextDelta { extras, .. }
      | ContentBlockDelta::ThinkingDelta { extras, .. }
      | ContentBlockDelta::SignatureDelta { extras, .. }
      | ContentBlockDelta::InputJsonDelta { extras, .. } => push_extras(extras, prefix, out),
      ContentBlockDelta::Other => {}
    }
  }
}

impl ExtraKeys for MessagesResponse {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.content.extra_keys_into(out, &join_path(prefix, "content"));
    self.usage.extra_keys_into(out, &join_path(prefix, "usage"));
  }
}

impl ExtraKeys for MessagesEvent {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    use MessagesEvent as E;
    match self {
      E::MessageStart { message, extras } => {
        push_extras(extras, prefix, out);
        message.extra_keys_into(out, &join_path(prefix, "message"));
      }
      E::ContentBlockStart {
        content_block, extras, ..
      } => {
        push_extras(extras, prefix, out);
        content_block.extra_keys_into(out, &join_path(prefix, "content_block"));
      }
      E::ContentBlockDelta { delta, extras, .. } => {
        push_extras(extras, prefix, out);
        delta.extra_keys_into(out, &join_path(prefix, "delta"));
      }
      E::ContentBlockStop { extras, .. } | E::MessageStop { extras } | E::Ping { extras } | E::Error { extras, .. } => {
        push_extras(extras, prefix, out)
      }
      E::MessageDelta { usage, extras, .. } => {
        push_extras(extras, prefix, out);
        usage.extra_keys_into(out, &join_path(prefix, "usage"));
      }
      E::Other(_) => {}
    }
  }
}
