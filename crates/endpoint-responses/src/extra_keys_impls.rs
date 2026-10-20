//! `ExtraKeys` implementations for responses types. Debug-only.

#![cfg(debug_assertions)]

use alloc::{string::String, vec::Vec};
use tokn_endpoint_core::{join_path, push_extras, ExtraKeys};

use crate::content::{InputContentPart, OutputContentPart, ReasoningPart};
use crate::event::ResponsesEvent;
use crate::item::{
  FunctionCallItem, FunctionCallOutputItem, InputItem, InputMessage, InputMessageContent, OutputItem, OutputMessage,
  ReasoningItem, TaggedFunctionCall, TaggedFunctionCallOutput, TaggedOutputMessage, TaggedReasoning,
};
use crate::request::{ResponsesInput, ResponsesRequest, ResponsesToolDef};
use crate::response::ResponsesResponse;
use crate::usage::ResponsesUsage;

impl ExtraKeys for ResponsesUsage {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for ResponsesRequest {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.input.extra_keys_into(out, &join_path(prefix, "input"));
    self.tools.extra_keys_into(out, &join_path(prefix, "tools"));
  }
}

impl ExtraKeys for ResponsesInput {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    if let ResponsesInput::Items(items) = self {
      items.extra_keys_into(out, prefix);
    }
  }
}

impl ExtraKeys for ResponsesToolDef {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for InputItem {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    match self {
      InputItem::Message(m) => m.extra_keys_into(out, prefix),
      InputItem::Reasoning(t) => t.extra_keys_into(out, prefix),
      InputItem::FunctionCall(t) => t.extra_keys_into(out, prefix),
      InputItem::FunctionCallOutput(t) => t.extra_keys_into(out, prefix),
      InputItem::Other(_) => {}
    }
  }
}

impl ExtraKeys for InputMessage {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.content.extra_keys_into(out, &join_path(prefix, "content"));
  }
}

impl ExtraKeys for InputMessageContent {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    if let InputMessageContent::Parts(parts) = self {
      parts.extra_keys_into(out, prefix);
    }
  }
}

impl ExtraKeys for InputContentPart {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    match self {
      InputContentPart::InputText { extras, .. } => push_extras(extras, prefix, out),
      InputContentPart::InputImage { fields }
      | InputContentPart::InputAudio { fields }
      | InputContentPart::InputFile { fields } => push_extras(fields, prefix, out),
      InputContentPart::Other => {}
    }
  }
}

impl ExtraKeys for TaggedReasoning {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    self.item.extra_keys_into(out, prefix);
  }
}

impl ExtraKeys for TaggedFunctionCall {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    self.item.extra_keys_into(out, prefix);
  }
}

impl ExtraKeys for TaggedFunctionCallOutput {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    self.item.extra_keys_into(out, prefix);
  }
}

impl ExtraKeys for ReasoningItem {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.content.extra_keys_into(out, &join_path(prefix, "content"));
    self.summary.extra_keys_into(out, &join_path(prefix, "summary"));
  }
}

impl ExtraKeys for ReasoningPart {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    match self {
      ReasoningPart::ReasoningText { extras, .. }
      | ReasoningPart::SummaryText { extras, .. }
      | ReasoningPart::Text { extras, .. } => push_extras(extras, prefix, out),
      ReasoningPart::Other => {}
    }
  }
}

impl ExtraKeys for FunctionCallItem {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for FunctionCallOutputItem {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
  }
}

impl ExtraKeys for OutputItem {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    match self {
      OutputItem::Message(m) => m.extra_keys_into(out, prefix),
      OutputItem::Reasoning(t) => t.extra_keys_into(out, prefix),
      OutputItem::FunctionCall(t) => t.extra_keys_into(out, prefix),
      OutputItem::Other(_) => {}
    }
  }
}

impl ExtraKeys for TaggedOutputMessage {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    self.message.extra_keys_into(out, prefix);
  }
}

impl ExtraKeys for OutputMessage {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.content.extra_keys_into(out, &join_path(prefix, "content"));
  }
}

impl ExtraKeys for OutputContentPart {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    match self {
      OutputContentPart::OutputText { extras, .. } | OutputContentPart::Refusal { extras, .. } => {
        push_extras(extras, prefix, out)
      }
      OutputContentPart::Other => {}
    }
  }
}

impl ExtraKeys for ResponsesResponse {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    push_extras(&self.extras, prefix, out);
    self.output.extra_keys_into(out, &join_path(prefix, "output"));
    self.tools.extra_keys_into(out, &join_path(prefix, "tools"));
    self.usage.extra_keys_into(out, &join_path(prefix, "usage"));
  }
}

impl ExtraKeys for ResponsesEvent {
  fn extra_keys_into(&self, out: &mut Vec<String>, prefix: &str) {
    use ResponsesEvent as E;
    match self {
      E::Created { response, extras, .. }
      | E::InProgress { response, extras, .. }
      | E::Completed { response, extras, .. }
      | E::Failed { response, extras, .. }
      | E::Incomplete { response, extras, .. } => {
        push_extras(extras, prefix, out);
        response.extra_keys_into(out, &join_path(prefix, "response"));
      }
      E::OutputItemAdded { extras, .. }
      | E::OutputItemDone { extras, .. }
      | E::ContentPartAdded { extras, .. }
      | E::ContentPartDone { extras, .. }
      | E::OutputTextDelta { extras, .. }
      | E::OutputTextDone { extras, .. }
      | E::ReasoningTextDelta { extras, .. }
      | E::ReasoningTextDone { extras, .. }
      | E::ReasoningSummaryPartAdded { extras, .. }
      | E::ReasoningSummaryTextDelta { extras, .. }
      | E::ReasoningSummaryTextDone { extras, .. }
      | E::FunctionCallArgumentsDelta { extras, .. }
      | E::FunctionCallArgumentsDone { extras, .. }
      | E::Error { extras, .. } => push_extras(extras, prefix, out),
      E::Other(_) => {}
    }
  }
}
