//! Request purposes for inspection and provider request policies.
//! Missing evidence leaves the purpose unknown.

use crate::provider::Endpoint;
use crate::request_event::RequestEndpoint;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestPurpose {
  Compaction,
}

impl RequestPurpose {
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Compaction => "compaction",
    }
  }
}

/// The concrete signal that identified a request's purpose. Prompt sources
/// name a recognized template, independently of the configured wire identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestClassificationSource {
  Endpoint,
  RequestField,
  CodexPrompt,
  ClaudeCodePrompt,
  OpencodePrompt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestClassification {
  pub purpose: RequestPurpose,
  pub source: RequestClassificationSource,
}

const CODEX_COMPACTION_PREFIX: &str = "You are performing a CONTEXT CHECKPOINT COMPACTION.";
const CODEX_HANDOFF_INSTRUCTION: &str = "Create a handoff summary for another LLM that will resume the task.";
const OPENCODE_LEGACY_SYSTEM: &str = "You are a helpful AI assistant tasked with summarizing conversations.";
const OPENCODE_LEGACY_PROMPT: &str = "Provide a detailed prompt for continuing our conversation above.";

/// Detect explicit compaction endpoints and recognized agent prompt templates.
/// Only the final input item or user block is inspected: historical prompts, tool
/// results, `context_management`, and replayed compaction items are not signals
/// that this request performs compaction.
pub fn classify_request(endpoint: &RequestEndpoint, body: &Value) -> Option<RequestClassification> {
  let path = endpoint.as_str().split(['?', '#']).next()?.trim_end_matches('/');
  let source = if path.ends_with("/responses/compact") {
    RequestClassificationSource::Endpoint
  } else {
    let operation = endpoint.resolved().or_else(|| Endpoint::infer_from(path))?;
    let explicit_field = match operation {
      // Codex remote compaction appends this request control after the history.
      // https://github.com/openai/codex/blob/main/codex-rs/core/src/compact_remote_v2_attempt.rs
      Endpoint::Responses => {
        body
          .get("input")
          .and_then(Value::as_array)
          .and_then(|items| items.last())
          .and_then(|item| item.get("type"))
          .and_then(Value::as_str)
          == Some("compaction_trigger")
      }
      // https://platform.claude.com/docs/en/build-with-claude/compaction-on-demand
      // The beta header also accompanies normal requests carrying an old summary.
      Endpoint::Messages => {
        body
          .get("compaction")
          .and_then(|value| value.get("type"))
          .and_then(Value::as_str)
          == Some("summarize")
      }
      Endpoint::ChatCompletions => false,
    };
    if explicit_field {
      RequestClassificationSource::RequestField
    } else {
      let text = active_user_text(body)?.trim();
      if is_codex_compaction(text) {
        RequestClassificationSource::CodexPrompt
      } else if is_opencode_compaction(body, text) {
        RequestClassificationSource::OpencodePrompt
      } else if is_claude_code_compaction(text) {
        RequestClassificationSource::ClaudeCodePrompt
      } else {
        return None;
      }
    }
  };
  Some(RequestClassification {
    purpose: RequestPurpose::Compaction,
    source,
  })
}

fn active_user_text(body: &Value) -> Option<&str> {
  let item = if let Some(input) = body.get("input") {
    if let Some(text) = input.as_str() {
      return Some(text);
    }
    match input.as_array() {
      Some(items) => items.last()?,
      None => input,
    }
  } else {
    body.get("messages")?.as_array()?.last()?
  };
  if item.get("role")?.as_str()? != "user" {
    return None;
  }
  if item
    .get("type")
    .and_then(Value::as_str)
    .is_some_and(|kind| kind != "message")
  {
    return None;
  }
  content_text(item.get("content")?)
}

fn content_text(content: &Value) -> Option<&str> {
  if let Some(text) = content.as_str() {
    return Some(text);
  }
  let block = content.as_array()?.last()?;
  match block.get("type")?.as_str()? {
    "text" | "input_text" => block.get("text")?.as_str(),
    _ => None,
  }
}

fn system_starts_with(body: &Value, prefix: &str) -> bool {
  [body.get("instructions"), body.get("system")]
    .into_iter()
    .flatten()
    .any(|content| content_starts_with(content, prefix))
    || body.get("messages").and_then(Value::as_array).is_some_and(|messages| {
      messages.iter().any(|message| {
        matches!(
          message.get("role").and_then(Value::as_str),
          Some("system" | "developer")
        ) && message
          .get("content")
          .is_some_and(|content| content_starts_with(content, prefix))
      })
    })
}

fn content_starts_with(content: &Value, prefix: &str) -> bool {
  content
    .as_str()
    .is_some_and(|text| text.trim_start().starts_with(prefix))
    || content.as_array().is_some_and(|blocks| {
      blocks.iter().any(|block| {
        matches!(block.get("type").and_then(Value::as_str), Some("text" | "input_text"))
          && block
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| text.trim_start().starts_with(prefix))
      })
    })
}

fn is_codex_compaction(text: &str) -> bool {
  // https://github.com/openai/codex/blob/main/codex-rs/prompts/templates/compact/prompt.md
  text
    .strip_prefix(CODEX_COMPACTION_PREFIX)
    .is_some_and(|tail| tail.trim_start().starts_with(CODEX_HANDOFF_INSTRUCTION))
}

fn is_claude_code_compaction(text: &str) -> bool {
  // Anthropic's published @anthropic-ai/claude-code 2.1.0 uses the summary-first
  // template. Newer observed requests prepend the tool prohibition:
  // https://github.com/anthropics/claude-code/issues/70459
  let summary_instruction = "Your task is to create a detailed summary of the conversation so far";
  let recognized_start = (text.starts_with("CRITICAL: Respond with TEXT ONLY.") && text.contains(summary_instruction))
    || (text
      .strip_prefix(summary_instruction)
      .is_some_and(|tail| tail.starts_with(','))
      && text.contains("code patterns, and architectural decisions"));
  recognized_start && text.contains("<analysis>") && text.contains("<summary>")
}

fn is_opencode_compaction(body: &Value, text: &str) -> bool {
  if system_starts_with(body, OPENCODE_LEGACY_SYSTEM) && text.starts_with(OPENCODE_LEGACY_PROMPT) {
    return true;
  }
  // https://github.com/anomalyco/opencode/blob/dev/packages/core/src/session/compaction.ts
  let Some(conversation) = text.strip_prefix("Here is the conversation so far:") else {
    return false;
  };
  if !conversation.trim_start().starts_with("<conversation>") {
    return false;
  }
  // Ignore the transcript itself, which may contain earlier compaction prompts.
  let Some((_, instructions)) = text.rsplit_once("</conversation>") else {
    return false;
  };
  let instructions = instructions.trim_start();
  instructions.starts_with("Create a new anchored summary")
    || (instructions.starts_with("Here is the summary of the conversation before")
      && instructions.contains("<prior-summary>")
      && instructions.rsplit_once("</prior-summary>").is_some_and(|(_, tail)| {
        tail
          .trim_start()
          .starts_with("The <prior-summary> summarizes everything")
      }))
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  fn compaction(source: RequestClassificationSource) -> Option<RequestClassification> {
    Some(RequestClassification {
      purpose: RequestPurpose::Compaction,
      source,
    })
  }

  fn codex_prompt() -> String {
    format!("{CODEX_COMPACTION_PREFIX}\n\n{CODEX_HANDOFF_INSTRUCTION}\nInclude progress and next steps.")
  }

  #[test]
  fn dedicated_endpoint_does_not_depend_on_body() {
    for path in [
      "/responses/compact",
      "/v1/responses/compact",
      "/backend-api/codex/responses/compact",
      "/v1/responses/compact/?client_version=test",
    ] {
      assert_eq!(
        classify_request(&RequestEndpoint::custom(path), &Value::Null),
        compaction(RequestClassificationSource::Endpoint),
        "{path}"
      );
    }
    for path in [
      "/responses",
      "/responses/compact-extra",
      "/responses/compact/history",
      "/compact",
    ] {
      assert_eq!(
        classify_request(&RequestEndpoint::custom(path), &Value::Null),
        None,
        "{path}"
      );
    }
  }

  #[test]
  fn codex_prompt_supports_endpoint_message_shapes() {
    let prompt = codex_prompt();
    for (endpoint, body) in [
      (Endpoint::Responses.into(), json!({"input": prompt})),
      (
        Endpoint::Responses.into(),
        json!({"input": {"role": "user", "content": prompt}}),
      ),
      (
        Endpoint::Responses.into(),
        json!({"input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": prompt}]}]}),
      ),
      (
        Endpoint::ChatCompletions.into(),
        json!({"messages": [{"role": "user", "content": prompt}]}),
      ),
      (
        Endpoint::Messages.into(),
        json!({"messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}]}),
      ),
      (
        RequestEndpoint::custom("/v1/responses?client_version=test"),
        json!({"input": prompt}),
      ),
    ] {
      assert_eq!(
        classify_request(&endpoint, &body),
        compaction(RequestClassificationSource::CodexPrompt)
      );
    }
  }

  #[test]
  fn responses_compaction_trigger_is_an_explicit_request_field() {
    let body = json!({"input": [
      {"role": "user", "content": "work on the task"},
      {"type": "compaction", "encrypted_content": "previous summary"},
      {"type": "compaction_trigger"}
    ]});
    for endpoint in [
      Endpoint::Responses.into(),
      RequestEndpoint::custom("/backend-api/codex/responses"),
      RequestEndpoint::custom("/v1/responses?client_version=test"),
    ] {
      assert_eq!(
        classify_request(&endpoint, &body),
        compaction(RequestClassificationSource::RequestField)
      );
    }
    for endpoint in [
      Endpoint::Messages.into(),
      Endpoint::ChatCompletions.into(),
      RequestEndpoint::custom("/search"),
    ] {
      assert_eq!(classify_request(&endpoint, &body), None);
    }
  }

  #[test]
  fn historical_or_nested_triggers_are_not_active_compaction() {
    for body in [
      json!({"input": [{"type": "compaction_trigger"}, {"role": "user", "content": "continue"}]}),
      json!({"input": [{"type": "compaction_trigger"}, {"type": "function_call_output", "output": "done"}]}),
      json!({"input": [{"type": "compaction", "encrypted_content": "previous summary"}]}),
      json!({"input": [{"role": "user", "content": [{"type": "compaction_trigger"}]}]}),
      json!({"input": [{"type": "compaction_trigger_extra"}]}),
      json!({"input": "Explain compaction_trigger"}),
      json!({"input": []}),
    ] {
      assert_eq!(classify_request(&Endpoint::Responses.into(), &body), None, "{body}");
    }
  }

  #[test]
  fn history_and_tool_results_do_not_classify_active_request() {
    let prompt = codex_prompt();
    for body in [
      json!({"input": [{"role": "user", "content": prompt}, {"role": "assistant", "content": "summary"}]}),
      json!({"input": [{"role": "user", "content": prompt}, {"role": "user", "content": "continue"}]}),
      json!({"input": [{"role": "user", "content": prompt}, {"type": "function_call_output", "output": "done"}]}),
      json!({"messages": [{"role": "tool", "content": prompt}]}),
      json!({"messages": [{"role": "user", "content": [{"type": "tool_result", "content": prompt}]}]}),
      json!({"messages": [{"role": "user", "content": [{"type": "text", "text": prompt}, {"type": "text", "text": "explain this template"}]}]}),
    ] {
      assert_eq!(classify_request(&Endpoint::Responses.into(), &body), None, "{body}");
    }
  }

  #[test]
  fn mentions_and_replayed_state_are_not_new_compaction() {
    for body in [
      json!({"input": "Please summarize the conversation so far."}),
      json!({"input": format!("Explain this prompt:\n{}", codex_prompt())}),
      json!({"input": CODEX_COMPACTION_PREFIX}),
      json!({"input": [{"type": "compaction", "encrypted_content": "previous summary"}, {"role": "user", "content": "continue"}]}),
      json!({"context_management": [{"type": "compaction", "compact_threshold": 10000}], "input": "continue"}),
      json!({"messages": [{"role": "assistant", "content": [{"type": "compaction", "content": "previous summary"}]}, {"role": "user", "content": "continue"}]}),
    ] {
      assert_eq!(classify_request(&Endpoint::Responses.into(), &body), None, "{body}");
    }
    assert_eq!(
      classify_request(&RequestEndpoint::custom("/search"), &json!({"input": codex_prompt()})),
      None
    );
  }

  #[test]
  fn anthropic_on_demand_field_is_endpoint_specific() {
    let body =
      json!({"compaction": {"type": "summarize"}, "messages": [{"role": "assistant", "content": "last turn"}]});
    assert_eq!(
      classify_request(&Endpoint::Messages.into(), &body),
      compaction(RequestClassificationSource::RequestField)
    );
    assert_eq!(classify_request(&Endpoint::Responses.into(), &body), None);
    for compaction_field in [
      Value::Null,
      json!(true),
      json!({"type": "other"}),
      json!({"instructions": "summarize"}),
    ] {
      assert_eq!(
        classify_request(&Endpoint::Messages.into(), &json!({"compaction": compaction_field})),
        None
      );
    }
  }

  #[test]
  fn claude_code_prompt_requires_distinctive_instructions() {
    let prompt = "CRITICAL: Respond with TEXT ONLY. Do not use tools.\nYour task is to create a detailed summary of the conversation so far.\nUse <analysis> and <summary>.";
    let body = json!({"messages": [{"role": "user", "content": [{"type": "text", "text": "earlier content"}, {"type": "text", "text": prompt}]}]});
    assert_eq!(
      classify_request(&Endpoint::Messages.into(), &body),
      compaction(RequestClassificationSource::ClaudeCodePrompt)
    );
    let legacy = "Your task is to create a detailed summary of the conversation so far, paying close attention to the user's explicit requests and your previous actions.\nCapture technical details, code patterns, and architectural decisions.\n<analysis>\n<summary>";
    assert_eq!(
      classify_request(
        &Endpoint::Messages.into(),
        &json!({"messages": [{"role": "user", "content": legacy}]})
      ),
      compaction(RequestClassificationSource::ClaudeCodePrompt)
    );
    for text in [
      "CRITICAL: Respond with TEXT ONLY. Explain this code.",
      "Your task is to create a detailed summary of the conversation so far.",
      "Explain the phrase CRITICAL: Respond with TEXT ONLY.",
    ] {
      assert_eq!(
        classify_request(
          &Endpoint::Messages.into(),
          &json!({"messages": [{"role": "user", "content": text}]})
        ),
        None
      );
    }
  }

  #[test]
  fn opencode_legacy_requires_system_and_user_template() {
    for body in [
      json!({"system": OPENCODE_LEGACY_SYSTEM, "messages": [{"role": "user", "content": OPENCODE_LEGACY_PROMPT}]}),
      json!({"system": [{"type": "text", "text": OPENCODE_LEGACY_SYSTEM}], "messages": [{"role": "user", "content": OPENCODE_LEGACY_PROMPT}]}),
      json!({"messages": [{"role": "system", "content": OPENCODE_LEGACY_SYSTEM}, {"role": "user", "content": OPENCODE_LEGACY_PROMPT}]}),
      json!({"instructions": OPENCODE_LEGACY_SYSTEM, "input": OPENCODE_LEGACY_PROMPT}),
    ] {
      assert_eq!(
        classify_request(&Endpoint::ChatCompletions.into(), &body),
        compaction(RequestClassificationSource::OpencodePrompt)
      );
    }
    assert_eq!(
      classify_request(
        &Endpoint::ChatCompletions.into(),
        &json!({"messages": [{"role": "user", "content": OPENCODE_LEGACY_PROMPT}]})
      ),
      None
    );
    assert_eq!(
      classify_request(
        &Endpoint::ChatCompletions.into(),
        &json!({"system": OPENCODE_LEGACY_SYSTEM, "messages": [{"role": "user", "content": "continue the task"}]})
      ),
      None
    );
  }

  #[test]
  fn opencode_anchored_templates_ignore_transcript_contents() {
    let conversation = format!(
      "Here is the conversation so far:\n\n<conversation>\n{}\n</conversation>\n\n",
      codex_prompt()
    );
    for instruction in [
      "Create a new anchored summary from the conversation history.",
      "Here is the summary of the conversation before the <conversation> above:\n\n<prior-summary>\nearlier summary\n</prior-summary>\n\nThe <prior-summary> summarizes everything that happened before the <conversation>. Construct a new summary that combines both.",
    ] {
      assert_eq!(
        classify_request(&Endpoint::Responses.into(), &json!({"input": format!("{conversation}{instruction}")})),
        compaction(RequestClassificationSource::OpencodePrompt)
      );
    }
    assert_eq!(
      classify_request(
        &Endpoint::Responses.into(),
        &json!({"input": format!("{conversation}Continue working.")})
      ),
      None
    );
  }

  #[test]
  fn classification_round_trips_with_snake_case_fields() {
    let classification = compaction(RequestClassificationSource::ClaudeCodePrompt).unwrap();
    let serialized = json!({"purpose": "compaction", "source": "claude_code_prompt"});
    assert_eq!(serde_json::to_value(classification).unwrap(), serialized);
    assert_eq!(
      serde_json::from_value::<RequestClassification>(serialized).unwrap(),
      classification
    );
  }
}
