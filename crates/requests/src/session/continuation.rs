//! Route-bound native conversation checkpoints.
//!
//! Native history is retained before any endpoint conversion. Replay appends an
//! explicitly supplied fresh suffix; callers own portable history validation.

use alloc::{collections::BTreeSet, string::String, vec::Vec};
use core::fmt;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use tokn_core::{util::digest::sha256_hex, Endpoint};

const STATE_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationPolicy {
  #[default]
  RequireCompatible,
  AllowRestart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationStatus {
  Started,
  Continued,
  Restarted,
}

/// Exact route identity. The backend URL is stored only as a SHA-256 digest.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderOrigin {
  pub provider: String,
  pub driver: String,
  pub account: String,
  pub model: String,
  #[serde(deserialize_with = "deserialize_endpoint")]
  pub endpoint: Endpoint,
  backend: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  provider_account_digest: Option<String>,
}

impl ProviderOrigin {
  pub fn new(
    provider: impl Into<String>,
    driver: impl Into<String>,
    account: impl Into<String>,
    backend: &str,
    endpoint: Endpoint,
    model: impl Into<String>,
  ) -> Self {
    Self {
      provider: provider.into(),
      driver: driver.into(),
      account: account.into(),
      model: model.into(),
      endpoint,
      backend: sha256_hex(backend.as_bytes()),
      provider_account_digest: None,
    }
  }

  pub fn backend_digest(&self) -> &str {
    &self.backend
  }

  /// Bind an explicitly supplied upstream account identity independently of
  /// credential material, so refreshing tokens does not change this scope.
  pub fn with_provider_account_id(mut self, account_id: Option<&str>) -> Self {
    self.provider_account_digest = account_id.map(|account_id| sha256_hex(account_id.as_bytes()));
    self
  }
}

impl fmt::Debug for ProviderOrigin {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ProviderOrigin").finish_non_exhaustive()
  }
}

/// Serialized native state contains conversation content and provider opaque
/// data. Treat it with the same confidentiality as the conversation itself.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderState {
  version: u32,
  origin: ProviderOrigin,
  context_digest: String,
  history: Vec<Value>,
}

impl ProviderState {
  pub fn origin(&self) -> &ProviderOrigin {
    &self.origin
  }

  /// Automatic fallback can discard opaque reasoning, but it must not silently
  /// omit user-visible native output that portable text/function messages
  /// cannot represent. Explicitly clearing the checkpoint is a caller decision.
  pub(crate) fn validate_restart(&self) -> Result<()> {
    if self.version != STATE_VERSION {
      return Err(ContinuationError::InvalidState("unsupported checkpoint version"));
    }
    validate_origin(&self.origin)?;
    if self.context_digest.len() != 64 || !self.context_digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
      return Err(ContinuationError::InvalidState("invalid native settings digest"));
    }
    validate_history(self.origin.endpoint, &self.history, true)?;
    for item in &self.history {
      match self.origin.endpoint {
        Endpoint::Responses => match item["type"].as_str() {
          Some("custom_tool_call" | "custom_tool_call_output") => {
            return Err(unportable_restart());
          }
          Some("message") | None if item["role"] == "assistant" => {
            validate_text_restart_content(&item["content"], &["text", "input_text", "output_text"])?;
          }
          _ => {}
        },
        Endpoint::ChatCompletions if item["role"] == "assistant" => {
          if item.get("refusal").is_some_and(|value| !value.is_null())
            || item.get("audio").is_some_and(|value| !value.is_null())
          {
            return Err(unportable_restart());
          }
          if !item["content"].is_null() {
            validate_text_restart_content(&item["content"], &["text"])?;
          }
        }
        Endpoint::Messages if item["role"] == "assistant" => {
          if let Some(blocks) = item["content"].as_array() {
            if blocks.iter().any(|block| {
              !matches!(
                block["type"].as_str(),
                Some("text" | "thinking" | "redacted_thinking" | "tool_use")
              )
            }) {
              return Err(unportable_restart());
            }
          }
        }
        _ => {}
      }
    }
    Ok(())
  }
}

fn unportable_restart() -> ContinuationError {
  ContinuationError::Unsupported("native assistant output cannot be preserved by automatic portable restart")
}

fn validate_text_restart_content(content: &Value, text_types: &[&str]) -> Result<()> {
  if content.is_string() {
    return Ok(());
  }
  let parts = content.as_array().ok_or_else(unportable_restart)?;
  if parts
    .iter()
    .any(|part| !part["type"].as_str().is_some_and(|kind| text_types.contains(&kind)) || !part["text"].is_string())
  {
    return Err(unportable_restart());
  }
  Ok(())
}

impl fmt::Debug for ProviderState {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ProviderState").finish_non_exhaustive()
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuationError {
  Unsupported(&'static str),
  Incompatible(&'static str),
  InvalidState(&'static str),
}

impl fmt::Display for ContinuationError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Unsupported(reason) => write!(f, "native continuation is unsupported: {reason}"),
      Self::Incompatible(reason) => write!(f, "native continuation is incompatible: {reason}"),
      Self::InvalidState(reason) => write!(f, "invalid native continuation state: {reason}"),
    }
  }
}
impl core::error::Error for ContinuationError {}

type Result<T> = core::result::Result<T, ContinuationError>;

/// Capture the complete native input and the provider's native assistant output.
/// Server-side handles are deliberately unsupported: native replay also works
/// when Responses storage is disabled and retains encrypted reasoning items.
pub fn capture(origin: ProviderOrigin, native_request: &Value, native_response: &Value) -> Result<ProviderState> {
  validate_origin(&origin)?;
  validate_model(&origin, native_request)?;
  let context_digest = context_digest(origin.endpoint, native_request)?;
  let mut history = input(origin.endpoint, native_request)?;
  validate_history(origin.endpoint, &history, false)?;
  history.extend(assistant_output(origin.endpoint, native_response)?);
  validate_history(origin.endpoint, &history, true)?;
  Ok(ProviderState {
    version: STATE_VERSION,
    origin,
    context_digest,
    history,
  })
}

/// Append only the newly prepared native suffix to the retained native history.
/// Both route identity and all non-stream request settings must match.
pub fn replay(state: &ProviderState, origin: &ProviderOrigin, fresh_native_suffix: &Value) -> Result<Value> {
  validate_origin(origin)?;
  validate_model(origin, fresh_native_suffix)?;
  if state.version != STATE_VERSION {
    return Err(ContinuationError::InvalidState("unsupported checkpoint version"));
  }
  if state.origin != *origin {
    return Err(ContinuationError::Incompatible("provider route changed"));
  }
  if state.context_digest != context_digest(origin.endpoint, fresh_native_suffix)? {
    return Err(ContinuationError::Incompatible(
      "request instructions, tools, or settings changed",
    ));
  }
  validate_history(origin.endpoint, &state.history, true)?;
  let mut suffix = input(origin.endpoint, fresh_native_suffix)?;
  if origin.endpoint == Endpoint::ChatCompletions {
    let instructions = chat_instruction_count(&suffix)?;
    suffix.drain(..instructions);
  }
  if origin.endpoint == Endpoint::Responses {
    let custom_calls: BTreeSet<_> = state
      .history
      .iter()
      .filter(|item| item["type"] == "custom_tool_call")
      .filter_map(|item| item["call_id"].as_str())
      .collect();
    for item in &mut suffix {
      if item["type"] == "function_call_output" && item["call_id"].as_str().is_some_and(|id| custom_calls.contains(id))
      {
        item["type"] = json!("custom_tool_call_output");
      }
    }
  }
  let mut history = state.history.clone();
  history.extend(suffix);
  validate_history(origin.endpoint, &history, false)?;
  let mut request = fresh_native_suffix.clone();
  request[history_key(origin.endpoint)] = Value::Array(history);
  Ok(request)
}

fn deserialize_endpoint<'de, D: Deserializer<'de>>(deserializer: D) -> core::result::Result<Endpoint, D::Error> {
  match String::deserialize(deserializer)?.as_str() {
    "chat_completions" => Ok(Endpoint::ChatCompletions),
    "responses" => Ok(Endpoint::Responses),
    "messages" => Ok(Endpoint::Messages),
    _ => Err(serde::de::Error::custom("unknown continuation endpoint")),
  }
}

fn validate_origin(origin: &ProviderOrigin) -> Result<()> {
  if [&origin.provider, &origin.driver, &origin.account, &origin.model]
    .iter()
    .any(|value| value.trim().is_empty())
    || origin.backend.len() != 64
    || !origin.backend.bytes().all(|byte| byte.is_ascii_hexdigit())
    || origin
      .provider_account_digest
      .as_deref()
      .is_some_and(|digest| digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
  {
    return Err(ContinuationError::InvalidState("incomplete provider route identity"));
  }
  Ok(())
}

fn validate_model(origin: &ProviderOrigin, request: &Value) -> Result<()> {
  if request["model"].as_str() != Some(origin.model.as_str()) {
    return Err(ContinuationError::Incompatible("native request model changed"));
  }
  Ok(())
}

fn history_key(endpoint: Endpoint) -> &'static str {
  if endpoint == Endpoint::Responses {
    "input"
  } else {
    "messages"
  }
}

fn context_digest(endpoint: Endpoint, request: &Value) -> Result<String> {
  let mut context = request
    .as_object()
    .ok_or(ContinuationError::InvalidState("native request is not an object"))?
    .clone();
  for key in ["previous_response_id", "conversation"] {
    if context.get(key).is_some_and(|value| !value.is_null()) {
      return Err(ContinuationError::Unsupported(
        "server-side conversation handles cannot be replayed",
      ));
    }
    context.remove(key);
  }
  if context.get("n").is_some_and(|value| value != &json!(1)) {
    return Err(ContinuationError::Unsupported(
      "multiple response choices cannot be replayed",
    ));
  }
  context.remove(history_key(endpoint));
  context.remove("stream");
  context.remove("stream_options");
  let instructions = if endpoint == Endpoint::ChatCompletions {
    let history = input(endpoint, request)?;
    Value::Array(history[..chat_instruction_count(&history)?].to_vec())
  } else {
    Value::Null
  };
  let mut settings = json!([context, instructions]);
  settings.sort_all_objects();
  let bytes = serde_json::to_vec(&settings).map_err(|_| ContinuationError::InvalidState("invalid request settings"))?;
  Ok(sha256_hex(&bytes))
}

fn chat_instruction_count(history: &[Value]) -> Result<usize> {
  let count = history
    .iter()
    .take_while(|message| matches!(message["role"].as_str(), Some("system" | "developer")))
    .count();
  if history[count..]
    .iter()
    .any(|message| matches!(message["role"].as_str(), Some("system" | "developer")))
  {
    return Err(ContinuationError::Unsupported(
      "Chat instructions must precede conversation history",
    ));
  }
  Ok(count)
}

fn input(endpoint: Endpoint, request: &Value) -> Result<Vec<Value>> {
  request[history_key(endpoint)]
    .as_array()
    .cloned()
    .ok_or(ContinuationError::Unsupported(
      "native input must be an explicit history array",
    ))
}

fn assistant_output(endpoint: Endpoint, response: &Value) -> Result<Vec<Value>> {
  if !response.is_object() || response.get("error").is_some_and(|error| !error.is_null()) {
    return Err(ContinuationError::InvalidState(
      "response is not a successful native object",
    ));
  }
  match endpoint {
    Endpoint::Messages => {
      if response["role"] != "assistant"
        || !matches!(
          response["stop_reason"].as_str(),
          Some("end_turn" | "tool_use" | "stop_sequence" | "refusal")
        )
      {
        return Err(ContinuationError::Unsupported(
          "Messages response is incomplete or has an unsupported stop reason",
        ));
      }
      let content = response["content"].as_array().ok_or(ContinuationError::InvalidState(
        "Messages response content is not an array",
      ))?;
      for block in content {
        match block["type"].as_str() {
          Some("text" | "tool_use") => {}
          Some("redacted_thinking") if block["data"].as_str().is_some_and(|data| !data.is_empty()) => {}
          Some("thinking")
            if block["signature"]
              .as_str()
              .is_some_and(|signature| !signature.is_empty()) => {}
          Some("thinking") => return Err(ContinuationError::Unsupported("thinking block has no native signature")),
          _ => {
            return Err(ContinuationError::Unsupported(
              "unsupported native Messages output block",
            ))
          }
        }
      }
      Ok(alloc::vec![json!({"role": "assistant", "content": content})])
    }
    Endpoint::Responses => {
      if response["status"] != "completed" {
        return Err(ContinuationError::Unsupported("Responses output is incomplete"));
      }
      let output = response["output"]
        .as_array()
        .ok_or(ContinuationError::InvalidState("Responses output is not an array"))?;
      for item in output {
        if item.get("status").is_some_and(|status| status != "completed") {
          return Err(ContinuationError::Unsupported("Responses output item is incomplete"));
        }
        match item["type"].as_str() {
          Some("message") => validate_responses_output_message(item)?,
          Some("function_call" | "custom_tool_call") => {}
          Some("reasoning")
            if item["encrypted_content"]
              .as_str()
              .is_some_and(|value| !value.is_empty()) => {}
          Some("reasoning") => {
            return Err(ContinuationError::Unsupported(
              "reasoning item has no encrypted native content",
            ))
          }
          _ => {
            return Err(ContinuationError::Unsupported(
              "unsupported native Responses output item",
            ))
          }
        }
      }
      Ok(output.clone())
    }
    Endpoint::ChatCompletions => {
      let choices = response["choices"]
        .as_array()
        .filter(|choices| choices.len() == 1)
        .ok_or(ContinuationError::Unsupported(
          "Chat response requires exactly one choice",
        ))?;
      if !matches!(choices[0]["finish_reason"].as_str(), Some("stop" | "tool_calls")) {
        return Err(ContinuationError::Unsupported("Chat response is incomplete"));
      }
      let message = &choices[0]["message"];
      if !message.is_object() || message["role"] != "assistant" {
        return Err(ContinuationError::InvalidState(
          "Chat response has no assistant message",
        ));
      }
      validate_chat_assistant(message)?;
      Ok(alloc::vec![message.clone()])
    }
  }
}

fn validate_responses_output_message(item: &Value) -> Result<()> {
  if item["role"] != "assistant" {
    return Err(ContinuationError::InvalidState(
      "Responses output message does not have the assistant role",
    ));
  }
  let content = item["content"].as_array().ok_or(ContinuationError::InvalidState(
    "Responses output message content is not an array",
  ))?;
  for part in content {
    match part["type"].as_str() {
      Some("output_text") if part["text"].is_string() => {}
      Some("refusal") if part["refusal"].is_string() => {}
      Some("output_text" | "refusal") => {
        return Err(ContinuationError::InvalidState(
          "Responses output content is missing its string payload",
        ));
      }
      None => return Err(ContinuationError::InvalidState("Responses output content has no type")),
      _ => {
        return Err(ContinuationError::Unsupported(
          "unsupported native Responses output content",
        ))
      }
    }
  }
  Ok(())
}

fn validate_chat_assistant(message: &Value) -> Result<()> {
  match message.get("content") {
    None | Some(Value::Null | Value::String(_)) => {}
    Some(Value::Array(parts)) => {
      for part in parts {
        match part["type"].as_str() {
          Some("text") if part["text"].is_string() => {}
          Some("refusal") if part["refusal"].is_string() => {}
          Some("text" | "refusal") => {
            return Err(ContinuationError::InvalidState(
              "Chat assistant content is missing its string payload",
            ));
          }
          None => return Err(ContinuationError::InvalidState("Chat assistant content has no type")),
          _ => {
            return Err(ContinuationError::Unsupported(
              "unsupported native Chat assistant content",
            ))
          }
        }
      }
    }
    _ => return Err(ContinuationError::InvalidState("invalid Chat assistant content")),
  }
  for key in ["refusal", "reasoning_content", "reasoning"] {
    if message
      .get(key)
      .is_some_and(|value| !value.is_null() && !value.is_string())
    {
      return Err(ContinuationError::InvalidState("invalid Chat assistant text field"));
    }
  }
  if message.get("audio").is_some_and(|value| !value.is_null()) {
    return Err(ContinuationError::Unsupported(
      "native Chat audio output cannot be replayed",
    ));
  }
  if let Some(calls) = message.get("tool_calls").filter(|value| !value.is_null()) {
    let calls = calls.as_array().ok_or(ContinuationError::InvalidState(
      "Chat assistant tool calls are not an array",
    ))?;
    for call in calls {
      if call.get("type").is_some_and(|kind| kind != "function") {
        return Err(ContinuationError::Unsupported(
          "unsupported native Chat assistant tool type",
        ));
      }
    }
  }
  Ok(())
}

fn required_string<'a>(object: &'a Value, key: &str) -> Result<&'a str> {
  object[key]
    .as_str()
    .filter(|value| !value.is_empty())
    .ok_or(ContinuationError::InvalidState(
      "native history is missing a required string",
    ))
}

fn add_call(pending: &mut BTreeSet<String>, seen: &mut BTreeSet<String>, id: &str) -> Result<()> {
  if !seen.insert(id.into()) {
    return Err(ContinuationError::InvalidState("duplicate native tool call id"));
  }
  pending.insert(id.into());
  Ok(())
}

fn resolve_call(pending: &mut BTreeSet<String>, id: &str) -> Result<()> {
  if !pending.remove(id) {
    return Err(ContinuationError::InvalidState(
      "tool result has no matching pending native call",
    ));
  }
  Ok(())
}

fn validate_history(endpoint: Endpoint, history: &[Value], allow_pending: bool) -> Result<()> {
  let mut pending = BTreeSet::new();
  let mut seen = BTreeSet::new();
  let mut custom_calls = BTreeSet::new();
  for item in history {
    if !item.is_object() {
      return Err(ContinuationError::InvalidState("native history item is not an object"));
    }
    match endpoint {
      Endpoint::Responses => match item["type"].as_str() {
        Some("function_call" | "custom_tool_call") => {
          required_string(item, "name")?;
          add_call(&mut pending, &mut seen, required_string(item, "call_id")?)?;
          if item["type"] == "function_call" {
            validate_arguments(&item["arguments"])?;
          } else {
            item["input"]
              .as_str()
              .ok_or(ContinuationError::InvalidState("custom tool input is not a string"))?;
            custom_calls.insert(required_string(item, "call_id")?);
          }
        }
        Some("function_call_output" | "custom_tool_call_output") => {
          let id = required_string(item, "call_id")?;
          if !item["output"].is_string() && !item["output"].is_array() {
            return Err(ContinuationError::InvalidState(
              "native tool result has no output content",
            ));
          }
          if custom_calls.contains(id) != (item["type"] == "custom_tool_call_output") {
            return Err(ContinuationError::InvalidState(
              "native tool result type does not match its call",
            ));
          }
          resolve_call(&mut pending, id)?;
        }
        Some("reasoning")
          if item["encrypted_content"]
            .as_str()
            .is_some_and(|value| !value.is_empty()) => {}
        Some("reasoning") => {
          return Err(ContinuationError::InvalidState(
            "reasoning input has no encrypted content",
          ))
        }
        Some("message") | None if item.get("role").is_some() => {
          let role = required_string(item, "role")?;
          if !matches!(role, "system" | "developer" | "user" | "assistant") {
            return Err(ContinuationError::InvalidState("invalid Responses message role"));
          }
          if !item["content"].is_string() && !item["content"].is_array() {
            return Err(ContinuationError::InvalidState(
              "native Responses message has no content",
            ));
          }
          if role != "assistant" && !pending.is_empty() {
            return Err(ContinuationError::InvalidState(
              "native tool results must precede the next message",
            ));
          }
        }
        _ => {
          return Err(ContinuationError::Unsupported(
            "unsupported native Responses input item",
          ))
        }
      },
      Endpoint::ChatCompletions => match required_string(item, "role")? {
        "tool" => {
          if !item["content"].is_string() && !item["content"].is_array() {
            return Err(ContinuationError::InvalidState(
              "native Chat tool result has no content",
            ));
          }
          resolve_call(&mut pending, required_string(item, "tool_call_id")?)?;
        }
        "assistant" => {
          if !pending.is_empty() {
            return Err(ContinuationError::InvalidState(
              "previous Chat tool calls have no results",
            ));
          }
          validate_chat_assistant(item)?;
          for call in item.get("tool_calls").and_then(Value::as_array).into_iter().flatten() {
            required_string(&call["function"], "name")?;
            validate_arguments(&call["function"]["arguments"])?;
            add_call(&mut pending, &mut seen, required_string(call, "id")?)?;
          }
        }
        "system" | "developer" | "user" if pending.is_empty() => {}
        _ => {
          return Err(ContinuationError::InvalidState(
            "invalid Chat role or unresolved tool calls",
          ))
        }
      },
      Endpoint::Messages => {
        let role = required_string(item, "role")?;
        if !matches!(role, "user" | "assistant") {
          return Err(ContinuationError::InvalidState("invalid Messages role"));
        }
        if role == "assistant" && !pending.is_empty() {
          return Err(ContinuationError::InvalidState(
            "previous Messages tool calls have no results",
          ));
        }
        let blocks = if let Some(content) = item["content"].as_array() {
          content.as_slice()
        } else if item["content"].is_string() && pending.is_empty() {
          &[]
        } else {
          return Err(ContinuationError::InvalidState(
            "invalid Messages content or unresolved tool calls",
          ));
        };
        for block in blocks {
          match block["type"].as_str() {
            Some("text") if !block["text"].is_string() => {
              return Err(ContinuationError::InvalidState("native text block has no text"));
            }
            Some("thinking") if !block["thinking"].is_string() => {
              return Err(ContinuationError::InvalidState("native thinking block has no text"));
            }
            Some("thinking") if role != "assistant" || block["signature"].as_str().is_none_or(str::is_empty) => {
              return Err(ContinuationError::InvalidState(
                "thinking block has no native signature",
              ));
            }
            Some("redacted_thinking") if role != "assistant" || block["data"].as_str().is_none_or(str::is_empty) => {
              return Err(ContinuationError::InvalidState(
                "redacted thinking block has no native payload",
              ));
            }
            Some("tool_use") if role == "assistant" => {
              required_string(block, "name")?;
              if !block["input"].is_object() {
                return Err(ContinuationError::InvalidState("Messages tool input is not an object"));
              }
              add_call(&mut pending, &mut seen, required_string(block, "id")?)?;
            }
            Some("tool_result") if role == "user" => {
              resolve_call(&mut pending, required_string(block, "tool_use_id")?)?;
            }
            Some(_) if role == "user" && !pending.is_empty() => {
              return Err(ContinuationError::InvalidState("Messages tool results must come first"));
            }
            Some("tool_use" | "tool_result") | None => {
              return Err(ContinuationError::InvalidState("invalid Messages tool block"));
            }
            _ => {}
          }
        }
      }
    }
  }
  if !allow_pending && !pending.is_empty() {
    return Err(ContinuationError::InvalidState("request omitted pending tool results"));
  }
  Ok(())
}

fn validate_arguments(arguments: &Value) -> Result<()> {
  let value = match arguments {
    Value::String(arguments) => {
      serde_json::from_str::<Value>(if arguments.trim().is_empty() { "{}" } else { arguments })
        .map_err(|_| ContinuationError::InvalidState("native tool arguments are incomplete JSON"))?
    }
    _ => {
      return Err(ContinuationError::InvalidState(
        "native tool arguments are not a string",
      ))
    }
  };
  if !value.is_object() {
    return Err(ContinuationError::InvalidState(
      "native tool arguments are not an object",
    ));
  }
  Ok(())
}

#[cfg(test)]
#[path = "continuation_tests.rs"]
mod tests;
