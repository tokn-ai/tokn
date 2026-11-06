//! Optional owned conversation history and immutable generation snapshots.
//!
//! One prepared generation is one model request. Applications own retrieval,
//! tool execution, and when to request another response. Persisting a
//! conversation retains its pending request and retrieved context for retry;
//! it does not make uncertain external tool effects safe to execute again.

use alloc::{
  borrow::Borrow,
  collections::BTreeSet,
  string::{String, ToString},
  vec::Vec,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokn_core::types::Shared;

use crate::{GenerateRequest, GenerateRequestBuilder, GenerateResponse, Message, ProviderState, Role, ToolCall};

/// Retrieved data supplied to one generation, with its source identity.
///
/// Context is retained in a pending request for exact retries. It is not
/// appended to portable history. Native continuation may still retain context
/// already consumed by the provider; omitting it later does not erase it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrievedContext {
  pub source_id: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub source_version: Option<String>,
  pub content: String,
}

impl RetrievedContext {
  pub fn new(source_id: impl Into<String>, content: impl Into<String>) -> Self {
    Self {
      source_id: source_id.into(),
      source_version: None,
      content: content.into(),
    }
  }

  pub fn with_source_version(mut self, source_version: impl Into<String>) -> Self {
    self.source_version = Some(source_version.into());
    self
  }
}

/// Whether input is already part of the next effective request history.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputDisposition {
  Applied,
  Queued,
}

/// SDK-owned correlation data, separate from the provider's response id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GenerationBinding {
  pub(crate) conversation_id: String,
  pub(crate) generation_id: u64,
  pub(crate) input_revision: u64,
}

/// One immutable, retryable request snapshot owned independently of a client.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreparedGeneration {
  binding: GenerationBinding,
  instruction_count: usize,
  #[serde(serialize_with = "serialize_request", deserialize_with = "deserialize_request")]
  request: Shared<GenerateRequest>,
}

impl PreparedGeneration {
  pub fn request(&self) -> &GenerateRequest {
    &self.request
  }

  pub fn generation_id(&self) -> u64 {
    self.binding.generation_id
  }

  pub fn context(&self) -> &[RetrievedContext] {
    &self.request.context
  }
}

/// A single writer's ordered history and latest native continuation checkpoint.
///
/// The caller supplies a stable unique id and controls persistence. Cloning or
/// restoring the same serialized conversation into concurrent writers requires
/// application-level coordination. Queued input has been accepted locally but
/// has not yet been supplied to the provider.
#[derive(Debug, Serialize)]
pub struct Conversation {
  id: String,
  revision: u64,
  next_generation_id: u64,
  history: Vec<Message>,
  #[serde(skip_serializing_if = "Option::is_none")]
  provider_state: Option<ProviderState>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pending: Option<PreparedGeneration>,
  #[serde(skip_serializing_if = "Vec::is_empty")]
  queued_inputs: Vec<Message>,
}

#[derive(Deserialize)]
struct StoredConversation {
  id: String,
  revision: u64,
  next_generation_id: u64,
  history: Vec<Message>,
  #[serde(default)]
  provider_state: Option<ProviderState>,
  #[serde(default)]
  pending: Option<PreparedGeneration>,
  #[serde(default)]
  queued_inputs: Vec<Message>,
}

impl<'de> Deserialize<'de> for Conversation {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> core::result::Result<Self, D::Error> {
    let stored = StoredConversation::deserialize(deserializer)?;
    let conversation = Self {
      id: stored.id,
      revision: stored.revision,
      next_generation_id: stored.next_generation_id,
      history: stored.history,
      provider_state: stored.provider_state,
      pending: stored.pending,
      queued_inputs: stored.queued_inputs,
    };
    conversation.validate_stored().map_err(serde::de::Error::custom)?;
    Ok(conversation)
  }
}

/// Conversation transitions reject mismatches before changing stored history.
#[derive(Debug, thiserror::Error)]
pub enum ConversationError {
  #[error("conversation id must not be empty")]
  EmptyId,
  #[error("user input must not be empty")]
  EmptyInput,
  #[error("a generation is already pending; retry or abandon its existing snapshot")]
  GenerationPending,
  #[error("there is no pending generation")]
  NoPendingGeneration,
  #[error("tool results are still required for calls {call_ids:?}")]
  ToolResultsPending { call_ids: Vec<String> },
  #[error("tool call '{call_id}' is not awaiting a result")]
  UnknownToolCall { call_id: String },
  #[error("tool call '{call_id}' already has a result")]
  DuplicateToolResult { call_id: String },
  #[error("conversation templates may contain system instructions, but not conversation history or state")]
  ConflictingTemplate,
  #[error("retrieved context needs a non-empty source id and content")]
  InvalidContext,
  #[error("invalid generation request: {message}")]
  InvalidRequest { message: String },
  #[error("prepared generation does not match this conversation's pending request")]
  MismatchedGeneration,
  #[error("response does not match the exact prepared request")]
  MismatchedResponse,
  #[error("failed or cancelled responses cannot be committed as completed history")]
  IncompleteResponse,
  #[error("assistant tool calls require distinct non-empty ids and names")]
  InvalidToolCalls,
  #[error("conversation revision or generation counter is exhausted")]
  CounterExhausted,
  #[error("invalid stored conversation: {message}")]
  InvalidStored { message: String },
}

type Result<T> = core::result::Result<T, ConversationError>;

impl Conversation {
  pub fn new(id: impl Into<String>) -> Result<Self> {
    let id = id.into();
    if id.trim().is_empty() {
      return Err(ConversationError::EmptyId);
    }
    Ok(Self {
      id,
      revision: 0,
      next_generation_id: 0,
      history: Vec::new(),
      provider_state: None,
      pending: None,
      queued_inputs: Vec::new(),
    })
  }

  pub fn id(&self) -> &str {
    &self.id
  }

  pub fn history(&self) -> &[Message] {
    &self.history
  }

  pub fn provider_state(&self) -> Option<&ProviderState> {
    self.provider_state.as_ref()
  }

  pub fn pending(&self) -> Option<&PreparedGeneration> {
    self.pending.as_ref()
  }

  pub fn queued_inputs(&self) -> &[Message] {
    &self.queued_inputs
  }

  /// Submit input now, or queue steering behind an active generation/tools.
  pub fn submit_user(&mut self, content: impl Into<String>) -> Result<InputDisposition> {
    let content = content.into();
    if content.trim().is_empty() {
      return Err(ConversationError::EmptyInput);
    }
    let message = Message::user(content);
    if self.pending.is_some() || !self.outstanding_tool_calls().is_empty() || !self.queued_inputs.is_empty() {
      self.queued_inputs.push(message);
      return Ok(InputDisposition::Queued);
    }
    let revision = self.next_revision()?;
    self.history.push(message);
    self.revision = revision;
    Ok(InputDisposition::Applied)
  }

  /// Calls awaiting results, in the assistant's declaration order.
  pub fn outstanding_tool_calls(&self) -> Vec<&ToolCall> {
    let Some(assistant) = self.history.iter().rposition(|message| message.role == Role::Assistant) else {
      return Vec::new();
    };
    self.history[assistant]
      .tool_calls
      .iter()
      .filter(|call| {
        !self.history[assistant + 1..]
          .iter()
          .any(|message| message.role == Role::Tool && message.tool_call_id == call.id)
      })
      .collect()
  }

  /// Record an already-executed tool's result in its completion order.
  ///
  /// This method never executes a tool. Persist the result before requesting
  /// another generation; uncertain external effects need reconciliation.
  pub fn record_tool_result(&mut self, call_id: impl Into<String>, content: impl Into<String>) -> Result<()> {
    if self.pending.is_some() {
      return Err(ConversationError::GenerationPending);
    }
    let call_id = call_id.into();
    let Some(assistant) = self.history.iter().rposition(|message| message.role == Role::Assistant) else {
      return Err(ConversationError::UnknownToolCall { call_id });
    };
    if !self.history[assistant]
      .tool_calls
      .iter()
      .any(|call| call.id.as_deref() == Some(call_id.as_str()))
    {
      return Err(ConversationError::UnknownToolCall { call_id });
    }
    if self.history[assistant + 1..]
      .iter()
      .any(|message| message.tool_call_id.as_deref() == Some(call_id.as_str()))
    {
      return Err(ConversationError::DuplicateToolResult { call_id });
    }
    let revision = self.next_revision()?;
    self.history.push(Message::tool(call_id, content));
    self.revision = revision;
    Ok(())
  }

  /// Freeze history, instructions, settings, and retrieved context for one call.
  ///
  /// Templates can contain system instructions. Conversation messages and
  /// provider state come from this object, so supplying them twice is an error.
  /// Queued steering becomes effective only after this preparation succeeds.
  pub fn prepare(
    &mut self,
    builder: GenerateRequestBuilder,
    context: Vec<RetrievedContext>,
  ) -> Result<PreparedGeneration> {
    if self.pending.is_some() {
      return Err(ConversationError::GenerationPending);
    }
    let call_ids: Vec<_> = self
      .outstanding_tool_calls()
      .into_iter()
      .filter_map(|call| call.id.clone())
      .collect();
    if !call_ids.is_empty() {
      return Err(ConversationError::ToolResultsPending { call_ids });
    }
    let mut request = builder.into_unvalidated_request();
    if request.messages.iter().any(|message| message.role != Role::System)
      || request.provider_state.is_some()
      || !request.context.is_empty()
      || request.generation_binding.is_some()
    {
      return Err(ConversationError::ConflictingTemplate);
    }
    validate_context(&context)?;
    let next_generation_id = self
      .next_generation_id
      .checked_add(1)
      .ok_or(ConversationError::CounterExhausted)?;
    let input_revision = if self.queued_inputs.is_empty() {
      self.revision
    } else {
      self.next_revision()?
    };
    let binding = GenerationBinding {
      conversation_id: self.id.clone(),
      generation_id: self.next_generation_id,
      input_revision,
    };
    let instruction_count = request.messages.len();
    request.messages.extend_from_slice(&self.history);
    request.messages.extend_from_slice(&self.queued_inputs);
    request.context = context;
    request.provider_state = self.provider_state.clone();
    request.generation_binding = Some(binding.clone());
    request.validate().map_err(invalid_request)?;
    let prepared = PreparedGeneration {
      binding,
      instruction_count,
      request: Shared::new(request),
    };
    self.history.append(&mut self.queued_inputs);
    self.revision = input_revision;
    self.next_generation_id = next_generation_id;
    self.pending = Some(prepared.clone());
    Ok(prepared)
  }

  /// Recover the exact pending snapshot without rerunning retrieval.
  pub fn retry(&self) -> Result<PreparedGeneration> {
    self.pending.clone().ok_or(ConversationError::NoPendingGeneration)
  }

  /// Append one authentic response and replace its checkpoint atomically.
  ///
  /// A rejected response leaves the pending snapshot available for retry.
  /// Accepted steering stays queued until the following preparation.
  pub fn commit(&mut self, prepared: impl Borrow<PreparedGeneration>, response: GenerateResponse) -> Result<()> {
    let prepared = prepared.borrow();
    self.match_pending(prepared)?;
    if !response.matches_prepared(prepared.request()).map_err(invalid_request)? {
      return Err(ConversationError::MismatchedResponse);
    }
    if !(200..300).contains(&response.http_status)
      || !matches!(response.status.as_deref(), None | Some("completed" | "incomplete"))
      || matches!(
        response.finish_reason.as_deref(),
        Some("failed" | "error" | "cancelled")
      )
    {
      return Err(ConversationError::IncompleteResponse);
    }
    validate_tool_calls(&response.tool_calls)?;
    let revision = self.next_revision()?;
    let assistant = Message::assistant_with_tool_calls(response.text, response.tool_calls);
    self.history.push(assistant);
    self.provider_state = response.provider_state;
    self.pending = None;
    self.revision = revision;
    Ok(())
  }

  /// Abandon an interrupted attempt, retaining its input and last checkpoint.
  pub fn abandon(&mut self, prepared: impl Borrow<PreparedGeneration>) -> Result<()> {
    self.match_pending(prepared.borrow())?;
    self.pending = None;
    Ok(())
  }

  /// Deliberately start a fresh provider lineage from retained portable history.
  ///
  /// This discards opaque continuation, including native context and signed
  /// reasoning. Recorded tool results and queued input remain intact. Abandon
  /// any pending generation first so a later reply cannot restore old state.
  pub fn restart_from_history(&mut self) -> Result<()> {
    if self.pending.is_some() {
      return Err(ConversationError::GenerationPending);
    }
    if self.provider_state.is_some() {
      let revision = self.next_revision()?;
      self.provider_state = None;
      self.revision = revision;
    }
    Ok(())
  }

  fn next_revision(&self) -> Result<u64> {
    self.revision.checked_add(1).ok_or(ConversationError::CounterExhausted)
  }

  fn match_pending(&self, prepared: &PreparedGeneration) -> Result<()> {
    let pending = self.pending.as_ref().ok_or(ConversationError::NoPendingGeneration)?;
    if pending != prepared
      || prepared.binding.conversation_id != self.id
      || prepared.binding.input_revision != self.revision
    {
      return Err(ConversationError::MismatchedGeneration);
    }
    Ok(())
  }

  fn validate_stored(&self) -> Result<()> {
    if self.id.trim().is_empty() {
      return Err(ConversationError::EmptyId);
    }
    validate_history(&self.history)?;
    if self.queued_inputs.iter().any(|message| {
      message.role != Role::User
        || message.content.trim().is_empty()
        || message.tool_call_id.is_some()
        || !message.tool_calls.is_empty()
    }) {
      return Err(invalid_stored("queued inputs must be non-empty user messages"));
    }
    if let Some(pending) = &self.pending {
      self.match_pending(pending)?;
      let request = pending.request();
      if pending.binding.generation_id >= self.next_generation_id
        || request.generation_binding.as_ref() != Some(&pending.binding)
        || pending.instruction_count > request.messages.len()
        || request.messages[..pending.instruction_count]
          .iter()
          .any(|message| message.role != Role::System)
        || request.messages[pending.instruction_count..] != self.history
        || request.provider_state != self.provider_state
        || !self.outstanding_tool_calls().is_empty()
      {
        return Err(invalid_stored(
          "pending request must match committed history and checkpoint",
        ));
      }
      validate_context(&request.context)?;
      request.validate().map_err(invalid_request)?;
    }
    Ok(())
  }
}

fn validate_context(context: &[RetrievedContext]) -> Result<()> {
  if context
    .iter()
    .any(|item| item.source_id.trim().is_empty() || item.content.trim().is_empty())
  {
    return Err(ConversationError::InvalidContext);
  }
  Ok(())
}

fn validate_tool_calls(calls: &[ToolCall]) -> Result<()> {
  let mut ids = BTreeSet::new();
  for call in calls {
    let Some(id) = call.id.as_deref().filter(|id| !id.trim().is_empty()) else {
      return Err(ConversationError::InvalidToolCalls);
    };
    if call.name.trim().is_empty() || !ids.insert(id) {
      return Err(ConversationError::InvalidToolCalls);
    }
  }
  Ok(())
}

fn validate_history(history: &[Message]) -> Result<()> {
  let mut pending = BTreeSet::new();
  for message in history {
    match message.role {
      Role::System => return Err(invalid_stored("system instructions belong to generation settings")),
      Role::User => {
        if !pending.is_empty()
          || message.content.trim().is_empty()
          || message.tool_call_id.is_some()
          || !message.tool_calls.is_empty()
        {
          return Err(invalid_stored("user input cannot interrupt unresolved tool calls"));
        }
      }
      Role::Assistant => {
        if !pending.is_empty() || message.tool_call_id.is_some() {
          return Err(invalid_stored(
            "assistant response cannot interrupt unresolved tool calls",
          ));
        }
        validate_tool_calls(&message.tool_calls)?;
        pending.extend(message.tool_calls.iter().filter_map(|call| call.id.as_deref()));
      }
      Role::Tool => {
        let Some(id) = message.tool_call_id.as_deref() else {
          return Err(invalid_stored("tool result requires its call id"));
        };
        if !message.tool_calls.is_empty() || !pending.remove(id) {
          return Err(invalid_stored(
            "tool result must resolve one outstanding call exactly once",
          ));
        }
      }
    }
  }
  Ok(())
}

fn invalid_request(error: crate::Error) -> ConversationError {
  ConversationError::InvalidRequest {
    message: error.to_string(),
  }
}

fn invalid_stored(message: &str) -> ConversationError {
  ConversationError::InvalidStored {
    message: message.into(),
  }
}

fn serialize_request<S: Serializer>(
  request: &Shared<GenerateRequest>,
  serializer: S,
) -> core::result::Result<S::Ok, S::Error> {
  request.as_ref().serialize(serializer)
}

fn deserialize_request<'de, D: Deserializer<'de>>(
  deserializer: D,
) -> core::result::Result<Shared<GenerateRequest>, D::Error> {
  GenerateRequest::deserialize(deserializer).map(Shared::new)
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::vec;
  use serde_json::{json, Value};
  use tokn_core::headers::HeaderMap;

  fn settings() -> GenerateRequestBuilder {
    GenerateRequest::builder("fixture-model").system("Follow the supplied instructions.")
  }

  fn response(prepared: &PreparedGeneration, text: &str, calls: Vec<ToolCall>) -> GenerateResponse {
    let mut response = GenerateResponse::from_raw(
      200,
      HeaderMap::new(),
      json!({"id": "response-fixture", "status": "completed", "output_text": text}),
    );
    response.tool_calls = calls;
    response.bind_prepared(prepared.request()).unwrap();
    response
  }

  fn stateful_response(prepared: &PreparedGeneration, text: &str) -> GenerateResponse {
    let request = prepared.request();
    let input: Vec<_> = request
      .messages
      .iter()
      .filter(|message| message.role != Role::System)
      .map(|message| serde_json::to_value(message).unwrap())
      .collect();
    let instructions = request
      .messages
      .iter()
      .filter(|message| message.role == Role::System)
      .map(|message| message.content.as_str())
      .collect::<Vec<_>>()
      .join("\n\n");
    let native_request = json!({"model": request.model, "instructions": instructions, "input": input});
    let native_response = json!({
      "status": "completed",
      "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]}],
    });
    let origin = tokn_requests::session::ProviderOrigin::new(
      "fixture-provider",
      "openai",
      "fixture-account",
      "https://provider.invalid",
      tokn_core::Endpoint::Responses,
      request.model.clone(),
    );
    let native = tokn_requests::session::continuation::capture(origin, &native_request, &native_response).unwrap();
    let mut response = response(prepared, text, Vec::new());
    response.provider_state = Some(ProviderState::capture(request, &response, native).unwrap());
    response.bind_prepared(request).unwrap();
    response
  }

  fn serialized(conversation: &Conversation) -> Value {
    serde_json::to_value(conversation).unwrap()
  }

  #[test]
  fn steering_waits_for_the_next_snapshot_and_retries_retain_retrieved_context() {
    let mut conversation = Conversation::new("conversation-a").unwrap();
    assert_eq!(
      conversation.submit_user("first question").unwrap(),
      InputDisposition::Applied
    );
    let context = RetrievedContext::new("source-a", "Original retrieved excerpt.").with_source_version("v1");
    let prepared = conversation.prepare(settings(), vec![context.clone()]).unwrap();
    assert_eq!(prepared.request().messages[0].role, Role::System);
    assert_eq!(prepared.request().messages[1].content, "first question");
    assert_eq!(prepared.context(), &[context]);
    assert!(matches!(
      conversation.prepare(settings(), Vec::new()),
      Err(ConversationError::GenerationPending)
    ));

    assert_eq!(
      conversation.submit_user("new instruction").unwrap(),
      InputDisposition::Queued
    );
    assert_eq!(conversation.history().len(), 1);
    assert_eq!(conversation.retry().unwrap(), prepared);
    conversation
      .commit(&prepared, response(&prepared, "first reply", Vec::new()))
      .unwrap();
    assert_eq!(conversation.history().len(), 2);
    assert_eq!(conversation.queued_inputs()[0].content, "new instruction");

    let next = conversation
      .prepare(settings(), vec![RetrievedContext::new("source-b", "New excerpt.")])
      .unwrap();
    assert_eq!(next.generation_id(), prepared.generation_id() + 1);
    assert_eq!(next.request().messages.len(), 4);
    assert_eq!(next.request().messages[2].content, "first reply");
    assert_eq!(next.request().messages[3].content, "new instruction");
    assert_eq!(next.context()[0].source_id, "source-b");
    assert!(conversation.queued_inputs().is_empty());
    assert!(conversation
      .history()
      .iter()
      .all(|message| !message.content.contains("excerpt")));
  }

  #[test]
  fn responses_must_match_the_conversation_and_exact_sent_snapshot() {
    let mut first = Conversation::new("conversation-a").unwrap();
    let mut second = Conversation::new("conversation-b").unwrap();
    first.submit_user("question").unwrap();
    second.submit_user("question").unwrap();
    let prepared = first.prepare(settings(), Vec::new()).unwrap();
    let other = second.prepare(settings(), Vec::new()).unwrap();
    let before = serialized(&first);
    assert!(matches!(
      first.commit(&other, response(&other, "wrong conversation", Vec::new())),
      Err(ConversationError::MismatchedGeneration)
    ));
    assert!(matches!(
      first.commit(&prepared, response(&other, "wrong response", Vec::new())),
      Err(ConversationError::MismatchedResponse)
    ));
    let mut changed_request = prepared.request().clone();
    changed_request.messages[1].content = "different input".into();
    let mut changed_response = response(&prepared, "unrelated answer", Vec::new());
    changed_response.bind_prepared(&changed_request).unwrap();
    assert!(matches!(
      first.commit(&prepared, changed_response),
      Err(ConversationError::MismatchedResponse)
    ));
    let mut edited_response = response(&prepared, "original answer", Vec::new());
    edited_response.text = "edited after receipt".into();
    assert!(matches!(
      first.commit(&prepared, edited_response),
      Err(ConversationError::MismatchedResponse)
    ));
    assert_eq!(serialized(&first), before);
    first
      .commit(&prepared, response(&prepared, "correct reply", Vec::new()))
      .unwrap();
    first.submit_user("next question").unwrap();
    first.prepare(settings(), Vec::new()).unwrap();
    assert!(matches!(
      first.commit(&prepared, response(&prepared, "stale reply", Vec::new())),
      Err(ConversationError::MismatchedGeneration)
    ));
  }

  #[test]
  fn parallel_tool_results_keep_completion_order_and_do_not_skip_outstanding_calls() {
    let mut conversation = Conversation::new("conversation-tools").unwrap();
    conversation.submit_user("use both tools").unwrap();
    let prepared = conversation.prepare(settings(), Vec::new()).unwrap();
    conversation
      .commit(
        &prepared,
        response(
          &prepared,
          "checking",
          vec![
            ToolCall::new("first", json!({})).id("call-a"),
            ToolCall::new("second", json!({})).id("call-b"),
          ],
        ),
      )
      .unwrap();
    assert_eq!(conversation.outstanding_tool_calls().len(), 2);
    assert_eq!(
      conversation.submit_user("also explain the result").unwrap(),
      InputDisposition::Queued
    );
    conversation
      .record_tool_result("call-b", "second finished first")
      .unwrap();
    assert!(matches!(
      conversation.prepare(settings(), Vec::new()),
      Err(ConversationError::ToolResultsPending { call_ids }) if call_ids == ["call-a"]
    ));
    let before = serialized(&conversation);
    assert!(matches!(
      conversation.record_tool_result("call-b", "duplicate"),
      Err(ConversationError::DuplicateToolResult { .. })
    ));
    assert!(matches!(
      conversation.record_tool_result("unknown", "orphan"),
      Err(ConversationError::UnknownToolCall { .. })
    ));
    assert_eq!(serialized(&conversation), before);
    conversation
      .record_tool_result("call-a", "first finished second")
      .unwrap();
    let next = conversation.prepare(settings(), Vec::new()).unwrap();
    assert_eq!(next.request().messages[3].tool_call_id.as_deref(), Some("call-b"));
    assert_eq!(next.request().messages[4].tool_call_id.as_deref(), Some("call-a"));
    assert_eq!(next.request().messages[5].content, "also explain the result");
  }

  #[test]
  fn persistence_recovers_the_exact_pending_request_and_queued_steering() {
    let mut conversation = Conversation::new("conversation-recovery").unwrap();
    conversation.submit_user("question").unwrap();
    let prepared = conversation
      .prepare(
        settings(),
        vec![RetrievedContext::new("reference", "Frozen source content.")],
      )
      .unwrap();
    conversation.submit_user("steer").unwrap();
    let encoded = serde_json::to_vec(&conversation).unwrap();
    let mut restored: Conversation = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(restored.retry().unwrap(), prepared);
    assert_eq!(restored.queued_inputs(), conversation.queued_inputs());
    restored
      .commit(&prepared, response(&prepared, "recovered answer", Vec::new()))
      .unwrap();
    assert_eq!(restored.history()[1].content, "recovered answer");
    assert!(restored.pending().is_none());
    assert!(!serialized(&restored).as_object().unwrap().contains_key("pending"));
    assert!(!serde_json::to_string(&restored)
      .unwrap()
      .contains("Frozen source content."));
  }

  #[test]
  fn invalid_preparation_does_not_consume_queued_input_or_change_pending_state() {
    let mut conversation = Conversation::new("conversation-a").unwrap();
    conversation.submit_user("first").unwrap();
    let prepared = conversation.prepare(settings(), Vec::new()).unwrap();
    conversation.submit_user("queued").unwrap();
    conversation.abandon(prepared).unwrap();
    let before = serialized(&conversation);
    assert!(matches!(
      conversation.prepare(settings().user("another history source"), Vec::new()),
      Err(ConversationError::ConflictingTemplate)
    ));
    assert!(matches!(
      conversation.prepare(settings(), vec![RetrievedContext::new("", "excerpt")]),
      Err(ConversationError::InvalidContext)
    ));
    assert!(matches!(
      conversation.prepare(GenerateRequest::builder(""), Vec::new()),
      Err(ConversationError::InvalidRequest { .. })
    ));
    assert_eq!(serialized(&conversation), before);
    let next = conversation.prepare(settings(), Vec::new()).unwrap();
    assert_eq!(next.request().messages[2].content, "queued");
  }

  #[test]
  fn failed_responses_and_invalid_tool_ids_leave_the_pending_snapshot_retryable() {
    let mut conversation = Conversation::new("conversation-a").unwrap();
    conversation.submit_user("question").unwrap();
    let prepared = conversation.prepare(settings(), Vec::new()).unwrap();
    let before = serialized(&conversation);
    for status in ["in_progress", "queued", "failed", "cancelled"] {
      let mut reply = response(&prepared, "partial", Vec::new());
      reply.status = Some(status.into());
      reply.bind_prepared(prepared.request()).unwrap();
      assert!(matches!(
        conversation.commit(&prepared, reply),
        Err(ConversationError::IncompleteResponse)
      ));
    }
    let duplicate_calls = vec![
      ToolCall::new("first", json!({})).id("duplicate"),
      ToolCall::new("second", json!({})).id("duplicate"),
    ];
    assert!(matches!(
      conversation.commit(&prepared, response(&prepared, "tools", duplicate_calls)),
      Err(ConversationError::InvalidToolCalls)
    ));
    assert_eq!(serialized(&conversation), before);
    let mut limited = response(&prepared, "partial but acknowledged", Vec::new());
    limited.status = Some("incomplete".into());
    limited.finish_reason = Some("length".into());
    limited.bind_prepared(prepared.request()).unwrap();
    conversation.commit(&prepared, limited).unwrap();
    assert_eq!(conversation.history()[1].content, "partial but acknowledged");
  }

  #[test]
  fn persisted_state_rejects_history_or_pending_snapshot_mismatches() {
    let mut conversation = Conversation::new("conversation-a").unwrap();
    conversation.submit_user("question").unwrap();
    conversation.prepare(settings(), Vec::new()).unwrap();
    for field in ["history", "request", "binding", "queued"] {
      let mut value = serialized(&conversation);
      match field {
        "history" => value["history"][0]["content"] = json!("edited question"),
        "request" => value["pending"]["request"]["messages"][1]["content"] = json!("edited snapshot"),
        "binding" => value["pending"]["binding"]["conversation_id"] = json!("other conversation"),
        "queued" => value["queued_inputs"] = json!([Message::tool("orphan", "result")]),
        _ => unreachable!(),
      }
      assert!(serde_json::from_value::<Conversation>(value).is_err(), "{field}");
    }
  }

  #[test]
  fn persistence_preserves_outstanding_tools_and_rejects_disordered_results() {
    let mut conversation = Conversation::new("conversation-tools").unwrap();
    conversation.submit_user("use both tools").unwrap();
    let prepared = conversation.prepare(settings(), Vec::new()).unwrap();
    conversation
      .commit(
        &prepared,
        response(
          &prepared,
          "checking",
          vec![
            ToolCall::new("first", json!({})).id("call-a"),
            ToolCall::new("second", json!({})).id("call-b"),
          ],
        ),
      )
      .unwrap();
    conversation.record_tool_result("call-b", "already executed").unwrap();
    conversation.submit_user("queued steering").unwrap();
    let saved = serialized(&conversation);
    let mut restored: Conversation = serde_json::from_value(saved.clone()).unwrap();
    assert_eq!(restored.outstanding_tool_calls().len(), 1);
    assert_eq!(restored.outstanding_tool_calls()[0].id.as_deref(), Some("call-a"));
    assert_eq!(restored.history()[2].tool_call_id.as_deref(), Some("call-b"));
    restored.record_tool_result("call-a", "reconciled result").unwrap();
    let next = restored.prepare(settings(), Vec::new()).unwrap();
    assert_eq!(next.request().messages[5].content, "queued steering");

    for message in [
      Message::tool("call-b", "duplicate result"),
      Message::tool("orphan", "unrequested result"),
      Message::user("input before remaining tools finish"),
      Message::assistant("output before remaining tools finish"),
    ] {
      let mut value = saved.clone();
      value["history"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::to_value(message).unwrap());
      assert!(serde_json::from_value::<Conversation>(value).is_err());
    }
  }

  #[test]
  fn explicit_restart_preserves_history_and_steering_without_invalidating_pending_work() {
    let mut conversation = Conversation::new("conversation-state").unwrap();
    conversation.submit_user("question").unwrap();
    let prepared = conversation.prepare(settings(), Vec::new()).unwrap();
    let reply = stateful_response(&prepared, "answer");
    let mut edited_reply = stateful_response(&prepared, "answer");
    edited_reply.provider_state = None;
    let before = serialized(&conversation);
    assert!(matches!(
      conversation.commit(&prepared, edited_reply),
      Err(ConversationError::MismatchedResponse)
    ));
    assert_eq!(serialized(&conversation), before);
    conversation.commit(&prepared, reply).unwrap();
    assert!(conversation.provider_state().is_some());
    conversation.submit_user("follow-up").unwrap();

    let before = serialized(&conversation);
    assert!(matches!(
      conversation.prepare(
        GenerateRequest::builder("fixture-model").system("Deliberately changed instructions."),
        Vec::new(),
      ),
      Err(ConversationError::InvalidRequest { .. })
    ));
    assert_eq!(serialized(&conversation), before);
    let pending = conversation.prepare(settings(), Vec::new()).unwrap();
    conversation.submit_user("queued steering").unwrap();
    let before = serialized(&conversation);
    assert!(matches!(
      conversation.restart_from_history(),
      Err(ConversationError::GenerationPending)
    ));
    assert_eq!(serialized(&conversation), before);
    conversation.abandon(&pending).unwrap();
    let history = conversation.history().to_vec();
    let queued = conversation.queued_inputs().to_vec();
    conversation.restart_from_history().unwrap();
    assert!(conversation.provider_state().is_none());
    assert_eq!(conversation.history(), history);
    assert_eq!(conversation.queued_inputs(), queued);
    let restarted = conversation
      .prepare(
        GenerateRequest::builder("fixture-model").system("Deliberately changed instructions."),
        Vec::new(),
      )
      .unwrap();
    assert!(restarted.request().provider_state.is_none());
    assert_eq!(restarted.request().messages[4].content, "queued steering");
  }

  #[test]
  fn restored_checkpoint_rejects_edited_prefix_when_effective_instructions_are_available() {
    let mut conversation = Conversation::new("conversation-state").unwrap();
    conversation.submit_user("question").unwrap();
    let prepared = conversation.prepare(settings(), Vec::new()).unwrap();
    conversation
      .commit(&prepared, stateful_response(&prepared, "answer"))
      .unwrap();
    let mut value = serialized(&conversation);
    value["history"][0]["content"] = json!("edited covered input");
    // Instructions belong to the next request settings, so the full covered
    // prefix is checked during preparation rather than duplicating it here.
    let mut restored: Conversation = serde_json::from_value(value).unwrap();
    restored.submit_user("follow-up").unwrap();
    let before = serialized(&restored);
    assert!(matches!(
      restored.prepare(settings(), Vec::new()),
      Err(ConversationError::InvalidRequest { .. })
    ));
    assert_eq!(serialized(&restored), before);
  }
}
