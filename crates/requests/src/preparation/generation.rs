//! Provider-aware lowering for typed, provider-neutral generation controls.
//!
//! Controls are applied after endpoint conversion, when both the selected
//! provider and its upstream endpoint are known. Unsupported combinations
//! fail locally instead of being silently dropped or reinterpreted.

use super::PreparationError;
use alloc::string::{String, ToString};
use serde_json::{Map, Value};
use tokn_core::generation::{GenerationOptions, ReasoningEffort, ReasoningMode, ReasoningOptions};
use tokn_core::provider::{Endpoint, ID_CODEX, ID_DEEPSEEK, ID_GITHUB_COPILOT, ID_LLAMA_CPP, ID_OPENAI, ZAI_PROVIDERS};

pub(crate) fn lower_generation_options(
  body: &mut Value,
  endpoint: Endpoint,
  provider_id: &str,
  model: &str,
  options: &GenerationOptions,
) -> Result<(), PreparationError> {
  if options.is_empty() {
    return Ok(());
  }
  let Some(obj) = body.as_object_mut() else {
    let control = if options.max_output_tokens.is_some() {
      "max_output_tokens"
    } else if options.top_k.is_some() {
      "top_k"
    } else {
      "reasoning"
    };
    return unsupported(
      control,
      provider_id,
      endpoint,
      "typed generation controls require a JSON object request body",
    );
  };

  if let Some(max_output_tokens) = options.max_output_tokens {
    lower_max_output_tokens(obj, endpoint, provider_id, model, max_output_tokens)?;
  }

  if let Some(top_k) = options.top_k {
    lower_top_k(obj, endpoint, provider_id, top_k)?;
  }

  let Some(reasoning) = options.reasoning.as_ref() else {
    return Ok(());
  };
  match endpoint {
    Endpoint::Responses => lower_responses_reasoning(obj, provider_id, reasoning),
    Endpoint::ChatCompletions => lower_chat_reasoning(obj, provider_id, model, reasoning),
    Endpoint::Messages => lower_messages_reasoning(obj, provider_id, model, reasoning),
  }
}

fn lower_max_output_tokens(
  obj: &mut Map<String, Value>,
  endpoint: Endpoint,
  provider_id: &str,
  model: &str,
  max_output_tokens: u64,
) -> Result<(), PreparationError> {
  match endpoint {
    Endpoint::Responses if provider_id == ID_CODEX => unsupported(
      "max_output_tokens",
      provider_id,
      endpoint,
      "the Codex Responses backend removes explicit output-token limits",
    ),
    Endpoint::Responses => {
      obj.remove("max_tokens");
      obj.remove("max_completion_tokens");
      obj.insert("max_output_tokens".into(), Value::from(max_output_tokens));
      Ok(())
    }
    Endpoint::ChatCompletions if uses_max_completion_tokens(provider_id, model) => {
      obj.remove("max_tokens");
      obj.remove("max_output_tokens");
      obj.insert("max_completion_tokens".into(), Value::from(max_output_tokens));
      Ok(())
    }
    Endpoint::ChatCompletions => {
      obj.remove("max_output_tokens");
      obj.remove("max_completion_tokens");
      obj.insert("max_tokens".into(), Value::from(max_output_tokens));
      Ok(())
    }
    Endpoint::Messages => {
      obj.remove("max_output_tokens");
      obj.remove("max_completion_tokens");
      obj.insert("max_tokens".into(), Value::from(max_output_tokens));
      Ok(())
    }
  }
}

fn uses_max_completion_tokens(provider_id: &str, model: &str) -> bool {
  if provider_id == ID_OPENAI {
    return true;
  }
  let model = model.to_ascii_lowercase();
  provider_id == ID_GITHUB_COPILOT
    && ["gpt-5", "o1", "o3", "o4"]
      .iter()
      .any(|prefix| model.starts_with(prefix))
}

pub(crate) fn ensure_model_supports_reasoning(
  endpoint: Endpoint,
  provider_id: &str,
  reasoning_supported: Option<bool>,
  options: &GenerationOptions,
) -> Result<(), PreparationError> {
  if options.reasoning.is_some() && reasoning_supported == Some(false) {
    return unsupported(
      "reasoning",
      provider_id,
      endpoint,
      "the selected model is known not to support reasoning",
    );
  }
  Ok(())
}

pub(crate) fn ensure_model_supports_effort(
  endpoint: Endpoint,
  provider_id: &str,
  model: &str,
  supported: Option<&[ReasoningEffort]>,
  options: &GenerationOptions,
) -> Result<(), PreparationError> {
  let Some(reasoning) = &options.reasoning else {
    return Ok(());
  };
  let disabled = ReasoningEffort::from("none");
  let effort = reasoning.effort.as_ref().or_else(|| {
    (reasoning.mode == Some(ReasoningMode::Disabled)
      && (endpoint == Endpoint::Responses
        || (endpoint == Endpoint::ChatCompletions
          && provider_id != ID_DEEPSEEK
          && !ZAI_PROVIDERS.contains(&provider_id)
          && !(provider_id == ID_GITHUB_COPILOT && is_claude_model(model)))))
    .then_some(&disabled)
  });
  if let (Some(effort), Some(supported)) = (effort, supported) {
    if !supported.contains(effort) {
      return unsupported(
        "reasoning.effort",
        provider_id,
        endpoint,
        "the selected model does not advertise this effort level",
      );
    }
  }
  Ok(())
}

fn lower_top_k(
  obj: &mut Map<String, Value>,
  endpoint: Endpoint,
  provider_id: &str,
  top_k: u64,
) -> Result<(), PreparationError> {
  if provider_id == ID_LLAMA_CPP && endpoint == Endpoint::ChatCompletions {
    obj.insert("top_k".into(), Value::from(top_k));
    return Ok(());
  }
  unsupported(
    "top_k",
    provider_id,
    endpoint,
    "typed top_k is currently supported only for llama.cpp Chat Completions routes",
  )
}

fn lower_responses_reasoning(
  obj: &mut Map<String, Value>,
  provider_id: &str,
  reasoning: &ReasoningOptions,
) -> Result<(), PreparationError> {
  reject_mode(
    reasoning,
    provider_id,
    Endpoint::Responses,
    &[ReasoningMode::Enabled, ReasoningMode::Adaptive],
    "Responses reasoning is controlled by effort rather than an enabled or adaptive mode",
  )?;
  reject_present(
    reasoning.budget_tokens,
    "reasoning.budget_tokens",
    provider_id,
    Endpoint::Responses,
    "the Responses API does not accept an explicit reasoning token budget",
  )?;

  if reasoning.effort.is_none() && reasoning.summary.is_none() && reasoning.mode.is_none() {
    return Ok(());
  }
  let target = object_field(obj, "reasoning");
  remove_fields(target, &["mode", "budget_tokens"]);
  if let Some(effort) = reasoning.effort.as_ref() {
    target.insert("effort".into(), Value::String(effort.as_str().to_string()));
  }
  if reasoning.mode == Some(ReasoningMode::Disabled) {
    target.insert("effort".into(), Value::String("none".into()));
  }
  if let Some(summary) = reasoning.summary.as_ref() {
    target.insert("summary".into(), Value::String(summary.as_str().to_string()));
  }
  Ok(())
}

fn lower_chat_reasoning(
  obj: &mut Map<String, Value>,
  provider_id: &str,
  model: &str,
  reasoning: &ReasoningOptions,
) -> Result<(), PreparationError> {
  if provider_id == ID_LLAMA_CPP {
    return unsupported(
      "reasoning",
      provider_id,
      Endpoint::ChatCompletions,
      "llama.cpp has no portable reasoning control",
    );
  }
  if provider_id == ID_DEEPSEEK {
    return lower_deepseek_reasoning(obj, Endpoint::ChatCompletions, provider_id, reasoning);
  }
  if ZAI_PROVIDERS.contains(&provider_id) {
    return lower_zai_reasoning(obj, provider_id, reasoning);
  }
  if provider_id == ID_GITHUB_COPILOT && is_claude_model(model) {
    return lower_claude_reasoning(obj, Endpoint::ChatCompletions, provider_id, model, reasoning);
  }

  reject_mode(
    reasoning,
    provider_id,
    Endpoint::ChatCompletions,
    &[ReasoningMode::Enabled, ReasoningMode::Adaptive],
    "Chat Completions accepts reasoning effort but not an enabled or adaptive mode",
  )?;
  reject_present(
    reasoning.budget_tokens,
    "reasoning.budget_tokens",
    provider_id,
    Endpoint::ChatCompletions,
    "Chat Completions does not accept an explicit reasoning token budget",
  )?;
  reject_present(
    reasoning.summary.as_ref(),
    "reasoning.summary",
    provider_id,
    Endpoint::ChatCompletions,
    "Chat Completions does not support reasoning summaries",
  )?;

  if reasoning.effort.is_some() || reasoning.mode == Some(ReasoningMode::Disabled) {
    obj.remove("reasoning");
    obj.remove("thinking");
  }
  if let Some(effort) = reasoning.effort.as_ref() {
    obj.insert("reasoning_effort".into(), Value::String(effort.as_str().to_string()));
  }
  if reasoning.mode == Some(ReasoningMode::Disabled) {
    obj.insert("reasoning_effort".into(), Value::String("none".into()));
  }
  Ok(())
}

pub(crate) fn lower_deepseek_reasoning(
  obj: &mut Map<String, Value>,
  endpoint: Endpoint,
  provider_id: &str,
  reasoning: &ReasoningOptions,
) -> Result<(), PreparationError> {
  let messages_endpoint = match endpoint {
    Endpoint::ChatCompletions => false,
    Endpoint::Messages => true,
    Endpoint::Responses => {
      return unsupported(
        "reasoning",
        provider_id,
        endpoint,
        "DeepSeek reasoning lowering supports only Chat Completions and Messages",
      );
    }
  };

  reject_mode(
    reasoning,
    provider_id,
    endpoint,
    &[ReasoningMode::Adaptive],
    "DeepSeek does not support adaptive reasoning",
  )?;
  reject_present(
    reasoning.budget_tokens,
    "reasoning.budget_tokens",
    provider_id,
    endpoint,
    "DeepSeek does not accept an explicit reasoning token budget",
  )?;
  reject_present(
    reasoning.summary.as_ref(),
    "reasoning.summary",
    provider_id,
    endpoint,
    "DeepSeek does not support reasoning summaries",
  )?;
  let thinking_enabled = reasoning.mode != Some(ReasoningMode::Disabled)
    && (reasoning.mode == Some(ReasoningMode::Enabled) || reasoning.effort.is_some());
  if thinking_enabled {
    reject_present(
      obj.get("temperature"),
      "temperature",
      provider_id,
      endpoint,
      "DeepSeek ignores temperature while thinking is enabled",
    )?;
    reject_present(
      obj.get("top_p"),
      "top_p",
      provider_id,
      endpoint,
      "DeepSeek ignores top_p while thinking is enabled",
    )?;
  }

  if reasoning.mode.is_none() && reasoning.effort.is_none() {
    return Ok(());
  }
  obj.remove("reasoning");
  let thinking = object_field(obj, "thinking");
  remove_fields(thinking, &["mode", "budget_tokens", "summary"]);
  if let Some(mode) = reasoning.mode.as_ref() {
    thinking.insert("type".into(), Value::String(reasoning_mode_str(*mode).into()));
  }
  if let Some(effort) = reasoning.effort.as_ref() {
    let effort = effort.as_str().to_string();
    thinking.insert("effort".into(), Value::String(effort.clone()));
    if messages_endpoint {
      object_field(obj, "output_config").insert("effort".into(), Value::String(effort));
    } else {
      obj.insert("reasoning_effort".into(), Value::String(effort));
    }
  }
  Ok(())
}

fn lower_zai_reasoning(
  obj: &mut Map<String, Value>,
  provider_id: &str,
  reasoning: &ReasoningOptions,
) -> Result<(), PreparationError> {
  reject_mode(
    reasoning,
    provider_id,
    Endpoint::ChatCompletions,
    &[ReasoningMode::Adaptive],
    "Z.AI does not support adaptive reasoning",
  )?;
  reject_present(
    reasoning.effort.as_ref(),
    "reasoning.effort",
    provider_id,
    Endpoint::ChatCompletions,
    "Z.AI exposes an enabled or disabled thinking mode, not reasoning effort",
  )?;
  reject_present(
    reasoning.budget_tokens,
    "reasoning.budget_tokens",
    provider_id,
    Endpoint::ChatCompletions,
    "Z.AI does not accept an explicit reasoning token budget",
  )?;
  reject_present(
    reasoning.summary.as_ref(),
    "reasoning.summary",
    provider_id,
    Endpoint::ChatCompletions,
    "Z.AI does not support reasoning summaries",
  )?;

  if let Some(mode) = reasoning.mode.as_ref() {
    obj.remove("reasoning");
    let thinking = object_field(obj, "thinking");
    thinking.insert("type".into(), Value::String(reasoning_mode_str(*mode).into()));
    if *mode == ReasoningMode::Enabled {
      thinking.insert("clear_thinking".into(), Value::Bool(false));
    } else {
      thinking.remove("clear_thinking");
    }
  }
  Ok(())
}

fn lower_messages_reasoning(
  obj: &mut Map<String, Value>,
  provider_id: &str,
  model: &str,
  reasoning: &ReasoningOptions,
) -> Result<(), PreparationError> {
  if provider_id == ID_DEEPSEEK {
    return lower_deepseek_reasoning(obj, Endpoint::Messages, provider_id, reasoning);
  }
  lower_claude_reasoning(obj, Endpoint::Messages, provider_id, model, reasoning)
}

fn lower_claude_reasoning(
  obj: &mut Map<String, Value>,
  endpoint: Endpoint,
  provider_id: &str,
  model: &str,
  reasoning: &ReasoningOptions,
) -> Result<(), PreparationError> {
  reject_present(
    reasoning.summary.as_ref(),
    "reasoning.summary",
    provider_id,
    endpoint,
    "Claude-compatible reasoning does not support summaries",
  )?;
  validate_claude_reasoning(obj, endpoint, provider_id, model, reasoning)?;

  if !reasoning.is_empty() {
    obj.remove("reasoning");
  }
  if let Some(thinking) = obj.get_mut("thinking").and_then(Value::as_object_mut) {
    remove_fields(thinking, &["mode", "effort", "summary"]);
    if matches!(
      reasoning.mode.as_ref(),
      Some(ReasoningMode::Adaptive | ReasoningMode::Disabled)
    ) {
      thinking.remove("budget_tokens");
    }
  }

  match reasoning.mode.as_ref() {
    Some(ReasoningMode::Enabled) => {
      let thinking = object_field(obj, "thinking");
      thinking.insert("type".into(), Value::String("enabled".into()));
      if let Some(budget_tokens) = reasoning.budget_tokens {
        thinking.insert("budget_tokens".into(), Value::from(budget_tokens));
      }
    }
    None if reasoning.budget_tokens.is_some() => {
      let thinking = object_field(obj, "thinking");
      thinking.insert("type".into(), Value::String("enabled".into()));
      thinking.insert(
        "budget_tokens".into(),
        Value::from(reasoning.budget_tokens.expect("checked as present")),
      );
    }
    Some(ReasoningMode::Adaptive) => {
      reject_present(
        reasoning.budget_tokens,
        "reasoning.budget_tokens",
        provider_id,
        endpoint,
        "adaptive reasoning does not accept a fixed token budget",
      )?;
      object_field(obj, "thinking").insert("type".into(), Value::String("adaptive".into()));
    }
    Some(ReasoningMode::Disabled) => {
      reject_present(
        reasoning.budget_tokens,
        "reasoning.budget_tokens",
        provider_id,
        endpoint,
        "disabled reasoning cannot have a token budget",
      )?;
      object_field(obj, "thinking").insert("type".into(), Value::String("disabled".into()));
    }
    None => {}
  }

  if let Some(effort) = reasoning.effort.as_ref() {
    object_field(obj, "output_config").insert("effort".into(), Value::String(effort.as_str().to_string()));
  }
  if obj
    .get("thinking")
    .and_then(Value::as_object)
    .is_some_and(Map::is_empty)
  {
    obj.remove("thinking");
  }
  Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClaudeThinkingSupport {
  ManualOnly,
  AdaptiveAndManual,
  AdaptiveOnly,
  Unknown,
}

fn validate_claude_reasoning(
  obj: &Map<String, Value>,
  endpoint: Endpoint,
  provider_id: &str,
  model: &str,
  reasoning: &ReasoningOptions,
) -> Result<(), PreparationError> {
  let support = claude_thinking_support(model);
  let manual = reasoning.mode == Some(ReasoningMode::Enabled) || reasoning.budget_tokens.is_some();

  if reasoning.mode == Some(ReasoningMode::Disabled) && claude_rejects_disabled_reasoning(model) {
    return unsupported(
      "reasoning.mode",
      provider_id,
      endpoint,
      "this Claude model has adaptive reasoning enabled by default and does not support disabling it",
    );
  }

  if manual {
    let Some(budget_tokens) = reasoning.budget_tokens else {
      return unsupported(
        "reasoning.budget_tokens",
        provider_id,
        endpoint,
        "manual Claude reasoning requires an explicit token budget",
      );
    };
    if budget_tokens < 1024 {
      return unsupported(
        "reasoning.budget_tokens",
        provider_id,
        endpoint,
        "manual Claude reasoning requires a budget of at least 1024 tokens",
      );
    }
    if support == ClaudeThinkingSupport::AdaptiveOnly {
      return unsupported(
        "reasoning.mode",
        provider_id,
        endpoint,
        "this Claude model supports adaptive reasoning instead of manual token budgets",
      );
    }
    let Some(max_tokens) = obj.get("max_tokens").and_then(Value::as_u64) else {
      return unsupported(
        "max_output_tokens",
        provider_id,
        endpoint,
        "manual Claude reasoning requires an explicit max_tokens limit",
      );
    };
    if budget_tokens >= max_tokens {
      return unsupported(
        "reasoning.budget_tokens",
        provider_id,
        endpoint,
        "manual Claude reasoning requires budget_tokens to be less than max_tokens",
      );
    }
  }

  if reasoning.mode == Some(ReasoningMode::Adaptive) && support == ClaudeThinkingSupport::ManualOnly {
    return unsupported(
      "reasoning.mode",
      provider_id,
      endpoint,
      "this Claude model supports manual reasoning budgets, not adaptive reasoning",
    );
  }
  validate_claude_sampling(
    obj,
    endpoint,
    provider_id,
    model,
    manual || reasoning.mode == Some(ReasoningMode::Adaptive),
  )?;
  Ok(())
}

fn claude_thinking_support(model: &str) -> ClaudeThinkingSupport {
  let model = normalize_claude_model(model);
  if model.contains("mythos-preview") || model.contains("-4-6") {
    return ClaudeThinkingSupport::AdaptiveAndManual;
  }
  if model.contains("-4-7")
    || model.contains("-4-8")
    || model.contains("claude-sonnet-5")
    || model.contains("claude-opus-5")
    || model.contains("claude-fable-5")
    || model.contains("claude-mythos-5")
  {
    return ClaudeThinkingSupport::AdaptiveOnly;
  }
  if model.contains("-4-5")
    || model.contains("-4-1")
    || model.contains("-4-0")
    || model.contains("claude-opus-41")
    || model == "claude-sonnet-4"
    || model.starts_with("claude-sonnet-4-20")
    || model.contains("claude-3-7")
  {
    return ClaudeThinkingSupport::ManualOnly;
  }
  ClaudeThinkingSupport::Unknown
}

fn claude_rejects_disabled_reasoning(model: &str) -> bool {
  let model = normalize_claude_model(model);
  model.contains("claude-fable-5") || model.contains("claude-mythos-5") || model.contains("mythos-preview")
}

pub(crate) fn validate_claude_sampling(
  obj: &Map<String, Value>,
  endpoint: Endpoint,
  provider_id: &str,
  model: &str,
  thinking_enabled: bool,
) -> Result<(), PreparationError> {
  if claude_rejects_sampling_controls(model) {
    if obj.get("temperature").is_some_and(|value| value.as_f64() != Some(1.0)) {
      return unsupported(
        "temperature",
        provider_id,
        endpoint,
        "this Claude model rejects non-default sampling controls",
      );
    }
    if obj.contains_key("top_p") {
      return unsupported(
        "top_p",
        provider_id,
        endpoint,
        "this Claude model rejects non-default sampling controls",
      );
    }
    return Ok(());
  }
  if !thinking_enabled {
    return Ok(());
  }
  if obj.get("temperature").is_some_and(|value| value.as_f64() != Some(1.0)) {
    return unsupported(
      "temperature",
      provider_id,
      endpoint,
      "Claude requires temperature to be omitted or set to 1 while thinking is enabled",
    );
  }
  if obj
    .get("top_p")
    .is_some_and(|value| value.as_f64().is_none_or(|top_p| !(0.95..=1.0).contains(&top_p)))
  {
    return unsupported(
      "top_p",
      provider_id,
      endpoint,
      "Claude requires top_p to be between 0.95 and 1 while thinking is enabled",
    );
  }
  Ok(())
}

fn claude_rejects_sampling_controls(model: &str) -> bool {
  let model = normalize_claude_model(model);
  model.contains("claude-fable-5")
    || model.contains("claude-mythos-5")
    || model.contains("mythos-preview")
    || model.contains("claude-opus-4-7")
    || model.contains("claude-opus-4-8")
    || model.contains("claude-opus-5")
    || model.contains("claude-sonnet-5")
}

fn normalize_claude_model(model: &str) -> String {
  model.to_ascii_lowercase().replace(['.', '_'], "-")
}

fn is_claude_model(model: &str) -> bool {
  normalize_claude_model(model).starts_with("claude-")
}

fn remove_fields(obj: &mut Map<String, Value>, fields: &[&str]) {
  for field in fields {
    obj.remove(*field);
  }
}

fn reasoning_mode_str(mode: ReasoningMode) -> &'static str {
  match mode {
    ReasoningMode::Enabled => "enabled",
    ReasoningMode::Disabled => "disabled",
    ReasoningMode::Adaptive => "adaptive",
  }
}

fn object_field<'a>(obj: &'a mut Map<String, Value>, field: &str) -> &'a mut Map<String, Value> {
  let value = obj
    .entry(field.to_string())
    .or_insert_with(|| Value::Object(Map::new()));
  if !value.is_object() {
    *value = Value::Object(Map::new());
  }
  value.as_object_mut().expect("field was normalized to an object")
}

fn reject_mode(
  reasoning: &ReasoningOptions,
  provider_id: &str,
  endpoint: Endpoint,
  rejected: &[ReasoningMode],
  reason: &'static str,
) -> Result<(), PreparationError> {
  if reasoning.mode.as_ref().is_some_and(|mode| rejected.contains(mode)) {
    return unsupported("reasoning.mode", provider_id, endpoint, reason);
  }
  Ok(())
}

fn reject_present<T>(
  value: Option<T>,
  control: &'static str,
  provider_id: &str,
  endpoint: Endpoint,
  reason: &'static str,
) -> Result<(), PreparationError> {
  if value.is_some() {
    return unsupported(control, provider_id, endpoint, reason);
  }
  Ok(())
}

fn unsupported<T>(
  control: &'static str,
  provider_id: &str,
  endpoint: Endpoint,
  reason: &'static str,
) -> Result<T, PreparationError> {
  Err(PreparationError::UnsupportedGenerationControl {
    control,
    provider_id: provider_id.into(),
    endpoint,
    reason,
  })
}
