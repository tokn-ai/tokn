//! Strict serde schema mirroring [models.dev]'s public `api.json`.
//!
//! [models.dev]: https://models.dev/api.json
//!
//! The file is a flat object keyed by provider id. Every provider has a
//! `models` map keyed by model id.
//!
//! We intentionally only deserialize fields we actually consume — the upstream
//! schema occasionally widens (e.g. `experimental` is sometimes a `bool` and
//! sometimes a richer object), so a narrow schema is also more robust.
//! `#[serde(deny_unknown_fields)]` is *not* used: unknown fields are tolerated.

use alloc::{collections::BTreeMap, string::String, vec::Vec};
use serde::Deserialize;
use tokn_core::generation::ReasoningEffort;
use tokn_core::provider::Endpoint;

/// Top-level: provider id → provider record.
pub type Catalogue = BTreeMap<String, Provider>;

#[derive(Debug, Clone, Deserialize)]
pub struct Provider {
  #[allow(dead_code)]
  pub id: String,
  #[allow(dead_code)]
  pub name: String,
  #[serde(default)]
  pub npm: Option<String>,
  #[serde(default)]
  pub models: BTreeMap<String, Model>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Model {
  pub id: String,
  #[serde(default)]
  pub name: String,
  #[serde(default)]
  pub provider: Option<ModelProvider>,
  #[serde(default)]
  pub attachment: bool,
  #[serde(default)]
  pub reasoning: bool,
  #[serde(default)]
  pub reasoning_options: Option<Vec<ReasoningOption>>,
  #[serde(default)]
  pub tool_call: bool,
  #[serde(default)]
  pub temperature: bool,
  #[serde(default)]
  pub modalities: Modalities,
  #[serde(default)]
  pub cost: Option<Cost>,
  #[serde(default)]
  pub limit: Limits,
  #[serde(default)]
  pub release_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelProvider {
  #[serde(default)]
  pub npm: Option<String>,
}

impl Provider {
  /// Resolve the wire protocol models.dev assigns to one model. Model-level
  /// adapter metadata overrides the provider default.
  pub fn endpoint_for_model(&self, model_id: &str) -> Option<Endpoint> {
    let model = self.models.get(model_id)?;
    let npm = model
      .provider
      .as_ref()
      .and_then(|provider| provider.npm.as_deref())
      .or(self.npm.as_deref())?;
    endpoint_for_npm(npm)
  }
}

fn endpoint_for_npm(npm: &str) -> Option<Endpoint> {
  match npm {
    "@ai-sdk/openai" => Some(Endpoint::Responses),
    "@ai-sdk/anthropic" => Some(Endpoint::Messages),
    "@ai-sdk/openai-compatible" => Some(Endpoint::ChatCompletions),
    _ => None,
  }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReasoningOption {
  Effort {
    values: Vec<ReasoningEffort>,
  },
  Toggle,
  BudgetTokens,
  #[serde(other)]
  Unknown,
}

impl Model {
  pub fn reasoning_efforts(&self) -> Option<Vec<ReasoningEffort>> {
    let options = self.reasoning_options.as_ref()?;
    let mut efforts = Vec::new();
    for option in options {
      match option {
        ReasoningOption::Effort { values } => {
          for value in values {
            if !efforts.contains(value) {
              efforts.push(value.clone());
            }
          }
        }
        ReasoningOption::Unknown => return None,
        ReasoningOption::Toggle | ReasoningOption::BudgetTokens => {}
      }
    }
    Some(efforts)
  }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Modalities {
  #[serde(default)]
  pub input: Vec<String>,
  #[serde(default)]
  pub output: Vec<String>,
}

/// USD per **1M** tokens.
#[derive(Debug, Clone, Deserialize)]
pub struct Cost {
  #[serde(default)]
  pub input: f64,
  #[serde(default)]
  pub output: f64,
  #[serde(default)]
  pub cache_read: Option<f64>,
  #[serde(default)]
  pub cache_write: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Limits {
  #[serde(default)]
  pub context: u32,
  #[serde(default)]
  pub output: u32,
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn model_adapter_overrides_provider_adapter_for_endpoint_routing() {
    let catalogue: Catalogue = serde_json::from_value(serde_json::json!({
      "opencode-go": {
        "id": "opencode-go",
        "name": "OpenCode Go",
        "npm": "@ai-sdk/openai-compatible",
        "models": {
          "chat": {"id": "chat"},
          "responses": {"id": "responses", "provider": {"npm": "@ai-sdk/openai"}},
          "messages": {"id": "messages", "provider": {"npm": "@ai-sdk/anthropic"}}
        }
      }
    }))
    .unwrap();
    let provider = catalogue.get("opencode-go").unwrap();
    assert_eq!(provider.endpoint_for_model("chat"), Some(Endpoint::ChatCompletions));
    assert_eq!(provider.endpoint_for_model("responses"), Some(Endpoint::Responses));
    assert_eq!(provider.endpoint_for_model("messages"), Some(Endpoint::Messages));
    assert_eq!(provider.endpoint_for_model("new-live-model"), None);
  }
}
