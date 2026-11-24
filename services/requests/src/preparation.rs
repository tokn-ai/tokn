//! Request-body and identity policy shared by standard and alloc-only execution.
//!
//! The selected destination supplies model metadata. Platform adapters supply
//! I/O; they do not decide how generation controls or provider identities work.

use alloc::string::ToString;
use core::fmt;
use serde_json::Value;
use tokn_core::{
  generation::{GenerationOptions, GenerationOptionsError, ReasoningEffort},
  headers::TemplateVars,
  types::SmolStr,
  AgentId, Endpoint,
};
use tokn_headers::{keys, HeaderMap, HeaderValue};

pub(crate) mod compaction;
pub(crate) mod generation;

pub use compaction::{apply_compaction_priority, apply_compaction_priority_routing_hint};
pub(crate) use generation::{ensure_model_supports_effort, ensure_model_supports_reasoning, lower_generation_options};

#[derive(Debug)]
pub enum PreparationError {
  InvalidGenerationOptions {
    source: GenerationOptionsError,
  },
  UnsupportedGenerationControl {
    control: &'static str,
    provider_id: SmolStr,
    endpoint: Endpoint,
    reason: &'static str,
  },
  RequestConversion {
    source: tokn_convert::error::ConvertError,
  },
}

impl fmt::Display for PreparationError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::InvalidGenerationOptions { source } => write!(f, "invalid generation options: {source}"),
      Self::UnsupportedGenerationControl {
        control,
        provider_id,
        endpoint,
        reason,
      } => write!(
        f,
        "generation control `{control}` is not supported by provider `{provider_id}` on {endpoint}: {reason}"
      ),
      Self::RequestConversion { source } => write!(f, "request conversion failed: {source}"),
    }
  }
}

impl core::error::Error for PreparationError {
  fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
    match self {
      Self::InvalidGenerationOptions { source } => Some(source),
      Self::RequestConversion { source } => Some(source),
      Self::UnsupportedGenerationControl { .. } => None,
    }
  }
}

/// Metadata about the resolved model, independent of how it was discovered.
pub struct GenerationTarget<'a> {
  pub endpoint: Endpoint,
  pub provider_id: &'a str,
  pub model: &'a str,
  pub reasoning_efforts: Option<&'a [ReasoningEffort]>,
  pub reasoning_supported: Option<bool>,
}

/// Rewrite and convert a managed request, then lower typed generation controls.
/// Provider-native transformation runs afterwards, before compaction policy.
pub fn prepare_managed_body(
  inbound_endpoint: Endpoint,
  inbound_body: &Value,
  target: GenerationTarget<'_>,
  generation_options: Option<&GenerationOptions>,
) -> Result<Value, PreparationError> {
  if let Some(options) = generation_options {
    options
      .validate()
      .map_err(|source| PreparationError::InvalidGenerationOptions { source })?;
    ensure_model_supports_effort(
      target.endpoint,
      target.provider_id,
      target.model,
      target.reasoning_efforts,
      options,
    )?;
    let reasoning_supported = if target.reasoning_efforts.is_some_and(|values| !values.is_empty()) {
      Some(true)
    } else {
      target.reasoning_supported
    };
    ensure_model_supports_reasoning(target.endpoint, target.provider_id, reasoning_supported, options)?;
  }
  let mut body = inbound_body.clone();
  if let Some(object) = body.as_object_mut() {
    object.insert("model".into(), Value::String(target.model.to_string()));
  }
  if target.endpoint != inbound_endpoint {
    body = tokn_convert::convert_request(inbound_endpoint, target.endpoint, &body)
      .map_err(|source| PreparationError::RequestConversion { source })?;
  }
  if let Some(options) = generation_options {
    lower_generation_options(&mut body, target.endpoint, target.provider_id, target.model, options)?;
  }
  Ok(body)
}

/// Verbatim routes cannot lower provider-neutral controls without changing the
/// caller's endpoint contract. Validate and reject them before transport work.
pub fn validate_relay_options(
  provider_id: &str,
  endpoint: Endpoint,
  options: Option<&GenerationOptions>,
) -> Result<(), PreparationError> {
  let Some(options) = options else {
    return Ok(());
  };
  options
    .validate()
    .map_err(|source| PreparationError::InvalidGenerationOptions { source })?;
  let control = if options.max_output_tokens.is_some() {
    Some("max_output_tokens")
  } else if options.top_k.is_some() {
    Some("top_k")
  } else if options.reasoning.is_some() {
    Some("reasoning")
  } else {
    None
  };
  if let Some(control) = control {
    return Err(PreparationError::UnsupportedGenerationControl {
      control,
      provider_id: SmolStr::new(provider_id),
      endpoint,
      reason: "verbatim routing cannot lower provider-neutral generation controls",
    });
  }
  Ok(())
}

/// Resolve the profile's explicit wire identity without adding platform policy.
pub fn wire_identity(driver: &str, identity: &tokn_policy::WireIdentity) -> Option<AgentId> {
  match identity {
    tokn_policy::WireIdentity::None => None,
    tokn_policy::WireIdentity::ProviderDefault => Some(AgentId::provider_default(driver).unwrap_or_default()),
    tokn_policy::WireIdentity::Named(id) => Some(AgentId::from(id.as_str())),
  }
}

/// Compose the selected identity and enforce the request's correlation id.
pub fn identity_headers(
  driver: &str,
  identity: Option<&AgentId>,
  vars: &mut TemplateVars,
  inbound: &HeaderMap,
  request_id: &str,
) -> HeaderMap {
  let mut headers = identity.map_or_else(HeaderMap::new, |identity| {
    tokn_headers::registry::build_wire_identity_headers(driver, identity.as_str(), vars, inbound)
  });
  vars.request_id.get_or_insert_with(|| SmolStr::new(request_id));
  headers.insert(&keys::X_REQUEST_ID, HeaderValue::from_string(request_id.to_string()));
  headers
}

#[cfg(feature = "std")]
impl From<PreparationError> for crate::pipeline::error::RequestsError {
  fn from(error: PreparationError) -> Self {
    match error {
      PreparationError::InvalidGenerationOptions { source } => Self::InvalidGenerationOptions { source },
      PreparationError::UnsupportedGenerationControl {
        control,
        provider_id,
        endpoint,
        reason,
      } => Self::UnsupportedGenerationControl {
        control,
        provider_id,
        endpoint,
        reason,
      },
      PreparationError::RequestConversion { source } => Self::RequestConversion { source },
    }
  }
}
