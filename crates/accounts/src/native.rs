//! Built-in provider registry for shared request preparation.
//!
//! Platform-independent driver selection is kept alongside the standard
//! registry. Provider modules own their wire contract; this module only dispatches.

use core::fmt;
use serde_json::Value;
use tokn_core::{
  headers::HeaderMap,
  provider::{PrepareError, PreparedRequest},
  types::SmolStr,
  upstream_url::CleartextHttpPolicy,
  Endpoint,
};

#[derive(Debug)]
pub enum Error {
  UnknownDriver(SmolStr),
  Prepare(PrepareError),
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::UnknownDriver(driver) => write!(f, "unknown provider driver '{driver}'"),
      Self::Prepare(error) => error.fmt(f),
    }
  }
}

impl core::error::Error for Error {
  fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
    match self {
      Self::Prepare(error) => Some(error),
      Self::UnknownDriver(_) => None,
    }
  }
}

/// Whether a built-in driver implements a native upstream API operation.
pub fn supports_endpoint(driver: &str, endpoint: Endpoint, base_url: &str) -> bool {
  match driver {
    "openai" => tokn_provider_openai::native::operation_segments(endpoint, base_url).is_some(),
    "codex" => endpoint == Endpoint::Responses,
    "github-copilot" => tokn_provider_copilot::native::operation_segments(endpoint, base_url).is_some(),
    "deepseek" => tokn_provider_deepseek::native::operation_segments(endpoint, base_url).is_some(),
    "llama-cpp" => tokn_provider_llama_cpp::native::operation_segments(endpoint, base_url).is_some(),
    "opencode-go" => tokn_provider_opencode::native::operation_segments(endpoint, base_url).is_some(),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::operation_segments(endpoint, base_url).is_some()
    }
    _ => false,
  }
}

/// Apply the selected driver's actual transforms, URL construction, headers,
/// credential scheme, and serialization. No transport is selected here.
pub fn prepare(
  driver: &str,
  endpoint: Endpoint,
  base_url: &str,
  credential: &str,
  body: Value,
  headers: HeaderMap,
) -> Result<PreparedRequest, Error> {
  prepare_with_policy(
    driver,
    endpoint,
    base_url,
    credential,
    body,
    headers,
    CleartextHttpPolicy::LoopbackOnly,
  )
}

/// Prepare using the cleartext policy explicitly admitted by configuration.
#[allow(clippy::too_many_arguments)]
pub fn prepare_with_policy(
  driver: &str,
  endpoint: Endpoint,
  base_url: &str,
  credential: &str,
  body: Value,
  headers: HeaderMap,
  cleartext: CleartextHttpPolicy,
) -> Result<PreparedRequest, Error> {
  let result = match driver {
    "openai" => {
      tokn_provider_openai::native::prepare_with_policy(base_url, endpoint, body, credential, headers, cleartext)
    }
    "codex" => {
      tokn_provider_openai::native::prepare_codex_with_policy(base_url, endpoint, body, credential, headers, cleartext)
    }
    "github-copilot" => {
      tokn_provider_copilot::native::prepare_with_policy(base_url, endpoint, body, credential, headers, cleartext)
    }
    "deepseek" => {
      tokn_provider_deepseek::native::prepare_with_policy(base_url, endpoint, body, credential, headers, cleartext)
    }
    "llama-cpp" => {
      tokn_provider_llama_cpp::native::prepare_with_policy(base_url, endpoint, body, credential, headers, cleartext)
    }
    "opencode-go" => {
      tokn_provider_opencode::native::prepare_with_policy(base_url, endpoint, body, credential, headers, cleartext)
    }
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::prepare_with_policy(base_url, endpoint, body, credential, headers, cleartext)
    }
    _ => return Err(Error::UnknownDriver(SmolStr::new(driver))),
  };
  result.map_err(Error::Prepare)
}

/// Official destination defaults shared with the standard provider constructors.
/// Prefer the configured provider's preset identity, then its driver identity.
pub fn default_base_url(provider_or_driver: &str) -> Option<&'static str> {
  match provider_or_driver {
    "openai" => Some(tokn_provider_openai::native::OPENAI_BASE_URL),
    "codex" => Some(tokn_provider_openai::native::CODEX_BASE_URL),
    "github-copilot" => Some(tokn_provider_copilot::native::COPILOT_BASE_URL),
    "deepseek" => Some(tokn_provider_deepseek::native::DEFAULT_BASE_URL),
    "llama-cpp" => Some(tokn_provider_llama_cpp::native::DEFAULT_BASE_URL),
    "opencode-go" => Some(tokn_provider_opencode::native::OPENCODE_GO_BASE_URL),
    "zai" => Some(tokn_provider_zai::native::ZAI_BASE_URL),
    "zai-coding-plan" => Some(tokn_provider_zai::native::ZAI_CODING_PLAN_BASE_URL),
    "zhipuai" => Some(tokn_provider_zai::native::ZHIPUAI_BASE_URL),
    "zhipuai-coding-plan" => Some(tokn_provider_zai::native::ZHIPUAI_CODING_PLAN_BASE_URL),
    _ => None,
  }
}

/// Apply each driver's actual per-model API rules. Known models can force a
/// native operation even when the provider supports several operations overall.
pub fn supports_model_endpoint(driver: &str, model: &str, endpoint: Endpoint, base_url: &str) -> bool {
  match driver {
    "openai" => tokn_provider_openai::native::supports_model_endpoint(model, endpoint, base_url),
    "codex" => endpoint == Endpoint::Responses,
    "github-copilot" => tokn_provider_copilot::native::supports_model_endpoint(model, endpoint, base_url),
    "deepseek" => tokn_provider_deepseek::native::supports_model_endpoint(model, endpoint, base_url),
    "llama-cpp" => tokn_provider_llama_cpp::native::supports_model_endpoint(model, endpoint, base_url),
    "opencode-go" => tokn_provider_opencode::native::supports_model_endpoint(model, endpoint, base_url),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::supports_model_endpoint(model, endpoint, base_url)
    }
    _ => false,
  }
}

/// Enforce the driver's shared header normalization after credential injection.
/// The pipeline's request id remains authoritative when a driver rebuilds an
/// allowlisted header map. Codex also needs its account context from credentials.
pub fn normalize_headers(
  driver: &str,
  headers: &mut HeaderMap,
  ctx: &tokn_core::provider::HeaderPatchCtx<'_>,
  account: &tokn_core::account::AccountConfig,
) -> Result<(), Error> {
  let request_id = headers.get("x-request-id").cloned();
  let normalized = match driver {
    "openai" => tokn_provider_openai::native::normalize_headers(headers, ctx),
    "codex" => {
      let account_id = tokn_provider_openai::jwt::account_id(account);
      tokn_provider_openai::native::inject_codex_credentials(
        headers,
        ctx.bearer_token.unwrap_or(""),
        account_id.as_deref(),
      );
      tokn_provider_openai::native::normalize_codex_headers(headers, ctx)
    }
    "github-copilot" => tokn_provider_copilot::native::normalize_headers(headers, ctx),
    "deepseek" => tokn_provider_deepseek::native::normalize_headers(headers, ctx),
    "llama-cpp" => tokn_provider_llama_cpp::native::normalize_headers(headers, ctx),
    "opencode-go" => tokn_provider_opencode::native::normalize_headers(headers, ctx),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::normalize_headers(headers, ctx)
    }
    _ => return Err(Error::UnknownDriver(SmolStr::new(driver))),
  };
  if let Some(normalized) = normalized {
    *headers = normalized;
  }
  if let Some(request_id) = request_id {
    headers.insert("x-request-id", request_id);
  }
  Ok(())
}

/// Native API path without selecting credentials or transforming payload bytes.
/// Used by fixed-destination relays, whose caller owns the wire request.
pub fn operation_segments(driver: &str, endpoint: Endpoint, base_url: &str) -> Option<&'static [&'static str]> {
  match driver {
    "openai" => tokn_provider_openai::native::operation_segments(endpoint, base_url),
    "codex" if endpoint == Endpoint::Responses => Some(&["responses"]),
    "github-copilot" => tokn_provider_copilot::native::operation_segments(endpoint, base_url),
    "deepseek" => tokn_provider_deepseek::native::operation_segments(endpoint, base_url),
    "llama-cpp" => tokn_provider_llama_cpp::native::operation_segments(endpoint, base_url),
    "opencode-go" => tokn_provider_opencode::native::operation_segments(endpoint, base_url),
    "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
      tokn_provider_zai::native::operation_segments(endpoint, base_url)
    }
    _ => None,
  }
}
