pub mod auth;
mod opencode_go;
mod quota;

pub use opencode_go::*;
pub use tokn_catalogue as catalogue;
pub use tokn_core::provider::{
  error, AuthKind, Endpoint, EndpointRule, HeaderPatchCtx, Provider, ProviderInfo, ProviderRequestKind, RequestCtx,
  Result, TemplateVars, ID_OPENCODE_GO,
};
pub use tokn_core::util;

use std::sync::Arc;
use tokn_auth::descriptor::{EndpointSpec, PathRewrite, ProviderDescriptor};
use tokn_auth::provider::CredentialFlavor;
use tokn_core::provider::ProviderTarget;

pub use crate::native::OPENCODE_GO_BASE_URL;

pub static DEFAULT_ENDPOINTS: &[Endpoint] = &[Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages];

/// Authoritative endpoint bindings for model families documented by OpenCode
/// Go. Runtime routing consults refreshed models.dev adapter metadata only for
/// models that do not match one of these rules.
pub use crate::native::MODEL_ENDPOINT_RULES;

pub(crate) fn operation_url(target: &ProviderTarget, endpoint: Endpoint) -> Result<reqwest::Url> {
  let segments = native::operation_segments(endpoint, target.base_url().as_str()).ok_or_else(|| {
    error::Error::UnsupportedEndpoint {
      provider: ID_OPENCODE_GO.to_string(),
      endpoint: endpoint.as_str(),
    }
  })?;
  Ok(target.base_url().operation_url(segments.iter().copied())?)
}

pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
  id: ID_OPENCODE_GO,
  display_name: "OpenCode Go",
  hosts: &["opencode.ai"],
  base_url: OPENCODE_GO_BASE_URL,
  credentials: &[CredentialFlavor::ApiKey],
  endpoints: &[
    EndpointSpec {
      endpoint: Endpoint::ChatCompletions,
      method: "POST",
      path: "/v1/chat/completions",
      aliases: &["/zen/go/v1/chat/completions"],
    },
    EndpointSpec {
      endpoint: Endpoint::Responses,
      method: "POST",
      path: "/v1/responses",
      aliases: &["/zen/go/v1/responses"],
    },
    EndpointSpec {
      endpoint: Endpoint::Messages,
      method: "POST",
      path: "/v1/messages",
      aliases: &["/zen/go/v1/messages"],
    },
  ],
  model_endpoint_rules: Some(MODEL_ENDPOINT_RULES),
  operation_url,
  rewrites: &[PathRewrite {
    method: "GET",
    src: "/zen/go/v1/models",
    path: "/v1/models",
  }],
  auth_urls: &[],
  matches_url,
  validate,
  build,
  build_auth: Some(crate::auth::provider_auth),
};

pub fn matches_url(host: &str, path: &str, _id: &'static str) -> bool {
  host == "opencode.ai" && (path.is_empty() || path == "/zen/go/v1" || path.starts_with("/zen/go/v1/"))
}

pub fn validate(account: &tokn_core::account::AccountConfig) -> Result<()> {
  OpenCodeGoProvider::validate_account(account)
}

pub fn build(
  account: Arc<tokn_core::account::AccountConfig>,
  target: ProviderTarget,
) -> Result<Arc<dyn tokn_core::provider::Provider>> {
  Ok(Arc::new(OpenCodeGoProvider::from_account_at(account, target)?))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn matches_only_the_opencode_go_path_prefix() {
    assert!(matches_url("opencode.ai", "", ID_OPENCODE_GO));
    assert!(matches_url("opencode.ai", "/zen/go/v1", ID_OPENCODE_GO));
    assert!(matches_url(
      "opencode.ai",
      "/zen/go/v1/chat/completions",
      ID_OPENCODE_GO
    ));
    assert!(!matches_url("opencode.ai", "/zen/go/v10", ID_OPENCODE_GO));
    assert!(!matches_url("opencode.ai", "/zen/go/v1-other", ID_OPENCODE_GO));
    assert!(!matches_url("example.com", "/zen/go/v1", ID_OPENCODE_GO));
  }
}
