//! Request-level boundary shared by local execution and IPC workers.

use async_trait::async_trait;
use axum::extract::Request;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokn_access::AccessContext;
use tokn_policy::{CanonicalAuthority, IngressAuthority};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RequestOrigin {
  Api,
  Proxy {
    scheme: ProxyScheme,
    authority: String,
    intercepted: bool,
  },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyScheme {
  Http,
  Https,
}

impl ProxyScheme {
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Http => "http",
      Self::Https => "https",
    }
  }
}

impl RequestOrigin {
  pub(crate) fn proxy(ingress: &IngressAuthority, scheme: &'static str) -> Self {
    Self::Proxy {
      scheme: if scheme == "https" {
        ProxyScheme::Https
      } else {
        ProxyScheme::Http
      },
      authority: ingress.authority().to_string(),
      intercepted: scheme == "https",
    }
  }

  pub(crate) fn ingress(&self) -> anyhow::Result<Option<IngressAuthority>> {
    match self {
      Self::Api => Ok(None),
      Self::Proxy {
        scheme,
        authority,
        intercepted,
      } => {
        anyhow::ensure!(
          *intercepted == matches!(scheme, ProxyScheme::Https),
          "invalid proxy origin"
        );
        let authority = CanonicalAuthority::parse(authority)?;
        Ok(Some(if *intercepted {
          IngressAuthority::from_connect_authority(authority)?
        } else {
          IngressAuthority::from_http(authority, std::num::NonZeroU16::new(80).unwrap())
        }))
      }
    }
  }
}

/// Trusted admission context. Client-supplied headers cannot create this context.
#[derive(Clone, Debug)]
pub struct DispatchContext {
  pub listener_id: String,
  pub origin: RequestOrigin,
  pub access: AccessContext,
  pub local_addr: Option<SocketAddr>,
  pub peer_addr: Option<SocketAddr>,
}

#[async_trait]
#[allow(
  clippy::double_must_use,
  reason = "async_trait adds must_use to methods returning must-use futures"
)]
pub trait RequestDispatcher: Send + Sync {
  async fn dispatch(&self, context: DispatchContext, request: Request) -> anyhow::Result<Response>;
}

pub(crate) fn authenticate_api(
  client_auth: tokn_policy::ClientAuthPlan,
  access: &tokn_access::AccessStore,
  headers: &mut axum::http::HeaderMap,
) -> Result<AccessContext, tokn_access::AuthenticationError> {
  if client_auth == tokn_policy::ClientAuthPlan::None {
    return Ok(AccessContext::unrestricted());
  }
  let token = headers
    .get(axum::http::header::AUTHORIZATION)
    .and_then(|value| value.to_str().ok())
    .and_then(|value| value.split_once(char::is_whitespace))
    .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
    .map(|(_, token)| token.trim())
    .filter(|token| !token.is_empty())
    .or_else(|| headers.get("x-api-key").and_then(|value| value.to_str().ok()));
  let context = access.authenticate(token)?;
  headers.remove(axum::http::header::AUTHORIZATION);
  headers.remove("x-api-key");
  Ok(context)
}
