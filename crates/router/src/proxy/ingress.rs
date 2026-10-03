//! A transport snapshot independent of whether execution is local or remote.

use crate::frontend::ProxyIngress;
use crate::v2::{ForwardProxyState, InboundConnectionInfo, LiveForwardProxyState, ProxyAuthenticationError};
use axum::extract::Request;
use axum::http::HeaderMap;
use axum::response::Response;
use std::sync::Arc;
use tokn_access::AccessContext;
use tokn_policy::{ClientAuthPlan, ConnectAction, ForwardProxyListenerPlan, IngressAuthority};

#[derive(Clone)]
pub(crate) enum IngressSource {
  Local(LiveForwardProxyState),
  Remote(Arc<ProxyIngress>),
}

impl From<LiveForwardProxyState> for IngressSource {
  fn from(state: LiveForwardProxyState) -> Self {
    Self::Local(state)
  }
}

impl IngressSource {
  pub(crate) fn current(&self) -> IngressState {
    match self {
      Self::Local(state) => IngressState::Local(state.current()),
      Self::Remote(state) => IngressState::Remote(state.clone()),
    }
  }
}

pub(crate) enum IngressState {
  Local(Arc<ForwardProxyState>),
  Remote(Arc<ProxyIngress>),
}

impl IngressState {
  pub(crate) fn connect_action_for(&self, ingress: &IngressAuthority) -> ConnectAction {
    match self {
      Self::Local(state) => state.connect_action_for(ingress),
      Self::Remote(state) => state.connect_action_for(ingress),
    }
  }

  pub(crate) fn pinned_tls_config(&self, ingress: &IngressAuthority) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    match self {
      Self::Local(state) => state.pinned_tls_config(ingress),
      Self::Remote(state) => state.pinned_tls_config(ingress),
    }
  }

  pub(crate) async fn authenticate_proxy(
    &self,
    headers: &mut HeaderMap,
  ) -> Result<AccessContext, ProxyAuthenticationError> {
    match self {
      Self::Local(state) => state.authenticate_proxy(headers).await,
      Self::Remote(state) => authenticate_proxy(state.listener.client_auth(), state.access.clone(), headers).await,
    }
  }

  pub(crate) async fn dispatch_http(
    &self,
    ingress: &IngressAuthority,
    scheme: &'static str,
    access: AccessContext,
    connection: InboundConnectionInfo,
    request: Request,
  ) -> Response {
    match self {
      Self::Local(state) => state.dispatch_http(ingress, scheme, access, connection, request).await,
      Self::Remote(state) => state.dispatch_http(ingress, scheme, access, connection, request).await,
    }
  }
}

pub(crate) async fn authenticate_proxy(
  client_auth: ClientAuthPlan,
  access: Arc<tokn_access::AccessStore>,
  headers: &mut HeaderMap,
) -> Result<AccessContext, ProxyAuthenticationError> {
  let authorization = headers
    .get_all(axum::http::header::PROXY_AUTHORIZATION)
    .iter()
    .map(|value| value.to_str().ok())
    .collect::<Option<Vec<_>>>();
  let token = match (client_auth, authorization.as_deref()) {
    (ClientAuthPlan::None, _) => None,
    (ClientAuthPlan::LocalKeys, Some([value])) => {
      let mut parts = value.split_ascii_whitespace();
      match (parts.next(), parts.next(), parts.next()) {
        (Some(scheme), Some(token), None) if scheme.eq_ignore_ascii_case("bearer") => Some(token.to_string()),
        _ => return Err(ProxyAuthenticationError::Rejected),
      }
    }
    (ClientAuthPlan::LocalKeys, _) => return Err(ProxyAuthenticationError::Rejected),
  };
  headers.remove(axum::http::header::PROXY_AUTHORIZATION);
  let Some(token) = token else {
    return Ok(AccessContext::unrestricted());
  };
  tokio::task::spawn_blocking(move || access.authenticate(Some(&token)))
    .await
    .map_err(|error| {
      tracing::error!(%error, "v2 proxy authentication task failed");
      ProxyAuthenticationError::Unavailable
    })?
    .map_err(|_| ProxyAuthenticationError::Rejected)
}

pub(crate) fn connect_action_for(listener: &ForwardProxyListenerPlan, ingress: &IngressAuthority) -> ConnectAction {
  listener
    .connect_rules()
    .iter()
    .find(|rule| {
      let matcher = rule.matcher();
      (matcher.hosts().is_empty() || matcher.hosts().iter().any(|pattern| pattern.matches(ingress.host())))
        && (matcher.ports().is_empty() || matcher.ports().contains(&ingress.port()))
    })
    .map(|rule| rule.action())
    .unwrap_or_else(|| listener.default_connect_action())
}
