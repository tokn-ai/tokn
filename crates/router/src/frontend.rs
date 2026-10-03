//! Stable client transport with request execution delegated to workers.

use crate::api::error::ApiError;
use crate::dispatch::{DispatchContext, RequestDispatcher, RequestOrigin};
use crate::proxy::ingress::IngressSource;
use crate::routing::{RoutingControl, UpdateWeights};
use crate::v2::mounts::{ApiMounts, ApiOperation};
use crate::v2::InboundConnectionInfo;
use axum::extract::{Request, State};
use axum::http::Method;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokn_access::{AccessContext, AccessStore};
use tokn_policy::{
  ConnectAction, CredentialPolicy, ForwardProxyListenerPlan, GatewayPlan, IngressAuthority, ListenerId, ListenerPlan,
  LlmApiListenerPlan,
};

pub struct Frontend {
  api: Vec<(SocketAddr, Router)>,
  proxy: Vec<(SocketAddr, Arc<ProxyIngress>)>,
}

impl Frontend {
  /// Link transport policy only: no provider pipelines, credentials, or request databases.
  pub fn new(
    plan: GatewayPlan,
    service: tokn_config::v2::ServicePlan,
    access: Arc<AccessStore>,
    dispatcher: Arc<dyn RequestDispatcher>,
  ) -> anyhow::Result<Self> {
    let mounts = Arc::new(ApiMounts::new(&plan)?);
    let client_credentials = Arc::new(
      plan
        .profiles()
        .iter()
        .filter_map(|(id, profile)| {
          plan
            .route(profile.route())
            .filter(|route| route.credential_policy() == CredentialPolicy::Client)
            .map(|_| id.clone())
        })
        .collect::<BTreeSet<_>>(),
    );
    let mut api = Vec::new();
    let mut proxy = Vec::new();
    for (id, listener) in plan.listeners() {
      match listener {
        ListenerPlan::LlmApi(listener) => {
          let state = Arc::new(ApiIngress {
            listener_id: id.clone(),
            listener: listener.clone(),
            mounts: mounts.clone(),
            client_credentials: client_credentials.clone(),
            access: access.clone(),
            dispatcher: dispatcher.clone(),
            max_wire_bytes: service.request_limits().max_wire_bytes(),
          });
          let cors_state = state.clone();
          let cors = crate::cors::layer_for_request(move |origin, parts| {
            let policy = cors_state.listener.cors();
            cors_state
              .mounts
              .get(parts.uri.path())
              .is_some_and(|entry| entry.enabled)
              && (policy.allowed_origins().contains(origin)
                || (policy.allow_localhost() && crate::cors::is_localhost_origin(origin)))
          });
          let app = Router::new()
            .fallback(dispatch_api)
            .layer(cors)
            .route("/healthz", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(crate::request_id::propagate_request_id))
            .layer(tower_http::request_id::SetRequestIdLayer::new(
              axum::http::HeaderName::from_static(crate::request_id::REQUEST_ID_HEADER),
              crate::request_id::MakeRouterRequestId,
            ))
            .with_state(state);
          api.push((listener.bind(), app));
        }
        ListenerPlan::ForwardProxy(listener) => {
          let ca = listener
            .tls()
            .map(|tls| crate::proxy::load_or_generate_ca(tls.ca_dir(), false).map(Arc::new))
            .transpose()?;
          proxy.push((
            listener.bind(),
            Arc::new(ProxyIngress {
              listener_id: id.clone(),
              listener: listener.clone(),
              ca,
              access: access.clone(),
              dispatcher: dispatcher.clone(),
              outbound: service.outbound().to_http_client_options(),
              max_wire_bytes: listener
                .request_body_max_bytes()
                .min(service.request_limits().max_wire_bytes()),
            }),
          ));
        }
      }
    }
    Ok(Self { api, proxy })
  }

  /// Expose control only on loopback API listeners. Explicit action headers
  /// keep browser navigations and form submissions from changing routing.
  pub fn with_routing_control(mut self, control: Arc<dyn RoutingControl>) -> Self {
    for (addr, app) in &mut self.api {
      if addr.ip().is_loopback() {
        *app = app
          .clone()
          .route("/admin/workers", get(worker_status).post(update_weights))
          .layer(axum::Extension(control.clone()));
      }
    }
    self
  }

  /// Preserve the CLI override for a single public listener.
  pub fn with_bind_override(mut self, bind: SocketAddr) -> anyhow::Result<Self> {
    anyhow::ensure!(
      self.api.len() + self.proxy.len() == 1,
      "--host and --port can only override a config with exactly one listener"
    );
    if let Some((addr, _)) = self.api.first_mut() {
      *addr = bind;
    }
    if let Some((addr, _)) = self.proxy.first_mut() {
      *addr = bind;
    }
    Ok(self)
  }

  /// Bind all public sockets before publishing discovery readiness.
  pub async fn bind(self) -> anyhow::Result<BoundFrontend> {
    let mut api = Vec::new();
    for (addr, app) in self.api {
      api.push((tokio::net::TcpListener::bind(addr).await?, app));
    }
    let mut proxy = Vec::new();
    for (addr, state) in self.proxy {
      proxy.push((tokio::net::TcpListener::bind(addr).await?, state));
    }
    anyhow::ensure!(
      !api.is_empty() || !proxy.is_empty(),
      "frontend requires at least one listener"
    );
    Ok(BoundFrontend { api, proxy })
  }

  pub async fn serve<F>(self, shutdown: F) -> anyhow::Result<()>
  where
    F: std::future::Future<Output = ()> + Send,
  {
    self.bind().await?.serve(shutdown).await
  }
}

pub struct BoundFrontend {
  api: Vec<(tokio::net::TcpListener, Router)>,
  proxy: Vec<(tokio::net::TcpListener, Arc<ProxyIngress>)>,
}

impl BoundFrontend {
  pub async fn serve<F>(self, shutdown: F) -> anyhow::Result<()>
  where
    F: Future<Output = ()> + Send,
  {
    use futures_util::future::BoxFuture;
    use futures_util::{stream::FuturesUnordered, StreamExt};
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut servers = FuturesUnordered::<BoxFuture<'static, anyhow::Result<()>>>::new();
    for (listener, app) in self.api {
      let addr = listener.local_addr()?;
      tracing::info!(%addr, "frontend API listener starting");
      let mut stopped = shutdown_rx.clone();
      servers.push(Box::pin(crate::server::serve_http(app, listener, async move {
        if !*stopped.borrow() {
          let _ = stopped.changed().await;
        }
      })));
    }
    for (listener, state) in self.proxy {
      let outbound = state.outbound.clone();
      let mut stopped = shutdown_rx.clone();
      servers.push(Box::pin(crate::proxy::serve_bound_v2_policy(
        listener,
        outbound,
        IngressSource::Remote(state),
        async move {
          if !*stopped.borrow() {
            let _ = stopped.changed().await;
          }
        },
      )));
    }
    anyhow::ensure!(!servers.is_empty(), "frontend requires at least one listener");
    tokio::pin!(shutdown);
    let result = tokio::select! {
      _ = &mut shutdown => Ok(()),
      result = servers.next() => result.expect("frontend has a listener"),
    };
    let _ = shutdown_tx.send(true);
    let mut outcome = result;
    while let Some(result) = servers.next().await {
      if outcome.is_ok() {
        outcome = result;
      }
    }
    outcome
  }
}

struct ApiIngress {
  listener_id: ListenerId,
  listener: LlmApiListenerPlan,
  mounts: Arc<ApiMounts>,
  client_credentials: Arc<BTreeSet<tokn_policy::ProfileId>>,
  access: Arc<AccessStore>,
  dispatcher: Arc<dyn RequestDispatcher>,
  max_wire_bytes: usize,
}

async fn dispatch_api(
  State(state): State<Arc<ApiIngress>>,
  connection: InboundConnectionInfo,
  mut request: Request,
) -> Response {
  let Some(entry) = state.mounts.get(request.uri().path()).filter(|entry| entry.enabled) else {
    return ApiError::not_found("API path is not exposed").into_response();
  };
  let client_owned =
    matches!(entry.operation, ApiOperation::Generate(_)) && state.client_credentials.contains(&entry.profile);
  let access = if client_owned {
    Ok(AccessContext::unrestricted())
  } else {
    crate::dispatch::authenticate_api(state.listener.client_auth(), &state.access, request.headers_mut())
  };
  let access = match access {
    Ok(access) => access,
    Err(_) => return ApiError::unauthorized("missing or invalid API key").into_response(),
  };
  // Worker mounts validate methods and execute against their own policy generation.
  if request.method() == Method::CONNECT {
    return ApiError::bad_request("CONNECT requires a forward-proxy listener").into_response();
  }
  let context = DispatchContext {
    listener_id: state.listener_id.to_string(),
    origin: RequestOrigin::Api,
    access,
    local_addr: connection.local_addr,
    peer_addr: connection.peer_addr,
  };
  dispatch(&*state.dispatcher, context, request, state.max_wire_bytes).await
}

pub(crate) struct ProxyIngress {
  listener_id: ListenerId,
  pub(crate) listener: ForwardProxyListenerPlan,
  pub(crate) access: Arc<AccessStore>,
  ca: Option<Arc<crate::proxy::ProxyCa>>,
  dispatcher: Arc<dyn RequestDispatcher>,
  outbound: tokn_core::util::http::HttpClientOptions,
  max_wire_bytes: usize,
}

impl ProxyIngress {
  pub(crate) fn connect_action_for(&self, ingress: &IngressAuthority) -> ConnectAction {
    crate::proxy::ingress::connect_action_for(&self.listener, ingress)
  }

  pub(crate) fn pinned_tls_config(&self, ingress: &IngressAuthority) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    self
      .ca
      .as_ref()
      .ok_or_else(|| anyhow::anyhow!("proxy listener has no CA"))?
      .pinned_server_config(ingress.host())
  }

  pub(crate) async fn dispatch_http(
    &self,
    ingress: &IngressAuthority,
    scheme: &'static str,
    access: AccessContext,
    connection: InboundConnectionInfo,
    request: Request,
  ) -> Response {
    let context = DispatchContext {
      listener_id: self.listener_id.to_string(),
      origin: RequestOrigin::proxy(ingress, scheme),
      access,
      local_addr: connection.local_addr,
      peer_addr: connection.peer_addr,
    };
    dispatch(&*self.dispatcher, context, request, self.max_wire_bytes).await
  }
}

async fn dispatch(
  dispatcher: &dyn RequestDispatcher,
  context: DispatchContext,
  request: Request,
  max_wire_bytes: usize,
) -> Response {
  // Existing ingress handlers buffer request bodies with a limit. Retain that
  // limit before IPC; response bodies remain streaming end-to-end.
  use std::error::Error as _;
  let (parts, body) = request.into_parts();
  let bytes = match axum::body::to_bytes(body, max_wire_bytes).await {
    Ok(bytes) => bytes,
    Err(error)
      if error
        .source()
        .is_some_and(|source| source.is::<http_body_util::LengthLimitError>()) =>
    {
      return ApiError::payload_too_large("request body exceeds the configured limit").into_response();
    }
    Err(_) => return ApiError::bad_request("could not read request body").into_response(),
  };
  let request = Request::from_parts(parts, axum::body::Body::from(bytes));
  match dispatcher.dispatch(context, request).await {
    Ok(response) => response,
    Err(error) => {
      tracing::warn!(%error, "worker dispatch failed");
      ApiError::bad_gateway("gateway worker unavailable").into_response()
    }
  }
}

async fn worker_status(
  axum::Extension(control): axum::Extension<Arc<dyn RoutingControl>>,
  headers: axum::http::HeaderMap,
) -> Response {
  if headers.get("x-tokn-admin").and_then(|value| value.to_str().ok()) != Some("workers") {
    return ApiError::forbidden("worker control requires x-tokn-admin: workers").into_response();
  }
  axum::Json(control.status()).into_response()
}

async fn update_weights(
  axum::Extension(control): axum::Extension<Arc<dyn RoutingControl>>,
  headers: axum::http::HeaderMap,
  axum::Json(update): axum::Json<UpdateWeights>,
) -> Response {
  if headers.get("x-tokn-admin").and_then(|value| value.to_str().ok()) != Some("workers") {
    return ApiError::forbidden("worker control requires x-tokn-admin: workers").into_response();
  }
  match control.update_weights(update.weights).await {
    Ok(report) => axum::Json(report).into_response(),
    Err(error) => {
      tracing::warn!(%error, "worker weight update rejected");
      (
        axum::http::StatusCode::UNPROCESSABLE_ENTITY,
        "worker weight update rejected; check weights and worker readiness",
      )
        .into_response()
    }
  }
}
