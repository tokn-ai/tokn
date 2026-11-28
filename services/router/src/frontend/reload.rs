use super::connect_policy::ConnectPolicy;
use super::*;
use crate::v2::ReloadError;
use arc_swap::ArcSwap;
use std::collections::BTreeMap;

/// One published set of admission policies and its matching execution dispatcher.
pub(super) struct FrontendGeneration {
  api: BTreeMap<ListenerId, Arc<ApiIngress>>,
  proxy: BTreeMap<ListenerId, Arc<ProxyIngress>>,
}

impl FrontendGeneration {
  pub(super) fn build(
    plan: &GatewayPlan,
    service: &ServiceSettings,
    access: &Arc<AccessStore>,
    events: &Arc<EventBus>,
    dispatcher: Arc<dyn RequestDispatcher>,
    previous: Option<&Self>,
    connect_policy: Option<&ConnectPolicy>,
  ) -> anyhow::Result<Self> {
    let mut proxy_listeners = connect_policy
      .map(|policy| policy.apply(plan))
      .transpose()?
      .unwrap_or_default();
    let mounts = Arc::new(ApiMounts::new(plan)?);
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
        .collect(),
    );
    let mut api = BTreeMap::new();
    let mut proxy = BTreeMap::new();
    for (id, listener) in plan.listeners() {
      match listener {
        ListenerPlan::LlmApi(listener) => {
          api.insert(
            id.clone(),
            Arc::new(ApiIngress {
              listener_id: id.clone(),
              listener: listener.clone(),
              mounts: mounts.clone(),
              client_credentials: Arc::clone(&client_credentials),
              access: access.clone(),
              events: events.clone(),
              dispatcher: dispatcher.clone(),
              max_wire_bytes: service.request_limits().max_wire_bytes(),
            }),
          );
        }
        ListenerPlan::ForwardProxy(listener) => {
          let listener = proxy_listeners.remove(id).unwrap_or_else(|| listener.clone());
          // Listener compatibility is checked before preparing a replacement. Keep
          // the original CA and its certificate cache across routing-only edits.
          let ca = match previous {
            Some(previous) => previous.proxy[id].ca.clone(),
            None => listener
              .tls()
              .map(|tls| crate::proxy::load_or_generate_ca(tls.ca_dir(), false).map(Arc::new))
              .transpose()?,
          };
          proxy.insert(
            id.clone(),
            Arc::new(ProxyIngress {
              listener_id: id.clone(),
              max_wire_bytes: listener
                .request_body_max_bytes()
                .min(service.request_limits().max_wire_bytes()),
              listener,
              ca,
              access: access.clone(),
              events: events.clone(),
              dispatcher: dispatcher.clone(),
              outbound: service.outbound().to_http_client_options(),
            }),
          );
        }
      }
    }
    Ok(Self { api, proxy })
  }

  fn validate_listeners(&self, plan: &GatewayPlan) -> Result<(), ReloadError> {
    if self.api.len() + self.proxy.len() != plan.listeners().len() {
      return Err(ReloadError::RestartRequired("listener set changed".into()));
    }
    for (id, listener) in plan.listeners() {
      let compatible = match listener {
        ListenerPlan::LlmApi(next) => self
          .api
          .get(id)
          .is_some_and(|old| old.listener.bind() == next.bind() && old.listener.client_auth() == next.client_auth()),
        ListenerPlan::ForwardProxy(next) => self.proxy.get(id).is_some_and(|old| {
          old.listener.bind() == next.bind()
            && old.listener.client_auth() == next.client_auth()
            && old.listener.tls() == next.tls()
        }),
      };
      if !compatible {
        return Err(ReloadError::RestartRequired(format!(
          "listener '{id}' identity, address, authentication or TLS changed"
        )));
      }
    }
    Ok(())
  }
}

/// Shared by all bound sockets. Requests retain their selected state until dispatch.
#[derive(Clone)]
pub struct FrontendReloadHandle {
  current: Arc<ArcSwap<FrontendGeneration>>,
  service: Arc<ServiceSettings>,
  access: Arc<AccessStore>,
  events: Arc<EventBus>,
}

impl FrontendReloadHandle {
  pub(super) fn new(
    initial: FrontendGeneration,
    service: ServiceSettings,
    access: Arc<AccessStore>,
    events: Arc<EventBus>,
  ) -> Self {
    Self {
      current: Arc::new(ArcSwap::from_pointee(initial)),
      service: Arc::new(service),
      access,
      events,
    }
  }

  /// Build and validate the complete replacement without publishing it to listeners.
  /// The coordinator supplies the dispatcher for requests admitted with this policy.
  pub fn prepare(
    &self,
    plan: &GatewayPlan,
    service: &ServiceSettings,
    dispatcher: Arc<dyn RequestDispatcher>,
  ) -> Result<PreparedFrontend, ReloadError> {
    self.prepare_with_connect_policy(plan, service, dispatcher, None)
  }

  pub(crate) fn prepare_with_connect_policy(
    &self,
    plan: &GatewayPlan,
    service: &ServiceSettings,
    dispatcher: Arc<dyn RequestDispatcher>,
    connect_policy: Option<&ConnectPolicy>,
  ) -> Result<PreparedFrontend, ReloadError> {
    crate::v2::ensure_service_reload_compatible(&self.service, service)?;
    let previous = self.current.load();
    previous.validate_listeners(plan)?;
    let next = FrontendGeneration::build(
      plan,
      service,
      &self.access,
      &self.events,
      dispatcher,
      Some(&previous),
      connect_policy,
    )
    .map_err(|e| ReloadError::Invalid(e.to_string()))?;
    Ok(PreparedFrontend {
      current: self.current.clone(),
      next: Arc::new(next),
    })
  }
}

/// A replacement ready to publish after its workers have accepted the same configuration.
pub struct PreparedFrontend {
  current: Arc<ArcSwap<FrontendGeneration>>,
  next: Arc<FrontendGeneration>,
}

impl PreparedFrontend {
  /// Publish all listener policies and their dispatcher in one operation.
  pub fn publish(self) {
    self.current.store(self.next);
  }
}

#[derive(Clone)]
pub(super) struct LiveApiIngress {
  id: ListenerId,
  current: Arc<ArcSwap<FrontendGeneration>>,
}

impl LiveApiIngress {
  pub(super) fn new(id: ListenerId, handle: &FrontendReloadHandle) -> Self {
    Self {
      id,
      current: handle.current.clone(),
    }
  }
  pub(super) fn current(&self) -> Arc<ApiIngress> {
    self.current.load().api[&self.id].clone()
  }
}

#[derive(Clone)]
pub(crate) struct LiveProxyIngress {
  id: ListenerId,
  current: Arc<ArcSwap<FrontendGeneration>>,
}

impl LiveProxyIngress {
  pub(super) fn new(id: ListenerId, handle: &FrontendReloadHandle) -> Self {
    Self {
      id,
      current: handle.current.clone(),
    }
  }
  pub(crate) fn current(&self) -> Arc<ProxyIngress> {
    self.current.load().proxy[&self.id].clone()
  }
}
