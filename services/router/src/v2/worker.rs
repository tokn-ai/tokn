//! Execution adapter used by the Unix-socket worker transport.

use super::*;

impl LiveRuntime {
  #[cfg(unix)]
  pub(crate) fn current_worker_generation(&self) -> Arc<RuntimeGeneration> {
    self.current.load_full()
  }

  #[cfg(unix)]
  pub(crate) fn prepare_worker_generation(
    &self,
    number: u64,
    states: RuntimeStates,
    accounts: usize,
  ) -> Result<Arc<RuntimeGeneration>, ReloadError> {
    let generation = RuntimeGeneration::new(number, accounts, states);
    ensure_reload_compatible(&self.current.load(), &generation)?;
    Ok(Arc::new(generation))
  }

  #[cfg(unix)]
  pub(crate) fn activate_worker_generation(&self, generation: &Arc<RuntimeGeneration>) {
    let _guard = self.replace_lock.lock();
    if self.generation() < generation.number {
      self.current.store(generation.clone());
      self.refresh_changed.send_replace(generation.number);
    }
  }

  #[cfg(unix)]
  pub fn worker_info(&self, version: impl Into<String>) -> crate::ipc::WorkerInfo {
    self.current.load().worker_info(version)
  }
}

impl RuntimeGeneration {
  #[cfg(unix)]
  pub(crate) fn worker_info(&self, version: impl Into<String>) -> crate::ipc::WorkerInfo {
    let listeners = self
      .llm_api
      .values()
      .map(|state| crate::ipc::WorkerListener {
        listener_id: state.listener_id.to_string(),
        kind: "api".into(),
        client_auth: crate::ipc::auth_name(state.client_auth()),
      })
      .chain(self.forward_proxy.values().map(|state| crate::ipc::WorkerListener {
        listener_id: state.listener_id.to_string(),
        kind: "proxy".into(),
        client_auth: crate::ipc::auth_name(state.listener.client_auth()),
      }))
      .collect();
    let api_admission = self
      .llm_api
      .values()
      .next()
      .map(|state| {
        state
          .mounts
          .entries()
          .map(|(path, entry)| crate::ipc::ApiAdmission {
            path: path.into(),
            enabled: entry.enabled,
            client_credentials: matches!(entry.operation, mounts::ApiOperation::Generate(_))
              && state
                .profiles
                .get(&entry.profile)
                .is_some_and(|profile| profile.credential_policy == CredentialPolicy::Client),
          })
          .collect()
      })
      .unwrap_or_default();
    crate::ipc::WorkerInfo {
      protocol_version: crate::ipc::PROTOCOL_VERSION,
      version: version.into(),
      listeners,
      api_admission,
    }
  }
}

#[async_trait::async_trait]
impl crate::dispatch::RequestDispatcher for LiveRuntime {
  async fn dispatch(&self, context: crate::dispatch::DispatchContext, request: Request) -> anyhow::Result<Response> {
    self.current.load_full().dispatch(context, request).await
  }
}

#[async_trait::async_trait]
impl crate::dispatch::RequestDispatcher for RuntimeGeneration {
  async fn dispatch(&self, context: crate::dispatch::DispatchContext, request: Request) -> anyhow::Result<Response> {
    let listener_id = ListenerId::new(context.listener_id)?;
    let connection = InboundConnectionInfo {
      local_addr: context.local_addr,
      peer_addr: context.peer_addr,
    };
    match &context.origin {
      crate::dispatch::RequestOrigin::Api => {
        let state = self
          .llm_api
          .get(&listener_id)
          .ok_or_else(|| anyhow::anyhow!("worker has no API listener '{listener_id}'"))?
          .clone();
        Ok(mounts::dispatch(Extension(state), Extension(context.access), connection, request).await)
      }
      crate::dispatch::RequestOrigin::Proxy { scheme, .. } => {
        let state = self
          .forward_proxy
          .get(&listener_id)
          .ok_or_else(|| anyhow::anyhow!("worker has no proxy listener '{listener_id}'"))?;
        let ingress = context
          .origin
          .ingress()?
          .expect("proxy origin has an ingress authority");
        Ok(
          state
            .dispatch_http(&ingress, scheme.as_str(), context.access, connection, request)
            .await,
        )
      }
    }
  }
}
