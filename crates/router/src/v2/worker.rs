//! Execution adapter used by the Unix-socket worker transport.

use super::*;

impl LiveRuntime {
  #[cfg(unix)]
  pub fn worker_info(&self, version: impl Into<String>) -> crate::ipc::WorkerInfo {
    let generation = self.current.load();
    let listeners = generation
      .llm_api
      .values()
      .map(|state| crate::ipc::WorkerListener {
        listener_id: state.listener_id.to_string(),
        kind: "api".into(),
        client_auth: crate::ipc::auth_name(state.client_auth()),
      })
      .chain(
        generation
          .forward_proxy
          .values()
          .map(|state| crate::ipc::WorkerListener {
            listener_id: state.listener_id.to_string(),
            kind: "proxy".into(),
            client_auth: crate::ipc::auth_name(state.listener.client_auth()),
          }),
      )
      .collect();
    let api_admission = generation
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
    let generation = self.current.load_full();
    let listener_id = ListenerId::new(context.listener_id)?;
    let connection = InboundConnectionInfo {
      local_addr: context.local_addr,
      peer_addr: context.peer_addr,
    };
    match &context.origin {
      crate::dispatch::RequestOrigin::Api => {
        let state = generation
          .llm_api
          .get(&listener_id)
          .ok_or_else(|| anyhow::anyhow!("worker has no API listener '{listener_id}'"))?
          .clone();
        Ok(mounts::dispatch(Extension(state), Extension(context.access), connection, request).await)
      }
      crate::dispatch::RequestOrigin::Proxy { scheme, .. } => {
        let state = generation
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
