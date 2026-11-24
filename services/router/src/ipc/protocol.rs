//! Version-one admission metadata and readiness descriptors.

use crate::dispatch::{DispatchContext, RequestOrigin};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;
use tokn_access::{AccessContext, ProviderAccess};
use tokn_policy::{GatewayPlan, ListenerPlan};

pub const PROTOCOL_VERSION: u32 = 1;
pub(super) const CONTEXT_HEADER: &str = "x-tokn-ipc-context";
pub(super) const READY_PATH: &str = "/_tokn/ready";
pub(super) const MAX_CONTEXT_BYTES: usize = 16 * 1024;
pub(super) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerListener {
  pub listener_id: String,
  pub kind: String,
  pub client_auth: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerInfo {
  pub protocol_version: u32,
  pub version: String,
  pub listeners: Vec<WorkerListener>,
  pub api_admission: Vec<ApiAdmission>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApiAdmission {
  pub path: String,
  pub enabled: bool,
  pub client_credentials: bool,
}

impl WorkerInfo {
  pub fn new(plan: &GatewayPlan, version: impl Into<String>) -> Result<Self> {
    let mounts = crate::v2::mounts::ApiMounts::new(plan)?;
    let api_admission = if plan
      .listeners()
      .values()
      .any(|listener| matches!(listener, ListenerPlan::LlmApi(_)))
    {
      mounts
        .entries()
        .map(|(path, entry)| ApiAdmission {
          path: path.into(),
          enabled: entry.enabled,
          client_credentials: matches!(entry.operation, crate::v2::mounts::ApiOperation::Generate(_))
            && plan
              .profile(&entry.profile)
              .and_then(|profile| plan.route(profile.route()))
              .is_some_and(|route| route.credential_policy() == tokn_policy::CredentialPolicy::Client),
        })
        .collect()
    } else {
      Vec::new()
    };
    Ok(Self {
      protocol_version: PROTOCOL_VERSION,
      version: version.into(),
      listeners: listener_info(plan),
      api_admission,
    })
  }
}

pub(crate) fn auth_name(auth: tokn_policy::ClientAuthPlan) -> String {
  match auth {
    tokn_policy::ClientAuthPlan::None => "none",
    tokn_policy::ClientAuthPlan::LocalKeys => "local_keys",
  }
  .into()
}

fn listener_info(plan: &GatewayPlan) -> Vec<WorkerListener> {
  plan
    .listeners()
    .iter()
    .map(|(id, listener)| WorkerListener {
      listener_id: id.to_string(),
      client_auth: auth_name(listener.client_auth()),
      kind: match listener {
        ListenerPlan::LlmApi(_) => "api",
        ListenerPlan::ForwardProxy(_) => "proxy",
      }
      .into(),
    })
    .collect()
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireContext {
  pub(super) protocol_version: u32,
  pub(super) listener_id: String,
  pub(super) origin: RequestOrigin,
  pub(super) uri: String,
  pub(super) key_id: Option<String>,
  pub(super) key_name: Option<String>,
  // None means unrestricted; an empty set means no providers, never unrestricted.
  pub(super) allowed_providers: Option<BTreeSet<String>>,
  pub(super) local_addr: Option<SocketAddr>,
  pub(super) peer_addr: Option<SocketAddr>,
}

impl WireContext {
  pub(super) fn new(context: DispatchContext, uri: String) -> Self {
    Self {
      protocol_version: PROTOCOL_VERSION,
      listener_id: context.listener_id,
      origin: context.origin,
      uri,
      key_id: context.access.key_id,
      key_name: context.access.key_name,
      allowed_providers: context.access.providers.provider_ids().cloned(),
      local_addr: context.local_addr,
      peer_addr: context.peer_addr,
    }
  }

  pub(super) fn into_context(self) -> DispatchContext {
    DispatchContext {
      listener_id: self.listener_id,
      origin: self.origin,
      access: AccessContext {
        key_id: self.key_id,
        key_name: self.key_name,
        providers: self
          .allowed_providers
          .map(ProviderAccess::Only)
          .unwrap_or(ProviderAccess::All),
      },
      local_addr: self.local_addr,
      peer_addr: self.peer_addr,
    }
  }
}
