//! Effective CONNECT decisions exchanged for one prepared worker generation.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokn_config::v2::{compile_connect_rules, RawConnectAction, RawConnectRule};
use tokn_policy::{ConnectAction, ForwardProxyListenerPlan, GatewayPlan, ListenerId, ListenerPlan};

const VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConnectPolicy {
  version: u32,
  listeners: Vec<ListenerDefault>,
  rules: Vec<RawConnectRule>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListenerDefault {
  id: String,
  default_connect: RawConnectAction,
}

impl ConnectPolicy {
  #[cfg(any(unix, test))]
  pub(crate) fn from_listeners<'a>(
    listeners: impl IntoIterator<Item = (&'a ListenerId, &'a ForwardProxyListenerPlan)>,
  ) -> Self {
    let mut defaults = Vec::new();
    let mut rules = Vec::new();
    for (id, listener) in listeners {
      defaults.push(ListenerDefault {
        id: id.to_string(),
        default_connect: listener.default_connect_action().into(),
      });
      for rule in listener.connect_rules() {
        rules.push(RawConnectRule {
          id: rule.id().to_string(),
          listener: id.to_string(),
          action: rule.action().into(),
          hosts: rule.matcher().hosts().iter().map(ToString::to_string).collect(),
          ports: rule.matcher().ports().to_vec(),
        });
      }
    }
    Self {
      version: VERSION,
      listeners: defaults,
      rules,
    }
  }

  pub(super) fn apply(&self, plan: &GatewayPlan) -> Result<BTreeMap<ListenerId, ForwardProxyListenerPlan>> {
    ensure!(
      self.version == VERSION,
      "unsupported worker CONNECT policy version {}",
      self.version
    );
    let mut rules = compile_connect_rules(&self.rules, plan.listeners())?;
    let mut replacements = BTreeMap::new();
    for default in &self.listeners {
      let Some((id, ListenerPlan::ForwardProxy(listener))) = plan.listeners().get_key_value(default.id.as_str()) else {
        anyhow::bail!("worker CONNECT policy references an unknown forward-proxy listener");
      };
      ensure!(
        !replacements.contains_key(id),
        "duplicate worker CONNECT policy for listener '{id}'"
      );
      let action = ConnectAction::from(default.default_connect);
      let rules = rules.remove(id).unwrap_or_default();
      let intercept =
        action == ConnectAction::Intercept || rules.iter().any(|rule| rule.action() == ConnectAction::Intercept);
      ensure!(
        !intercept || listener.tls().is_some(),
        "worker CONNECT policy requires a CA on listener '{id}'"
      );
      replacements.insert(
        id.clone(),
        listener.clone().with_connect_policy(rules.into_boxed_slice(), action),
      );
    }
    let proxy_count = plan
      .listeners()
      .values()
      .filter(|listener| matches!(listener, ListenerPlan::ForwardProxy(_)))
      .count();
    ensure!(
      replacements.len() == proxy_count,
      "worker CONNECT policy does not include every forward-proxy listener"
    );
    Ok(replacements)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::path::Path;
  use tokn_policy::IngressAuthority;

  const LISTENER: &str = r#"
schema_version = 2
[listeners.proxy]
kind = "forward_proxy"
bind = "127.0.0.1:8080"
client_auth = "none"
default_http_action = { kind = "reject" }
default_connect = "reject"
ca_dir = "test-ca"
request_body_max_bytes = 512
"#;

  const RULES: &str = r#"
[[connect_rules]]
id = "except"
listener = "proxy"
hosts = ["special.example"]
ports = [8443]
action = "tunnel"
[[connect_rules]]
id = "inspect"
listener = "proxy"
hosts = ["*.example", "::1"]
ports = [443, 8443]
action = "intercept"
"#;

  fn plan(source: &str) -> GatewayPlan {
    tokn_config::v2::parse_config(source, Path::new("config.toml"))
      .unwrap()
      .into_parts()
      .0
  }

  fn capture(plan: &GatewayPlan) -> ConnectPolicy {
    let (id, ListenerPlan::ForwardProxy(proxy)) = plan.listeners().first_key_value().unwrap() else {
      panic!("expected test proxy");
    };
    ConnectPolicy::from_listeners([(id, proxy)])
  }

  #[test]
  fn wire_policy_preserves_rule_order_and_only_replaces_connect_decisions() {
    let worker = plan(&format!("{LISTENER}{RULES}"));
    let wire = serde_json::to_vec(&capture(&worker)).unwrap();
    let mut frontend_config = LISTENER
      .replace("512", "1024")
      .replace("test-ca", "frontend-ca")
      .replace("default_connect = \"reject\"", "default_connect = \"tunnel\"");
    // The older projection used this ID for HTTP routing. It need not match IDs used by the new worker.
    frontend_config.push_str(
      r#"
[[bindings]]
id = "inspect"
listener = "proxy"
hosts = ["prior.example"]
action = { kind = "reject" }
"#,
    );
    let frontend = plan(&frontend_config);
    let policy: ConnectPolicy = serde_json::from_slice(&wire).unwrap();
    let replacement = &policy.apply(&frontend).unwrap()["proxy"];
    let ListenerPlan::ForwardProxy(before) = &frontend.listeners()["proxy"] else {
      unreachable!()
    };
    assert_eq!(replacement.request_body_max_bytes(), 1024);
    assert_eq!(replacement.tls(), before.tls());
    assert_eq!(replacement.http_bindings(), before.http_bindings());
    for (authority, action) in [
      ("special.example:8443", ConnectAction::Tunnel),
      ("special.example:443", ConnectAction::Intercept),
      ("other.example:8443", ConnectAction::Intercept),
      ("other.example:80", ConnectAction::Reject),
      ("example:443", ConnectAction::Reject),
      ("[::1]:443", ConnectAction::Intercept),
    ] {
      let ingress = IngressAuthority::from_connect(authority).unwrap();
      assert_eq!(crate::proxy::ingress::connect_action_for(replacement, &ingress), action);
    }
  }

  #[test]
  fn policy_rejects_missing_or_extra_listeners_invalid_rules_and_unavailable_tls() {
    let frontend = plan(LISTENER);
    let policy = capture(&plan(&format!("{LISTENER}{RULES}")));
    let no_tls = plan(&LISTENER.replace("ca_dir = \"test-ca\"", ""));
    assert!(policy.apply(&no_tls).is_err());
    let mut invalid = policy.clone();
    invalid.version += 1;
    assert!(invalid.apply(&frontend).is_err());
    let mut invalid = policy.clone();
    invalid.listeners.clear();
    assert!(invalid.apply(&frontend).is_err());
    let mut invalid = policy.clone();
    invalid.listeners.push(invalid.listeners[0].clone());
    assert!(invalid.apply(&frontend).is_err());
    let mut invalid = policy.clone();
    invalid.listeners[0].id = "unknown".into();
    assert!(invalid.apply(&frontend).is_err());
    let mut invalid = policy.clone();
    invalid.rules[0].ports = vec![0];
    assert!(invalid.apply(&frontend).is_err());
    let mut invalid = policy;
    invalid.rules.reverse();
    assert!(invalid.apply(&frontend).is_err(), "shadowed rule must be rejected");
  }
}
