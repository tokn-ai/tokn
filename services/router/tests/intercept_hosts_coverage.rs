//! Integration test that lives in the `router` crate so it can reach both
//! `crate::proxy::INTERCEPT_HOSTS` (router-private) and the moved-out
//! `tokn_accounts::registry`. The body is identical to the test that used to
//! live in `router/src/accounts/registry.rs` before the
//! `tokn-accounts` extraction.

#[test]
fn proxy_intercept_hosts_cover_all_descriptor_hosts() {
  use std::collections::HashSet;
  use tokn_accounts::registry::Registry;
  use tokn_core::provider::ID_LLAMA_CPP;

  let registry = Registry::builtin();
  // `INTERCEPT_HOSTS` is `pub(crate)` in router; re-export it via a tiny
  // helper to keep the integration test self-contained.
  let mut hosts: HashSet<&'static str> = tokn_router::proxy_intercept_hosts().iter().copied().collect();
  hosts.insert("api.github.com");
  let registry_hosts: HashSet<&'static str> = registry
    .iter()
    .filter(|descriptor| descriptor.id != ID_LLAMA_CPP)
    .flat_map(|descriptor| descriptor.hosts.iter().copied())
    .collect();
  assert!(
    registry_hosts.is_subset(&hosts),
    "INTERCEPT_HOSTS must contain all hosts registered by provider descriptors, missing: {:?}",
    registry_hosts.difference(&hosts)
  );
  for descriptor in registry.iter().filter(|descriptor| descriptor.id != ID_LLAMA_CPP) {
    for host in descriptor.hosts {
      assert!(
        hosts.contains(host),
        "INTERCEPT_HOSTS missing descriptor host {host} for {}",
        descriptor.id
      );
    }
  }
}
