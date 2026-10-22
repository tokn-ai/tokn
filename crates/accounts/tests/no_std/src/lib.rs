//! Instantiate the same routing scenarios as the standard account tests using
//! only core/alloc, including a target without pointer atomics.

#![no_std]

extern crate alloc;

#[path = "../../scenarios/mod.rs"]
mod scenarios;

pub use scenarios::{configured_routing_transcript, pool_selection_transcript, storage_round_trip};

/// Instantiate the real configuration compiler alongside the local selection
/// kernel. No listener, filesystem path, or atomic ownership is required.
pub fn execution_compilation() {
  let raw: tokn_config::v2::RawConfig = serde_json::from_str(
    r#"{
    "schema_version": 2,
    "defaults": {"provider":{"kind":"fixed","provider":"embedded"}},
    "providers":{"embedded":{"driver":"openai","base_url":"https://api.openai.com/v1"}}
  }"#,
  )
  .unwrap();
  let plan = tokn_config::v2::compile_execution(&raw).unwrap();
  assert_eq!(plan.profiles().len(), 1);
  assert_eq!(plan.account_pools().len(), 1);
}

#[cfg(test)]
mod tests {
  #[test]
  fn shared_execution_compilation() {
    super::execution_compilation();
  }

  #[test]
  fn shared_configured_routing() {
    super::configured_routing_transcript();
  }

  #[test]
  fn shared_pool_selection() {
    super::pool_selection_transcript();
  }

  #[test]
  fn local_storage() {
    super::storage_round_trip();
  }
}
