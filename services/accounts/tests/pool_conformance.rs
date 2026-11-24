extern crate alloc;

#[path = "scenarios/mod.rs"]
mod scenarios;

#[test]
fn shared_pool_selection() {
  scenarios::pool_selection_transcript();
}

#[test]
fn shared_storage_operations() {
  scenarios::storage_round_trip();
}

#[test]
fn shared_configured_routing() {
  scenarios::configured_routing_transcript();
}
