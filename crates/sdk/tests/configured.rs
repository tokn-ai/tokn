extern crate alloc;
#[path = "scenarios/configured.rs"]
mod scenarios;

#[test]
fn all_native_formats() {
  scenarios::buffered_round_trip();
}
#[test]
fn local_streaming() {
  scenarios::streaming_round_trip();
}
#[test]
fn shared_cross_format_conversion() {
  scenarios::cross_format_round_trip();
}
#[test]
fn shared_retry_fallback_and_affinity() {
  scenarios::retry_fallback_affinity();
}
#[test]
fn stream_errors_and_borrow_release() {
  scenarios::stream_errors_and_cancellation();
}

#[test]
fn provider_defaults_and_wire_identity() {
  scenarios::configured_provider_policies();
}
#[test]
fn relay_payload_and_replay_policy() {
  scenarios::configured_relay_policies();
}

#[test]
fn client_credential_relay_and_transport_replay() {
  scenarios::configured_client_relay();
}
#[test]
fn shared_status_classification() {
  scenarios::configured_status_policy();
}

#[test]
fn transport_encoding_capability() {
  scenarios::configured_transport_encoding();
}
