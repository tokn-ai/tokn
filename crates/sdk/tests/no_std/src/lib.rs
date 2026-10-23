//! Instantiate the configured SDK, shared pipeline, routing, conversion and
//! streaming with a borrowing local transport and no pointer atomics.
#![no_std]
extern crate alloc;

#[path = "../../scenarios/configured.rs"]
mod scenarios;
pub use scenarios::{
  buffered_round_trip, configured_client_relay, configured_provider_policies, configured_relay_policies,
  configured_status_policy, configured_transport_encoding, cross_format_round_trip,
  response_body_failure_does_not_retry, retry_fallback_affinity, retry_schedule_and_exhaustion,
  stream_errors_and_cancellation, streaming_round_trip, transport_failure_account_feedback,
};

#[cfg(test)]
mod tests {
  #[test]
  fn transport_encoding_capability() {
    super::configured_transport_encoding();
  }

  #[test]
  fn client_credential_relay_and_transport_replay() {
    super::configured_client_relay();
  }
  #[test]
  fn shared_status_classification() {
    super::configured_status_policy();
  }
  #[test]
  fn provider_defaults_and_wire_identity() {
    super::configured_provider_policies();
  }
  #[test]
  fn relay_payload_and_replay_policy() {
    super::configured_relay_policies();
  }
  #[test]
  fn all_native_formats() {
    super::buffered_round_trip();
  }
  #[test]
  fn local_streaming() {
    super::streaming_round_trip();
  }
  #[test]
  fn shared_cross_format_conversion() {
    super::cross_format_round_trip();
  }
  #[test]
  fn shared_retry_fallback_and_affinity() {
    super::retry_fallback_affinity();
  }
  #[test]
  fn shared_retry_schedule_and_exhaustion() {
    super::retry_schedule_and_exhaustion();
  }
  #[test]
  fn transport_failure_account_feedback() {
    super::transport_failure_account_feedback();
  }
  #[test]
  fn response_body_failure_does_not_retry() {
    super::response_body_failure_does_not_retry();
  }
  #[test]
  fn stream_errors_and_borrow_release() {
    super::stream_errors_and_cancellation();
  }
}

#[cfg(test)]
extern crate std;
#[cfg(test)]
#[path = "../../reentrant.rs"]
mod reentrant;
