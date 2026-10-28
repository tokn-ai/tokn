//! Instantiate the configured SDK, shared pipeline, routing, conversion and
//! streaming with a borrowing local transport and no pointer atomics.
#![no_std]
extern crate alloc;

#[path = "../../scenarios/configured.rs"]
mod scenarios;
pub use scenarios::{
  buffered_content_detection, buffered_response_limits, buffered_round_trip, buffered_usage_events,
  configured_client_relay, configured_provider_policies, configured_relay_policies, configured_status_policy,
  configured_transport_encoding, copilot_refresh_uses_injected_transport_and_reuses_credentials,
  cross_format_round_trip, event_timestamps_and_local_sink, explicit_profiles_do_not_imply_default,
  recoverable_refresh_failure_reselects_accounts, response_body_failure_does_not_retry, retry_fallback_affinity,
  retry_schedule_and_exhaustion, stream_errors_and_cancellation, streaming_cancellation_lifecycle,
  streaming_failure_lifecycle, streaming_round_trip, streaming_terminal_lifecycle, streaming_usage_events,
  transport_failure_account_feedback, typed_event_failures, typed_event_retry_attribution, typed_event_summaries,
};
pub use scenarios::{
  buffered_generation_round_trip, generation_cancellation_releases_exchange, generation_upstream_errors,
  generation_validation_and_controls,
};

#[cfg(test)]
mod tests {
  #[test]
  fn buffered_generation_round_trip() {
    super::buffered_generation_round_trip();
  }
  #[test]
  fn generation_validation_and_controls() {
    super::generation_validation_and_controls();
  }
  #[test]
  fn generation_upstream_errors() {
    super::generation_upstream_errors();
  }
  #[test]
  fn generation_cancellation_releases_exchange() {
    super::generation_cancellation_releases_exchange();
  }

  #[test]
  fn recoverable_refresh_failure_reselects_accounts() {
    super::recoverable_refresh_failure_reselects_accounts();
  }

  #[test]
  fn copilot_refresh_uses_injected_transport_and_reuses_credentials() {
    super::copilot_refresh_uses_injected_transport_and_reuses_credentials();
  }

  #[test]
  fn explicit_profiles_do_not_imply_default() {
    super::explicit_profiles_do_not_imply_default();
  }

  #[test]
  fn typed_event_summaries() {
    super::typed_event_summaries();
  }
  #[test]
  fn buffered_usage_events() {
    super::buffered_usage_events();
  }
  #[test]
  fn streaming_usage_events() {
    super::streaming_usage_events();
  }
  #[test]
  fn typed_event_retry_attribution() {
    super::typed_event_retry_attribution();
  }
  #[test]
  fn typed_event_failures() {
    super::typed_event_failures();
  }
  #[test]
  fn event_timestamps_and_local_sink() {
    super::event_timestamps_and_local_sink();
  }
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
  fn buffered_content_detection() {
    super::buffered_content_detection();
  }
  #[test]
  fn buffered_response_limits() {
    super::buffered_response_limits();
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
  #[test]
  fn streaming_terminal_lifecycle() {
    super::streaming_terminal_lifecycle();
  }
  #[test]
  fn streaming_failure_lifecycle() {
    super::streaming_failure_lifecycle();
  }
  #[test]
  fn streaming_cancellation_lifecycle() {
    super::streaming_cancellation_lifecycle();
  }
}

#[cfg(test)]
extern crate std;
#[cfg(test)]
#[path = "../../reentrant.rs"]
mod reentrant;
