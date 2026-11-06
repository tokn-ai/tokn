extern crate alloc;
#[path = "scenarios/configured.rs"]
mod scenarios;

#[test]
fn native_continuation_binds_upstream_account_without_binding_credentials() {
  scenarios::native_continuation_binds_upstream_account_without_binding_credentials();
}

#[test]
fn conversation_failed_generation_retries_exact_snapshot() {
  scenarios::conversation_failed_generation_retries_exact_snapshot();
}

#[test]
fn conversation_tool_steer_context_and_restore() {
  scenarios::conversation_tool_steer_context_and_restore();
}

#[test]
fn native_continuation_prefers_compatible_fallback() {
  scenarios::native_continuation_prefers_compatible_fallback();
}

#[test]
fn native_continuation_rechecks_route_after_retry() {
  scenarios::native_continuation_rechecks_route_after_retry();
}

#[test]
fn native_continuation_rejects_changed_history_and_settings() {
  scenarios::native_continuation_rejects_changed_history_and_settings();
}

#[test]
fn native_continuation_requires_explicit_restart() {
  scenarios::native_continuation_requires_explicit_restart();
}

#[test]
fn buffered_generation_round_trip() {
  scenarios::buffered_generation_round_trip();
}
#[test]
fn generation_validation_and_controls() {
  scenarios::generation_validation_and_controls();
}
#[test]
fn generation_upstream_errors() {
  scenarios::generation_upstream_errors();
}
#[test]
fn generation_cancellation_releases_exchange() {
  scenarios::generation_cancellation_releases_exchange();
}

#[test]
fn explicit_profiles_do_not_imply_default() {
  scenarios::explicit_profiles_do_not_imply_default();
}

#[test]
fn copilot_refresh_uses_injected_transport_and_reuses_credentials() {
  scenarios::copilot_refresh_uses_injected_transport_and_reuses_credentials();
}

#[test]
fn recoverable_refresh_failure_reselects_accounts() {
  scenarios::recoverable_refresh_failure_reselects_accounts();
}

#[test]
fn typed_event_summaries() {
  scenarios::typed_event_summaries();
}
#[test]
fn buffered_usage_events() {
  scenarios::buffered_usage_events();
}
#[test]
fn streaming_usage_events() {
  scenarios::streaming_usage_events();
}
#[test]
fn typed_event_retry_attribution() {
  scenarios::typed_event_retry_attribution();
}
#[test]
fn typed_event_failures() {
  scenarios::typed_event_failures();
}
#[test]
fn event_timestamps_and_local_sink() {
  scenarios::event_timestamps_and_local_sink();
}

#[test]
fn all_native_formats() {
  scenarios::buffered_round_trip();
}
#[test]
fn buffered_content_detection() {
  scenarios::buffered_content_detection();
}
#[test]
fn buffered_response_limits() {
  scenarios::buffered_response_limits();
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
fn shared_retry_schedule_and_exhaustion() {
  scenarios::retry_schedule_and_exhaustion();
}
#[test]
fn transport_failure_account_feedback() {
  scenarios::transport_failure_account_feedback();
}
#[test]
fn response_body_failure_does_not_retry() {
  scenarios::response_body_failure_does_not_retry();
}
#[test]
fn stream_errors_and_borrow_release() {
  scenarios::stream_errors_and_cancellation();
}
#[test]
fn streaming_terminal_lifecycle() {
  scenarios::streaming_terminal_lifecycle();
}
#[test]
fn streaming_failure_lifecycle() {
  scenarios::streaming_failure_lifecycle();
}
#[test]
fn streaming_cancellation_lifecycle() {
  scenarios::streaming_cancellation_lifecycle();
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

#[test]
fn multimedia_native_inputs() {
  scenarios::multimedia_native_inputs();
}

#[test]
fn multimedia_output_continuation() {
  scenarios::multimedia_output_continuation();
}

#[test]
fn multimedia_tool_context_and_snapshot() {
  scenarios::multimedia_tool_context_and_snapshot();
}

#[test]
fn multimedia_unsupported_fails_before_transport() {
  scenarios::multimedia_unsupported_fails_before_transport();
}

/// Each JSONL line is one native image block, exported locally from history.
/// No private fixtures or filesystem paths are embedded in this test.
#[test]
#[ignore = "requires TOKN_TEST_IMAGE_PARTS_JSONL pointing to a local image corpus"]
fn multimedia_history_images() {
  use std::io::BufRead;
  let path = std::env::var_os("TOKN_TEST_IMAGE_PARTS_JSONL").expect("set TOKN_TEST_IMAGE_PARTS_JSONL");
  let file = std::fs::File::open(path).unwrap_or_else(|_| panic!("cannot read local image corpus"));
  let mut images = 0_usize;
  let mut requests = 0;
  for line in std::io::BufReader::new(file).lines() {
    let line = line.unwrap_or_else(|_| panic!("cannot read image corpus line"));
    let part: serde_json::Value = serde_json::from_str(&line).unwrap_or_else(|_| panic!("invalid image corpus JSON"));
    let endpoint = match part["type"].as_str() {
      Some("input_image") => tokn_sdk::Endpoint::Responses,
      Some("image_url") => tokn_sdk::Endpoint::ChatCompletions,
      Some("image") => tokn_sdk::Endpoint::Messages,
      _ => panic!("unsupported image corpus block"),
    };
    let content =
      tokn_convert::media::content_from_part(endpoint, &part).unwrap_or_else(|_| panic!("cannot decode corpus image"));
    let Some(tokn_sdk::Content::Image(image)) = content else {
      panic!("corpus image requires materialization");
    };
    requests += scenarios::multimedia_history_image(image);
    images += 1;
    if images.is_multiple_of(64) {
      println!("history image SDK replay progress: {images} images; {requests} offline requests");
    }
  }
  assert!(images > 0, "image corpus was empty");
  println!("history image SDK replay: {images} images; {requests} offline requests");
}
