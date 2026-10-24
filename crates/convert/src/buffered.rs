//! Normalize a complete managed response without owning its transport.
//!
//! Buffered callers accept JSON or SSE. Same-endpoint JSON keeps its original
//! bytes, empty non-SSE bodies stay empty, and SSE is accumulated into the shared IR
//! before being rendered directly to the requested endpoint. Opaque relays do
//! not use this module; their bytes belong to the upstream protocol.

use crate::{error::ConvertError, provider::Endpoint};
use serde_json::Value;
use thiserror::Error;
use tokn_core::types::Bytes;

#[derive(Debug)]
pub struct BufferedResponse {
  pub body_json: Option<Value>,
  pub body_bytes: Bytes,
  /// Whether SSE was accumulated into a JSON response. Adapters use this to
  /// replace the upstream content type and remove the original body length.
  pub accumulated_sse: bool,
}

/// Separate failure kinds let callers retain their existing error contracts.
#[derive(Debug, Error)]
pub enum BufferedError {
  #[error("{source}")]
  InvalidJson { source: serde_json::Error },
  #[error("{source}")]
  Conversion { source: ConvertError },
  #[error("{source}")]
  Serialization { source: serde_json::Error },
}

/// Recognize buffered SSE from its media type or its first non-whitespace
/// `event:`/`data:` field. The body check supports upstreams whose content type
/// was missing or overwritten in transit.
pub fn is_sse(content_type: Option<&str>, body: &[u8]) -> bool {
  let header_matches = content_type.is_some_and(|value| {
    value
      .split(';')
      .next()
      .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("text/event-stream"))
  });
  let first = body
    .iter()
    .position(|byte| !byte.is_ascii_whitespace())
    .unwrap_or(body.len());
  header_matches || body[first..].starts_with(b"event:") || body[first..].starts_with(b"data:")
}

/// Normalize already-read response bytes. The caller enforces the total body
/// limit; `max_event_bytes` bounds individual events when the body is SSE.
///
/// `observe` receives parsed upstream JSON before cross-endpoint conversion, or
/// the final rendered JSON after SSE accumulation. It runs before serialization
/// and does not run for an empty non-SSE body or a parsing/accumulation failure. This
/// lets platform adapters record usage at the same point as standard execution.
pub fn convert_response(
  from: Endpoint,
  to: Endpoint,
  body: Bytes,
  content_type: Option<&str>,
  max_event_bytes: usize,
  mut observe: impl FnMut(&Value),
) -> Result<BufferedResponse, BufferedError> {
  let accumulated_sse = is_sse(content_type, &body);
  let body_json = if accumulated_sse {
    let response = crate::sse::accumulate_slice(from, &body, max_event_bytes)
      .map_err(|source| BufferedError::Conversion { source })?;
    let rendered = crate::render_response(to, &response).map_err(|source| BufferedError::Conversion { source })?;
    observe(&rendered);
    rendered
  } else {
    if body.is_empty() {
      return Ok(BufferedResponse {
        body_json: None,
        body_bytes: body,
        accumulated_sse: false,
      });
    }
    let upstream = serde_json::from_slice(&body).map_err(|source| BufferedError::InvalidJson { source })?;
    observe(&upstream);
    if from == to {
      return Ok(BufferedResponse {
        body_json: Some(upstream),
        body_bytes: body,
        accumulated_sse: false,
      });
    }
    crate::convert_response(from, to, &upstream).map_err(|source| BufferedError::Conversion { source })?
  };
  let body_bytes = serde_json::to_vec(&body_json)
    .map(Bytes::from)
    .map_err(|source| BufferedError::Serialization { source })?;
  Ok(BufferedResponse {
    body_json: Some(body_json),
    body_bytes,
    accumulated_sse,
  })
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::vec::Vec;
  use serde_json::json;

  fn convert(body: Bytes, content_type: Option<&str>) -> Result<BufferedResponse, BufferedError> {
    convert_response(
      Endpoint::ChatCompletions,
      Endpoint::ChatCompletions,
      body,
      content_type,
      usize::MAX,
      |_| {},
    )
  }

  #[test]
  fn same_endpoint_json_keeps_exact_bytes_and_unknown_fields() {
    let original = Bytes::from_static(b"\n {\"extension\" : [1, 2], \"value\": 1.00, \"choices\": [] } \n");
    let output = convert(original.clone(), Some("application/json")).unwrap();
    assert_eq!(output.body_bytes, original);
    assert_eq!(output.body_json.unwrap()["extension"], json!([1, 2]));
    assert!(!output.accumulated_sse);
  }

  #[test]
  fn empty_body_is_preserved_without_observation() {
    let output = convert_response(
      Endpoint::ChatCompletions,
      Endpoint::Messages,
      Bytes::new(),
      Some("application/json"),
      1,
      |_| panic!("empty body has no JSON to observe"),
    )
    .unwrap();
    assert!(output.body_bytes.is_empty());
    assert!(output.body_json.is_none());
    assert!(!output.accumulated_sse);
    assert!(matches!(
      convert(Bytes::from_static(b" \n "), None),
      Err(BufferedError::InvalidJson { .. })
    ));
  }

  #[test]
  fn buffered_sse_detection_accepts_media_parameters_or_wire_fields() {
    for (content_type, body, expected) in [
      (Some(" Text/Event-Stream ; charset=utf-8"), b"".as_slice(), true),
      (Some("application/json"), b" \r\nevent: done\n".as_slice(), true),
      (None, b"\tdata: [DONE]\n\n".as_slice(), true),
      (Some("text/event-streaming"), b"{}".as_slice(), false),
      (None, b"{\"data:\":1}".as_slice(), false),
      (None, b"".as_slice(), false),
    ] {
      assert_eq!(is_sse(content_type, body), expected, "{content_type:?} {body:?}");
    }
  }

  #[test]
  fn json_observation_precedes_conversion_including_failed_conversion() {
    let body = json!({"usage":{"prompt_tokens":7},"choices":[]});
    let mut observed = Vec::new();
    let error = convert_response(
      Endpoint::ChatCompletions,
      Endpoint::Messages,
      Bytes::from(serde_json::to_vec(&body).unwrap()),
      Some("application/json"),
      usize::MAX,
      |value| observed.push(value.clone()),
    )
    .unwrap_err();
    assert!(matches!(
      error,
      BufferedError::Conversion {
        source: ConvertError::MissingField { field: "choices" }
      }
    ));
    assert_eq!(observed, [body]);
  }

  #[test]
  fn cross_endpoint_json_renders_requested_format() {
    let body = json!({
      "id":"chat-1","model":"test-model",
      "choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],
      "usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}
    });
    let output = convert_response(
      Endpoint::ChatCompletions,
      Endpoint::Messages,
      Bytes::from(serde_json::to_vec(&body).unwrap()),
      Some("application/json"),
      usize::MAX,
      |value| assert_eq!(value, &body),
    )
    .unwrap();
    let rendered = output.body_json.unwrap();
    assert_eq!(rendered["type"], "message");
    assert_eq!(rendered["content"][0]["text"], "hello");
    assert_eq!(rendered["usage"]["input_tokens"], 3);
    assert_eq!(serde_json::from_slice::<Value>(&output.body_bytes).unwrap(), rendered);
    assert!(!output.accumulated_sse);
  }

  #[test]
  fn responses_sse_renders_directly_without_losing_or_duplicating_content() {
    let frames = [
      json!({"type":"response.created","response":{"id":"resp-1","model":"test-model"}}),
      json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup"}}),
      json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"query\":\"rust\"}"}),
      json!({"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":"hello"}),
      json!({"type":"response.reasoning_summary_text.delta","output_index":2,"summary_index":0,"delta":"thought"}),
      json!({"type":"response.completed","response":{"id":"resp-1","model":"test-model","usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":1}}}}),
    ];
    let mut bytes = Vec::new();
    for frame in frames {
      bytes.extend_from_slice(b"data: ");
      bytes.extend(serde_json::to_vec(&frame).unwrap());
      bytes.extend_from_slice(b"\n\n");
    }
    let mut observed = Vec::new();
    let output = convert_response(
      Endpoint::Responses,
      Endpoint::ChatCompletions,
      Bytes::from(bytes),
      None,
      usize::MAX,
      |value| observed.push(value.clone()),
    )
    .unwrap();
    let rendered = output.body_json.unwrap();
    assert_eq!(rendered["id"], "resp-1");
    assert_eq!(rendered["model"], "test-model");
    assert_eq!(rendered["choices"][0]["message"]["content"], "hello");
    assert_eq!(rendered["choices"][0]["message"]["reasoning_content"], "thought");
    assert_eq!(rendered["choices"][0]["finish_reason"], "tool_calls");
    let tool = &rendered["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(tool["id"], "call-1");
    assert_eq!(tool["function"]["name"], "lookup");
    assert_eq!(tool["function"]["arguments"], "{\"query\":\"rust\"}");
    assert_eq!(rendered["usage"]["prompt_tokens"], 7);
    assert_eq!(rendered["usage"]["completion_tokens"], 3);
    assert_eq!(rendered["usage"]["total_tokens"], 10);
    assert_eq!(rendered["usage"]["prompt_tokens_details"]["cached_tokens"], 2);
    assert_eq!(rendered["usage"]["completion_tokens_details"]["reasoning_tokens"], 1);
    assert_eq!(observed.as_slice(), core::slice::from_ref(&rendered));
    assert_eq!(serde_json::from_slice::<Value>(&output.body_bytes).unwrap(), rendered);
    assert!(output.accumulated_sse);
  }

  #[test]
  fn malformed_or_incomplete_sse_and_event_limits_fail_before_observation() {
    for (body, content_type, limit) in [
      (b"not json".as_slice(), None, usize::MAX),
      (b"data: not json\n\n".as_slice(), None, usize::MAX),
      (b"data: {\"choices\":[]}\n\n".as_slice(), None, usize::MAX),
      (b"data: [DONE]\n\n".as_slice(), None, 2),
      (b"{}".as_slice(), Some("text/event-stream"), usize::MAX),
      (b"".as_slice(), Some("text/event-stream"), usize::MAX),
    ] {
      let result = convert_response(
        Endpoint::ChatCompletions,
        Endpoint::Messages,
        Bytes::copy_from_slice(body),
        content_type,
        limit,
        |_| panic!("invalid body should not be observed"),
      );
      match result.unwrap_err() {
        BufferedError::InvalidJson { .. } => assert_eq!(body, b"not json"),
        BufferedError::Conversion {
          source: ConvertError::Sse { .. },
        } => {}
        other => panic!("unexpected failure: {other}"),
      }
    }
  }
}
