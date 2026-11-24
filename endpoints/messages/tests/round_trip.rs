use serde_json::json;
use tokn_endpoint_messages::{MessagesEvent, MessagesRequest, MessagesResponse};

#[test]
fn round_trip_request() {
  let body = json!({
    "model": "claude-sonnet",
    "max_tokens": 1024,
    "system": "be terse",
    "messages": [
      { "role": "user", "content": "hi" },
      {
        "role": "assistant",
        "content": [
          { "type": "thinking", "thinking": "think" },
          { "type": "text", "text": "hello" },
          { "type": "tool_use", "id": "toolu_1", "name": "lookup", "input": { "q": "rust" } }
        ]
      },
      { "role": "user", "content": [
        { "type": "tool_result", "tool_use_id": "toolu_1", "content": "ok" }
      ] }
    ],
    "tools": [{ "name": "lookup" }],
    "stream": true
  });

  let req: MessagesRequest = serde_json::from_value(body).expect("parse");
  assert_eq!(req.model, "claude-sonnet");
  assert_eq!(req.max_tokens, 1024);
  assert_eq!(req.messages.len(), 3);
}

#[test]
fn round_trip_response() {
  let body = json!({
    "id": "msg_1",
    "type": "message",
    "role": "assistant",
    "model": "claude-sonnet",
    "content": [
      { "type": "thinking", "thinking": "think" },
      { "type": "text", "text": "hi" }
    ],
    "stop_reason": "end_turn",
    "usage": { "input_tokens": 1, "output_tokens": 2 }
  });

  let resp: MessagesResponse = serde_json::from_value(body).expect("parse");
  assert_eq!(resp.content.len(), 2);
  assert_eq!(resp.stop_reason.as_deref(), Some("end_turn"));
}

#[test]
fn parse_streaming_events() {
  let events = [
    json!({ "type": "message_start", "message": { "id": "msg_1", "type": "message", "role": "assistant", "model": "claude", "content": [] } }),
    json!({ "type": "content_block_start", "index": 0, "content_block": { "type": "text", "text": "" } }),
    json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "text_delta", "text": "hi" } }),
    json!({ "type": "content_block_delta", "index": 1, "delta": { "type": "input_json_delta", "partial_json": "{" } }),
    json!({ "type": "content_block_stop", "index": 0 }),
    json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" }, "usage": { "output_tokens": 5 } }),
    json!({ "type": "message_stop" }),
    json!({ "type": "ping" }),
  ];
  for e in events {
    let parsed: MessagesEvent = serde_json::from_value(e.clone()).expect("parse event");
    assert_eq!(parsed.kind(), e.get("type").and_then(|v| v.as_str()).unwrap());
  }
}

#[test]
fn lenient_param_type_mismatch_falls_into_extras() {
  let body = json!({
    "model": "claude-sonnet",
    "max_tokens": 1024,
    "messages": [{ "role": "user", "content": "hi" }],
    "temperature": "hot",
    "top_p": 0.9,
    "top_k": "many",
    "service_tier": 7
  });

  let req: MessagesRequest = serde_json::from_value(body).expect("lenient parse");
  assert!(req.params.temperature.is_none(), "bad temperature should not bind");
  assert_eq!(req.params.top_p, Some(0.9));
  assert!(req.params.top_k.is_none(), "bad top_k should not bind");
  assert!(req.params.service_tier.is_none(), "bad service_tier should not bind");
  assert_eq!(req.extras.get("temperature"), Some(&json!("hot")));
  assert_eq!(req.extras.get("top_k"), Some(&json!("many")));
  assert_eq!(req.extras.get("service_tier"), Some(&json!(7)));
}
