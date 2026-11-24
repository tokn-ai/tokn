use serde_json::json;
use tokn_endpoint_responses::{InputItem, ResponsesEvent, ResponsesRequest, ResponsesResponse};

#[test]
fn round_trip_request_with_mixed_input_items() {
  let body = json!({
    "model": "gpt-5",
    "instructions": "be terse",
    "input": [
      { "role": "user", "content": [{ "type": "input_text", "text": "hi" }] },
      { "type": "reasoning", "content": [{ "type": "reasoning_text", "text": "think" }], "summary": [] },
      { "type": "function_call", "call_id": "c1", "name": "lookup", "arguments": "{}" },
      { "type": "function_call_output", "call_id": "c1", "output": "result" }
    ],
    "stream": true,
    "store": false
  });

  let req: ResponsesRequest = serde_json::from_value(body).expect("parse");
  let items = match req.input {
    tokn_endpoint_responses::ResponsesInput::Items(items) => items,
    _ => panic!("expected items"),
  };
  assert_eq!(items.len(), 4);
  matches!(items[0], InputItem::Message(_));
  matches!(items[1], InputItem::Reasoning(_));
  matches!(items[2], InputItem::FunctionCall(_));
  matches!(items[3], InputItem::FunctionCallOutput(_));
}

#[test]
fn round_trip_response() {
  let body = json!({
    "id": "resp_1",
    "object": "response",
    "status": "completed",
    "model": "gpt-5",
    "output": [
      {
        "type": "message",
        "id": "msg_1",
        "status": "completed",
        "role": "assistant",
        "content": [{ "type": "output_text", "text": "hi", "annotations": [] }]
      },
      {
        "type": "function_call",
        "id": "fc_1",
        "call_id": "call_1",
        "name": "lookup",
        "arguments": "{}",
        "status": "completed"
      }
    ],
    "output_text": "hi",
    "usage": { "input_tokens": 1, "output_tokens": 2, "total_tokens": 3 }
  });

  let resp: ResponsesResponse = serde_json::from_value(body).expect("parse");
  assert_eq!(resp.output.len(), 2);
}

#[test]
fn parse_streaming_events() {
  let events = [
    json!({ "type": "response.created", "response": { "id": "resp_1" } }),
    json!({ "type": "response.output_text.delta", "delta": "h", "response_id": "resp_1", "output_index": 0, "content_index": 0 }),
    json!({ "type": "response.reasoning_text.delta", "delta": "th", "response_id": "resp_1" }),
    json!({ "type": "response.function_call_arguments.delta", "delta": "{", "response_id": "resp_1", "output_index": 1, "call_id": "c1" }),
    json!({ "type": "response.completed", "response": { "id": "resp_1", "status": "completed" } }),
  ];
  for e in events {
    let parsed: ResponsesEvent = serde_json::from_value(e.clone()).expect("parse event");
    assert_eq!(parsed.kind(), e.get("type").and_then(|v| v.as_str()).unwrap());
  }
}

#[test]
fn lenient_param_type_mismatch_falls_into_extras() {
  let body = json!({
    "model": "gpt-5",
    "input": "hi",
    "temperature": "hot",
    "top_p": 0.9,
    "parallel_tool_calls": "yes",
    "background": "yes",
    "truncation": "auto"
  });

  let req: ResponsesRequest = serde_json::from_value(body).expect("lenient parse");
  assert!(req.params.temperature.is_none(), "bad temperature should not bind");
  assert_eq!(req.params.top_p, Some(0.9));
  assert!(
    req.params.parallel_tool_calls.is_none(),
    "bad parallel_tool_calls should not bind"
  );
  assert!(req.params.background.is_none(), "bad background should not bind");
  assert_eq!(req.params.truncation.as_deref(), Some("auto"));
  assert_eq!(req.extras.get("temperature"), Some(&json!("hot")));
  assert_eq!(req.extras.get("parallel_tool_calls"), Some(&json!("yes")));
  assert_eq!(req.extras.get("background"), Some(&json!("yes")));
}
