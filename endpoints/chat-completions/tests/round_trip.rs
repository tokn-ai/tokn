use serde_json::json;
use tokn_endpoint_chat_completions::{ChatChunk, ChatEvent, ChatRequest, ChatResponse};

#[test]
fn round_trip_request() {
  let body = json!({
    "model": "gpt-4o",
    "messages": [
      { "role": "system", "content": "be terse" },
      { "role": "user", "content": [{ "type": "text", "text": "hi" }] },
      {
        "role": "assistant",
        "content": null,
        "tool_calls": [{
          "id": "call_1",
          "type": "function",
          "function": { "name": "lookup", "arguments": "{\"q\":\"rust\"}" }
        }]
      },
      { "role": "tool", "tool_call_id": "call_1", "content": "ok" }
    ],
    "tools": [{ "type": "function", "function": { "name": "lookup" } }],
    "temperature": 0.2,
    "stream": true,
    "custom_field": "kept"
  });

  let req: ChatRequest = serde_json::from_value(body.clone()).expect("parse");
  let round = serde_json::to_value(&req).expect("serialize");
  assert_eq!(round.get("custom_field").and_then(|v| v.as_str()), Some("kept"));
  assert_eq!(round.get("model").and_then(|v| v.as_str()), Some("gpt-4o"));
  let messages = round.get("messages").and_then(|v| v.as_array()).expect("messages");
  assert_eq!(messages.len(), 4);
}

#[test]
fn round_trip_response() {
  let body = json!({
    "id": "chatcmpl_1",
    "object": "chat.completion",
    "model": "gpt-4o",
    "choices": [{
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "hi",
        "reasoning_content": "think",
        "tool_calls": [{
          "id": "call_1",
          "type": "function",
          "function": { "name": "lookup", "arguments": "{}" }
        }]
      },
      "finish_reason": "stop"
    }],
    "usage": { "prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3 }
  });

  let resp: ChatResponse = serde_json::from_value(body).expect("parse");
  assert_eq!(resp.choices.len(), 1);
  assert_eq!(resp.choices[0].message.tool_calls.len(), 1);
  assert_eq!(resp.choices[0].message.reasoning_content.as_deref(), Some("think"));
}

#[test]
fn round_trip_event() {
  let chunk = json!({
    "id": "chatcmpl_1",
    "object": "chat.completion.chunk",
    "model": "gpt-4o",
    "choices": [{
      "index": 0,
      "delta": { "content": "hi", "reasoning_content": "th" },
      "finish_reason": null
    }]
  });

  let parsed: ChatEvent = serde_json::from_value(chunk).expect("parse chunk");
  match parsed {
    ChatEvent::Chunk(c) => {
      let _: Box<ChatChunk> = c;
    }
    ChatEvent::Done => panic!("expected chunk"),
  }

  let done: ChatEvent = serde_json::from_value(json!("[DONE]")).expect("parse done");
  matches!(done, ChatEvent::Done);
}

#[test]
fn lenient_param_type_mismatch_falls_into_extras() {
  // `temperature` should be a number but the client sent a string; the
  // good `top_p` field should still parse normally, and the bad
  // `temperature` value should end up in `extras` instead of failing
  // the whole request.
  let body = json!({
    "model": "gpt-4o",
    "messages": [{ "role": "user", "content": "hi" }],
    "temperature": "hot",
    "top_p": 0.9,
    "parallel_tool_calls": "yes",
    "repetition_penalty": 1.2,
    "min_p": "bad"
  });

  let req: ChatRequest = serde_json::from_value(body).expect("lenient parse");
  assert!(req.params.temperature.is_none(), "bad temperature should not bind");
  assert_eq!(req.params.top_p, Some(0.9));
  assert!(
    req.params.parallel_tool_calls.is_none(),
    "bad parallel_tool_calls should not bind"
  );
  assert_eq!(req.extra_params.repetition_penalty, Some(1.2));
  assert!(req.extra_params.min_p.is_none(), "bad min_p should not bind");
  assert_eq!(req.extras.get("temperature"), Some(&json!("hot")));
  assert_eq!(req.extras.get("parallel_tool_calls"), Some(&json!("yes")));
  assert_eq!(req.extras.get("min_p"), Some(&json!("bad")));
}
