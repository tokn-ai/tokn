use super::*;
use serde_json::json;

fn origin(endpoint: Endpoint) -> ProviderOrigin {
  ProviderOrigin::new(
    "configured",
    "driver",
    "account",
    "https://backend.example/v1",
    endpoint,
    "model",
  )
}

fn messages_request(messages: Value) -> Value {
  json!({
    "model": "model", "system": [{"type": "text", "text": "stable system", "cache_control": {"type": "ephemeral"}}],
    "thinking": {"type": "enabled", "budget_tokens": 1024}, "max_tokens": 4096,
    "tools": [{"name": "lookup", "input_schema": {"type": "object"}}], "messages": messages,
  })
}

fn messages_response(id: &str, text: &str) -> Value {
  json!({
    "type": "message", "role": "assistant", "stop_reason": "tool_use",
    "content": [
      {"type": "thinking", "thinking": "private thought", "signature": "native-signature", "opaque": {"keep": true}},
      {"type": "text", "text": text},
      {"type": "tool_use", "id": id, "name": "lookup", "input": {"q": text}},
      {"type": "redacted_thinking", "data": "native-redacted-payload"},
    ],
  })
}

#[test]
fn messages_replay_retains_signed_blocks_and_two_tool_rounds_without_duplicating_history() {
  let origin = origin(Endpoint::Messages);
  let request = messages_request(json!([{"role": "user", "content": "first question"}]));
  let first = messages_response("call-1", "first lookup");
  let state = capture(origin.clone(), &request, &first).unwrap();
  let state: ProviderState = serde_json::from_value(serde_json::to_value(state).unwrap()).unwrap();
  let suffix = messages_request(json!([{"role": "user", "content": [
    {"type": "tool_result", "tool_use_id": "call-1", "content": [{"type": "text", "text": "first result"}]},
    {"type": "text", "text": "new RAG context"},
  ]}]));
  let continued = replay(&state, &origin, &suffix).unwrap();
  assert_eq!(continued["messages"].as_array().unwrap().len(), 3);
  assert_eq!(continued["messages"][0], request["messages"][0]);
  assert_eq!(continued["messages"][1]["content"], first["content"]);
  assert_eq!(continued["messages"][2], suffix["messages"][0]);
  assert_eq!(continued["system"], request["system"]);

  let second = messages_response("call-2", "second lookup");
  let second_state = capture(origin.clone(), &continued, &second).unwrap();
  let last_suffix = messages_request(json!([{"role": "user", "content": [
    {"type": "tool_result", "tool_use_id": "call-2", "content": "second result"},
  ]}]));
  let last = replay(&second_state, &origin, &last_suffix).unwrap();
  assert_eq!(last["messages"].as_array().unwrap().len(), 5);
  assert_eq!(last["messages"][1]["content"], first["content"]);
  assert_eq!(last["messages"][3]["content"], second["content"]);
  assert_eq!(last["messages"][4], last_suffix["messages"][0]);
}

#[test]
fn responses_replay_retains_encrypted_items_and_native_item_order() {
  let origin = origin(Endpoint::Responses);
  let request = json!({"model": "model", "instructions": "stable", "store": false,
    "include": ["reasoning.encrypted_content"], "input": [{"role": "user", "content": "question"}]});
  let response = json!({"status": "completed", "output": [
    {"type": "reasoning", "id": "reasoning-1", "summary": [{"type": "summary_text", "text": "summary"}],
      "encrypted_content": "opaque-ciphertext", "provider_extension": {"keep": 3}},
    {"type": "message", "id": "message-1", "role": "assistant", "status": "completed",
      "content": [{"type": "output_text", "text": "checking", "annotations": [{"type": "citation", "extra": true}]}]},
    {"type": "function_call", "id": "item-1", "call_id": "call-1", "status": "completed", "name": "lookup", "arguments": "{\"q\":1}"},
  ]});
  let state = capture(origin.clone(), &request, &response).unwrap();
  let suffix = json!({"model": "model", "instructions": "stable", "store": false,
  "include": ["reasoning.encrypted_content"], "input": [
    {"type": "function_call_output", "call_id": "call-1", "output": "result"},
    {"role": "user", "content": "fresh context"},
  ]});
  let continued = replay(&state, &origin, &suffix).unwrap();
  assert_eq!(continued["input"].as_array().unwrap().len(), 6);
  assert_eq!(
    &continued["input"].as_array().unwrap()[1..4],
    response["output"].as_array().unwrap()
  );
  assert_eq!(continued["input"][4], suffix["input"][0]);
  assert_eq!(continued["input"][5], suffix["input"][1]);
  assert!(continued.get("previous_response_id").is_none());
}

#[test]
fn responses_custom_calls_keep_native_input_and_restore_native_result_type() {
  let origin = origin(Endpoint::Responses);
  let request = json!({"model": "model", "input": [{"role": "user", "content": "edit"}]});
  let response = json!({"status": "completed", "output": [
    {"type": "custom_tool_call", "id": "item-1", "call_id": "custom-1", "name": "apply_patch", "input": "*** Begin Patch\n*** End Patch", "status": "completed"},
  ]});
  let state = capture(origin.clone(), &request, &response).unwrap();
  // The portable SDK represents every tool result with a generic Tool message.
  let suffix = json!({"model": "model", "input": [
    {"type": "function_call_output", "call_id": "custom-1", "output": "done"},
  ]});
  let replayed = replay(&state, &origin, &suffix).unwrap();
  assert_eq!(replayed["input"][1], response["output"][0]);
  assert_eq!(
    replayed["input"][2],
    json!({"type": "custom_tool_call_output", "call_id": "custom-1", "output": "done"})
  );
  assert!(capture(
    origin,
    &replayed,
    &json!({"status": "completed", "output": [
      {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "finished"}]},
    ]})
  )
  .is_ok());
}

#[test]
fn chat_replay_retains_native_reasoning_details_and_exact_argument_strings() {
  let origin = origin(Endpoint::ChatCompletions);
  let request = json!({"model": "model", "messages": [{"role": "user", "content": "question"}], "temperature": 0.2});
  let response = json!({"choices": [{"finish_reason": "tool_calls", "message": {
    "role": "assistant", "content": null, "reasoning_content": "private",
    "reasoning_details": [{"type": "reasoning.encrypted", "data": "opaque-data", "signature": "native-signature"}],
    "tool_calls": [{"type": "function", "id": "call-1", "function": {"name": "lookup", "arguments": "{ \"q\" : 1 }"}}],
  }}]});
  let state = capture(origin.clone(), &request, &response).unwrap();
  let suffix = json!({"model": "model", "temperature": 0.2, "messages": [{"role": "tool", "tool_call_id": "call-1", "content": "result"}]});
  let continued = replay(&state, &origin, &suffix).unwrap();
  assert_eq!(continued["messages"][1], response["choices"][0]["message"]);
  assert_eq!(continued["messages"][2], suffix["messages"][0]);
}

#[test]
fn chat_instructions_are_bound_and_replayed_once() {
  let origin = origin(Endpoint::ChatCompletions);
  let request = json!({"model": "model", "messages": [
    {"role": "system", "content": "stable system"},
    {"role": "developer", "content": "stable developer", "provider_extension": true},
    {"role": "user", "content": "question"},
  ]});
  let response = json!({"choices": [{"finish_reason": "stop", "message": {"role": "assistant", "content": "answer"}}]});
  let state = capture(origin.clone(), &request, &response).unwrap();
  let mut suffix = request.clone();
  suffix["messages"][2]["content"] = json!("next question");
  let continued = replay(&state, &origin, &suffix).unwrap();
  assert_eq!(continued["messages"].as_array().unwrap().len(), 5);
  assert_eq!(continued["messages"][0], request["messages"][0]);
  assert_eq!(continued["messages"][1], request["messages"][1]);
  assert_eq!(continued["messages"][4], suffix["messages"][2]);
  suffix["messages"][0]["content"] = json!("changed system");
  assert!(matches!(
    replay(&state, &origin, &suffix),
    Err(ContinuationError::Incompatible(_))
  ));
}

#[test]
fn parallel_messages_tool_results_may_span_adjacent_user_messages() {
  let origin = origin(Endpoint::Messages);
  let request = messages_request(json!([{"role": "user", "content": "question"}]));
  let response = json!({"role": "assistant", "stop_reason": "tool_use", "content": [
    {"type": "tool_use", "id": "a", "name": "lookup", "input": {}},
    {"type": "tool_use", "id": "b", "name": "lookup", "input": {}},
  ]});
  let state = capture(origin.clone(), &request, &response).unwrap();
  let suffix = messages_request(json!([
    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "a", "content": "first"}]},
    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "b", "content": "second"}]},
  ]));
  assert!(replay(&state, &origin, &suffix).is_ok());
}

#[test]
fn route_and_settings_changes_fail_closed() {
  let origin = origin(Endpoint::Messages);
  let request = messages_request(json!([{"role": "user", "content": "question"}]));
  let response =
    json!({"role": "assistant", "stop_reason": "end_turn", "content": [{"type": "text", "text": "answer"}]});
  let state = capture(origin.clone(), &request, &response).unwrap();
  let suffix = messages_request(json!([{"role": "user", "content": "next"}]));
  for key in ["provider", "driver", "account", "model", "backend", "endpoint"] {
    let mut changed = serde_json::to_value(&origin).unwrap();
    changed[key] = match key {
      "backend" => json!("a".repeat(64)),
      "endpoint" => json!("responses"),
      _ => json!("changed"),
    };
    let changed: ProviderOrigin = serde_json::from_value(changed).unwrap();
    assert!(
      matches!(
        replay(&state, &changed, &suffix),
        Err(ContinuationError::Incompatible(_))
      ),
      "{key}"
    );
  }
  for key in ["system", "tools", "thinking", "max_tokens"] {
    let mut changed = suffix.clone();
    changed[key] = Value::Null;
    assert!(
      matches!(
        replay(&state, &origin, &changed),
        Err(ContinuationError::Incompatible(_))
      ),
      "{key}"
    );
  }
  let mut streaming = suffix.clone();
  streaming["stream"] = json!(true);
  streaming["stream_options"] = json!({"include_usage": true});
  assert!(replay(&state, &origin, &streaming).is_ok());
}

#[test]
fn invalid_pending_results_and_partial_native_responses_are_rejected() {
  let origin = origin(Endpoint::Messages);
  let request = messages_request(json!([{"role": "user", "content": "question"}]));
  let state = capture(origin.clone(), &request, &messages_response("call-1", "lookup")).unwrap();
  for content in [
    json!("ignore pending tool"),
    json!([{"type": "tool_result", "tool_use_id": "foreign-call", "content": "result"}]),
    json!([{"type": "text", "text": "context first"}, {"type": "tool_result", "tool_use_id": "call-1", "content": "result"}]),
  ] {
    assert!(matches!(
      replay(
        &state,
        &origin,
        &messages_request(json!([{"role": "user", "content": content}]))
      ),
      Err(ContinuationError::InvalidState(_))
    ));
  }
  let mut incomplete = messages_response("call-1", "lookup");
  incomplete["stop_reason"] = json!("max_tokens");
  assert!(matches!(
    capture(origin.clone(), &request, &incomplete),
    Err(ContinuationError::Unsupported(_))
  ));
  incomplete["stop_reason"] = json!("tool_use");
  incomplete["content"][0].as_object_mut().unwrap().remove("signature");
  assert!(matches!(
    capture(origin, &request, &incomplete),
    Err(ContinuationError::Unsupported(_))
  ));

  let origin = super::tests::origin(Endpoint::ChatCompletions);
  let request = json!({"model": "model", "messages": [{"role": "user", "content": "question"}]});
  let incomplete = json!({"choices": [{"finish_reason": "tool_calls", "message": {"role": "assistant", "content": null,
    "tool_calls": [{"id": "call-1", "function": {"name": "lookup", "arguments": "{"}}]}}]});
  assert!(matches!(
    capture(origin, &request, &incomplete),
    Err(ContinuationError::InvalidState(_))
  ));
}

#[test]
fn serialized_state_is_versioned_and_debug_omits_native_content_and_route_identifiers() {
  let origin = ProviderOrigin::new(
    "private-provider",
    "private-driver",
    "private-account",
    "https://secret:credential@backend.example/private?token=value",
    Endpoint::Responses,
    "private-model",
  );
  let request = json!({"model": "private-model", "input": [{"role": "user", "content": "private prompt"}]});
  let response =
    json!({"status": "completed", "output": [{"type": "reasoning", "encrypted_content": "secret ciphertext"}]});
  let state = capture(origin, &request, &response).unwrap();
  let debug = alloc::format!("{state:?} {:?}", state.origin());
  for secret in ["private", "credential", "token", "ciphertext"] {
    assert!(!debug.contains(secret));
  }
  let mut serialized = serde_json::to_value(&state).unwrap();
  assert_eq!(serialized["version"], STATE_VERSION);
  let serialized_text = serialized.to_string();
  assert!(!serialized_text.contains("backend.example"));
  assert!(!serialized_text.contains("credential"));
  serialized["version"] = json!(STATE_VERSION + 1);
  let future: ProviderState = serde_json::from_value(serialized).unwrap();
  assert!(matches!(
    replay(&future, state.origin(), &request),
    Err(ContinuationError::InvalidState(_))
  ));
}

#[test]
fn server_handles_and_unreplayable_reasoning_are_explicitly_unsupported() {
  let origin = origin(Endpoint::Responses);
  let response = json!({"status": "completed", "output": [{"type": "message", "role": "assistant", "content": []}]});
  for key in ["previous_response_id", "conversation"] {
    let mut request = json!({"model": "model", "input": []});
    request[key] = json!("server-handle");
    assert!(matches!(
      capture(origin.clone(), &request, &response),
      Err(ContinuationError::Unsupported(_))
    ));
  }
  let request = json!({"model": "model", "input": []});
  let reasoning = json!({"status": "completed", "output": [{"type": "reasoning", "id": "server-only", "summary": []}]});
  assert!(matches!(
    capture(origin, &request, &reasoning),
    Err(ContinuationError::Unsupported(_))
  ));
}

#[test]
fn responses_output_requires_assistant_messages_and_valid_output_parts() {
  let request = json!({"model": "model", "input": [{"role": "user", "content": "question"}]});
  for role in [
    json!("user"),
    json!("system"),
    json!("developer"),
    json!("tool"),
    Value::Null,
  ] {
    let response = json!({"status": "completed", "output": [
      {"type": "message", "role": role, "content": [{"type": "output_text", "text": "answer"}]},
    ]});
    assert!(matches!(
      capture(origin(Endpoint::Responses), &request, &response),
      Err(ContinuationError::InvalidState(_))
    ));
  }
  for content in [
    json!(42),
    json!({}),
    Value::Null,
    json!(["not a content object"]),
    json!([{"type": "output_text", "text": 42}]),
    json!([{"type": "refusal", "refusal": false}]),
    json!([{"type": "output_audio", "data": "opaque audio"}]),
  ] {
    let response = json!({"status": "completed", "output": [
      {"type": "message", "role": "assistant", "content": content},
    ]});
    assert!(capture(origin(Endpoint::Responses), &request, &response).is_err());
  }
}

#[test]
fn chat_output_rejects_invalid_content_and_keeps_refusals_and_null_tool_messages() {
  let request = json!({"model": "model", "messages": [{"role": "user", "content": "question"}]});
  for content in [
    json!(42),
    json!(false),
    json!({"text": "object content"}),
    json!(["not a content object"]),
    json!([{"type": "text", "text": false}]),
    json!([{"type": "refusal", "refusal": 42}]),
    json!([{"type": "image_url", "image_url": {"url": "opaque image"}}]),
  ] {
    let response =
      json!({"choices": [{"finish_reason": "stop", "message": {"role": "assistant", "content": content}}]});
    assert!(capture(origin(Endpoint::ChatCompletions), &request, &response).is_err());
  }
  for (key, value) in [
    ("refusal", json!(42)),
    ("reasoning_content", json!({"text": "not a string"})),
    ("tool_calls", json!({"not": "an array"})),
    ("audio", json!({"id": "unhandled audio"})),
  ] {
    let mut message = json!({"role": "assistant", "content": null});
    message[key] = value;
    let response = json!({"choices": [{"finish_reason": "stop", "message": message}]});
    assert!(capture(origin(Endpoint::ChatCompletions), &request, &response).is_err());
  }

  for message in [
    json!({"role": "assistant", "content": "answer"}),
    json!({"role": "assistant", "content": [{"type": "text", "text": "answer", "opaque": true}]}),
    json!({"role": "assistant", "content": null, "refusal": "declined"}),
    json!({"role": "assistant", "content": [{"type": "refusal", "refusal": "declined"}]}),
    json!({"role": "assistant", "content": null, "tool_calls": [
      {"type": "function", "id": "call-1", "function": {"name": "lookup", "arguments": "{}"}},
    ]}),
  ] {
    let with_tool = message.get("tool_calls").is_some();
    let response =
      json!({"choices": [{"finish_reason": if with_tool { "tool_calls" } else { "stop" }, "message": message}]});
    let route = origin(Endpoint::ChatCompletions);
    let state = capture(route.clone(), &request, &response).unwrap();
    let suffix = if with_tool {
      json!({"model": "model", "messages": [{"role": "tool", "tool_call_id": "call-1", "content": "result"}]})
    } else {
      json!({"model": "model", "messages": [{"role": "user", "content": "next"}]})
    };
    assert_eq!(replay(&state, &route, &suffix).unwrap()["messages"][1], message);
  }
}

#[test]
fn responses_refusals_remain_native_and_cannot_silently_restart_as_empty_text() {
  let route = origin(Endpoint::Responses);
  let request = json!({"model": "model", "input": [{"role": "user", "content": "question"}]});
  let refusal = json!({"type": "message", "role": "assistant", "content": [
    {"type": "refusal", "refusal": "declined", "opaque": {"retain": true}},
  ]});
  let response = json!({"status": "completed", "output": [refusal]});
  let state = capture(route.clone(), &request, &response).unwrap();
  let suffix = json!({"model": "model", "input": [{"role": "user", "content": "next"}]});
  assert_eq!(replay(&state, &route, &suffix).unwrap()["input"][1], refusal);
  assert!(matches!(
    state.validate_restart(),
    Err(ContinuationError::Unsupported(_))
  ));
}

#[test]
fn automatic_restart_requires_portable_visible_output_but_can_drop_opaque_reasoning() {
  let responses_request = json!({"model": "model", "input": [{"role": "user", "content": "question"}]});
  let custom = json!({"status": "completed", "output": [
    {"type": "custom_tool_call", "call_id": "custom-1", "name": "edit", "input": "raw text"},
  ]});
  let state = capture(origin(Endpoint::Responses), &responses_request, &custom).unwrap();
  assert!(matches!(
    state.validate_restart(),
    Err(ContinuationError::Unsupported(_))
  ));

  let chat_request = json!({"model": "model", "messages": [{"role": "user", "content": "question"}]});
  for message in [
    json!({"role": "assistant", "content": null, "refusal": "declined"}),
    json!({"role": "assistant", "content": [{"type": "refusal", "refusal": "declined"}]}),
  ] {
    let reply = json!({"choices": [{"finish_reason": "stop", "message": message}]});
    let state = capture(origin(Endpoint::ChatCompletions), &chat_request, &reply).unwrap();
    assert!(matches!(
      state.validate_restart(),
      Err(ContinuationError::Unsupported(_))
    ));
  }

  for (endpoint, request, reply) in [
    (
      Endpoint::ChatCompletions,
      chat_request,
      json!({"choices": [{"finish_reason": "stop", "message": {
        "role": "assistant", "content": "answer", "reasoning_content": "private",
        "reasoning_details": [{"signature": "opaque"}],
      }}]}),
    ),
    (
      Endpoint::Responses,
      responses_request,
      json!({"status": "completed", "output": [
        {"type": "reasoning", "encrypted_content": "opaque", "summary": []},
        {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "answer"}]},
      ]}),
    ),
    (
      Endpoint::Messages,
      messages_request(json!([{"role": "user", "content": "question"}])),
      messages_response("call-1", "answer"),
    ),
  ] {
    let state = capture(origin(endpoint), &request, &reply).unwrap();
    state.validate_restart().unwrap();
  }
}

#[test]
fn corrupted_native_content_cannot_be_replayed_or_hidden_by_restart() {
  let route = origin(Endpoint::ChatCompletions);
  let request = json!({"model": "model", "messages": [{"role": "user", "content": "question"}]});
  let reply = json!({"choices": [{"finish_reason": "stop", "message": {"role": "assistant", "content": "answer"}}]});
  let state = capture(route.clone(), &request, &reply).unwrap();
  let mut saved = serde_json::to_value(&state).unwrap();
  saved["history"][1]["content"] = json!(42);
  let corrupted: ProviderState = serde_json::from_value(saved).unwrap();
  assert!(matches!(
    replay(&corrupted, &route, &request),
    Err(ContinuationError::InvalidState(_))
  ));
  assert!(matches!(
    corrupted.validate_restart(),
    Err(ContinuationError::InvalidState(_))
  ));
  for (key, value) in [
    ("version", json!(STATE_VERSION + 1)),
    ("context_digest", json!("invalid")),
  ] {
    let mut saved = serde_json::to_value(&state).unwrap();
    saved[key] = value;
    let corrupted: ProviderState = serde_json::from_value(saved).unwrap();
    assert!(matches!(
      corrupted.validate_restart(),
      Err(ContinuationError::InvalidState(_))
    ));
  }
}
