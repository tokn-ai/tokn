//! Execute conversation checkpoints through the public SDK and inspect what
//! the transport actually receives, including the isolated alloc-only client.

use super::*;
use alloc::string::ToString;
use tokn_sdk::{
  ContinuationPolicy, ContinuationStatus, Conversation, ConversationError, Error, GenerateRequest,
  GenerateRequestBuilder, InputDisposition, Message, RetrievedContext, Tool,
};

const CHAT_TOOL: &[u8] = br#"{"id":"first","choices":[{"message":{"role":"assistant","content":"searching","reasoning_content":"native reasoning","reasoning_details":[{"signature":"opaque-chat"}],"tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"query\":\"rust\"}"}}]},"finish_reason":"tool_calls"}]}"#;
const RESPONSES_TOOL: &[u8] = br#"{"id":"first","status":"completed","output":[{"type":"reasoning","id":"rs-1","summary":[{"type":"summary_text","text":"native reasoning"}],"encrypted_content":"opaque-responses"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"searching"}]},{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"rust\"}"}]}"#;
const MESSAGES_TOOL: &[u8] = br#"{"id":"first","role":"assistant","content":[{"type":"thinking","thinking":"native reasoning","signature":"opaque-messages"},{"type":"text","text":"searching"},{"type":"tool_use","id":"call-1","name":"lookup","input":{"query":"rust"}}],"stop_reason":"tool_use"}"#;
const RESPONSES_TOOL_SSE: &[u8] = concat!(
  "event: response.completed\n",
  "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"first\",\"status\":\"completed\",\"output\":[{\"type\":\"reasoning\",\"id\":\"rs-1\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"native reasoning\"}],\"encrypted_content\":\"opaque-responses\"},{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"searching\"}]},{\"type\":\"function_call\",\"id\":\"fc-1\",\"call_id\":\"call-1\",\"name\":\"lookup\",\"arguments\":\"{\\\"query\\\":\\\"rust\\\"}\"}]}}\n\n",
).as_bytes();
const CHAT_DONE: &[u8] =
  br#"{"id":"last","choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}"#;
const RESPONSES_DONE: &[u8] = br#"{"id":"last","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}"#;
const MESSAGES_DONE: &[u8] =
  br#"{"id":"last","role":"assistant","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn"}"#;
const RESPONSES_OPAQUE_DONE: &[u8] = br#"{"id":"first","status":"completed","output":[{"type":"reasoning","id":"rs-1","summary":[],"encrypted_content":"opaque-responses"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}]}"#;
const TEMPORARY_FAILURE: Reply = Reply {
  status: 503,
  content_type: "application/json",
  bytes: br#"{"error":{"message":"temporary failure"}}"#,
};

fn template(model: &str) -> GenerateRequestBuilder {
  GenerateRequest::builder(model)
    .system("Use verified documents")
    .max_output_tokens(128)
    .tool(Tool::function("lookup", json!({"type":"object"})))
}

fn metadata_client<'a>(
  sent: &'a RefCell<Vec<Request>>,
  driver: &str,
  model: &str,
  endpoint: Endpoint,
  replies: impl IntoIterator<Item = Reply>,
) -> TestClient<'a> {
  let models = [(
    model.into(),
    tokn_sdk::ModelCapabilities {
      endpoints: Some(vec![endpoint]),
      ..Default::default()
    },
  )]
  .into_iter()
  .collect();
  Client::with_model_metadata(
    plan(driver),
    vec![account_config("primary", "test-key", "active")],
    [(tokn_sdk::policy::ProviderId::new("upstream").unwrap(), models)]
      .into_iter()
      .collect(),
    Adapter::new(MemoryTransport::new(sent, replies), TestRuntime::default()),
  )
  .unwrap()
}

fn native_body(request: &Request) -> Value {
  let body: Value = serde_json::from_slice(&request.body).unwrap();
  for key in [
    "provider_state",
    "continuation_policy",
    "capture_provider_state",
    "generation_binding",
  ] {
    assert!(
      body.get(key).is_none(),
      "local continuation metadata must stay outside HTTP payloads"
    );
  }
  if body.get("input").is_some() {
    assert!(body["include"]
      .as_array()
      .unwrap()
      .contains(&json!("reasoning.encrypted_content")));
  } else {
    assert!(
      body.get("include").is_none(),
      "Responses continuation controls must not leak to another endpoint"
    );
  }
  body
}

pub fn conversation_tool_steer_context_and_restore() {
  for (driver, model, endpoint, first, last, opaque) in [
    (
      "llama-cpp",
      "gpt-5",
      Endpoint::ChatCompletions,
      Reply::json(CHAT_TOOL),
      CHAT_DONE,
      "opaque-chat",
    ),
    (
      "openai",
      "gpt-5",
      Endpoint::Responses,
      Reply::json(RESPONSES_TOOL),
      RESPONSES_DONE,
      "opaque-responses",
    ),
    (
      "github-copilot",
      "claude-sonnet",
      Endpoint::Messages,
      Reply::json(MESSAGES_TOOL),
      MESSAGES_DONE,
      "opaque-messages",
    ),
    (
      "openai",
      "gpt-5",
      Endpoint::Responses,
      Reply::sse(RESPONSES_TOOL_SSE),
      RESPONSES_DONE,
      "opaque-responses",
    ),
  ] {
    let sent = RefCell::new(Vec::new());
    let client = metadata_client(&sent, driver, model, endpoint, [first, Reply::json(last)]);
    let mut conversation = Conversation::new("conversation-fixture").unwrap();
    assert_eq!(
      conversation.submit_user("Find the answer").unwrap(),
      InputDisposition::Applied
    );
    let prepared = conversation
      .prepare(
        template(model),
        vec![RetrievedContext::new("guide", "first-document-content").with_source_version("v1")],
      )
      .unwrap();
    assert_eq!(
      conversation.submit_user("Keep the answer brief").unwrap(),
      InputDisposition::Queued
    );
    assert_eq!(conversation.retry().unwrap(), prepared);
    let reply = run(client.send(prepared.request())).unwrap();
    assert_eq!(reply.text, "searching");
    assert_eq!(reply.continuation_status, ContinuationStatus::Started);
    assert_eq!(reply.tool_calls.len(), 1);
    conversation.commit(&prepared, reply).unwrap();
    assert!(matches!(
      conversation.prepare(template(model), vec![]),
      Err(ConversationError::ToolResultsPending { .. })
    ));

    let stored = serde_json::to_vec(&conversation).unwrap();
    let mut conversation: Conversation = serde_json::from_slice(&stored).unwrap();
    assert_eq!(conversation.queued_inputs(), &[Message::user("Keep the answer brief")]);
    conversation
      .record_tool_result("call-1", "tool-result-content")
      .unwrap();
    let prepared = conversation
      .prepare(
        template(model),
        vec![RetrievedContext::new("guide", "second-document-content").with_source_version("v2")],
      )
      .unwrap();
    let stored = serde_json::to_vec(&conversation).unwrap();
    let mut conversation: Conversation = serde_json::from_slice(&stored).unwrap();
    assert_eq!(
      conversation.retry().unwrap(),
      prepared,
      "recovery keeps the exact retrieved context"
    );
    let reply = run(client.send(conversation.retry().unwrap().request())).unwrap();
    assert_eq!(reply.text, "done");
    assert_eq!(reply.continuation_status, ContinuationStatus::Continued);
    conversation.commit(&prepared, reply).unwrap();
    assert!(conversation.pending().is_none());
    assert!(conversation.queued_inputs().is_empty());
    assert!(conversation
      .history()
      .iter()
      .all(|message| !message.content.contains("document-content")));

    let sent = sent.borrow();
    assert_eq!(sent.len(), 2);
    let body = native_body(&sent[1]);
    let encoded = body.to_string();
    for text in [
      "Find the answer",
      "Keep the answer brief",
      "first-document-content",
      "second-document-content",
      "tool-result-content",
      opaque,
    ] {
      assert_eq!(
        encoded.matches(text).count(),
        1,
        "native history must contain each input/output exactly once: {driver}, {text}"
      );
    }
    assert_eq!(encoded.matches("Use verified documents").count(), 1);
    let first: Value = serde_json::from_slice(first.bytes).unwrap_or(Value::Null);
    match endpoint {
      Endpoint::ChatCompletions => assert!(body["messages"]
        .as_array()
        .unwrap()
        .contains(&first["choices"][0]["message"])),
      Endpoint::Messages => {
        let assistant = body["messages"]
          .as_array()
          .unwrap()
          .iter()
          .find(|message| message["role"] == "assistant")
          .unwrap();
        assert_eq!(assistant["content"], first["content"]);
      }
      Endpoint::Responses => {
        let input = body["input"].as_array().unwrap();
        assert!(input.iter().any(|item| item["encrypted_content"] == "opaque-responses"));
        assert!(input
          .iter()
          .any(|item| item["type"] == "function_call_output" && item["call_id"] == "call-1"));
      }
    }
  }
}

fn initial_request() -> GenerateRequest {
  GenerateRequest::builder("gpt-5")
    .system("Be concise")
    .user("initial input")
    .extra("include", json!(["message.output_text.logprobs"]))
    .capture_provider_state(true)
    .build()
    .unwrap()
}

fn next_request(initial: &GenerateRequest, response: &tokn_sdk::GenerateResponse) -> GenerateRequest {
  initial
    .clone()
    .into_builder()
    .message(response.assistant_message())
    .user("next input")
    .provider_state(response.provider_state.clone().unwrap())
    .build()
    .unwrap()
}

fn one_provider_plan(provider: &str, driver: &str, backend: &str) -> tokn_sdk::policy::ExecutionRules {
  let raw = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":provider},"wire_identity":"none"},
    "providers":{provider:{"driver":driver,"base_url":backend}}
  }))
  .unwrap();
  tokn_sdk::config::v2::compile_execution(&raw).unwrap()
}

pub fn native_continuation_requires_explicit_restart() {
  let first_sent = RefCell::new(Vec::new());
  let client = make_client(&first_sent, [Reply::json(RESPONSES_OPAQUE_DONE)], "openai");
  let initial = initial_request();
  let reply = run(client.send(&initial)).unwrap();
  let next = next_request(&initial, &reply);

  for (provider, account, backend) in [
    ("upstream", "other-account", "https://example.com/v1"),
    ("other-provider", "primary", "https://example.com/v1"),
    ("upstream", "primary", "https://other.example/v1"),
  ] {
    let sent = RefCell::new(Vec::new());
    let account: AccountConfig =
      serde_json::from_value(json!({"id":account,"provider":provider,"api_key":"new-key"})).unwrap();
    let client = Client::new(
      one_provider_plan(provider, "openai", backend),
      vec![account],
      Adapter::new(
        MemoryTransport::new(&sent, [Reply::json(RESPONSES_DONE)]),
        TestRuntime::default(),
      ),
    )
    .unwrap();
    assert!(matches!(
      run(client.send(&next)),
      Err(Error::Continuation {
        source: tokn_sdk::session::ContinuationError::Incompatible(_)
      })
    ));
    assert!(sent.borrow().is_empty(), "strict routing must fail before HTTP I/O");
    let restart = next
      .clone()
      .into_builder()
      .continuation_policy(ContinuationPolicy::AllowRestart)
      .build()
      .unwrap();
    let reply = run(client.send(&restart)).unwrap();
    assert_eq!(reply.continuation_status, ContinuationStatus::Restarted);
    let sent = sent.borrow();
    assert_eq!(sent.len(), 1);
    let body = native_body(&sent[0]);
    let encoded = body.to_string();
    assert!(
      !encoded.contains("opaque-responses"),
      "explicit restart uses portable history"
    );
    for text in ["initial input", "hello", "next input"] {
      assert_eq!(encoded.matches(text).count(), 1);
    }
  }
}

pub fn native_continuation_prefers_compatible_fallback() {
  let first_sent = RefCell::new(Vec::new());
  let first = Client::new(
    plan("openai"),
    vec![account_config("backup", "backup-key", "fallback")],
    Adapter::new(
      MemoryTransport::new(&first_sent, [Reply::json(RESPONSES_OPAQUE_DONE)]),
      TestRuntime::default(),
    ),
  )
  .unwrap();
  let initial = initial_request();
  let reply = run(first.send(&initial)).unwrap();
  let next = next_request(&initial, &reply)
    .into_builder()
    .continuation_policy(ContinuationPolicy::AllowRestart)
    .build()
    .unwrap();
  let sent = RefCell::new(Vec::new());
  let client = Client::new(
    plan("openai"),
    vec![
      account_config("primary", "primary-key", "active"),
      account_config("backup", "backup-key", "fallback"),
    ],
    Adapter::new(
      MemoryTransport::new(&sent, [Reply::json(RESPONSES_DONE)]),
      TestRuntime::default(),
    ),
  )
  .unwrap();
  let reply = run(client.send(&next)).unwrap();
  assert_eq!(reply.continuation_status, ContinuationStatus::Continued);
  let sent = sent.borrow();
  assert_eq!(
    sent[0].headers.get("authorization").unwrap().as_str(),
    "Bearer backup-key"
  );
  assert!(native_body(&sent[0]).to_string().contains("opaque-responses"));
}

pub fn native_continuation_binds_upstream_account_without_binding_credentials() {
  let first_sent = RefCell::new(Vec::new());
  let mut account = account_config("primary", "before-key", "active");
  account.provider_account_id = Some("upstream-account-before".into());
  let first = Client::new(
    plan("openai"),
    vec![account.clone()],
    Adapter::new(
      MemoryTransport::new(&first_sent, [Reply::json(RESPONSES_OPAQUE_DONE)]),
      TestRuntime::default(),
    ),
  )
  .unwrap();
  let initial = initial_request();
  let reply = run(first.send(&initial)).unwrap();
  let next = next_request(&initial, &reply);
  let stored = serde_json::to_string(next.provider_state.as_ref().unwrap()).unwrap();
  assert!(!stored.contains("upstream-account-before"));
  assert!(!stored.contains("before-key"));

  for (upstream_account, expect_compatible) in [
    (Some("upstream-account-before"), true),
    (Some("upstream-account-after"), false),
    (None, false),
  ] {
    let sent = RefCell::new(Vec::new());
    let mut account = account.clone();
    account.api_key = Some("rotated-key".to_string().into());
    account.provider_account_id = upstream_account.map(Into::into);
    let client = Client::new(
      plan("openai"),
      vec![account],
      Adapter::new(
        MemoryTransport::new(&sent, [Reply::json(RESPONSES_DONE)]),
        TestRuntime::default(),
      ),
    )
    .unwrap();
    let result = run(client.send(&next));
    if expect_compatible {
      assert_eq!(result.unwrap().continuation_status, ContinuationStatus::Continued);
      assert!(native_body(&sent.borrow()[0]).to_string().contains("opaque-responses"));
    } else {
      assert!(matches!(
        result,
        Err(Error::Continuation {
          source: tokn_sdk::session::ContinuationError::Incompatible(_)
        })
      ));
      assert!(
        sent.borrow().is_empty(),
        "upstream identity mismatch must fail before I/O"
      );
      let restart = next
        .clone()
        .into_builder()
        .continuation_policy(ContinuationPolicy::AllowRestart)
        .build()
        .unwrap();
      assert_eq!(
        run(client.send(&restart)).unwrap().continuation_status,
        ContinuationStatus::Restarted
      );
      assert!(!native_body(&sent.borrow()[0]).to_string().contains("opaque-responses"));
    }
    let sent = sent.borrow();
    assert_eq!(sent.len(), 1);
    assert_eq!(
      sent[0].headers.get("authorization").unwrap().as_str(),
      "Bearer rotated-key"
    );
  }
}

pub fn native_continuation_rechecks_route_after_retry() {
  for allow_restart in [false, true] {
    let sent = RefCell::new(Vec::new());
    let client = Client::new(
      retry_plan(1, 2),
      vec![
        account_config("primary", "primary-key", "active"),
        account_config("backup", "backup-key", "fallback"),
      ],
      Adapter::new(
        MemoryTransport::new(
          &sent,
          [
            Reply::json(RESPONSES_OPAQUE_DONE),
            TEMPORARY_FAILURE,
            Reply::json(RESPONSES_DONE),
          ],
        ),
        TestRuntime::default(),
      ),
    )
    .unwrap();
    let initial = initial_request();
    let reply = run(client.send(&initial)).unwrap();
    let mut next = next_request(&initial, &reply);
    if allow_restart {
      next.continuation_policy = ContinuationPolicy::AllowRestart;
    }
    let result = run(client.send(&next));
    if allow_restart {
      assert_eq!(result.unwrap().continuation_status, ContinuationStatus::Restarted);
    } else {
      assert!(matches!(
        result,
        Err(Error::Continuation {
          source: tokn_sdk::session::ContinuationError::Incompatible(_)
        })
      ));
    }
    let sent = sent.borrow();
    assert_eq!(sent.len(), if allow_restart { 3 } else { 2 });
    assert_eq!(
      sent[1].headers.get("authorization").unwrap().as_str(),
      "Bearer primary-key"
    );
    assert!(native_body(&sent[1]).to_string().contains("opaque-responses"));
    if allow_restart {
      assert_eq!(
        sent[2].headers.get("authorization").unwrap().as_str(),
        "Bearer backup-key"
      );
      assert!(!native_body(&sent[2]).to_string().contains("opaque-responses"));
    }
  }
}

pub fn native_continuation_rejects_changed_history_and_settings() {
  let sent = RefCell::new(Vec::new());
  let client = make_client(
    &sent,
    [Reply::json(RESPONSES_OPAQUE_DONE), Reply::json(RESPONSES_DONE)],
    "openai",
  );
  let initial = initial_request();
  let reply = run(client.send(&initial)).unwrap();
  let next = next_request(&initial, &reply);
  let mut altered = next.clone();
  altered.messages[1].content = "changed input".into();
  assert!(matches!(
    run(client.send(&altered)),
    Err(Error::ProviderStateHistoryMismatch)
  ));
  assert_eq!(sent.borrow().len(), 1);
  let altered = next.into_builder().max_output_tokens(64).build().unwrap();
  assert!(matches!(
    run(client.send(&altered)),
    Err(Error::Continuation {
      source: tokn_sdk::session::ContinuationError::Incompatible(_)
    })
  ));
  assert_eq!(sent.borrow().len(), 1);
  let restarted = altered
    .into_builder()
    .continuation_policy(ContinuationPolicy::AllowRestart)
    .build()
    .unwrap();
  assert_eq!(
    run(client.send(&restarted)).unwrap().continuation_status,
    ContinuationStatus::Restarted
  );
  assert_eq!(sent.borrow().len(), 2);
}

pub fn conversation_failed_generation_retries_exact_snapshot() {
  let sent = RefCell::new(Vec::new());
  let client = Client::new(
    plan("openai"),
    vec![account_config("primary", "test-key", "active")],
    Adapter::new(
      MemoryTransport::new(
        &sent,
        [Reply::sse(RESPONSES_TOOL_SSE), Reply::json(RESPONSES_OPAQUE_DONE)],
      )
      .with_read_failure_after(17),
      TestRuntime::default(),
    ),
  )
  .unwrap();
  let mut conversation = Conversation::new("recoverable-conversation").unwrap();
  conversation.submit_user("initial input").unwrap();
  let prepared = conversation
    .prepare(
      template("gpt-5"),
      vec![RetrievedContext::new("guide", "immutable-retrieval-content").with_source_version("v1")],
    )
    .unwrap();
  assert_eq!(
    conversation.submit_user("steer after this request").unwrap(),
    InputDisposition::Queued
  );
  assert!(run(client.send(prepared.request())).is_err());
  assert!(
    conversation.provider_state().is_none(),
    "failed native output cannot produce a checkpoint"
  );
  assert_eq!(conversation.retry().unwrap(), prepared);

  let stored = serde_json::to_vec(&conversation).unwrap();
  let mut conversation: Conversation = serde_json::from_slice(&stored).unwrap();
  let retry = conversation.retry().unwrap();
  assert_eq!(retry, prepared);
  let response = run(client.send(retry.request())).unwrap();
  assert_eq!(response.continuation_status, ContinuationStatus::Started);
  conversation.commit(&retry, response).unwrap();
  assert!(conversation.provider_state().is_some());
  assert_eq!(
    conversation.queued_inputs(),
    &[Message::user("steer after this request")]
  );
  let next = conversation.prepare(template("gpt-5"), vec![]).unwrap();
  assert_eq!(
    next.request().messages.last(),
    Some(&Message::user("steer after this request"))
  );
  let sent = sent.borrow();
  assert_eq!(
    sent.len(),
    2,
    "response read failures require an explicit application retry"
  );
  assert_eq!(
    sent[0].body, sent[1].body,
    "recovery reuses the exact input and retrieval snapshot"
  );
  let body = native_body(&sent[1]).to_string();
  assert!(body.contains("immutable-retrieval-content"));
  assert!(!body.contains("steer after this request"));
}
