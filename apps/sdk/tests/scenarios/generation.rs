//! Friendly generation exercises the same borrowed, non-Send transport in
//! the workspace and the isolated alloc-only consumer, including bare metal.

use super::*;
use alloc::boxed::Box;
use tokn_sdk::{Error, GenerateRequest, ReasoningEffort, Tool};

const CHAT_GENERATION: &[u8] = br#"{"id":"generation","model":"model","choices":[{"index":0,"message":{"role":"assistant","content":"hello","reasoning_content":"considered","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"query\":\"rust\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":3,"completion_tokens":5,"total_tokens":8}}"#;
const RESPONSES_GENERATION: &[u8] = br#"{"id":"generation","model":"model","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]},{"type":"reasoning","summary":[{"type":"summary_text","text":"considered"}]},{"type":"function_call","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"rust\"}"}],"usage":{"input_tokens":3,"output_tokens":5,"total_tokens":8}}"#;
const MESSAGES_GENERATION: &[u8] = br#"{"id":"generation","type":"message","model":"model","role":"assistant","content":[{"type":"text","text":"hello"},{"type":"thinking","thinking":"considered"},{"type":"tool_use","id":"call-1","name":"lookup","input":{"query":"rust"}}],"stop_reason":"tool_use","usage":{"input_tokens":3,"output_tokens":5}}"#;

pub fn buffered_generation_round_trip() {
  for (driver, model, endpoint, reply, path) in [
    (
      "llama-cpp",
      "gpt-5",
      Endpoint::ChatCompletions,
      CHAT_GENERATION,
      "/chat/completions",
    ),
    (
      "openai",
      "gpt-5",
      Endpoint::Responses,
      RESPONSES_GENERATION,
      "/responses",
    ),
    (
      "github-copilot",
      "claude-sonnet",
      Endpoint::Messages,
      MESSAGES_GENERATION,
      "/v1/messages",
    ),
  ] {
    let sent = RefCell::new(Vec::new());
    let models = [(
      model.into(),
      tokn_sdk::ModelCapabilities {
        endpoints: Some(vec![endpoint]),
        ..Default::default()
      },
    )]
    .into_iter()
    .collect();
    let client = Client::with_model_metadata(
      plan(driver),
      vec![account_config("primary", "test-key", "active")],
      [(tokn_sdk::policy::ProviderId::new("upstream").unwrap(), models)]
        .into_iter()
        .collect(),
      Adapter::new(
        MemoryTransport::new(&sent, [Reply::json(reply), Reply::json(reply)]),
        TestRuntime::default(),
      ),
    )
    .unwrap();
    let request = GenerateRequest::builder(model)
      .system("Be concise")
      .user("hello")
      .max_output_tokens(128)
      .tool(Tool::function(
        "lookup",
        json!({"type": "object", "properties": {"query": {"type": "string"}}}),
      ))
      .tool_choice("lookup")
      .build()
      .unwrap();
    let encoded = serde_json::to_vec(&request).unwrap();
    let decoded: GenerateRequest = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, request);
    let response = run(decoded.bind(&client).send()).unwrap();
    assert_eq!(response.http_status, 200);
    assert_eq!(response.id.as_deref(), Some("generation"));
    assert_eq!(response.text, "hello");
    assert_eq!(response.reasoning.as_deref(), Some("considered"));
    assert_eq!(response.finish_reason.as_deref(), Some("tool_calls"));
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].id.as_deref(), Some("call-1"));
    assert_eq!(response.tool_calls[0].name, "lookup");
    assert_eq!(response.tool_calls[0].arguments, json!({"query": "rust"}));
    let usage = response.usage.unwrap();
    assert_eq!(usage.input_tokens, Some(3));
    assert_eq!(usage.output_tokens, Some(5));
    assert_eq!(usage.total_tokens, Some(8));
    let repeated = run(client.send(&request)).unwrap();
    assert_eq!(response.raw, repeated.raw);
    let requests = sent.borrow();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body, requests[1].body);
    assert!(requests[0].url.ends_with(path));
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["model"], model);
    assert_ne!(body["stream"], true);
    if driver == "openai" {
      assert_eq!(body["max_output_tokens"], 128);
      assert_eq!(body["tool_choice"], json!({"type": "function", "name": "lookup"}));
    } else if endpoint == Endpoint::Messages {
      assert_eq!(body["max_tokens"], 128);
      assert_eq!(body["tool_choice"], json!({"type": "tool", "name": "lookup"}));
    } else {
      assert_eq!(body["max_tokens"], 128);
      assert_eq!(
        body["tool_choice"],
        json!({"type": "function", "function": {"name": "lookup"}})
      );
    }
  }
}

pub fn generation_validation_and_controls() {
  let sent = RefCell::new(Vec::new());
  let client = make_client(&sent, [Reply::json(RESPONSES_JSON)], "openai");
  for call in [
    client.generate("gpt-5").user("hello").top_p(2.0),
    client.generate("gpt-5").user("hello").max_output_tokens(0),
    client.generate("gpt-5").user("hello").top_k(40),
  ] {
    assert!(matches!(run(call.send()), Err(Error::InvalidGenerateRequest { .. })));
    assert!(
      sent.borrow().is_empty(),
      "invalid generation must not send an HTTP request"
    );
  }
  let unknown = run(client.generate("gpt-5").user("hello").profile("missing").send());
  assert!(matches!(unknown, Err(Error::UnknownProfile { profile }) if profile == "missing"));
  assert!(sent.borrow().is_empty());
  let call = GenerateRequest::builder("gpt-5")
    .user("hello")
    .reasoning_effort(ReasoningEffort::High)
    .max_output_tokens(256)
    .bind(&client);
  let cloned = call.clone();
  drop(call);
  let response = run(cloned.send()).expect("validation errors release the borrowed services");
  assert_eq!(response.text, "hello");
  let requests = sent.borrow();
  assert_eq!(requests.len(), 1);
  let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
  assert_eq!(body["reasoning"]["effort"], "high");
  assert_eq!(body["max_output_tokens"], 256);
}

pub fn generation_upstream_errors() {
  for status in [400, 502] {
    let sent = RefCell::new(Vec::new());
    let client = make_client(
      &sent,
      [Reply {
        status,
        content_type: "application/json",
        bytes: br#"{"error":{"message":"generation denied"}}"#,
      }],
      "openai",
    );
    let error = run(client.generate("gpt-5").user("hello").send()).unwrap_err();
    assert!(
      matches!(error, Error::GenerateResponseStatus { status: actual, body } if actual == status && body.contains("generation denied"))
    );
    assert_eq!(sent.borrow().len(), 1);
  }
  let sent = RefCell::new(Vec::new());
  let client = make_client(&sent, [Reply::json(b"not JSON"), Reply::json(RESPONSES_JSON)], "openai");
  let malformed = run(client.generate("gpt-5").user("hello").send());
  assert!(matches!(malformed, Err(Error::Request { .. })), "{malformed:?}");
  assert_eq!(
    run(client.generate("gpt-5").user("hello").send()).unwrap().text,
    "hello"
  );
}

pub fn generation_cancellation_releases_exchange() {
  let sent = RefCell::new(Vec::new());
  let runtime = TestRuntime::default();
  let trace = Rc::new(RefCell::new(TransportTrace::default()));
  let client = Client::new(
    plan("openai"),
    vec![account_config("primary", "test-key", "active")],
    Adapter::new(
      MemoryTransport::new(&sent, [Reply::json(RESPONSES_JSON), Reply::json(RESPONSES_JSON)]).with_trace(trace.clone()),
      runtime.clone(),
    ),
  )
  .unwrap();
  let mut active = Box::pin(client.generate("gpt-5").user("hello").request_id("cancelled").send());
  let mut context = Context::from_waker(Waker::noop());
  assert!(active.as_mut().poll(&mut context).is_pending());
  assert_eq!(sent.borrow().len(), 1);
  let busy = run(client.generate("gpt-5").user("hello").send()).unwrap_err();
  assert!(matches!(busy, Error::Request { .. }));
  assert_eq!(sent.borrow().len(), 1, "busy acquisition must not send");
  drop(active);
  assert_eq!(trace.borrow().closed, 1);
  let response = run(client.generate("gpt-5").user("hello").request_id("surviving").send()).unwrap();
  assert_eq!(response.text, "hello");
  assert_eq!(sent.borrow().len(), 2);
  assert_eq!(trace.borrow().closed, 2);
  let state = runtime.0.borrow();
  let completions: Vec<_> = state
    .events
    .iter()
    .filter(|event| event.stage == "completed")
    .map(|event| (event.request_id.as_str(), event.success))
    .collect();
  assert_eq!(completions, [("cancelled", Some(false)), ("surviving", Some(true))]);
}
