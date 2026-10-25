//! Public event contracts exercised with the same std and alloc-only consumer.

use super::*;
use tokn_sdk::{
  usage::{Usage, UsageDetails, UsageType},
  EventRuntime, RecordEvent, RequestEvent, RequestEventPayload, RequestEventSink, StageEvent,
};

const CHAT_USAGE_SSE: &[u8] = concat!(
  "data: {\"id\":\"chat-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n",
  "data: {\"id\":\"chat-test\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
  "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7,\"total_tokens\":18,\"prompt_tokens_details\":{\"cached_tokens\":3},\"completion_tokens_details\":{\"reasoning_tokens\":2}}}\n\n",
  "data: [DONE]\n\n"
).as_bytes();
const RESPONSES_USAGE_SSE: &[u8] = concat!(
  "event: response.completed\n",
  "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":11,\"output_tokens\":7,\"total_tokens\":18,\"input_tokens_details\":{\"cached_tokens\":3},\"output_tokens_details\":{\"reasoning_tokens\":2}}}}\n\n"
).as_bytes();
const MESSAGES_USAGE_SSE: &[u8] = concat!(
  "event: message_start\n",
  "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-test\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":4,\"output_tokens\":0,\"cache_read_input_tokens\":3,\"cache_creation_input_tokens\":2}}}\n\n",
  "event: message_delta\n",
  "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\n",
  "event: message_stop\n",
  "data: {\"type\":\"message_stop\"}\n\n"
).as_bytes();

fn recorded_client<'a>(
  requests: &'a RefCell<Vec<Request>>,
  replies: impl IntoIterator<Item = Reply>,
  driver: &str,
  events: &'a RefCell<Vec<RequestEvent>>,
) -> Client<Injected<MemoryTransport<'a>, EventRuntime<TestRuntime, impl RequestEventSink + 'a>>> {
  Client::new(
    plan(driver),
    vec![account_config("primary", "test-key", "active")],
    Injected::new(MemoryTransport::new(requests, replies), TestRuntime::default())
      .with_event_sink(move |event| events.borrow_mut().push(event)),
  )
  .unwrap()
}

fn request_body(endpoint: Endpoint) -> Value {
  match endpoint {
    Endpoint::ChatCompletions => serde_json::to_value(chat()).unwrap(),
    Endpoint::Responses => serde_json::to_value(responses()).unwrap(),
    Endpoint::Messages => serde_json::to_value(messages()).unwrap(),
  }
}

fn usage_records(events: &[RequestEvent]) -> Vec<Usage> {
  events
    .iter()
    .filter_map(|event| match &event.payload {
      RequestEventPayload::Record(RecordEvent::Usage(usage)) => Some(usage.clone()),
      _ => None,
    })
    .collect()
}

fn completions(events: &[RequestEvent]) -> Vec<(u32, bool, u32)> {
  events
    .iter()
    .filter_map(|event| match event.payload {
      RequestEventPayload::Stage(StageEvent::Completed { success, attempts }) => {
        Some((event.attempt, success, attempts))
      }
      _ => None,
    })
    .collect()
}

fn chat_usage() -> Usage {
  Usage {
    input_tokens: Some(11),
    output_tokens: Some(7),
    total_tokens: Some(18),
    usage_type: Some(UsageType::Chat),
    details: UsageDetails {
      cache_read: Some(3),
      cache_write: None,
      reasoning: Some(2),
    },
  }
}

/// Stage summaries retain the caller's request facts and distinguish its
/// endpoint from the prepared upstream endpoint and wire body.
pub fn typed_event_summaries() {
  let sent = RefCell::new(Vec::new());
  let events = RefCell::new(Vec::new());
  let client = recorded_client(&sent, [Reply::json(CHAT_JSON)], "openai", &events);
  let body = request_body(Endpoint::Messages);
  run(
    client.execute(
      Endpoint::Messages,
      body.clone(),
      RequestOptions::default()
        .with_request_id("summary-request")
        .with_session_id("summary-session")
        .with_header("x-vendor", "preserve"),
    ),
  )
  .unwrap();
  let events = events.borrow();
  assert!(events
    .iter()
    .all(|event| event.request_id == "summary-request" && event.attempt == 0));
  let names: Vec<_> = events
    .iter()
    .filter_map(|event| match &event.payload {
      RequestEventPayload::Stage(stage) => Some(match stage {
        StageEvent::Started { request_endpoint } => {
          assert_eq!(request_endpoint.resolved(), Some(Endpoint::Messages));
          "started"
        }
        StageEvent::Extract(summary) => {
          assert_eq!(summary.model, "model");
          assert!(!summary.stream);
          assert_eq!(summary.session_id.as_deref(), Some("summary-session"));
          assert_eq!(*summary.body_json, body);
          assert_eq!(serde_json::from_slice::<Value>(&summary.raw_body).unwrap(), body);
          assert_eq!(summary.raw_body, summary.decoded_body);
          assert_eq!(summary.headers.get("x-vendor").unwrap().as_str(), "preserve");
          "extract"
        }
        StageEvent::Resolve(summary) => {
          assert_eq!(summary.resolved_endpoint, Some(Endpoint::Messages));
          assert_eq!(summary.upstream_endpoint, Some(Endpoint::ChatCompletions));
          assert_eq!(summary.model, "model");
          assert_eq!(summary.upstream_model, "model");
          assert_eq!(summary.account_id, "primary");
          assert_eq!(summary.provider_id, "openai");
          "resolve"
        }
        StageEvent::BuildHeaders(summary) => {
          assert_eq!(summary.headers.get("x-request-id").unwrap().as_str(), "summary-request");
          assert!(!summary.headers.contains_key("x-vendor"));
          "build_headers"
        }
        StageEvent::ConvertRequest(summary) => {
          let sent = sent.borrow();
          assert_eq!(summary.upstream_wire_body, sent[0].body);
          assert_eq!(summary.debug_outbound_body, sent[0].body);
          assert_eq!(
            *summary.upstream_body,
            serde_json::from_slice::<Value>(&sent[0].body).unwrap()
          );
          assert_eq!(summary.upstream_body["messages"][0]["content"], "hello");
          assert_eq!(summary.content_encoding, None);
          "convert_request"
        }
        StageEvent::Send(summary) => {
          assert_eq!(summary.status, 200);
          assert_eq!(summary.upstream_endpoint, Some(Endpoint::ChatCompletions));
          assert!(!summary.stream);
          "send"
        }
        StageEvent::ConvertResponse(summary) => {
          assert_eq!(summary.status, 200);
          assert_eq!(summary.body.as_ref().unwrap()["content"][0]["text"], "hello");
          "convert_response"
        }
        StageEvent::Completed { success, attempts } => {
          assert!(*success);
          assert_eq!(*attempts, 1);
          "completed"
        }
        StageEvent::Error { .. } => panic!("successful response has no error event"),
      }),
      _ => None,
    })
    .collect();
  assert_eq!(
    names,
    [
      "started",
      "extract",
      "resolve",
      "build_headers",
      "convert_request",
      "send",
      "convert_response",
      "completed"
    ]
  );
}

/// JSON usage describes upstream data, even when conversion fails. Buffered
/// SSE usage describes its rendered output, matching the std stage contract.
pub fn buffered_usage_events() {
  const MESSAGES_WITH_CACHE: &[u8] = br#"{"id":"msg-test","type":"message","role":"assistant","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":7,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}}"#;
  for (endpoint, bytes, expected) in [
    (Endpoint::ChatCompletions, CHAT_JSON, UsageType::Chat),
    (Endpoint::Responses, RESPONSES_JSON, UsageType::Responses),
    (Endpoint::Messages, MESSAGES_WITH_CACHE, UsageType::Messages),
  ] {
    let sent = RefCell::new(Vec::new());
    let events = RefCell::new(Vec::new());
    let client = recorded_client(&sent, [Reply::json(bytes)], "github-copilot", &events);
    run(client.execute(endpoint, request_body(endpoint), RequestOptions::default())).unwrap();
    let events = events.borrow();
    let usages = usage_records(&events);
    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].usage_type, Some(expected));
    if expected == UsageType::Messages {
      assert_eq!(usages[0].input_tokens, Some(9));
      assert_eq!(usages[0].details.cache_read, Some(3));
      assert_eq!(usages[0].details.cache_write, Some(2));
    } else {
      assert_eq!(usages[0].input_tokens, Some(1));
      assert_eq!(usages[0].output_tokens, Some(2));
      assert_eq!(usages[0].total_tokens, Some(3));
    }
    assert_eq!(completions(&events), [(0, true, 1)]);
  }

  for bytes in [CHAT_JSON, &br#"{"choices":[],"usage":{"prompt_tokens":7}}"#[..]] {
    let sent = RefCell::new(Vec::new());
    let events = RefCell::new(Vec::new());
    let client = recorded_client(&sent, [Reply::json(bytes)], "openai", &events);
    let response = run(client.messages().create(&messages()));
    assert_eq!(response.is_ok(), bytes == CHAT_JSON);
    let events = events.borrow();
    let usages = usage_records(&events);
    assert_eq!(usages.len(), 1);
    assert_eq!(usages[0].usage_type, Some(UsageType::Chat));
    if response.is_err() {
      assert_eq!(usages[0].input_tokens, Some(7));
      let usage_index = events
        .iter()
        .position(|event| matches!(event.payload, RequestEventPayload::Record(RecordEvent::Usage(_))))
        .unwrap();
      let error_index = events
        .iter()
        .position(|event| matches!(event.payload, RequestEventPayload::Stage(StageEvent::Error { .. })))
        .unwrap();
      assert!(usage_index < error_index);
      assert_eq!(completions(&events), [(0, false, 1)]);
    }
  }

  let sent = RefCell::new(Vec::new());
  let events = RefCell::new(Vec::new());
  let client = recorded_client(&sent, [Reply::sse(CHAT_USAGE_SSE)], "deepseek", &events);
  run(client.responses().create(&responses())).unwrap();
  let usages = usage_records(&events.borrow());
  assert_eq!(usages.len(), 1);
  let mut expected = chat_usage();
  expected.usage_type = Some(UsageType::Responses);
  assert_eq!(usages[0], expected);

  // Opaque relays can report recognizable JSON usage without requiring JSON
  // or applying the managed SSE accumulator to arbitrary body bytes.
  for (bytes, usage_count) in [
    (CHAT_JSON, 1),
    (&b"{\"usage\":{\"prompt_tokens\":0}}"[..], 1),
    (&b"{}"[..], 0),
    (&b"not JSON\xff"[..], 0),
    (CHAT_USAGE_SSE, 0),
  ] {
    let raw = serde_json::from_value(json!({
      "schema_version":2,
      "profiles":{"default":{"route":"relay"}},
      "routes":{"relay":{"kind":"relay","destination":{"kind":"fixed_provider","provider":"upstream"},"credentials":{"kind":"client"},"retry":{"kind":"never"}}},
      "providers":{"upstream":{"driver":"openai","base_url":"https://example.com/v1"}}
    })).unwrap();
    let sent = RefCell::new(Vec::new());
    let events = RefCell::new(Vec::new());
    let client = Client::new(
      tokn_sdk::config::v2::compile_execution(&raw).unwrap(),
      Vec::new(),
      Injected::new(
        MemoryTransport::new(
          &sent,
          [Reply {
            status: 503,
            content_type: "application/octet-stream",
            bytes,
          }],
        ),
        TestRuntime::default(),
      )
      .with_event_sink(|event| events.borrow_mut().push(event)),
    )
    .unwrap();
    let response = run(client.execute(
      Endpoint::ChatCompletions,
      request_body(Endpoint::ChatCompletions),
      RequestOptions::default(),
    ))
    .unwrap();
    assert_eq!(response.status, 503);
    let events = events.borrow();
    let usages = usage_records(&events);
    assert_eq!(usages.len(), usage_count);
    let resolved = events
      .iter()
      .find_map(|event| match &event.payload {
        RequestEventPayload::Stage(StageEvent::Resolve(summary)) => Some(summary),
        _ => None,
      })
      .unwrap();
    assert_eq!(resolved.resolved_endpoint, Some(Endpoint::ChatCompletions));
    assert_eq!(resolved.upstream_endpoint, Some(Endpoint::ChatCompletions));
    assert_eq!(resolved.provider_id, "upstream");
    assert_eq!(resolved.account_id, "proxy");
    if bytes == b"{\"usage\":{\"prompt_tokens\":0}}" {
      assert_eq!(usages[0].input_tokens, Some(0));
      assert_eq!(usages[0].output_tokens, None);
      assert_eq!(usages[0].total_tokens, None);
    }
    assert_eq!(completions(&events), [(0, true, 1)]);
    assert_eq!(sent.borrow().len(), 1);
  }
}

/// Live observations come from upstream frames exactly once, before optional
/// translation. Multiple provider snapshots remain separate observations.
pub fn streaming_usage_events() {
  const REPEATED_PARTIAL_USAGE: &[u8] = concat!(
    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":0}}\n\n",
    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":0}}\n\n",
    "data: [DONE]\n\n"
  )
  .as_bytes();
  let zero_usage = Usage {
    input_tokens: Some(0),
    usage_type: Some(UsageType::Chat),
    ..Usage::default()
  };
  for (endpoint, driver, bytes, expected) in [
    (
      Endpoint::ChatCompletions,
      "openai",
      REPEATED_PARTIAL_USAGE,
      vec![zero_usage.clone(), zero_usage],
    ),
    (
      Endpoint::ChatCompletions,
      "github-copilot",
      CHAT_USAGE_SSE,
      vec![chat_usage()],
    ),
    (Endpoint::Messages, "openai", CHAT_USAGE_SSE, vec![chat_usage()]),
    (Endpoint::Responses, "deepseek", CHAT_USAGE_SSE, vec![chat_usage()]),
    (
      Endpoint::Responses,
      "github-copilot",
      RESPONSES_USAGE_SSE,
      vec![Usage {
        usage_type: Some(UsageType::Responses),
        ..chat_usage()
      }],
    ),
    (
      Endpoint::Messages,
      "github-copilot",
      MESSAGES_USAGE_SSE,
      vec![
        Usage {
          input_tokens: Some(9),
          output_tokens: Some(0),
          total_tokens: None,
          usage_type: Some(UsageType::Messages),
          details: UsageDetails {
            cache_read: Some(3),
            cache_write: Some(2),
            reasoning: None,
          },
        },
        // The existing shape detector recognizes this marker-free partial
        // snapshot as Responses; event portability does not change the parser.
        Usage {
          output_tokens: Some(7),
          usage_type: Some(UsageType::Responses),
          ..Usage::default()
        },
      ],
    ),
  ] {
    let sent = RefCell::new(Vec::new());
    let events = RefCell::new(Vec::new());
    let client = recorded_client(&sent, [Reply::sse(bytes)], driver, &events);
    let mut stream = run(client.execute(endpoint, stream_request(endpoint), RequestOptions::default()))
      .unwrap()
      .into_stream()
      .unwrap();
    assert!(usage_records(&events.borrow()).is_empty());
    assert!(completions(&events.borrow()).is_empty());
    while run(stream.next_event()).unwrap().is_some() {}
    assert!(run(stream.next_event()).unwrap().is_none());
    drop(stream);
    let events = events.borrow();
    assert_eq!(usage_records(&events), expected);
    assert_eq!(completions(&events), [(0, true, 1)]);
    assert!(!events
      .iter()
      .any(|event| matches!(event.payload, RequestEventPayload::Stage(StageEvent::Error { .. }))));
  }
}

/// Event IDs and attempts follow the existing retry lifecycle, including the
/// failed attempt's completion and usage belonging only to the successful one.
pub fn typed_event_retry_attribution() {
  let sent = RefCell::new(Vec::new());
  let events = RefCell::new(Vec::new());
  let runtime = TestRuntime::default();
  let client = Client::new(
    retry_plan(1, 2),
    vec![
      account_config("primary", "primary-key", "active"),
      account_config("backup", "backup-key", "fallback"),
    ],
    Injected::new(
      MemoryTransport::new(
        &sent,
        [
          Reply {
            status: 503,
            content_type: "application/json",
            bytes: b"{}",
          },
          Reply::json(CHAT_JSON),
        ],
      ),
      runtime.clone(),
    )
    .with_event_sink(|event| events.borrow_mut().push(event)),
  )
  .unwrap();
  run(
    client
      .chat_completions()
      .create_with(&chat(), RequestOptions::default().with_request_id("typed-retry")),
  )
  .unwrap();
  let events = events.borrow();
  assert!(events.iter().all(|event| event.request_id == "typed-retry"));
  assert_eq!(completions(&events), [(0, false, 1), (1, true, 2)]);
  let accounts: Vec<_> = events
    .iter()
    .filter_map(|event| match &event.payload {
      RequestEventPayload::Stage(StageEvent::Resolve(summary)) => Some((event.attempt, summary.account_id.as_str())),
      _ => None,
    })
    .collect();
  assert_eq!(accounts, [(0, "primary"), (1, "backup")]);
  let failures: Vec<_> = events
    .iter()
    .filter_map(|event| match &event.payload {
      RequestEventPayload::Stage(StageEvent::Error { recoverable, .. }) => Some((event.attempt, *recoverable)),
      _ => None,
    })
    .collect();
  assert_eq!(failures, [(0, true)]);
  let usage_attempts: Vec<_> = events
    .iter()
    .filter_map(|event| {
      matches!(event.payload, RequestEventPayload::Record(RecordEvent::Usage(_))).then_some(event.attempt)
    })
    .collect();
  assert_eq!(usage_attempts, [1]);
  assert_eq!(runtime.0.borrow().sleeps, [Duration::from_millis(2)]);
  assert_completions(&runtime, &[false, true]);
}

/// Both error returns and abandoned streams emit one typed failure/completion
/// pair, with no second emission from another pull, repeated cancel, or Drop.
pub fn typed_event_failures() {
  for mode in 0..4 {
    let sent = RefCell::new(Vec::new());
    let events = RefCell::new(Vec::new());
    let bytes = if mode == 0 {
      &b"data: {\"error\":{\"message\":\"failed\"}}\n\n"[..]
    } else {
      CHAT_SSE
    };
    let client = recorded_client(&sent, [Reply::sse(bytes)], "openai", &events);
    let mut stream = run(client.execute(
      Endpoint::ChatCompletions,
      stream_request(Endpoint::ChatCompletions),
      RequestOptions::default(),
    ))
    .unwrap()
    .into_stream()
    .unwrap();
    match mode {
      0 => {
        assert!(run(stream.next_event()).is_err());
        assert!(run(stream.next_event()).unwrap().is_none());
      }
      1 => {
        stream.cancel();
        stream.cancel();
        assert!(run(stream.next_event()).unwrap().is_none());
      }
      2 => {
        assert!(run(stream.next_event()).unwrap().is_some());
      }
      _ => {
        let mut pull = pin!(stream.next_event());
        assert!(pull.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
      }
    }
    drop(stream);
    let events = events.borrow();
    assert_eq!(completions(&events), [(0, false, 1)]);
    let failures = events
      .iter()
      .filter(|event| matches!(event.payload, RequestEventPayload::Stage(StageEvent::Error { .. })))
      .count();
    assert_eq!(failures, 1);
    assert!(usage_records(&events).is_empty());
    assert_eq!(sent.borrow().len(), 1);
  }
}

/// A sink may borrow application state and capture Rc. Runtime timestamps use
/// the application's millisecond clock, with a safe seconds-only default.
pub fn event_timestamps_and_local_sink() {
  struct Clock(i64);
  impl Runtime for Clock {
    fn now(&self) -> Duration {
      Duration::ZERO
    }
    fn unix_seconds(&self) -> u64 {
      0
    }
    fn unix_millis(&self) -> i64 {
      self.0
    }
    async fn sleep(&mut self, _: Duration) {}
  }
  struct SecondsClock;
  impl Runtime for SecondsClock {
    fn now(&self) -> Duration {
      Duration::ZERO
    }
    fn unix_seconds(&self) -> u64 {
      u64::MAX
    }
    async fn sleep(&mut self, _: Duration) {}
  }
  assert_eq!(SecondsClock.unix_millis(), i64::MAX);
  assert_eq!(TestRuntime::default().unix_millis(), 1_700_000_000_000);
  for expected in [-1, 1_700_000_000_123] {
    let sent = RefCell::new(Vec::new());
    let events = RefCell::new(Vec::new());
    let events_ref = &events;
    let local = Rc::new(core::cell::Cell::new(0));
    let observed = local.clone();
    let client = Client::new(
      plan("openai"),
      vec![account_config("primary", "test-key", "active")],
      Injected::new(MemoryTransport::new(&sent, [Reply::json(CHAT_JSON)]), Clock(expected)).with_event_sink(
        move |event| {
          observed.set(observed.get() + 1);
          events_ref.borrow_mut().push(event);
        },
      ),
    )
    .unwrap();
    run(client.chat_completions().create(&chat())).unwrap();
    let events = events.borrow();
    assert_eq!(local.get(), events.len());
    assert!(!events.is_empty());
    assert!(events.iter().all(|event| event.ts == expected));
    let serialized = serde_json::to_value(&events[0]).unwrap();
    assert_eq!(serialized["ts"], expected);
    assert_eq!(serialized["payload"]["category"], "stage");
  }
}
