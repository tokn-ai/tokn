#![cfg(feature = "std")]

use serde_json::{json, Value};
use std::{cell::RefCell, collections::VecDeque, rc::Rc, time::Duration};
use tokn_sdk::{
  account::AccountConfig,
  transport::{Request, ResponseHead, Transport},
  Adapter, Client, ContinuationStatus, Conversation, Endpoint, GenerateEvent, GenerateRequest, InputDisposition,
  ModelCapabilities, RetrievedContext, Runtime, Tool,
};

struct Reply {
  bytes: Vec<u8>,
  fail_after: Option<usize>,
}

struct StreamTransport {
  requests: Rc<RefCell<Vec<Request>>>,
  replies: VecDeque<Reply>,
  active: Option<Reply>,
  position: usize,
}

#[derive(Debug)]
struct Interrupted;

impl std::fmt::Display for Interrupted {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str("fixture stream interrupted")
  }
}

impl Transport for StreamTransport {
  type Error = Interrupted;

  async fn send(&mut self, request: Request) -> Result<ResponseHead, Self::Error> {
    self.requests.borrow_mut().push(request);
    self.active = Some(self.replies.pop_front().expect("one reply per request"));
    self.position = 0;
    let mut head = ResponseHead {
      status: 200,
      headers: Default::default(),
    };
    head.headers.insert("content-type", "text/event-stream");
    Ok(head)
  }

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    tokio::task::yield_now().await;
    let reply = self.active.as_ref().expect("response is open");
    if reply.fail_after.is_some_and(|limit| self.position >= limit) {
      return Err(Interrupted);
    }
    let remaining = reply.bytes.len().saturating_sub(self.position);
    let until_failure = reply
      .fail_after
      .map_or(usize::MAX, |limit| limit.saturating_sub(self.position));
    let count = buffer.len().min(7).min(remaining).min(until_failure);
    buffer[..count].copy_from_slice(&reply.bytes[self.position..self.position + count]);
    self.position += count;
    Ok(count)
  }

  fn close_response(&mut self) {
    self.active = None;
  }
}

struct Time;

impl Runtime for Time {
  fn now(&self) -> Duration {
    Duration::ZERO
  }
  fn unix_seconds(&self) -> u64 {
    1_700_000_000
  }
  async fn sleep(&mut self, _: Duration) {
    panic!("stream reads must not automatically retry")
  }
}

fn frames(values: Vec<Value>) -> Vec<u8> {
  let mut bytes = Vec::new();
  for value in values {
    bytes.extend_from_slice(b"data: ");
    bytes.extend(serde_json::to_vec(&value).unwrap());
    bytes.extend_from_slice(b"\n\n");
  }
  bytes
}

fn stream(endpoint: Endpoint, with_tool: bool) -> Vec<u8> {
  let text = if with_tool { "searching" } else { "done" };
  match endpoint {
    Endpoint::Responses => {
      let mut events = vec![
        json!({"type":"response.created","response":{"id":"response-fixture","status":"in_progress"}}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":text}),
      ];
      let mut output =
        vec![json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]})];
      if with_tool {
        events.extend([
          json!({"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":""}}),
          json!({"type":"response.function_call_arguments.delta","output_index":2,"delta":"{}"}),
        ]);
        output.extend([
          json!({"type":"reasoning","encrypted_content":"stream-cipher","summary":[]}),
          json!({"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{}"}),
        ]);
      }
      events.push(
        json!({"type":"response.completed","response":{"id":"response-fixture","status":"completed","output":output}}),
      );
      frames(events)
    }
    Endpoint::ChatCompletions => {
      let mut events =
        vec![json!({"id":"chat-fixture","choices":[{"index":0,"delta":{"role":"assistant","content":text}}]})];
      if with_tool {
        events.push(json!({"id":"chat-fixture","choices":[{"index":0,"delta":{"reasoning_content":"thinking","reasoning_details":[{"signature":"stream-cipher"}],"tool_calls":[{"index":3,"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}]}));
      }
      events.push(json!({"id":"chat-fixture","choices":[{"index":0,"delta":{},"finish_reason":if with_tool { "tool_calls" } else { "stop" }}]}));
      let mut bytes = frames(events);
      bytes.extend_from_slice(b"data: [DONE]\n\n");
      bytes
    }
    Endpoint::Messages => {
      let mut events = vec![
        json!({"type":"message_start","message":{"id":"message-fixture","role":"assistant","content":[],"usage":{"input_tokens":3}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
        json!({"type":"content_block_stop","index":0}),
      ];
      if with_tool {
        events.extend([
          json!({"type":"content_block_start","index":1,"content_block":{"type":"thinking","thinking":""}}),
          json!({"type":"content_block_delta","index":1,"delta":{"type":"thinking_delta","thinking":"thinking"}}),
          json!({"type":"content_block_delta","index":1,"delta":{"type":"signature_delta","signature":"stream-cipher"}}),
          json!({"type":"content_block_stop","index":1}),
          json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"call-1","name":"lookup","input":{}}}),
          json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{}"}}),
          json!({"type":"content_block_stop","index":2}),
        ]);
      }
      events.extend([
        json!({"type":"message_delta","delta":{"stop_reason":if with_tool { "tool_use" } else { "end_turn" }},"usage":{"output_tokens":4}}),
        json!({"type":"message_stop"}),
      ]);
      frames(events)
    }
  }
}

fn client(
  endpoint: Endpoint,
  requests: Rc<RefCell<Vec<Request>>>,
  replies: Vec<Reply>,
) -> Client<Adapter<StreamTransport, Time>> {
  let (driver, model) = match endpoint {
    Endpoint::ChatCompletions => ("llama-cpp", "gpt-5"),
    Endpoint::Responses => ("openai", "gpt-5"),
    Endpoint::Messages => ("github-copilot", "claude-sonnet"),
  };
  let raw = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"upstream"},"wire_identity":"none"},
    "providers":{"upstream":{"driver":driver,"base_url":"https://example.com/v1"}}
  }))
  .unwrap();
  let rules = tokn_sdk::config::v2::compile_execution(&raw).unwrap();
  let account: AccountConfig =
    serde_json::from_value(json!({"id":"account-fixture","provider":"upstream","api_key":"fixture-key"})).unwrap();
  let models = [(
    model.into(),
    ModelCapabilities {
      endpoints: Some(vec![endpoint]),
      ..Default::default()
    },
  )]
  .into_iter()
  .collect();
  Client::with_model_metadata(
    rules,
    vec![account],
    [(tokn_sdk::policy::ProviderId::new("upstream").unwrap(), models)]
      .into_iter()
      .collect(),
    Adapter::new(
      StreamTransport {
        requests,
        replies: replies.into(),
        active: None,
        position: 0,
      },
      Time,
    ),
  )
  .unwrap()
}

fn template(endpoint: Endpoint) -> tokn_sdk::GenerateRequestBuilder {
  GenerateRequest::builder(if endpoint == Endpoint::Messages {
    "claude-sonnet"
  } else {
    "gpt-5"
  })
  .system("Preserve instructions")
  .tool(Tool::function("lookup", json!({"type":"object"})))
}

#[tokio::test]
async fn live_native_generations_commit_tools_and_adopt_queued_steering_at_the_next_request() {
  for endpoint in [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages] {
    let requests = Rc::new(RefCell::new(Vec::new()));
    let client = client(
      endpoint,
      requests.clone(),
      vec![
        Reply {
          bytes: stream(endpoint, true),
          fail_after: None,
        },
        Reply {
          bytes: stream(endpoint, false),
          fail_after: None,
        },
      ],
    );
    let mut conversation = Conversation::new("live-conversation").unwrap();
    conversation.submit_user("initial input").unwrap();
    let prepared = conversation
      .prepare(
        template(endpoint),
        vec![RetrievedContext::new("guide", "original context")],
      )
      .unwrap();
    let mut queued = false;
    let mut events = Vec::new();
    let response = client
      .send_streaming(prepared.request(), |event| {
        if !queued && matches!(&event, GenerateEvent::TextDelta { text } if text == "searching") {
          assert_eq!(
            conversation.submit_user("steer from live callback").unwrap(),
            InputDisposition::Queued
          );
          queued = true;
        }
        events.push(event);
      })
      .await
      .unwrap();
    assert!(queued);
    assert!(events
      .iter()
      .any(|event| matches!(event, GenerateEvent::ToolCallDelta { .. })));
    assert_eq!(response.text, "searching");
    assert_eq!(response.continuation_status, ContinuationStatus::Started);
    assert_eq!(response.tool_calls.len(), 1);
    conversation.commit(&prepared, response).unwrap();
    conversation.record_tool_result("call-1", "single tool result").unwrap();
    let bytes = serde_json::to_vec(&conversation).unwrap();
    let mut conversation: Conversation = serde_json::from_slice(&bytes).unwrap();
    let next = conversation
      .prepare(template(endpoint), vec![RetrievedContext::new("guide", "new context")])
      .unwrap();
    let response = client.send_streaming(next.request(), |_| {}).await.unwrap();
    assert_eq!(response.text, "done");
    assert_eq!(response.continuation_status, ContinuationStatus::Continued);
    conversation.commit(&next, response).unwrap();
    let requests = requests.borrow();
    assert_eq!(requests.len(), 2);
    let first: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let second: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(first["stream"], true);
    assert_eq!(second["stream"], true);
    assert!(!first.to_string().contains("steer from live callback"));
    let second = second.to_string();
    for text in [
      "initial input",
      "steer from live callback",
      "original context",
      "new context",
      "single tool result",
      "stream-cipher",
    ] {
      assert_eq!(second.matches(text).count(), 1, "{endpoint:?}: {text}");
    }
  }
}

#[tokio::test]
async fn interrupted_live_generation_restores_exact_pending_input_without_committing_provisional_tools() {
  let endpoint = Endpoint::Responses;
  let bytes = stream(endpoint, true);
  let terminal = bytes
    .windows(b"\n\ndata: ".len())
    .rposition(|window| window == b"\n\ndata: ")
    .map(|position| position + 2)
    .expect("terminal snapshot is the final SSE frame");
  let requests = Rc::new(RefCell::new(Vec::new()));
  let client = client(
    endpoint,
    requests.clone(),
    vec![
      Reply {
        bytes: bytes.clone(),
        fail_after: Some(terminal),
      },
      Reply {
        bytes,
        fail_after: None,
      },
      Reply {
        bytes: stream(endpoint, false),
        fail_after: None,
      },
    ],
  );
  let mut conversation = Conversation::new("interrupted-live").unwrap();
  conversation.submit_user("initial input").unwrap();
  let prepared = conversation
    .prepare(
      template(endpoint),
      vec![RetrievedContext::new("guide", "retry context")],
    )
    .unwrap();
  let mut queued = false;
  let mut tool_delta_seen = false;
  let result = client
    .send_streaming(prepared.request(), |event| {
      tool_delta_seen |= matches!(&event, GenerateEvent::ToolCallDelta { .. });
      if !queued && matches!(&event, GenerateEvent::TextDelta { .. }) {
        conversation.submit_user("steer while interrupted").unwrap();
        queued = true;
      }
    })
    .await;
  assert!(result.is_err());
  assert!(queued && tool_delta_seen);
  assert!(conversation.provider_state().is_none());
  assert!(
    conversation.outstanding_tool_calls().is_empty(),
    "provisional calls cannot enter committed history"
  );
  assert_eq!(
    requests.borrow().len(),
    1,
    "stream failure must not automatically replay the request"
  );
  let stored = serde_json::to_vec(&conversation).unwrap();
  let mut conversation: Conversation = serde_json::from_slice(&stored).unwrap();
  let retry = conversation.retry().unwrap();
  assert_eq!(retry, prepared);
  let response = client.send_streaming(retry.request(), |_| {}).await.unwrap();
  conversation.commit(&retry, response).unwrap();
  assert_eq!(conversation.outstanding_tool_calls().len(), 1);
  conversation.record_tool_result("call-1", "executed once").unwrap();
  let next = conversation.prepare(template(endpoint), vec![]).unwrap();
  let response = client.send_streaming(next.request(), |_| {}).await.unwrap();
  conversation.commit(&next, response).unwrap();
  let requests = requests.borrow();
  assert_eq!(requests.len(), 3);
  assert_eq!(
    requests[0].body, requests[1].body,
    "interrupted generation retries its exact input snapshot"
  );
  let last: Value = serde_json::from_slice(&requests[2].body).unwrap();
  for text in [
    "initial input",
    "retry context",
    "steer while interrupted",
    "executed once",
    "stream-cipher",
  ] {
    assert_eq!(last.to_string().matches(text).count(), 1);
  }
}
