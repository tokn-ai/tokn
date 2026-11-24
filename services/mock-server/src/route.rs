use axum::body::Bytes;
use axum::http::{Method, StatusCode};
use serde::Serialize;
use serde_json::{json, Value};
use tokn_endpoint_chat_completions::{
  ChatChoice, ChatChunk, ChatContent, ChatDelta, ChatMessage, ChatResponse, ChatUsage, ChunkChoice,
};
use tokn_endpoint_core::{Extras, FinishReason, Role};
use tokn_endpoint_messages::{ContentBlock, MessagesResponse};
use tokn_endpoint_responses::{OutputContentPart, OutputItem, OutputMessage, ResponsesResponse, TaggedOutputMessage};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MockEndpoint {
  Models,
  ChatCompletions,
  Responses,
  Messages,
  Custom { method: Method, path: String },
}

impl MockEndpoint {
  pub(crate) fn method(&self) -> Method {
    match self {
      Self::Models => Method::GET,
      Self::ChatCompletions => Method::POST,
      Self::Responses => Method::POST,
      Self::Messages => Method::POST,
      Self::Custom { method, .. } => method.clone(),
    }
  }

  pub(crate) fn path(&self) -> &str {
    match self {
      Self::Models => "/models",
      Self::ChatCompletions => "/chat/completions",
      Self::Responses => "/responses",
      Self::Messages => "/messages",
      Self::Custom { path, .. } => path.as_str(),
    }
  }
}

#[derive(Clone, Debug)]
pub struct MockRoute {
  pub endpoint: MockEndpoint,
  pub response: MockResponse,
}

impl MockRoute {
  pub fn new(endpoint: MockEndpoint, response: MockResponse) -> Self {
    Self { endpoint, response }
  }

  pub fn models<I, S>(ids: I) -> Self
  where
    I: IntoIterator<Item = S>,
    S: Into<String>,
  {
    let data: Vec<Value> = ids
      .into_iter()
      .map(|id| {
        let id = id.into();
        json!({"id": id, "object": "model"})
      })
      .collect();
    Self::new(
      MockEndpoint::Models,
      MockResponse::json(json!({"object": "list", "data": data})),
    )
  }

  pub fn chat_completions() -> Self {
    Self::new(
      MockEndpoint::ChatCompletions,
      MockResponse::json(ChatResponse {
        id: Some("chatcmpl-mock".into()),
        object: Some("chat.completion".into()),
        created: None,
        model: None,
        choices: vec![ChatChoice {
          index: 0,
          message: ChatMessage {
            role: Role::Assistant,
            content: Some(ChatContent::Text("mock response".into())),
            name: None,
            tool_call_id: None,
            tool_calls: Vec::new(),
            reasoning_content: None,
            audio: None,
            extras: Extras::default(),
          },
          finish_reason: Some(FinishReason::Stop),
          extras: Extras::default(),
        }],
        usage: Some(ChatUsage {
          prompt_tokens: Some(1),
          completion_tokens: Some(1),
          total_tokens: Some(2),
          prompt_tokens_details: None,
          completion_tokens_details: None,
          extras: Extras::default(),
        }),
        extras: Extras::default(),
      }),
    )
  }

  pub fn chat_completions_stream() -> Self {
    Self::new(
      MockEndpoint::ChatCompletions,
      MockResponse::sse_data([
        sse_chat_chunk(Some(Role::Assistant), Some("hel"), None, None),
        sse_chat_chunk(None, Some("lo"), None, None),
        sse_chat_chunk(
          None,
          None,
          Some(FinishReason::Stop),
          Some(ChatUsage {
            prompt_tokens: Some(3),
            completion_tokens: Some(2),
            total_tokens: Some(5),
            prompt_tokens_details: None,
            completion_tokens_details: None,
            extras: Extras::default(),
          }),
        ),
        "[DONE]".to_string(),
      ]),
    )
  }

  pub fn responses() -> Self {
    Self::new(
      MockEndpoint::Responses,
      MockResponse::json(ResponsesResponse {
        id: Some("resp-mock".into()),
        object: Some("response".into()),
        created_at: None,
        completed_at: None,
        status: Some("completed".into()),
        model: None,
        instructions: None,
        output: vec![OutputItem::Message(TaggedOutputMessage {
          kind: "message".into(),
          message: OutputMessage {
            id: None,
            status: None,
            role: Role::Assistant,
            content: vec![OutputContentPart::OutputText {
              text: "mock response".into(),
              annotations: Vec::new(),
              logprobs: None,
              extras: Extras::default(),
            }],
            extras: Extras::default(),
          },
        })],
        output_text: None,
        tools: Vec::new(),
        metadata: None,
        usage: None,
        error: None,
        incomplete_details: None,
        params: Default::default(),
        extra_params: Default::default(),
        extras: Extras::default(),
      }),
    )
  }

  pub fn messages() -> Self {
    Self::new(
      MockEndpoint::Messages,
      MockResponse::json(MessagesResponse {
        id: Some("msg-mock".into()),
        kind: Some("message".into()),
        role: Some(Role::Assistant),
        model: None,
        content: vec![ContentBlock::Text {
          text: "mock response".into(),
          cache_control: None,
          extras: Extras::default(),
        }],
        stop_reason: None,
        stop_sequence: None,
        stop_details: None,
        usage: None,
        extras: Extras::default(),
      }),
    )
  }
}

fn sse_chat_chunk(
  role: Option<Role>,
  content: Option<&str>,
  finish_reason: Option<FinishReason>,
  usage: Option<ChatUsage>,
) -> String {
  serde_json::to_string(&ChatChunk {
    id: Some("chatcmpl-stream".into()),
    object: Some("chat.completion.chunk".into()),
    created: None,
    model: None,
    choices: vec![ChunkChoice {
      index: 0,
      delta: ChatDelta {
        role,
        content: content.map(str::to_string),
        reasoning_content: None,
        audio: None,
        tool_calls: Vec::new(),
        extras: Extras::default(),
      },
      finish_reason,
      extras: Extras::default(),
    }],
    usage,
    extras: Extras::default(),
  })
  .expect("serialize mock chat chunk")
}

#[derive(Clone, Debug)]
pub struct MockResponse {
  pub status: StatusCode,
  pub headers: Vec<(String, String)>,
  pub body: Bytes,
}

impl MockResponse {
  pub fn json(value: impl Serialize) -> Self {
    Self {
      status: StatusCode::OK,
      headers: vec![("content-type".into(), "application/json".into())],
      body: Bytes::from(serde_json::to_vec(&value).expect("serialize mock JSON response")),
    }
  }

  pub fn sse(body: impl Into<Bytes>) -> Self {
    Self {
      status: StatusCode::OK,
      headers: vec![
        ("content-type".into(), "text/event-stream".into()),
        ("cache-control".into(), "no-cache".into()),
      ],
      body: body.into(),
    }
  }

  pub fn sse_data<I, S>(events: I) -> Self
  where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
  {
    let mut body = String::new();
    for event in events {
      body.push_str("data: ");
      body.push_str(event.as_ref());
      body.push_str("\n\n");
    }
    Self::sse(body)
  }
}
