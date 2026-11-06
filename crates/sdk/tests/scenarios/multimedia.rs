//! Public SDK media contracts exercised against recorded native HTTP bodies.
use super::*;
use alloc::string::ToString;
use tokn_sdk::{
  content::{AudioFormat, ProviderAssetReference, TranscriptContent, VideoFrame},
  AudioContent, AudioOutputOptions, Content, ContentBody, ContinuationStatus, Conversation, DocumentContent,
  GenerateRequest, ImageContent, ImageGenerationOptions, MediaResource, MediaSource, ModelCapabilities,
  RetrievedContext, Tool, VideoContent,
};

#[path = "history_images.rs"]
mod history_images;
pub use history_images::multimedia_history_image;

const AUDIO: &[u8] = br#"{"id":"speech","choices":[{"index":0,"message":{"role":"assistant","content":"hello","audio":{"id":"audio-fixture","data":"AQID","transcript":"spoken hello","expires_at":2000000000}},"finish_reason":"stop"}]}"#;
const IMAGE: &[u8] = br#"{"id":"drawing","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Here it is"}]},{"type":"image_generation_call","id":"image-fixture","status":"completed","output_format":"png","result":"AQID"}]}"#;
const TOOL: &[u8] = br#"{"id":"tool","status":"completed","output":[{"type":"function_call","call_id":"capture-1","name":"capture","arguments":"{}"}]}"#;

fn image() -> Content {
  ImageContent::new(MediaResource::inline("image/png", vec![1, 2, 3])).into()
}

fn media_client<'a>(
  sent: &'a RefCell<Vec<Request>>,
  endpoint: Endpoint,
  replies: impl IntoIterator<Item = Reply>,
) -> TestClient<'a> {
  let driver = match endpoint {
    Endpoint::Messages => "github-copilot",
    _ => "openai",
  };
  let models = [(
    "gpt-5".into(),
    ModelCapabilities {
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

pub fn multimedia_native_inputs() {
  assert_eq!(
    multimedia_history_image(ImageContent::new(MediaResource::inline("image/png", vec![1, 2, 3]))),
    10
  );
  for (endpoint, reply) in [
    (Endpoint::Responses, RESPONSES_JSON),
    (Endpoint::ChatCompletions, CHAT_JSON),
    (Endpoint::Messages, MESSAGES_JSON),
  ] {
    let sent = RefCell::new(Vec::new());
    let client = media_client(&sent, endpoint, [Reply::json(reply)]);
    let document = DocumentContent::new(MediaResource::inline("application/pdf", b"%PDF-fixture".to_vec()))
      .with_filename("guide.pdf");
    let request = GenerateRequest::builder("gpt-5")
      .max_output_tokens(64)
      .user(vec![
        Content::text("before"),
        image(),
        Content::text("between"),
        document.into(),
        Content::text("after"),
      ])
      .build()
      .unwrap();
    assert_eq!(run(client.send(&request)).unwrap().text, "hello");
    let sent = sent.borrow();
    let body: Value = serde_json::from_slice(&sent[0].body).unwrap();
    let parts = if endpoint == Endpoint::Responses {
      &body["input"][0]["content"]
    } else {
      &body["messages"][0]["content"]
    };
    assert_eq!(parts.as_array().unwrap().len(), 5);
    for (index, text) in [(0, "before"), (2, "between"), (4, "after")] {
      assert_eq!(parts[index]["text"], text);
    }
    let (image_kind, file_kind) = match endpoint {
      Endpoint::Responses => ("input_image", "input_file"),
      Endpoint::ChatCompletions => ("image_url", "file"),
      Endpoint::Messages => ("image", "document"),
    };
    assert_eq!(parts[1]["type"], image_kind);
    assert_eq!(parts[3]["type"], file_kind);
    assert!(!body.to_string().contains("\"resource\""));
  }
  let sent = RefCell::new(Vec::new());
  let client = media_client(&sent, Endpoint::Responses, [Reply::json(RESPONSES_JSON)]);
  let frame = |timestamp_ms| VideoFrame {
    timestamp_ms,
    image: ImageContent::new(MediaResource::inline("image/png", vec![1, 2, 3])),
  };
  let request = GenerateRequest::builder("gpt-5")
    .user(Content::Video(VideoContent::frames(vec![frame(0), frame(1500)])))
    .build()
    .unwrap();
  run(client.send(request)).unwrap();
  let body: Value = serde_json::from_slice(&sent.borrow()[0].body).unwrap();
  let parts = body["input"][0]["content"].as_array().unwrap();
  assert_eq!(parts.iter().filter(|p| p["type"] == "input_image").count(), 2);
  assert!(parts
    .iter()
    .any(|p| p["text"].as_str().is_some_and(|s| s.contains("1500"))));

  let sent = RefCell::new(Vec::new());
  let client = media_client(
    &sent,
    Endpoint::Messages,
    [Reply::json(MESSAGES_JSON), Reply::json(MESSAGES_JSON)],
  );
  let scope = run(
    client.send(
      GenerateRequest::builder("gpt-5")
        .prompt("establish asset scope")
        .capture_provider_state(true)
        .build()
        .unwrap(),
    ),
  )
  .unwrap()
  .provider_state
  .unwrap()
  .asset_scope();
  let reference = ProviderAssetReference {
    id: "file-image".into(),
    namespace: "anthropic.files".into(),
    scope,
    expires_at: None,
  };
  let mut document = DocumentContent::new(MediaResource::inline("application/pdf", b"%PDF-fixture".to_vec()));
  document.title = Some("Reference manual".into());
  let request = GenerateRequest::builder("gpt-5")
    .user(vec![
      ImageContent::new(MediaResource::provider(reference)).into(),
      document.into(),
    ])
    .build()
    .unwrap();
  let response = run(client.send(request)).unwrap();
  assert_eq!(response.endpoint, Endpoint::Messages);
  let body: Value = serde_json::from_slice(&sent.borrow()[1].body).unwrap();
  assert_eq!(
    body["messages"][0]["content"][0]["source"],
    json!({"type":"file","file_id":"file-image"})
  );
  assert_eq!(body["messages"][0]["content"][1]["title"], "Reference manual");

  for source in [
    MediaResource::remote("https://source.invalid/recording.wav"),
    MediaResource::provider(ProviderAssetReference {
      id: "foreign-audio".into(),
      namespace: "anthropic.files".into(),
      scope: tokn_sdk::session::ProviderOrigin::new(
        "foreign",
        "anthropic",
        "foreign-account",
        "https://source.invalid",
        Endpoint::Messages,
        "other",
      )
      .asset_scope(),
      expires_at: Some(0),
    }),
  ] {
    let sent = RefCell::new(Vec::new());
    let client = media_client(&sent, Endpoint::Responses, [Reply::json(RESPONSES_JSON)]);
    let mut transcript = TranscriptContent::new("spoken words");
    transcript.source = Some(source);
    let mut conversation = Conversation::new("transcript-provenance").unwrap();
    conversation.submit_user(transcript).unwrap();
    let prepared = conversation.prepare(GenerateRequest::builder("gpt-5"), vec![]).unwrap();
    run(client.send(prepared.request())).unwrap();
    let wire = String::from_utf8(sent.borrow()[0].body.to_vec()).unwrap();
    assert!(wire.contains("spoken words"));
    assert!(!wire.contains("foreign-audio"));
    assert!(!wire.contains("source.invalid"));
  }
}

pub fn multimedia_output_continuation() {
  let sent = RefCell::new(Vec::new());
  let client = media_client(
    &sent,
    Endpoint::ChatCompletions,
    [Reply::json(AUDIO), Reply::json(CHAT_JSON)],
  );
  let mut conversation = Conversation::new("audio-conversation").unwrap();
  conversation
    .submit_user(Content::Audio(AudioContent::new(MediaResource::inline(
      "audio/wav",
      vec![4, 5, 6],
    ))))
    .unwrap();
  let template = || GenerateRequest::builder("gpt-5").audio_output(AudioOutputOptions::new("alloy", AudioFormat::Wav));
  let first = conversation.prepare(template(), vec![]).unwrap();
  let reply = run(client.send(first.request())).unwrap();
  assert_eq!(reply.endpoint, Endpoint::ChatCompletions);
  let audio = reply
    .content
    .iter()
    .find_map(|part| {
      if let Content::Audio(audio) = part {
        Some(audio)
      } else {
        None
      }
    })
    .unwrap();
  assert_eq!(audio.format, Some(AudioFormat::Wav));
  assert_eq!(audio.transcript.as_ref().unwrap().text, "spoken hello");
  assert!(matches!(&audio.resource.source, MediaSource::Inline { data } if data.as_ref() == [1,2,3]));
  conversation.commit(&first, reply).unwrap();
  let mut conversation: Conversation = serde_json::from_slice(&serde_json::to_vec(&conversation).unwrap()).unwrap();
  conversation.submit_user("Describe the sound").unwrap();
  let next = conversation.prepare(template(), vec![]).unwrap();
  let reply = run(client.send(next.request())).unwrap();
  assert_eq!(reply.continuation_status, ContinuationStatus::Continued);
  conversation.commit(&next, reply).unwrap();
  let body: Value = serde_json::from_slice(&sent.borrow()[1].body).unwrap();
  let assistant = body["messages"]
    .as_array()
    .unwrap()
    .iter()
    .find(|m| m["role"] == "assistant")
    .unwrap();
  assert_eq!(assistant["audio"], json!({"id":"audio-fixture"}));

  let sent = RefCell::new(Vec::new());
  let client = media_client(
    &sent,
    Endpoint::Responses,
    [Reply::json(IMAGE), Reply::json(RESPONSES_JSON)],
  );
  let mut conversation = Conversation::new("image-conversation").unwrap();
  conversation.submit_user("Draw a chart").unwrap();
  let template = || GenerateRequest::builder("gpt-5").image_generation(ImageGenerationOptions::default());
  let first = conversation.prepare(template(), vec![]).unwrap();
  let reply = run(client.send(first.request())).unwrap();
  assert_eq!(reply.content.len(), 2);
  assert!(matches!(reply.content[1], Content::Image(_)));
  conversation.commit(&first, reply).unwrap();
  conversation.submit_user("Make it blue").unwrap();
  let next = conversation.prepare(template(), vec![]).unwrap();
  run(client.send(next.request())).unwrap();
  let body: Value = serde_json::from_slice(&sent.borrow()[1].body).unwrap();
  assert_eq!(body["tools"][0]["type"], "image_generation");
  assert_eq!(
    body["input"]
      .as_array()
      .unwrap()
      .iter()
      .filter(|item| item["type"] == "image_generation_call")
      .count(),
    1
  );
}

pub fn multimedia_tool_context_and_snapshot() {
  let sent = RefCell::new(Vec::new());
  let client = media_client(
    &sent,
    Endpoint::Responses,
    [Reply::json(TOOL), Reply::json(RESPONSES_JSON)],
  );
  let mut conversation = Conversation::new("media-tool").unwrap();
  conversation.submit_user("Inspect the screen").unwrap();
  let template = || GenerateRequest::builder("gpt-5").tool(Tool::function("capture", json!({"type":"object"})));
  let first = conversation.prepare(template(), vec![]).unwrap();
  conversation
    .commit(&first, run(client.send(first.request())).unwrap())
    .unwrap();
  let before = serde_json::to_value(&conversation).unwrap();
  assert!(conversation
    .record_tool_result(
      "capture-1",
      ImageContent::new(MediaResource::remote("https://assets.invalid/screenshot.png"))
    )
    .is_err());
  assert_eq!(serde_json::to_value(&conversation).unwrap(), before);
  assert_eq!(conversation.outstanding_tool_calls().len(), 1);
  conversation
    .record_tool_result("capture-1", vec![Content::text("Screenshot"), image()])
    .unwrap();
  let next = conversation
    .prepare(
      template(),
      vec![RetrievedContext::new(
        "reference",
        vec![Content::text("Reference"), image()],
      )],
    )
    .unwrap();
  let restored: Conversation = serde_json::from_slice(&serde_json::to_vec(&conversation).unwrap()).unwrap();
  assert_eq!(restored.retry().unwrap(), next);
  run(client.send(next.request())).unwrap();
  let body: Value = serde_json::from_slice(&sent.borrow()[1].body).unwrap();
  let output = body["input"]
    .as_array()
    .unwrap()
    .iter()
    .find(|item| item["type"] == "function_call_output")
    .unwrap();
  assert_eq!(output["output"][0]["text"], "Screenshot");
  assert_eq!(output["output"][1]["type"], "input_image");
  assert!(!conversation
    .history()
    .iter()
    .any(|message| message.content.text().contains("Reference")));
  let mut conversation = Conversation::new("remote-media").unwrap();
  let content: ContentBody = ImageContent::new(MediaResource::remote("https://assets.invalid/image.png")).into();
  assert!(conversation.submit_user(content.clone()).is_err());
  assert!(conversation.history().is_empty());
  assert!(conversation.pending().is_none());
  let content = content
    .try_map_resources(|_| Ok::<_, ()>(MediaResource::inline("image/png", vec![1, 2, 3])))
    .unwrap();
  let mut conversation = Conversation::new("retained-media").unwrap();
  conversation.submit_user(content).unwrap();
  conversation.prepare(GenerateRequest::builder("gpt-5"), vec![]).unwrap();

  let detached = GenerateRequest::builder("gpt-5")
    .user(ImageContent::new(MediaResource::asset(
      "image-asset",
      Some("v1".into()),
    )))
    .build()
    .unwrap();
  let resolved = detached
    .try_map_media(|_| Ok::<_, ()>(MediaResource::inline("image/png", vec![1, 2, 3])))
    .unwrap();
  let sent = RefCell::new(Vec::new());
  let client = media_client(&sent, Endpoint::Responses, [Reply::json(RESPONSES_JSON)]);
  assert!(run(client.send(&detached)).is_err());
  assert!(sent.borrow().is_empty());
  run(client.send(resolved)).unwrap();
}

pub fn multimedia_unsupported_fails_before_transport() {
  for content in [
    Content::Audio(AudioContent::new(MediaResource::inline("audio/wav", vec![1, 2, 3]))),
    Content::Video(VideoContent::new(MediaResource::inline("video/mp4", vec![1, 2, 3]))),
  ] {
    let sent = RefCell::new(Vec::new());
    let client = media_client(&sent, Endpoint::Messages, []);
    let request = GenerateRequest::builder("gpt-5")
      .max_output_tokens(64)
      .user(content)
      .build()
      .unwrap();
    assert!(run(client.send(request)).is_err());
    assert!(sent.borrow().is_empty());
  }
}
