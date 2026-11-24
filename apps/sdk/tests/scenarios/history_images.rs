//! Reusable image replay checks. Private fixtures never appear in diagnostics.
use super::*;

const MESSAGES_TOOL: &[u8] = br#"{"id":"tool","type":"message","role":"assistant","content":[{"type":"tool_use","id":"capture-1","name":"capture","input":{}}],"stop_reason":"tool_use"}"#;

fn checked<T, E>(result: Result<T, E>, stage: &str) -> T {
  result.unwrap_or_else(|_| panic!("image replay failed during {stage}"))
}

fn restore(conversation: &Conversation) -> Conversation {
  let bytes = checked(serde_json::to_vec(conversation), "snapshot serialization");
  checked(serde_json::from_slice(&bytes), "snapshot restoration")
}

fn native_images(value: &Value, endpoint: Endpoint, images: &mut Vec<MediaResource>) {
  match value {
    Value::Array(values) => {
      for value in values {
        native_images(value, endpoint, images);
      }
    }
    Value::Object(object) => {
      let kind = object.get("type").and_then(Value::as_str);
      let resource = match (endpoint, kind) {
        (Endpoint::Responses, Some("input_image")) => Some(checked(
          MediaResource::from_data_url(value["image_url"].as_str().expect("inline image URL")),
          "Responses image decoding",
        )),
        (Endpoint::ChatCompletions, Some("image_url")) => Some(checked(
          MediaResource::from_data_url(value["image_url"]["url"].as_str().expect("inline image URL")),
          "Chat image decoding",
        )),
        (Endpoint::Messages, Some("image")) => Some(checked(
          MediaResource::from_base64(
            value["source"]["media_type"].as_str().expect("image media type"),
            value["source"]["data"].as_str().expect("inline image data"),
          ),
          "Messages image decoding",
        )),
        _ => None,
      };
      if let Some(resource) = resource {
        images.push(resource);
      } else {
        for value in object.values() {
          native_images(value, endpoint, images);
        }
      }
    }
    _ => {}
  }
}

fn check_wire(request: &Request, endpoint: Endpoint, image: &ImageContent, count: usize) -> Value {
  let body: Value = checked(serde_json::from_slice(&request.body), "outgoing JSON decoding");
  let mut images = Vec::new();
  native_images(&body, endpoint, &mut images);
  assert!(images.len() == count, "outgoing image count changed");
  assert!(
    images.iter().all(|resource| resource == &image.resource),
    "outgoing image bytes or MIME type changed"
  );
  body
}

/// Exercise an inline PNG/JPEG through all native endpoints, persisted pending
/// requests, native continuation, retrieved context, and image tool results.
/// Returns the number of requests handled entirely by the memory transport.
pub fn multimedia_history_image(image: ImageContent) -> usize {
  let content = Content::Image(image.clone());
  let encoded = checked(serde_json::to_vec(&content), "content serialization");
  let decoded: Content = checked(serde_json::from_slice(&encoded), "content restoration");
  assert!(decoded == content, "content round trip changed the image");
  let ordered = vec![Content::text("before"), content.clone(), Content::text("after")];
  let mut sends = 0;

  for (endpoint, reply) in [
    (Endpoint::Responses, RESPONSES_JSON),
    (Endpoint::ChatCompletions, CHAT_JSON),
    (Endpoint::Messages, MESSAGES_JSON),
  ] {
    let sent = RefCell::new(Vec::new());
    let client = media_client(&sent, endpoint, [Reply::json(reply), Reply::json(reply)]);
    let mut conversation = checked(Conversation::new("image-replay"), "conversation creation");
    checked(conversation.submit_user(ordered.clone()), "image intake");
    let prepared = checked(
      conversation.prepare(
        GenerateRequest::builder("gpt-5").max_output_tokens(64),
        vec![RetrievedContext::new("reference", content.clone())],
      ),
      "image and context preparation",
    );
    let mut restored = restore(&conversation);
    assert!(
      checked(restored.retry(), "restored retry") == prepared,
      "restored pending request changed"
    );
    let response = checked(run(client.send(prepared.request())), "first generation");
    checked(restored.commit(&prepared, response), "first commit");
    assert!(
      restored.history()[0].content.clone().into_parts() == ordered,
      "conversation history changed image ordering"
    );
    // Retrieved context stays outside portable history while the provider's
    // native continuation retains what it already consumed.
    assert!(restored.history().len() == 2, "retrieval entered portable history");
    let body = check_wire(&sent.borrow()[0], endpoint, &image, 2);
    let messages = body[if endpoint == Endpoint::Responses {
      "input"
    } else {
      "messages"
    }]
    .as_array()
    .expect("outgoing messages");
    let parts = messages
      .iter()
      .filter_map(|message| message["content"].as_array())
      .find(|parts| parts.first().is_some_and(|part| part["text"] == "before"))
      .expect("original image-bearing message");
    assert!(
      parts.len() == 3 && parts[2]["text"] == "after",
      "native content ordering changed"
    );

    let mut restored = restore(&restored);
    checked(restored.submit_user("Describe it again"), "followup intake");
    let next = checked(
      restored.prepare(GenerateRequest::builder("gpt-5").max_output_tokens(64), vec![]),
      "followup preparation",
    );
    let response = checked(run(client.send(next.request())), "continued generation");
    assert!(
      response.continuation_status == ContinuationStatus::Continued,
      "native continuation was lost"
    );
    checked(restored.commit(&next, response), "followup commit");
    check_wire(&sent.borrow()[1], endpoint, &image, 2);
    sends += sent.borrow().len();
  }

  for (endpoint, tool_reply, final_reply) in [
    (Endpoint::Responses, TOOL, RESPONSES_JSON),
    (Endpoint::Messages, MESSAGES_TOOL, MESSAGES_JSON),
  ] {
    let sent = RefCell::new(Vec::new());
    let client = media_client(&sent, endpoint, [Reply::json(tool_reply), Reply::json(final_reply)]);
    let mut conversation = checked(Conversation::new("image-tool-replay"), "tool conversation creation");
    checked(conversation.submit_user("Inspect the capture"), "tool prompt intake");
    let template = || GenerateRequest::builder("gpt-5").tool(Tool::function("capture", json!({"type":"object"})));
    let first = checked(conversation.prepare(template(), vec![]), "tool preparation");
    let response = checked(run(client.send(first.request())), "tool generation");
    checked(conversation.commit(&first, response), "tool call commit");
    checked(
      conversation.record_tool_result("capture-1", ordered.clone()),
      "image tool result intake",
    );
    let next = checked(conversation.prepare(template(), vec![]), "tool result preparation");
    let restored = restore(&conversation);
    assert!(
      checked(restored.retry(), "tool retry") == next,
      "restored tool result changed"
    );
    checked(run(client.send(next.request())), "tool result generation");
    check_wire(&sent.borrow()[1], endpoint, &image, 1);
    sends += sent.borrow().len();
  }
  sends
}
