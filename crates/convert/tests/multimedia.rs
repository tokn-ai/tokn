use serde_json::{json, Value};
use tokn_convert::{
  convert_request, convert_response,
  ir::ContentPart,
  media,
  provider::Endpoint,
  sse::{EndpointTranslator, EventTransformer, SseAccumulator, SseEvent},
};
use tokn_core::content::{
  Content, ImageContent, ImageDetail, MediaResource, TranscriptContent, TranscriptSegment, VideoContent, VideoFrame,
};

fn request(endpoint: Endpoint, parts: Vec<Value>) -> Value {
  if endpoint == Endpoint::Responses {
    json!({"model":"test-model","input":[{"role":"user","content":parts}]})
  } else {
    json!({"model":"test-model","messages":[{"role":"user","content":parts}]})
  }
}

fn parts(endpoint: Endpoint, request: &Value) -> &Vec<Value> {
  request[if endpoint == Endpoint::Responses {
    "input"
  } else {
    "messages"
  }][0]["content"]
    .as_array()
    .unwrap()
}

#[test]
fn images_convert_semantically_between_every_pair_and_keep_bytes_and_order() {
  let endpoints = [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages];
  let image = Content::Image(ImageContent::new(MediaResource::inline(
    "image/png",
    b"image bytes".to_vec(),
  )));
  for from in endpoints {
    let mut source = media::content_to_parts(from, &Content::text("before"), "user").unwrap();
    source.extend(media::content_to_parts(from, &image, "user").unwrap());
    source.extend(media::content_to_parts(from, &Content::text("after"), "user").unwrap());
    for to in endpoints {
      let converted = convert_request(from, to, &request(from, source.clone())).unwrap();
      let output = parts(to, &converted);
      assert_eq!(output[0]["text"], "before");
      assert_eq!(output[2]["text"], "after");
      assert_eq!(media::content_from_part(to, &output[1]).unwrap().unwrap(), image);
      assert_eq!(
        output[1]["type"],
        match to {
          Endpoint::ChatCompletions => "image_url",
          Endpoint::Responses => "input_image",
          Endpoint::Messages => "image",
        }
      );
    }
  }
}

#[test]
fn documents_and_nested_tool_media_keep_their_resource_semantics() {
  let source = request(
    Endpoint::Messages,
    vec![json!({"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"cGRm"}})],
  );
  let responses = convert_request(Endpoint::Messages, Endpoint::Responses, &source).unwrap();
  assert_eq!(
    parts(Endpoint::Responses, &responses)[0]["file_data"],
    "data:application/pdf;base64,cGRm"
  );
  let chat = convert_request(Endpoint::Responses, Endpoint::ChatCompletions, &responses).unwrap();
  assert_eq!(
    parts(Endpoint::ChatCompletions, &chat)[0]["file"]["file_data"],
    "data:application/pdf;base64,cGRm"
  );

  let tool = json!({"model":"test-model","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":[{"type":"text","text":"chart"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aW1n"}}]}]}]});
  let responses = convert_request(Endpoint::Messages, Endpoint::Responses, &tool).unwrap();
  assert_eq!(responses["input"][0]["output"][0]["type"], "input_text");
  assert_eq!(
    responses["input"][0]["output"][1]["image_url"],
    "data:image/png;base64,aW1n"
  );
  let messages = convert_request(Endpoint::Responses, Endpoint::Messages, &responses).unwrap();
  assert_eq!(
    messages["messages"][0]["content"][0]["content"][1]["source"]["data"],
    "aW1n"
  );
  assert!(convert_request(Endpoint::Messages, Endpoint::ChatCompletions, &tool).is_err());
}

#[test]
fn unsupported_sources_and_native_metadata_fail_instead_of_being_forwarded() {
  for part in [
    json!({"type":"input_image","file_id":"file-1"}),
    json!({"type":"input_video","video_url":"https://example.invalid/clip.mp4"}),
    json!({"type":"future_media","bytes":"opaque"}),
  ] {
    let source = request(Endpoint::Responses, vec![part]);
    assert_eq!(
      convert_request(Endpoint::Responses, Endpoint::Responses, &source).unwrap(),
      source
    );
    assert!(convert_request(Endpoint::Responses, Endpoint::Messages, &source).is_err());
  }
  let detail = Content::Image(
    ImageContent::new(MediaResource::remote("https://example.invalid/image.png")).with_detail(ImageDetail::High),
  );
  assert!(media::content_to_parts(Endpoint::Messages, &detail, "user").is_err());
  let source = request(
    Endpoint::Responses,
    vec![json!({"type":"input_file","file_url":"https://example.invalid/report.pdf"})],
  );
  assert!(convert_request(Endpoint::Responses, Endpoint::ChatCompletions, &source).is_err());
  let title = request(
    Endpoint::Messages,
    vec![
      json!({"type":"document","title":"Report","source":{"type":"base64","media_type":"application/pdf","data":"cGRm"}}),
    ],
  );
  assert!(convert_request(Endpoint::Messages, Endpoint::Responses, &title).is_err());
}

#[test]
fn explicit_frames_and_transcripts_preserve_timing_without_sampling() {
  let image = ImageContent::new(MediaResource::inline("image/png", b"frame".to_vec()));
  let video = Content::Video(VideoContent::frames(vec![VideoFrame {
    timestamp_ms: 1234,
    image,
  }]));
  let output = media::content_to_parts(Endpoint::Responses, &video, "user").unwrap();
  assert_eq!(output[0]["text"], "Video frame at 1234 ms");
  assert_eq!(output[1]["type"], "input_image");
  assert!(media::content_to_parts(
    Endpoint::Responses,
    &Content::Video(VideoContent::new(MediaResource::remote(
      "https://example.invalid/clip.mp4"
    ))),
    "user"
  )
  .is_err());
  let mut transcript = TranscriptContent::new("hello");
  transcript.segments.push(TranscriptSegment {
    text: "hello".into(),
    start_ms: 100,
    end_ms: 500,
    speaker: Some("speaker-a".into()),
  });
  let output = media::content_to_parts(Endpoint::ChatCompletions, &Content::Transcript(transcript), "user").unwrap();
  let document: Value = serde_json::from_str(output[0]["text"].as_str().unwrap()).unwrap();
  assert_eq!(document["transcript"]["segments"][0]["speaker"], "speaker-a");
  assert_eq!(document["transcript"]["segments"][0]["start_ms"], 100);
}

#[test]
fn audio_is_typed_and_cross_endpoint_conversion_is_explicitly_rejected() {
  let source = request(
    Endpoint::ChatCompletions,
    vec![json!({"type":"input_audio","input_audio":{"format":"wav","data":"YXVkaW8="}})],
  );
  let ir = tokn_convert::value::chat::request_from_value(&source).unwrap();
  assert!(
    matches!(&ir.messages[0].content[0], ContentPart::Media { content } if matches!(content.as_ref(), Content::Audio(_)))
  );
  assert!(convert_request(Endpoint::ChatCompletions, Endpoint::Responses, &source).is_err());
  let response = json!({"choices":[{"message":{"role":"assistant","content":null,"audio":{"id":"audio-1","data":"YXVkaW8=","transcript":"spoken","expires_at":500}},"finish_reason":"stop"}]});
  let ir = tokn_convert::value::chat::response_from_value(&response).unwrap();
  let ContentPart::Media { content } = &ir.content[0] else {
    panic!("audio content missing")
  };
  let Content::Audio(audio) = content.as_ref() else {
    panic!("expected audio")
  };
  assert_eq!(audio.resource.bytes().unwrap().as_ref(), b"audio");
  assert_eq!(audio.resource.media_type, None);
  assert_eq!(audio.transcript.as_ref().unwrap().text, "spoken");
  assert_eq!(
    tokn_convert::value::chat::response_to_value(&ir).unwrap()["choices"][0]["message"]["audio"],
    response["choices"][0]["message"]["audio"]
  );
  assert!(convert_response(Endpoint::ChatCompletions, Endpoint::Responses, &response).is_err());
}

#[test]
fn streamed_audio_decodes_each_padded_chunk_and_retains_late_expiry() {
  let mut accumulator = SseAccumulator::new(Endpoint::ChatCompletions);
  for audio in [
    json!({"id":"audio-1","data":"YQ==","transcript":"a"}),
    json!({"data":"Yg==","transcript":"b"}),
    json!({"expires_at":500}),
  ] {
    accumulator
      .push_value(&json!({"choices":[{"delta":{"audio":audio},"finish_reason":null}]}))
      .unwrap();
  }
  accumulator.validate_complete().unwrap();
  let response = tokn_convert::value::chat::response_to_value(&accumulator.finish()).unwrap();
  assert_eq!(
    response["choices"][0]["message"]["audio"],
    json!({"id":"audio-1","data":"YWI=","transcript":"ab","expires_at":500})
  );
  let mut invalid = SseAccumulator::new(Endpoint::ChatCompletions);
  assert!(invalid
    .push_value(&json!({"choices":[{"delta":{"audio":{"data":"!invalid!"}}}]}))
    .is_err());
  let mut foreign = EndpointTranslator::new(Endpoint::ChatCompletions, Endpoint::Responses);
  assert!(foreign
    .transform(SseEvent::json(
      None,
      json!({"choices":[{"delta":{"audio":{"data":"YQ=="}}}]})
    ))
    .is_err());
}

#[test]
fn image_previews_are_provisional_and_completed_images_are_not_duplicated() {
  let preview = json!({"type":"response.image_generation_call.partial_image","output_index":0,"partial_image_index":0,"partial_image_b64":"cHJldmlldw=="});
  let item =
    json!({"id":"ig-1","type":"image_generation_call","status":"completed","result":"ZmluYWw=","output_format":"webp"});
  let mut accumulator = SseAccumulator::new(Endpoint::Responses);
  let deltas = accumulator.push_value(&preview).unwrap();
  assert!(matches!(&deltas[0], tokn_convert::ir::IrDelta::ImagePreview { .. }));
  assert!(accumulator.validate_complete().is_err());
  accumulator
    .push_value(&json!({"type":"response.output_item.done","output_index":0,"item":item}))
    .unwrap();
  accumulator
    .push_value(&json!({"type":"response.completed","response":{"status":"completed","output":[item]}}))
    .unwrap();
  accumulator.validate_complete().unwrap();
  let response = accumulator.finish();
  assert_eq!(response.content.len(), 1);
  let output = tokn_convert::value::responses::response_to_value(&response).unwrap();
  assert_eq!(output["output"][0]["result"], "ZmluYWw=");
  assert_eq!(output["output"][0]["output_format"], "webp");
  let mut foreign = EndpointTranslator::new(Endpoint::Responses, Endpoint::ChatCompletions);
  assert!(foreign
    .transform(SseEvent::json(
      Some("response.image_generation_call.partial_image"),
      preview
    ))
    .is_err());
}

#[test]
fn typed_endpoint_schemas_preserve_unknown_media_and_known_audio() {
  let unknown = json!({"type":"future_image","source":{"url":"https://example.invalid/image"}});
  let chat: tokn_endpoint_chat_completions::ContentPart = serde_json::from_value(unknown.clone()).unwrap();
  assert_eq!(serde_json::to_value(chat).unwrap(), unknown);
  let responses: tokn_endpoint_responses::OutputContentPart = serde_json::from_value(unknown.clone()).unwrap();
  assert_eq!(serde_json::to_value(responses).unwrap(), unknown);
  let messages: tokn_endpoint_messages::ContentBlock = serde_json::from_value(unknown.clone()).unwrap();
  assert_eq!(serde_json::to_value(messages).unwrap(), unknown);
  let audio = json!({"role":"assistant","audio":{"id":"audio-1","data":"YQ==","transcript":"a","expires_at":500}});
  let typed: tokn_endpoint_chat_completions::ChatMessage = serde_json::from_value(audio.clone()).unwrap();
  assert_eq!(serde_json::to_value(typed).unwrap(), audio);
}

#[test]
fn terminal_image_snapshots_preserve_interleaved_content_order() {
  let output = json!([
    {"type":"message","role":"assistant","content":[{"type":"output_text","text":"before"}]},
    {"type":"image_generation_call","id":"ig-1","status":"completed","result":"aW1n"},
    {"type":"message","role":"assistant","content":[{"type":"output_text","text":"after"}]}
  ]);
  for preceding_deltas in [false, true] {
    let mut accumulator = SseAccumulator::new(Endpoint::Responses);
    if preceding_deltas {
      accumulator
        .push_value(&json!({"type":"response.output_text.delta","output_index":0,"delta":"before"}))
        .unwrap();
      accumulator
        .push_value(&json!({"type":"response.output_text.delta","output_index":2,"delta":"after"}))
        .unwrap();
      accumulator
        .push_value(&json!({"type":"response.output_item.done","output_index":1,"item":output[1]}))
        .unwrap();
    }
    accumulator
      .push_value(&json!({"type":"response.completed","response":{"status":"completed","output":output}}))
      .unwrap();
    let response = tokn_convert::value::responses::response_to_value(&accumulator.finish()).unwrap();
    assert_eq!(response["output"][0]["content"][0]["text"], "before");
    assert_eq!(response["output"][1]["result"], "aW1n");
    assert_eq!(response["output"][2]["content"][0]["text"], "after");
  }
}

#[test]
fn responses_text_summary_does_not_duplicate_structured_content() {
  let response = json!({"output_text":"summary","output":[{"type":"message","content":[{"type":"output_text","text":"structured"}]}]});
  let ir = tokn_convert::value::responses::response_from_value(&response).unwrap();
  assert_eq!(ir.content.len(), 1);
  assert!(matches!(&ir.content[0], ContentPart::Text { text } if text == "structured"));
  let fallback =
    tokn_convert::value::responses::response_from_value(&json!({"output_text":"summary","output":[]})).unwrap();
  assert!(matches!(&fallback.content[0], ContentPart::Text { text } if text == "summary"));
}

#[test]
fn route_validation_matches_media_codec_constraints_without_decoding_bytes() {
  let high = request(
    Endpoint::Responses,
    vec![json!({"type":"input_image","detail":"high","image_url":"https://example.invalid/image.png"})],
  );
  assert!(media::validate_request(Endpoint::Responses, Endpoint::Messages, &high).is_err());
  assert!(media::validate_request(Endpoint::Responses, Endpoint::ChatCompletions, &high).is_ok());
  let titled = request(
    Endpoint::Messages,
    vec![json!({"type":"document","title":"Report","source":{"type":"text","media_type":"text/plain","data":"body"}})],
  );
  assert!(media::validate_request(Endpoint::Messages, Endpoint::Responses, &titled).is_err());
  assert!(media::validate_request(Endpoint::Messages, Endpoint::Messages, &titled).is_ok());
  let tool = json!({"model":"test-model","input":[{"type":"function_call_output","call_id":"call-1","output":[{"type":"input_image","image_url":"https://example.invalid/image.png"}]}]});
  assert!(media::validate_request(Endpoint::Responses, Endpoint::ChatCompletions, &tool).is_err());
  assert!(media::validate_request(Endpoint::Responses, Endpoint::Messages, &tool).is_ok());
  let docx = request(
    Endpoint::Responses,
    vec![
      json!({"type":"input_file","filename":"report.docx","file_data":"data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,bytes-are-not-decoded-by-route-validation"}),
    ],
  );
  assert!(media::validate_request(Endpoint::Responses, Endpoint::Messages, &docx).is_err());
  assert!(media::validate_request(Endpoint::Responses, Endpoint::ChatCompletions, &docx).is_err());
  let reasoning = json!({"model":"test-model","input":[{"type":"reasoning","summary":[{"type":"summary_text","text":"reason"}],"content":[{"type":"reasoning_text","text":"reason"}]},{"role":"user","content":"continue"}]});
  assert!(media::validate_request(Endpoint::Responses, Endpoint::ChatCompletions, &reasoning).is_ok());
}

#[test]
fn buffered_sse_preserves_audio_and_images_and_rejects_cross_endpoint_loss() {
  let audio = b"data: {\"choices\":[{\"delta\":{\"audio\":{\"id\":\"audio-1\",\"data\":\"YQ==\"}},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[{\"delta\":{\"audio\":{\"expires_at\":500}},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";
  let converted = tokn_convert::buffered::convert_response(
    Endpoint::ChatCompletions,
    Endpoint::ChatCompletions,
    audio.as_slice().to_vec().into(),
    Some("text/event-stream"),
    usize::MAX,
    |_| {},
  )
  .unwrap();
  assert_eq!(
    converted.body_json.unwrap()["choices"][0]["message"]["audio"]["expires_at"],
    500
  );
  assert!(tokn_convert::buffered::convert_response(
    Endpoint::ChatCompletions,
    Endpoint::Responses,
    audio.as_slice().to_vec().into(),
    Some("text/event-stream"),
    usize::MAX,
    |_| {}
  )
  .is_err());
  let image = b"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"image_generation_call\",\"id\":\"ig-1\",\"status\":\"completed\",\"result\":\"aW1n\",\"output_format\":\"png\"}]}}\n\n";
  let converted = tokn_convert::buffered::convert_response(
    Endpoint::Responses,
    Endpoint::Responses,
    image.as_slice().to_vec().into(),
    Some("text/event-stream"),
    usize::MAX,
    |_| {},
  )
  .unwrap();
  assert_eq!(converted.body_json.unwrap()["output"][0]["result"], "aW1n");
  assert!(tokn_convert::buffered::convert_response(
    Endpoint::Responses,
    Endpoint::Messages,
    image.as_slice().to_vec().into(),
    Some("text/event-stream"),
    usize::MAX,
    |_| {}
  )
  .is_err());
}
