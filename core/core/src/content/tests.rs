use alloc::{format, string::ToString};

use super::*;
use crate::types::Bytes;
use serde_json::json;

fn image() -> ImageContent {
  ImageContent::new(MediaResource::inline("image/png", Bytes::from_static(b"image fixture")))
}

fn scope() -> ProviderAssetScope {
  ProviderAssetScope {
    provider: "provider-id".into(),
    account: "account-id".into(),
    backend: "a".repeat(64),
    upstream_account_digest: None,
  }
}

#[test]
fn text_history_keeps_legacy_serialization_and_ordered_media_round_trips() {
  let text = ContentBody::from("hello");
  assert_eq!(serde_json::to_value(&text).unwrap(), json!("hello"));
  assert_eq!(serde_json::from_value::<ContentBody>(json!("hello")).unwrap(), text);
  let body = ContentBody::from(vec![Content::text("before"), image().into(), Content::text("after")]);
  let serialized = serde_json::to_value(&body).unwrap();
  assert_eq!(serialized[1]["type"], "image");
  assert_eq!(serialized[1]["resource"]["source"]["data"], "aW1hZ2UgZml4dHVyZQ==");
  assert_eq!(serde_json::from_value::<ContentBody>(serialized).unwrap(), body);
  assert_eq!(body.text(), "beforeafter");
  assert!(!body.is_text_only());
  assert_eq!(
    body.iter().map(ContentRef::kind).collect::<Vec<_>>(),
    vec![MediaKind::Text, MediaKind::Image, MediaKind::Text]
  );
  assert!(!ContentBody::from(image()).is_blank());
  assert!(ContentBody::from(" \n").is_blank());
}

#[test]
fn resources_check_bytes_and_encoding_without_exposing_payloads_in_debug() {
  let resource = MediaResource::from_data_url("data:image/png;base64,c2VjcmV0LWJ5dGVz").unwrap();
  assert_eq!(resource.bytes().unwrap().as_ref(), b"secret-bytes");
  assert_eq!(resource.data_url().unwrap(), "data:image/png;base64,c2VjcmV0LWJ5dGVz");
  assert!(!format!("{resource:?}").contains("secret-bytes"));
  assert!(!format!("{resource:?}").contains("c2VjcmV0"));
  let mut bad_digest = resource.clone();
  bad_digest.digest = Some("0".repeat(64));
  assert_eq!(bad_digest.validate(), Err(ContentError::DigestMismatch));
  assert_eq!(
    MediaResource::from_base64("image/png", "!!!!"),
    Err(ContentError::InvalidBase64)
  );
  assert_eq!(
    MediaResource::from_data_url("data:image/png,raw"),
    Err(ContentError::InvalidDataUrl)
  );
  assert_eq!(
    MediaResource::inline("image/png", Bytes::new()).validate(),
    Err(ContentError::EmptyResource)
  );
  assert_eq!(
    MediaResource::remote("file:///not-network").validate(),
    Err(ContentError::InvalidUrl)
  );
  assert_eq!(
    MediaResource::remote("https://user:secret@example.invalid/image").validate(),
    Err(ContentError::InvalidUrl)
  );
  let signed = MediaResource::remote("https://example.invalid/image?token=private");
  assert!(signed.validate().is_ok());
  assert!(!format!("{signed:?}").contains("private"));
}

#[test]
fn provider_references_keep_scope_and_expiry_but_redact_debug() {
  let reference = ProviderAssetReference {
    id: "private-file-id".into(),
    namespace: "openai.files".into(),
    scope: scope(),
    expires_at: Some(123),
  };
  let resource = MediaResource::provider(reference.clone());
  resource.validate().unwrap();
  let restored: MediaResource = serde_json::from_value(serde_json::to_value(&resource).unwrap()).unwrap();
  assert_eq!(restored, resource);
  let debug = format!("{resource:?}");
  assert!(!debug.contains("private-file-id"));
  assert!(!debug.contains("account-id"));
  let mut invalid = reference;
  invalid.scope.backend = "https://backend.invalid/".into();
  assert_eq!(invalid.validate(), Err(ContentError::InvalidDigest));
}

#[test]
fn video_and_transcript_validation_preserves_timing_semantics() {
  let mut video = VideoContent::frames(vec![
    VideoFrame {
      timestamp_ms: 0,
      image: image(),
    },
    VideoFrame {
      timestamp_ms: 100,
      image: image(),
    },
  ]);
  video.validate().unwrap();
  video.duration_ms = Some(50);
  assert_eq!(video.validate(), Err(ContentError::InvalidFrameOrder));
  let mut transcript = TranscriptContent::new("two speakers overlap");
  transcript.segments = vec![
    TranscriptSegment {
      text: "first".into(),
      start_ms: 0,
      end_ms: 200,
      speaker: Some("one".into()),
    },
    TranscriptSegment {
      text: "second".into(),
      start_ms: 100,
      end_ms: 300,
      speaker: Some("two".into()),
    },
  ];
  transcript.validate().unwrap();
  transcript.segments[1].end_ms = 50;
  assert_eq!(transcript.validate(), Err(ContentError::InvalidTranscriptTiming));
  let mut audio = AudioContent::new(MediaResource::inline("audio/wav", Bytes::from_static(b"audio fixture")));
  audio.format = Some(AudioFormat::Mp3);
  assert_eq!(audio.validate(), Err(ContentError::AudioFormatMismatch));
}

#[test]
fn recursive_resource_resolution_is_atomic_and_visits_nested_media() {
  let asset_image = ImageContent::new(MediaResource::asset("image", Some("v1".into())));
  let mut transcript = TranscriptContent::new("speech");
  transcript.source = Some(MediaResource::asset("audio", Some("v2".into())));
  let body = ContentBody::from(vec![
    VideoContent::frames(vec![VideoFrame {
      timestamp_ms: 0,
      image: asset_image,
    }])
    .into(),
    transcript.into(),
  ]);
  let original = body.clone();
  let mut visits = 0;
  body.visit_resources(&mut |_| visits += 1);
  assert_eq!(visits, 2);
  let error = body.try_map_resources(|resource| {
    let MediaSource::Asset(reference) = &resource.source else {
      panic!("expected asset")
    };
    if reference.id == "audio" {
      return Err("unavailable");
    }
    Ok(image().resource)
  });
  assert_eq!(error.unwrap_err(), "unavailable");
  assert_eq!(body, original);
  let resolved = body
    .try_map_resources(|resource| {
      let MediaSource::Asset(reference) = &resource.source else {
        panic!("expected asset")
      };
      Ok::<_, ContentError>(MediaResource::inline(
        if reference.id == "audio" {
          "audio/wav"
        } else {
          "image/png"
        },
        Bytes::from_static(b"resolved fixture"),
      ))
    })
    .unwrap();
  resolved.validate().unwrap();
  resolved.visit_resources(&mut |resource| assert!(resource.bytes().is_some()));
  assert_eq!(resolved.text(), "speech");
  assert_eq!(
    ContentError::DigestMismatch.to_string(),
    "media bytes do not match their digest"
  );
}
