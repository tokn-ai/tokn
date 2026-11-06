//! Semantic media codecs. These functions perform no fetching or transcoding.
//!
//! Unknown native content and provider-owned handles must retain their endpoint
//! provenance. A raw foreign block is never a valid substitute for conversion.

use alloc::{boxed::Box, format, string::String, string::ToString, vec::Vec};
use serde_json::{json, Value};
use tokn_core::content::{
  AudioContent, AudioFormat, Content, DocumentContent, ImageContent, ImageDetail, MediaResource, MediaSource,
  TextContent, VideoSource,
};

use crate::{error::ConvertError, ir::ContentPart, provider::Endpoint, Result};

pub(crate) fn unsupported(message: impl Into<String>) -> ConvertError {
  ConvertError::UnsupportedFeature {
    message: message.into(),
  }
}

fn invalid(error: impl core::fmt::Display) -> ConvertError {
  ConvertError::bad_shape("media", error.to_string())
}

fn string<'a>(value: &'a Value, field: &'static str) -> Result<&'a str> {
  value
    .get(field)
    .and_then(Value::as_str)
    .filter(|s| !s.is_empty())
    .ok_or_else(|| ConvertError::bad_shape(field, "expected a nonempty string"))
}

fn url_resource(url: &str) -> Result<MediaResource> {
  if url.starts_with("data:") {
    MediaResource::from_data_url(url).map_err(invalid)
  } else {
    let resource = MediaResource::remote(url);
    resource.validate().map_err(invalid)?;
    Ok(resource)
  }
}

fn image_detail(value: Option<&Value>) -> Result<Option<ImageDetail>> {
  value
    .map(|value| serde_json::from_value(value.clone()).map_err(ConvertError::from))
    .transpose()
}

/// Decode a known portable content block. `None` means the block requires its
/// native endpoint semantics (including a provider-owned reference).
pub fn content_from_part(endpoint: Endpoint, value: &Value) -> Result<Option<Content>> {
  let kind = value.get("type").and_then(Value::as_str).unwrap_or_default();
  let content = match (endpoint, kind) {
    (_, "text" | "input_text" | "output_text") => Content::Text(TextContent::new(
      value
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| ConvertError::bad_shape("text", "expected a string"))?,
    )),
    (Endpoint::ChatCompletions, "image_url") => {
      let source = &value["image_url"];
      let mut image = ImageContent::new(url_resource(string(source, "url")?)?);
      image.detail = image_detail(source.get("detail"))?;
      Content::Image(image)
    }
    (Endpoint::Responses, "input_image") => {
      if value.get("file_id").is_some() {
        return Ok(None);
      }
      let mut image = ImageContent::new(url_resource(string(value, "image_url")?)?);
      image.detail = image_detail(value.get("detail"))?;
      Content::Image(image)
    }
    (Endpoint::Messages, "image" | "document") => {
      let source = &value["source"];
      let resource = match source.get("type").and_then(Value::as_str) {
        Some("base64") => {
          MediaResource::from_base64(string(source, "media_type")?, string(source, "data")?).map_err(invalid)?
        }
        Some("url") => url_resource(string(source, "url")?)?,
        Some("text") if kind == "document" => {
          MediaResource::inline("text/plain", string(source, "data")?.as_bytes().to_vec())
        }
        _ => return Ok(None),
      };
      if kind == "image" {
        Content::Image(ImageContent::new(resource))
      } else {
        let mut document = DocumentContent::new(resource);
        document.title = value.get("title").and_then(Value::as_str).map(str::to_string);
        Content::Document(document)
      }
    }
    (Endpoint::ChatCompletions, "input_audio") => {
      let input = &value["input_audio"];
      let format = match string(input, "format")? {
        "wav" => AudioFormat::Wav,
        "mp3" => AudioFormat::Mp3,
        _ => {
          return Err(unsupported(
            "Chat audio input supports only wav or mp3 without transcoding",
          ))
        }
      };
      let resource = MediaResource::from_base64(audio_mime(&format), string(input, "data")?).map_err(invalid)?;
      let mut audio = AudioContent::new(resource);
      audio.format = Some(format);
      Content::Audio(audio)
    }
    (Endpoint::ChatCompletions, "file") | (Endpoint::Responses, "input_file") => {
      let file = if endpoint == Endpoint::ChatCompletions {
        &value["file"]
      } else {
        value
      };
      if file.get("file_id").is_some() {
        return Ok(None);
      }
      let resource = if let Some(data) = file.get("file_data").and_then(Value::as_str) {
        if !data.starts_with("data:") {
          return Err(unsupported(
            "file_data without a MIME-bearing data URL cannot be converted safely",
          ));
        }
        MediaResource::from_data_url(data).map_err(invalid)?
      } else {
        url_resource(string(file, "file_url")?)?
      };
      let mut document = DocumentContent::new(resource);
      document.filename = file.get("filename").and_then(Value::as_str).map(str::to_string);
      Content::Document(document)
    }
    _ => return Ok(None),
  };
  content.validate().map_err(invalid)?;
  Ok(Some(content))
}

/// A complete Chat audio output. Its format is optional because the wire reply
/// may require the request's audio format to interpret its bytes.
pub fn audio_from_chat(value: &Value) -> Result<Content> {
  let format: Option<AudioFormat> = value
    .get("format")
    .map(|format| serde_json::from_value(format.clone()))
    .transpose()?;
  let resource = MediaResource::from_base64(
    format.as_ref().map(audio_mime).unwrap_or("application/octet-stream"),
    string(value, "data")?,
  )
  .map_err(invalid)?;
  let mut audio = AudioContent::new(resource);
  if format.is_none() {
    audio.resource.media_type = None;
  }
  audio.format = format;
  if let Some(text) = value.get("transcript").and_then(Value::as_str) {
    audio.transcript = Some(tokn_core::content::TranscriptContent::new(text));
  }
  Ok(Content::Audio(audio))
}

/// Decode a completed Responses image-generation item.
pub fn image_from_responses(value: &Value) -> Result<Content> {
  let mime = match value.get("output_format").and_then(Value::as_str) {
    Some("png") => Some("image/png"),
    Some("jpeg" | "jpg") => Some("image/jpeg"),
    Some("webp") => Some("image/webp"),
    None => None,
    _ => return Err(unsupported("unknown generated image format")),
  };
  let mut resource =
    MediaResource::from_bytes(tokn_core::content::decode_base64(string(value, "result")?).map_err(invalid)?);
  resource.media_type = mime.map(str::to_string);
  resource.validate().map_err(invalid)?;
  Ok(Content::Image(ImageContent::new(resource)))
}

pub(crate) fn image_output_format(image: &ImageContent) -> Result<Option<&'static str>> {
  match image.resource.media_type.as_deref() {
    Some("image/png") => Ok(Some("png")),
    Some("image/jpeg") => Ok(Some("jpeg")),
    Some("image/webp") => Ok(Some("webp")),
    None => Ok(None),
    _ => Err(unsupported("generated image has an unsupported encoding")),
  }
}

fn audio_mime(format: &AudioFormat) -> &'static str {
  match format {
    AudioFormat::Wav => "audio/wav",
    AudioFormat::Mp3 => "audio/mpeg",
    AudioFormat::Flac => "audio/flac",
    AudioFormat::Opus => "audio/opus",
    AudioFormat::Pcm16 => "audio/pcm",
    AudioFormat::Aac => "audio/aac",
  }
}

fn resource_url(resource: &MediaResource) -> Result<String> {
  match &resource.source {
    MediaSource::Inline { .. } => resource.data_url().map_err(invalid),
    MediaSource::Remote { url } => Ok(url.clone()),
    _ => Err(unsupported("this endpoint needs resolved media bytes or a URL")),
  }
}

fn file_id(resource: &MediaResource, endpoint: Endpoint) -> Result<Option<&str>> {
  match &resource.source {
    MediaSource::Provider(reference) => {
      let namespace = if endpoint == Endpoint::Messages {
        "anthropic.files"
      } else {
        "openai.files"
      };
      if reference.namespace != namespace {
        return Err(unsupported("provider media namespace does not match the endpoint"));
      }
      Ok(Some(&reference.id))
    }
    MediaSource::Asset(_) => Err(unsupported("application assets must be resolved before encoding")),
    _ => Ok(None),
  }
}

fn messages_source(resource: &MediaResource) -> Result<Value> {
  if let Some(id) = file_id(resource, Endpoint::Messages)? {
    return Ok(json!({"type":"file", "file_id":id}));
  }
  match &resource.source {
    MediaSource::Inline { data } if resource.media_type.as_deref() == Some("text/plain") => {
      let text = core::str::from_utf8(data).map_err(invalid)?;
      Ok(json!({"type":"text", "media_type":"text/plain", "data":text}))
    }
    MediaSource::Inline { .. } => Ok(
      json!({"type":"base64", "media_type":resource.media_type.as_deref().ok_or_else(|| unsupported("inline Messages media requires a MIME type"))?, "data":resource.encode_base64()}),
    ),
    MediaSource::Remote { url } => Ok(json!({"type":"url", "url":url})),
    _ => Err(unsupported("unresolved Messages media resource")),
  }
}

/// Encode application content for a concrete message role. This never fetches
/// resources, transcodes codecs, samples video, or silently flattens a transcript.
pub fn content_to_parts(endpoint: Endpoint, content: &Content, role: &str) -> Result<Vec<Value>> {
  content.validate().map_err(invalid)?;
  let text_type = if endpoint == Endpoint::Responses {
    if role == "assistant" {
      "output_text"
    } else {
      "input_text"
    }
  } else {
    "text"
  };
  let part = match content {
    Content::Text(text) => json!({"type":text_type, "text":text.text}),
    Content::Transcript(transcript) => {
      // A labelled JSON document retains timing and speaker semantics in the
      // text-only representation accepted by these conversational endpoints.
      json!({"type":text_type, "text":serde_json::to_string(&json!({"transcript":{"text":transcript.text,"segments":transcript.segments,"language":transcript.language}}))?})
    }
    Content::Image(image) => {
      if matches!(role, "system" | "developer" | "assistant" | "tool") {
        return Err(unsupported(
          "this endpoint has no portable image content for the requested role",
        ));
      }
      if image
        .resource
        .media_type
        .as_deref()
        .is_some_and(|mime| !matches!(mime, "image/jpeg" | "image/png" | "image/gif" | "image/webp"))
      {
        return Err(unsupported(
          "image input requires JPEG, PNG, GIF, or WebP bytes without transcoding",
        ));
      }
      match endpoint {
        Endpoint::ChatCompletions => {
          if image.detail == Some(ImageDetail::Original) {
            return Err(unsupported("Chat cannot preserve original image detail"));
          }
          if matches!(image.resource.source, MediaSource::Provider(_)) {
            return Err(unsupported("Chat images require a URL or inline bytes"));
          }
          let mut image_url = json!({"url":resource_url(&image.resource)?});
          if let Some(detail) = &image.detail {
            image_url["detail"] = serde_json::to_value(detail)?;
          }
          json!({"type":"image_url", "image_url":image_url})
        }
        Endpoint::Responses => {
          let mut part = if let Some(id) = file_id(&image.resource, endpoint)? {
            json!({"type":"input_image", "file_id":id})
          } else {
            json!({"type":"input_image", "image_url":resource_url(&image.resource)?})
          };
          if let Some(detail) = &image.detail {
            part["detail"] = serde_json::to_value(detail)?;
          }
          part
        }
        Endpoint::Messages => {
          if image.detail.is_some_and(|detail| detail != ImageDetail::Auto) {
            return Err(unsupported("Messages cannot preserve the requested image detail hint"));
          }
          if image
            .resource
            .media_type
            .as_deref()
            .is_some_and(|mime| !matches!(mime, "image/jpeg" | "image/png" | "image/gif" | "image/webp"))
          {
            return Err(unsupported("Messages images require JPEG, PNG, GIF, or WebP bytes"));
          }
          json!({"type":"image", "source":messages_source(&image.resource)?})
        }
      }
    }
    Content::Audio(audio) => {
      if endpoint != Endpoint::ChatCompletions || role != "user" {
        return Err(unsupported("portable audio input requires a Chat user message"));
      }
      let format = audio
        .format
        .as_ref()
        .ok_or_else(|| unsupported("audio input requires an explicit encoding"))?;
      if !matches!(format, AudioFormat::Wav | AudioFormat::Mp3) {
        return Err(unsupported(
          "Chat audio input requires wav or mp3; transcoding is explicit",
        ));
      }
      json!({"type":"input_audio", "input_audio":{"data":audio.resource.encode_base64().ok_or_else(|| unsupported("Chat audio input requires resolved inline bytes"))?, "format":format}})
    }
    Content::Document(document) => {
      if matches!(role, "system" | "developer" | "assistant" | "tool") {
        return Err(unsupported("document content is unsupported for the requested role"));
      }
      let id = file_id(&document.resource, endpoint)?;
      match endpoint {
        Endpoint::Messages => {
          if document
            .resource
            .media_type
            .as_deref()
            .is_some_and(|mime| !matches!(mime, "application/pdf" | "text/plain"))
          {
            return Err(unsupported(
              "Messages documents support PDF or plain text without extraction",
            ));
          }
          let mut part = json!({"type":"document", "source":messages_source(&document.resource)?});
          if let Some(title) = document.title.as_ref().or(document.filename.as_ref()) {
            part["title"] = Value::String(title.clone());
          }
          part
        }
        Endpoint::ChatCompletions | Endpoint::Responses => {
          if document.title.is_some() {
            return Err(unsupported(
              "the target endpoint cannot preserve a document title independently of its filename",
            ));
          }
          if endpoint == Endpoint::ChatCompletions
            && document
              .resource
              .media_type
              .as_deref()
              .is_some_and(|mime| mime != "application/pdf")
          {
            return Err(unsupported("Chat document inputs support PDF files without extraction"));
          }
          let mut file = if let Some(id) = id {
            json!({"file_id":id})
          } else {
            match &document.resource.source {
              MediaSource::Inline { .. } => json!({"file_data":document.resource.data_url().map_err(invalid)?}),
              MediaSource::Remote { url } if endpoint == Endpoint::Responses => json!({"file_url":url}),
              _ => {
                return Err(unsupported(
                  "Chat file inputs require inline bytes or a scoped file reference",
                ))
              }
            }
          };
          if let Some(filename) = &document.filename {
            file["filename"] = Value::String(filename.clone());
          }
          if endpoint == Endpoint::ChatCompletions {
            json!({"type":"file", "file":file})
          } else {
            file["type"] = json!("input_file");
            file
          }
        }
      }
    }
    Content::Video(video) => {
      let VideoSource::Frames(frames) = &video.source else {
        return Err(unsupported(
          "native video input is unsupported by these endpoints; supply explicit timestamped frames",
        ));
      };
      let mut parts = Vec::new();
      for frame in frames {
        parts.push(json!({"type":text_type, "text":format!("Video frame at {} ms", frame.timestamp_ms)}));
        parts.extend(content_to_parts(endpoint, &Content::Image(frame.image.clone()), role)?);
      }
      return Ok(parts);
    }
  };
  Ok(vec![part])
}

pub(crate) fn parse_part(endpoint: Endpoint, value: &Value) -> Result<ContentPart> {
  match content_from_part(endpoint, value)? {
    Some(Content::Text(text)) => Ok(ContentPart::Text { text: text.text }),
    Some(content) => Ok(ContentPart::Media {
      content: Box::new(content),
    }),
    None => Ok(ContentPart::Native {
      endpoint: endpoint.as_str().into(),
      value: value.clone(),
    }),
  }
}

pub(crate) fn native_part(endpoint: Endpoint, origin: &str, value: &Value) -> Result<Value> {
  if origin == endpoint.as_str() {
    Ok(value.clone())
  } else {
    Err(unsupported(format!(
      "{} content cannot be translated from {origin} to {endpoint}",
      value["type"].as_str().unwrap_or("native")
    )))
  }
}

pub(crate) fn raw_to_parts(endpoint: Endpoint, value: &Value, role: &str) -> Result<Vec<Value>> {
  let content = content_from_part(endpoint, value)?
    .ok_or_else(|| unsupported("raw content has no verified representation in the destination endpoint"))?;
  content_to_parts(endpoint, &content, role)
}

pub(crate) fn tool_parts(endpoint: Endpoint, content: &Value) -> Result<Option<Vec<Content>>> {
  let Some(parts) = content.as_array() else {
    return Ok(None);
  };
  parts
    .iter()
    .map(|part| {
      content_from_part(endpoint, part)?
        .ok_or_else(|| unsupported("tool result contains a native block that cannot be portably converted"))
    })
    .collect::<Result<Vec<_>>>()
    .map(Some)
}

pub(crate) fn render_tool_parts(endpoint: Endpoint, parts: &[Content]) -> Result<Value> {
  if endpoint == Endpoint::ChatCompletions {
    let mut text = String::new();
    for part in parts {
      let Content::Text(part) = part else {
        return Err(unsupported(
          "Chat tool results support text only; project multimedia explicitly",
        ));
      };
      text.push_str(&part.text);
    }
    return Ok(Value::String(text));
  }
  let mut rendered = Vec::new();
  for part in parts {
    rendered.extend(content_to_parts(endpoint, part, "user")?);
  }
  Ok(Value::Array(rendered))
}

/// Check endpoint representability without decoding/copying binary media. Model
/// capability checks and provider-reference scope checks belong to routing.
pub fn validate_request(from: Endpoint, to: Endpoint, body: &Value) -> Result<()> {
  if from == to {
    return Ok(());
  }
  if let Some(parts) = body.get("system").and_then(Value::as_array) {
    validate_parts(from, to, parts, "system")?;
  }
  if (body
    .get("modalities")
    .and_then(Value::as_array)
    .is_some_and(|values| values.iter().any(|v| v == "audio"))
    || body.get("audio").is_some())
    && to != Endpoint::ChatCompletions
  {
    return Err(unsupported("audio output requires Chat Completions"));
  }
  if body
    .get("tools")
    .and_then(Value::as_array)
    .is_some_and(|tools| tools.iter().any(|t| t["type"] == "image_generation"))
    && to != Endpoint::Responses
  {
    return Err(unsupported("image generation requires the Responses endpoint"));
  }
  let items = body
    .get(if from == Endpoint::Responses {
      "input"
    } else {
      "messages"
    })
    .and_then(Value::as_array);
  if let Some(items) = items {
    for item in items {
      if item["type"] == "image_generation_call" && to != Endpoint::Responses {
        return Err(unsupported(
          "generated image history cannot be translated to this endpoint",
        ));
      }
      if from == Endpoint::Responses && item["type"] == "reasoning" {
        // Reasoning items have their own content/summary vocabulary and are
        // decoded by the reasoning adapter, not the message-media codec.
        continue;
      }
      if let Some(parts) = item
        .get("content")
        .or_else(|| item.get("output"))
        .and_then(Value::as_array)
      {
        let role = if matches!(
          item["type"].as_str(),
          Some("function_call_output" | "custom_tool_call_output")
        ) {
          "tool"
        } else {
          item["role"].as_str().unwrap_or("user")
        };
        validate_parts(from, to, parts, role)?;
      }
      if item.get("audio").is_some() && to != Endpoint::ChatCompletions {
        return Err(unsupported("Chat audio history cannot be translated to this endpoint"));
      }
    }
  }
  Ok(())
}

fn validate_parts(from: Endpoint, to: Endpoint, parts: &[Value], role: &str) -> Result<()> {
  for part in parts {
    let kind = part["type"].as_str().unwrap_or_default();
    if !matches!(
      kind,
      "text"
        | "input_text"
        | "output_text"
        | "thinking"
        | "redacted_thinking"
        | "tool_use"
        | "tool_result"
        | "image"
        | "input_image"
        | "image_url"
        | "document"
        | "file"
        | "input_file"
        | "input_audio"
    ) {
      return Err(unsupported(
        "native content has no verified representation in the target endpoint",
      ));
    }
    if matches!(kind, "input_audio" | "audio" | "output_audio") && to != Endpoint::ChatCompletions {
      return Err(unsupported("audio has no representation in the target endpoint"));
    }
    if matches!(kind, "video" | "video_url" | "input_video") {
      return Err(unsupported("native video has no cross-endpoint representation"));
    }
    if part.get("file_id").is_some()
      || part.pointer("/source/file_id").is_some()
      || part.pointer("/file/file_id").is_some()
    {
      return Err(unsupported(
        "unscoped provider file references cannot cross endpoint boundaries",
      ));
    }
    if kind == "tool_result" {
      if let Some(nested) = part["content"].as_array() {
        validate_parts(from, to, nested, "tool")?;
      }
    }
    if matches!(
      kind,
      "image" | "input_image" | "image_url" | "document" | "file" | "input_file"
    ) {
      if matches!(role, "assistant" | "system" | "developer") || (role == "tool" && to == Endpoint::ChatCompletions) {
        return Err(unsupported("the target message role cannot contain this media"));
      }
      let mime = part_media_type(part);
      if matches!(kind, "image" | "input_image" | "image_url") {
        let detail = part
          .get("detail")
          .or_else(|| part.pointer("/image_url/detail"))
          .and_then(Value::as_str);
        if to == Endpoint::Messages && detail.is_some_and(|detail| detail != "auto") {
          return Err(unsupported("Messages cannot preserve the requested image detail hint"));
        }
        if to == Endpoint::ChatCompletions && detail == Some("original") {
          return Err(unsupported("Chat cannot preserve original image detail"));
        }
        if mime.is_some_and(|mime| !matches!(mime, "image/jpeg" | "image/png" | "image/gif" | "image/webp")) {
          return Err(unsupported(
            "the target endpoint cannot consume this image encoding without transcoding",
          ));
        }
      }
      if matches!(kind, "document" | "file" | "input_file") {
        if to != Endpoint::Messages && part.get("title").is_some_and(Value::is_string) {
          return Err(unsupported(
            "the target endpoint cannot preserve a document title independently of its filename",
          ));
        }
        if (to == Endpoint::Messages && mime.is_some_and(|mime| !matches!(mime, "application/pdf" | "text/plain")))
          || (to == Endpoint::ChatCompletions && mime.is_some_and(|mime| mime != "application/pdf"))
        {
          return Err(unsupported(
            "the target endpoint cannot consume this document encoding without extraction",
          ));
        }
      }
      if matches!(kind, "document" | "file" | "input_file")
        && to == Endpoint::ChatCompletions
        && (part.get("file_url").is_some() || part.pointer("/source/type").is_some_and(|v| v == "url"))
      {
        return Err(unsupported("Chat file input cannot use a remote URL"));
      }
    }
  }
  let _ = from;
  Ok(())
}

fn part_media_type(part: &Value) -> Option<&str> {
  if part.pointer("/source/type").is_some_and(|kind| kind == "text") {
    return Some("text/plain");
  }
  if let Some(mime) = part.pointer("/source/media_type").and_then(Value::as_str) {
    return Some(mime);
  }
  let data = part
    .get("image_url")
    .and_then(Value::as_str)
    .or_else(|| part.pointer("/image_url/url").and_then(Value::as_str))
    .or_else(|| part.get("file_data").and_then(Value::as_str))
    .or_else(|| part.pointer("/file/file_data").and_then(Value::as_str))?;
  data.strip_prefix("data:")?.split_once(';').map(|(mime, _)| mime)
}

/// Reject media events that the destination protocol cannot represent before
/// any text-only emitter can accidentally discard them.
pub fn validate_stream_event(from: Endpoint, to: Endpoint, value: &Value) -> Result<()> {
  if value
    .pointer("/choices/0/delta/audio")
    .is_some_and(|audio| !audio.is_null())
    && to != Endpoint::ChatCompletions
  {
    return Err(unsupported(
      "streamed audio has no representation in the target endpoint",
    ));
  }
  let kind = value["type"].as_str().unwrap_or_default();
  let image = kind.starts_with("response.image_generation_call.")
    || value
      .pointer("/item/type")
      .is_some_and(|kind| kind == "image_generation_call")
    || value
      .pointer("/response/output")
      .and_then(Value::as_array)
      .is_some_and(|items| items.iter().any(|item| item["type"] == "image_generation_call"));
  if image && to != Endpoint::Responses {
    return Err(unsupported("streamed generated images require a Responses destination"));
  }
  if from != to {
    if let Some(part) = value.get("part").or_else(|| value.get("content_block")) {
      let kind = part["type"].as_str().unwrap_or_default();
      if matches!(
        kind,
        "image" | "audio" | "video" | "document" | "output_audio" | "output_image"
      ) {
        return Err(unsupported(
          "native media output block has no representation in the target endpoint",
        ));
      }
    }
    if kind.contains("audio") || kind.contains("video") {
      return Err(unsupported(
        "native media event has no representation in the target endpoint",
      ));
    }
  }
  Ok(())
}
