use alloc::{
  collections::BTreeMap,
  string::{String, ToString},
  vec::Vec,
};

use crate::{conversation::GenerationBinding, ContinuationStatus, ProviderState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "std")]
use tokn_convert::ir::Usage as IrUsage;
use tokn_core::headers::HeaderMap;
use tokn_core::{
  content::{AudioFormat, Content, ContentBody},
  Endpoint,
};

/// Request evidence supplements omitted media metadata consistently for
/// previews, completed parts and buffered output. Provider metadata wins.
#[derive(Clone, Copy)]
pub(super) struct OutputContentMetadata {
  audio_format: Option<AudioFormat>,
  image_media_type: Option<&'static str>,
}

impl OutputContentMetadata {
  pub(super) fn for_request(request: &super::GenerateRequest) -> Self {
    let audio_format = request.audio_output.as_ref().map(|options| options.format).or_else(|| {
      match request.extras.get("audio")?.get("format")?.as_str()? {
        "wav" => Some(AudioFormat::Wav),
        "mp3" => Some(AudioFormat::Mp3),
        "flac" => Some(AudioFormat::Flac),
        "opus" => Some(AudioFormat::Opus),
        "pcm16" => Some(AudioFormat::Pcm16),
        "aac" => Some(AudioFormat::Aac),
        _ => None,
      }
    });
    let image_media_type = request
      .image_generation
      .as_ref()
      .map(|options| options.output_format.media_type())
      .or_else(|| image_tool_media_type(request.extras.get("tools")?));
    Self {
      audio_format,
      image_media_type,
    }
  }

  pub(super) fn enrich_content(self, content: &mut Content) {
    match content {
      Content::Audio(audio) => {
        if audio.format.is_none() {
          audio.format = audio
            .resource
            .media_type
            .as_deref()
            .and_then(AudioFormat::from_media_type)
            .or(self.audio_format);
        }
        if audio.resource.media_type.is_none() {
          audio.resource.media_type = audio.format.map(|format| format.media_type().into());
        }
      }
      Content::Image(image) if image.resource.media_type.is_none() => {
        image.resource.media_type = self.image_media_type.map(Into::into);
      }
      _ => {}
    }
  }

  #[cfg(feature = "std")]
  pub(super) fn enrich_event(self, event: &mut super::GenerateEvent) {
    match event {
      super::GenerateEvent::ContentCompleted { content, .. } | super::GenerateEvent::ImagePreview { content, .. } => {
        self.enrich_content(content);
      }
      _ => {}
    }
  }
}

fn image_tool_media_type(tools: &Value) -> Option<&'static str> {
  let mut media_type = None;
  for tool in tools
    .as_array()?
    .iter()
    .filter(|tool| tool.get("type").and_then(Value::as_str) == Some("image_generation"))
  {
    // PNG is the native image-generation tool's default only when a tool is
    // actually requested. Without that evidence an unknown type stays unknown.
    let current = match tool.get("output_format") {
      None => "image/png",
      Some(value) => match value.as_str()? {
        "png" => "image/png",
        "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        _ => return None,
      },
    };
    if media_type.is_some_and(|existing| existing != current) {
      return None;
    }
    media_type = Some(current);
  }
  media_type
}

/// Friendly buffered output from a provider-neutral generation.
#[derive(Debug)]
pub struct GenerateResponse {
  pub http_status: u16,
  pub headers: HeaderMap,
  pub id: Option<String>,
  pub model: Option<String>,
  pub status: Option<String>,
  pub finish_reason: Option<String>,
  pub text: String,
  /// Ordered text and media suitable for display or subsequent model input.
  pub content: Vec<Content>,
  pub reasoning: Option<String>,
  pub tool_calls: Vec<ToolCall>,
  pub usage: Option<Usage>,
  /// Endpoint used for `raw` and the semantic event stream.
  pub endpoint: Endpoint,
  /// Full payload in `endpoint` format after routing and endpoint conversion.
  pub raw: Value,
  /// Native checkpoint for the next compatible generation.
  pub provider_state: Option<ProviderState>,
  pub continuation_status: ContinuationStatus,
  pub(crate) generation_binding: Option<GenerationBinding>,
  pub(crate) prepared_request_digest: Option<String>,
}

impl GenerateResponse {
  pub(super) fn finish(
    request: &super::GenerateRequest,
    status: u16,
    headers: HeaderMap,
    raw: Value,
    continuation: Option<tokn_requests::session::ContinuationResponse>,
  ) -> crate::Result<Self> {
    let mut reply = Self::from_endpoint(status, headers, raw, request.source_endpoint())?;
    let metadata = OutputContentMetadata::for_request(request);
    for content in &mut reply.content {
      metadata.enrich_content(content);
    }
    if let Some(continuation) = continuation {
      reply.provider_state = Some(ProviderState::capture(request, &reply, continuation.state)?);
      reply.continuation_status = continuation.outcome;
    }
    if request.generation_binding.is_some() {
      reply.bind_prepared(request)?;
    }
    Ok(reply)
  }

  fn receipt(&self, request: &super::GenerateRequest) -> crate::Result<String> {
    super::state::digest(&(
      request,
      self.http_status,
      &self.status,
      &self.finish_reason,
      &self.text,
      &self.content,
      &self.tool_calls,
      &self.provider_state,
      self.continuation_status,
    ))
  }

  pub(crate) fn bind_prepared(&mut self, request: &super::GenerateRequest) -> crate::Result<()> {
    self.prepared_request_digest = Some(self.receipt(request)?);
    self.generation_binding = request.generation_binding.clone();
    Ok(())
  }

  pub(crate) fn matches_prepared(&self, request: &super::GenerateRequest) -> crate::Result<bool> {
    Ok(
      request.generation_binding.is_some()
        && self.generation_binding == request.generation_binding
        && self.prepared_request_digest.as_deref() == Some(self.receipt(request)?.as_str()),
    )
  }

  /// Portable assistant history entry corresponding to this generation.
  pub fn assistant_message(&self) -> super::Message {
    let content = if self.content.iter().all(|part| matches!(part, Content::Text(_))) {
      ContentBody::Text(self.text.clone())
    } else {
      ContentBody::Parts(self.content.clone())
    };
    super::Message::assistant_with_tool_calls(content, self.tool_calls.clone())
  }

  #[cfg(test)]
  pub(crate) fn from_raw(http_status: u16, headers: HeaderMap, raw: Value) -> Self {
    Self::from_endpoint(http_status, headers, raw, Endpoint::Responses).expect("valid response fixture")
  }

  fn from_endpoint(http_status: u16, headers: HeaderMap, raw: Value, endpoint: Endpoint) -> crate::Result<Self> {
    let ir = match endpoint {
      Endpoint::Responses => tokn_convert::value::responses::response_from_value(&raw),
      Endpoint::ChatCompletions => tokn_convert::value::chat::response_from_value(&raw),
      Endpoint::Messages => tokn_convert::value::messages::response_from_value(&raw),
    }
    .map_err(|source| crate::Error::DecodeGenerateResponse { source })?;
    for part in &ir.content {
      let value = match part {
        tokn_convert::ir::ContentPart::Native { value, .. } | tokn_convert::ir::ContentPart::Raw { value } => value,
        _ => continue,
      };
      if matches!(
        value.get("type").and_then(Value::as_str),
        Some(
          "output_audio"
            | "audio"
            | "input_audio"
            | "input_video"
            | "video"
            | "output_video"
            | "video_url"
            | "output_image"
            | "input_image"
            | "image"
            | "image_url"
            | "input_file"
            | "output_file"
            | "file"
            | "document"
            | "image_generation_call"
        )
      ) {
        return Err(crate::Error::DecodeGenerateResponse {
          source: tokn_convert::error::ConvertError::UnsupportedFeature {
            message: "the endpoint returned a media representation that cannot be decoded into portable content".into(),
          },
        });
      }
    }
    let mut content: Vec<_> = ir
      .content
      .iter()
      .filter_map(|part| match part {
        tokn_convert::ir::ContentPart::Text { text } => Some(Content::text(text)),
        tokn_convert::ir::ContentPart::Media { content } => Some(content.as_ref().clone()),
        _ => None,
      })
      .collect();
    let text = tokn_convert::ir::text_from_parts(&ir.content);
    if content.is_empty() && !text.is_empty() {
      content.push(Content::text(&text));
    }
    let reasoning = if endpoint == Endpoint::Responses {
      output_reasoning(&raw)
    } else {
      tokn_convert::ir::reasoning_from_parts(&ir.content)
    };
    let tool_calls = if endpoint == Endpoint::Responses {
      output_tool_calls(&raw)
    } else {
      ir.tool_calls
        .into_iter()
        .map(|call| ToolCall {
          id: call.id,
          name: call.name,
          arguments: call.arguments,
        })
        .collect()
    };
    let finish_reason = if endpoint == Endpoint::Responses {
      finish_reason(&raw, !tool_calls.is_empty())
    } else {
      ir.finish_reason
    };
    Ok(Self {
      http_status,
      headers,
      id: raw.get("id").and_then(Value::as_str).map(str::to_string),
      model: raw.get("model").and_then(Value::as_str).map(str::to_string),
      status: raw.get("status").and_then(Value::as_str).map(str::to_string),
      finish_reason,
      text,
      content,
      reasoning,
      tool_calls,
      usage: raw
        .get("usage")
        .filter(|usage| usage.is_object())
        .map(Usage::from_value),
      raw,
      endpoint,
      provider_state: None,
      continuation_status: ContinuationStatus::Started,
      generation_binding: None,
      prepared_request_digest: None,
    })
  }
}

/// A normalized tool call returned by a generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
  pub id: Option<String>,
  pub name: String,
  pub arguments: Value,
}

impl ToolCall {
  pub fn new(name: impl Into<String>, arguments: Value) -> Self {
    Self {
      id: None,
      name: name.into(),
      arguments,
    }
  }

  pub fn id(mut self, id: impl Into<String>) -> Self {
    self.id = Some(id.into());
    self
  }
}

/// Provider-neutral token usage.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Usage {
  pub input_tokens: Option<u64>,
  pub output_tokens: Option<u64>,
  pub total_tokens: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub input_tokens_details: Option<Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub output_tokens_details: Option<Value>,
  #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
  pub extras: BTreeMap<String, Value>,
}

impl Usage {
  fn from_value(value: &Value) -> Self {
    let Some(object) = value.as_object() else {
      return Self::default();
    };
    let mut extras: BTreeMap<String, Value> = object.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
    for key in [
      "input_tokens",
      "prompt_tokens",
      "output_tokens",
      "completion_tokens",
      "total_tokens",
      "input_tokens_details",
      "prompt_tokens_details",
      "output_tokens_details",
      "completion_tokens_details",
    ] {
      extras.remove(key);
    }
    Self {
      input_tokens: object
        .get("input_tokens")
        .or_else(|| object.get("prompt_tokens"))
        .and_then(Value::as_u64),
      output_tokens: object
        .get("output_tokens")
        .or_else(|| object.get("completion_tokens"))
        .and_then(Value::as_u64),
      total_tokens: object.get("total_tokens").and_then(Value::as_u64),
      input_tokens_details: object
        .get("input_tokens_details")
        .or_else(|| object.get("prompt_tokens_details"))
        .cloned(),
      output_tokens_details: object
        .get("output_tokens_details")
        .or_else(|| object.get("completion_tokens_details"))
        .cloned(),
      extras,
    }
  }

  #[cfg(feature = "std")]
  pub(super) fn from_ir(usage: IrUsage) -> Self {
    Self {
      input_tokens: usage.input_tokens,
      output_tokens: usage.output_tokens,
      total_tokens: usage.total_tokens,
      input_tokens_details: usage.input_tokens_details,
      output_tokens_details: usage.output_tokens_details,
      extras: usage.extras,
    }
  }
}

fn output_reasoning(response: &Value) -> Option<String> {
  let mut parts = Vec::new();
  for item in response.get("output").and_then(Value::as_array).into_iter().flatten() {
    match item.get("type").and_then(Value::as_str) {
      Some("reasoning") => {
        collect_reasoning_parts(item.get("content"), &mut parts);
        collect_reasoning_parts(item.get("summary"), &mut parts);
      }
      Some("message") => {
        for part in item
          .get("content")
          .and_then(Value::as_array)
          .into_iter()
          .flatten()
          .filter(|part| part.get("type").and_then(Value::as_str) == Some("reasoning"))
        {
          collect_reasoning_parts(Some(part), &mut parts);
        }
      }
      _ => {}
    }
  }
  let reasoning = parts.concat();
  (!reasoning.is_empty()).then_some(reasoning)
}

fn collect_reasoning_parts(value: Option<&Value>, output: &mut Vec<String>) {
  match value {
    Some(Value::Array(parts)) => {
      output.extend(
        parts
          .iter()
          .filter_map(|part| part.get("text").or_else(|| part.get("summary")).and_then(Value::as_str))
          .map(str::to_string),
      );
    }
    Some(Value::Object(part)) => {
      if let Some(text) = part.get("text").or_else(|| part.get("summary")).and_then(Value::as_str) {
        output.push(text.to_string());
      }
    }
    Some(Value::String(text)) => output.push(text.clone()),
    _ => {}
  }
}

fn output_tool_calls(response: &Value) -> Vec<ToolCall> {
  response
    .get("output")
    .and_then(Value::as_array)
    .into_iter()
    .flatten()
    .filter(|item| {
      matches!(
        item.get("type").and_then(Value::as_str),
        Some("function_call" | "custom_tool_call")
      )
    })
    .map(|item| ToolCall {
      id: item
        .get("call_id")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string),
      name: item.get("name").and_then(Value::as_str).unwrap_or_default().to_string(),
      arguments: decode_arguments(
        item
          .get("arguments")
          .or_else(|| item.get("input"))
          .unwrap_or(&Value::Null),
      ),
    })
    .collect()
}

fn decode_arguments(value: &Value) -> Value {
  match value {
    Value::String(arguments) => serde_json::from_str(arguments).unwrap_or_else(|_| value.clone()),
    _ => value.clone(),
  }
}

pub(super) fn finish_reason(response: &Value, has_tool_calls: bool) -> Option<String> {
  if let Some(reason) = response.get("finish_reason").and_then(Value::as_str) {
    return Some(reason.to_string());
  }
  match response.get("status").and_then(Value::as_str) {
    Some("incomplete") => Some(
      response
        .pointer("/incomplete_details/reason")
        .and_then(Value::as_str)
        .map(|reason| match reason {
          "max_output_tokens" => "length",
          other => other,
        })
        .unwrap_or("incomplete")
        .to_string(),
    ),
    Some("completed") if has_tool_calls => Some("tool_calls".into()),
    Some("completed") => Some("stop".into()),
    Some("failed") => Some("error".into()),
    Some(status) => Some(status.to_string()),
    None if has_tool_calls => Some("tool_calls".into()),
    None => None,
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn media_metadata_uses_request_evidence_and_preserves_provider_encoding() {
    use crate::{AudioOutputOptions, GenerateRequest, ImageGenerationOptions, ImageOutputFormat};
    use tokn_core::content::{AudioContent, ImageContent, MediaResource};

    let request = GenerateRequest::builder("media")
      .prompt("produce media")
      .build()
      .unwrap();
    let unknown_image = || Content::Image(ImageContent::new(MediaResource::from_bytes(alloc::vec![1, 2, 3])));
    let mut content = unknown_image();
    OutputContentMetadata::for_request(&request).enrich_content(&mut content);
    assert!(matches!(&content, Content::Image(image) if image.resource.media_type.is_none()));

    let jpeg = request
      .clone()
      .into_builder()
      .image_generation(ImageGenerationOptions {
        output_format: ImageOutputFormat::Jpeg,
        ..ImageGenerationOptions::default()
      })
      .build()
      .unwrap();
    OutputContentMetadata::for_request(&jpeg).enrich_content(&mut content);
    assert!(matches!(&content, Content::Image(image) if image.resource.media_type.as_deref() == Some("image/jpeg")));

    let mut provider_image = Content::Image(ImageContent::new(MediaResource::inline("image/webp", alloc::vec![1])));
    OutputContentMetadata::for_request(&jpeg).enrich_content(&mut provider_image);
    assert!(
      matches!(&provider_image, Content::Image(image) if image.resource.media_type.as_deref() == Some("image/webp"))
    );

    let raw_tool = request
      .clone()
      .into_builder()
      .extra("tools", json!([{"type":"image_generation","output_format":"webp"}]))
      .build()
      .unwrap();
    let mut raw_image = unknown_image();
    OutputContentMetadata::for_request(&raw_tool).enrich_content(&mut raw_image);
    assert!(matches!(&raw_image, Content::Image(image) if image.resource.media_type.as_deref() == Some("image/webp")));

    let audio_request = request
      .into_builder()
      .audio_output(AudioOutputOptions::new("voice", AudioFormat::Mp3))
      .build()
      .unwrap();
    let mut audio = Content::Audio(AudioContent::new(MediaResource::from_bytes(alloc::vec![1])));
    OutputContentMetadata::for_request(&audio_request).enrich_content(&mut audio);
    assert!(
      matches!(&audio, Content::Audio(audio) if audio.format == Some(AudioFormat::Mp3) && audio.resource.media_type.as_deref() == Some("audio/mpeg"))
    );
  }

  #[test]
  fn unsupported_media_cannot_be_reported_as_an_empty_success() {
    let result = GenerateResponse::from_endpoint(
      200,
      HeaderMap::new(),
      json!({
        "status": "completed", "output": [{"type":"message","role":"assistant",
        "content":[{"type":"output_audio","data":"AQID"}]}]
      }),
      Endpoint::Responses,
    );
    assert!(matches!(result, Err(crate::Error::DecodeGenerateResponse { .. })));
  }

  #[test]
  fn projects_structured_text_without_duplication() {
    let response = GenerateResponse::from_raw(
      200,
      HeaderMap::new(),
      json!({
        "id": "resp_1",
        "status": "completed",
        "output_text": "outdated convenience projection",
        "output": [{
          "type": "message",
          "content": [{"type": "output_text", "text": "hello"}]
        }],
        "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
      }),
    );

    assert_eq!(response.text, "hello");
    assert_eq!(response.content, alloc::vec![Content::text("hello")]);
    assert_eq!(response.finish_reason.as_deref(), Some("stop"));
    assert_eq!(response.usage.expect("usage").total_tokens, Some(3));
  }

  #[test]
  fn derives_tool_and_incomplete_finish_reasons() {
    assert_eq!(
      finish_reason(
        &json!({
          "status": "completed",
          "output": [{"type": "function_call"}]
        }),
        true,
      )
      .as_deref(),
      Some("tool_calls")
    );
    assert_eq!(
      finish_reason(
        &json!({
          "status": "incomplete",
          "incomplete_details": {"reason": "max_output_tokens"}
        }),
        false,
      )
      .as_deref(),
      Some("length")
    );
  }
}
