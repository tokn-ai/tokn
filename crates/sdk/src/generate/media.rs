//! Media values stay independent of endpoint-specific generation settings.

use alloc::{
  string::{String, ToString},
  vec::Vec,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokn_core::{
  content::{Content, ContentBody, ContentRef, MediaResource, MediaSource, ProviderAssetReference, VideoSource},
  Endpoint,
};

use super::{GenerateRequest, Role};
use crate::{Error, Result};

pub use tokn_core::content::AudioFormat as AudioOutputFormat;

/// Speech output settings. Voice names belong to the selected provider.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioOutputOptions {
  pub voice: String,
  pub format: AudioOutputFormat,
}

impl AudioOutputOptions {
  pub fn new(voice: impl Into<String>, format: AudioOutputFormat) -> Self {
    Self {
      voice: voice.into(),
      format,
    }
  }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageOutputFormat {
  #[default]
  Png,
  Jpeg,
  Webp,
}

impl ImageOutputFormat {
  pub fn media_type(self) -> &'static str {
    match self {
      Self::Png => "image/png",
      Self::Jpeg => "image/jpeg",
      Self::Webp => "image/webp",
    }
  }
}

/// Options for the native Responses image-generation tool.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageGenerationOptions {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub model: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub size: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub quality: Option<String>,
  #[serde(default)]
  pub output_format: ImageOutputFormat,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub background: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub partial_images: Option<u8>,
}

impl GenerateRequest {
  /// Resolve application assets or remote URLs explicitly before freezing a
  /// conversation request. The callback may also verify digests or replace an
  /// expiring provider handle with retained bytes. No I/O is implicit here.
  pub fn try_map_media<E>(
    &self,
    mut resolve: impl FnMut(&MediaResource) -> core::result::Result<MediaResource, E>,
  ) -> core::result::Result<Self, E> {
    let mut request = self.clone();
    for message in &mut request.messages {
      message.content = message.content.try_map_resources(&mut resolve)?;
    }
    for context in &mut request.context {
      context.content = context.content.try_map_resources(&mut resolve)?;
    }
    Ok(request)
  }

  pub(crate) fn source_endpoint(&self) -> Endpoint {
    let requirements = self.media_requirements();
    if requirements.messages {
      Endpoint::Messages
    } else if requirements.audio {
      Endpoint::ChatCompletions
    } else {
      Endpoint::Responses
    }
  }

  fn media_requirements(&self) -> EndpointRequirements {
    let mut requirements = EndpointRequirements {
      audio: self.audio_output.is_some(),
      ..Default::default()
    };
    for body in self
      .messages
      .iter()
      .map(|message| &message.content)
      .chain(self.context.iter().map(|context| &context.content))
    {
      for part in body.iter() {
        match part {
          ContentRef::Part(Content::Audio(_)) => requirements.audio = true,
          ContentRef::Part(Content::Document(document)) if document.title.is_some() => requirements.messages = true,
          _ => {}
        }
      }
      visit_input_resources(body, &mut |resource| {
        if let MediaSource::Provider(reference) = &resource.source {
          match reference.namespace.as_str() {
            "anthropic.files" => requirements.messages = true,
            "openai.files" => requirements.openai_files = true,
            _ => {}
          }
        }
      });
    }
    requirements
  }

  pub(crate) fn media_assets(&self) -> Vec<ProviderAssetReference> {
    let mut assets = Vec::new();
    for body in self
      .messages
      .iter()
      .map(|m| &m.content)
      .chain(self.context.iter().map(|c| &c.content))
    {
      visit_input_resources(body, &mut |resource| {
        if let MediaSource::Provider(reference) = &resource.source {
          if !assets.contains(reference.as_ref()) {
            assets.push(reference.as_ref().clone());
          }
        }
      });
    }
    assets
  }

  pub(crate) fn validate_snapshot_media(&self) -> Result<()> {
    for body in self
      .messages
      .iter()
      .map(|m| &m.content)
      .chain(self.context.iter().map(|c| &c.content))
    {
      validate_snapshot_content(body)?;
    }
    Ok(())
  }

  pub(super) fn validate_media(&self) -> Result<()> {
    let invalid = |message: &str| Error::InvalidGenerateRequest {
      message: message.into(),
    };
    let requirements = self.media_requirements();
    if requirements.messages && (requirements.audio || requirements.openai_files || self.image_generation.is_some()) {
      return Err(invalid("Messages-only media cannot be combined with audio, OpenAI file references, or the Responses image-generation tool"));
    }
    if self.image_generation.is_some() && requirements.audio {
      return Err(invalid(
        "audio and the Responses image-generation tool require different native endpoints",
      ));
    }
    if let Some(audio) = &self.audio_output {
      if audio.voice.trim().is_empty() {
        return Err(invalid("audio output requires a non-empty voice"));
      }
      if self.extras.contains_key("audio") || self.extras.contains_key("modalities") {
        return Err(invalid("typed audio_output conflicts with extras audio/modalities"));
      }
    }
    if let Some(image) = &self.image_generation {
      if image.partial_images.is_some_and(|value| value > 3) {
        return Err(invalid("partial_images must be between zero and three"));
      }
      if [&image.model, &image.size, &image.quality, &image.background]
        .iter()
        .any(|s| s.as_ref().is_some_and(|s| s.trim().is_empty()))
      {
        return Err(invalid("image-generation options cannot be empty strings"));
      }
      if self.extras.contains_key("tools") {
        return Err(invalid("typed image_generation conflicts with extras tools"));
      }
    }
    for body in self
      .messages
      .iter()
      .map(|m| &m.content)
      .chain(self.context.iter().map(|c| &c.content))
    {
      body.validate().map_err(|e| invalid(&e.to_string()))?;
    }
    Ok(())
  }

  pub(super) fn render_media_options(&self, body: &mut Value) {
    if let Some(audio) = &self.audio_output {
      body["modalities"] = json!(["text", "audio"]);
      body["audio"] = json!(audio);
    }
    if let Some(image) = &self.image_generation {
      let mut tool = json!(image);
      tool["type"] = json!("image_generation");
      if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        tools.push(tool);
      } else {
        body["tools"] = json!([tool]);
      }
    }
  }
}

#[derive(Default)]
struct EndpointRequirements {
  audio: bool,
  messages: bool,
  openai_files: bool,
}

pub(crate) fn validate_snapshot_content(body: &ContentBody) -> Result<()> {
  let mut unresolved = false;
  visit_input_resources(body, &mut |resource| {
    unresolved |= matches!(resource.source, MediaSource::Remote { .. } | MediaSource::Asset(_));
  });
  if unresolved {
    return Err(Error::InvalidGenerateRequest { message: "conversation media must use retained inline bytes or scoped provider assets; materialize remote URLs and application assets before submitting content".into() });
  }
  Ok(())
}

/// Reject immutable input that none of the supported LLM endpoints can
/// represent. Generated media can remain in history with native continuation
/// even when it is not a valid portable input, so this is an intake-only check.
pub(crate) fn validate_input_content(body: &ContentBody, role: Role) -> Result<()> {
  validate_snapshot_content(body)?;
  if body.is_text_only()
    || [Endpoint::Responses, Endpoint::ChatCompletions, Endpoint::Messages]
      .into_iter()
      .any(|endpoint| {
        // Responses and Messages wrap tool media in user-content subparts.
        // Chat tool messages accept text only, unlike Chat user messages.
        let wire_role = if endpoint == Endpoint::ChatCompletions && role == Role::Tool {
          "tool"
        } else {
          "user"
        };
        body.iter().all(|part| match part {
          ContentRef::Text(_) => true,
          ContentRef::Part(content) => tokn_convert::media::content_to_parts(endpoint, content, wire_role).is_ok(),
        })
      })
  {
    return Ok(());
  }
  Err(Error::InvalidGenerateRequest {
    message: "conversation input cannot be represented by any supported LLM endpoint; materialize a supported format before submitting content".into(),
  })
}

/// Visit only resources encoded as model input. Transcript source references
/// are display/provenance metadata and do not constrain model routing.
fn visit_input_resources(body: &ContentBody, visitor: &mut impl FnMut(&MediaResource)) {
  for part in body.iter() {
    match part {
      ContentRef::Part(Content::Image(image)) => visitor(&image.resource),
      ContentRef::Part(Content::Audio(audio)) => visitor(&audio.resource),
      ContentRef::Part(Content::Document(document)) => visitor(&document.resource),
      ContentRef::Part(Content::Video(video)) => match &video.source {
        VideoSource::Resource(resource) => visitor(resource),
        VideoSource::Frames(frames) => {
          for frame in frames {
            visitor(&frame.image.resource);
          }
        }
      },
      _ => {}
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::vec;
  use tokn_core::content::{AudioContent, DocumentContent, ImageContent};

  fn titled_document() -> Content {
    let mut document = DocumentContent::new(MediaResource::inline("application/pdf", b"%PDF-fixture".to_vec()));
    document.title = Some("Document title".into());
    document.into()
  }

  #[test]
  fn incompatible_media_endpoint_requirements_fail_during_validation() {
    let audio = AudioContent::new(MediaResource::inline("audio/wav", vec![1, 2, 3]));
    assert!(GenerateRequest::builder("model")
      .user(vec![titled_document(), audio.into()])
      .build()
      .is_err());
    assert!(GenerateRequest::builder("model")
      .user(titled_document())
      .image_generation(ImageGenerationOptions::default())
      .build()
      .is_err());
    let scope = crate::session::ProviderOrigin::new(
      "provider",
      "openai",
      "account",
      "https://backend.invalid",
      Endpoint::Responses,
      "model",
    )
    .asset_scope();
    let resources: Vec<Content> = ["openai.files", "anthropic.files"]
      .into_iter()
      .map(|namespace| {
        ImageContent::new(MediaResource::provider(ProviderAssetReference {
          id: "file-fixture".into(),
          namespace: namespace.into(),
          scope: scope.clone(),
          expires_at: None,
        }))
        .into()
      })
      .collect();
    assert!(GenerateRequest::builder("model").user(resources).build().is_err());
  }
}
