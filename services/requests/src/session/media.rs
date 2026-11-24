//! Route requirements from media actually present in an inbound LLM request.

use super::{Binding, Error, ModelCapabilities};
use alloc::vec::Vec;
use serde_json::Value;
use tokn_core::{
  content::{MediaKind, ProviderAssetReference},
  util::digest::sha256_hex,
  Endpoint,
};

#[derive(Default)]
pub(super) struct Requirements {
  input: Vec<MediaKind>,
  output: Vec<MediaKind>,
  image_generation: bool,
  structured_media: bool,
}

impl Requirements {
  pub(super) fn from_request(endpoint: Endpoint, body: &Value) -> Self {
    let mut requirements = Self::default();
    match endpoint {
      Endpoint::Responses => {
        requirements.content(&body["instructions"]);
        if let Some(items) = body["input"].as_array() {
          for item in items {
            if matches!(
              item["type"].as_str(),
              Some("function_call_output" | "custom_tool_call_output")
            ) {
              requirements.content(&item["output"]);
            } else {
              requirements.content(&item["content"]);
            }
          }
        } else {
          requirements.content(&body["input"]);
        }
      }
      Endpoint::ChatCompletions | Endpoint::Messages => {
        requirements.content(&body["system"]);
        if let Some(messages) = body["messages"].as_array() {
          for message in messages {
            requirements.content(&message["content"]);
            if message.get("audio").is_some_and(|audio| !audio.is_null()) {
              requirements.structured_media = true;
              requirements.input(MediaKind::Audio);
            }
          }
        }
      }
    }
    if let Some(modalities) = body["modalities"].as_array() {
      for modality in modalities {
        if let Some(kind) = match modality.as_str() {
          Some("text") => Some(MediaKind::Text),
          Some("image") => Some(MediaKind::Image),
          Some("audio") => Some(MediaKind::Audio),
          Some("video") => Some(MediaKind::Video),
          _ => None,
        } {
          if !requirements.output.contains(&kind) {
            requirements.output.push(kind);
          }
        }
      }
    } else {
      requirements.output.push(MediaKind::Text);
    }
    // Hosted tools produce media independently of the language model's own
    // output modalities. Their availability is an endpoint capability.
    requirements.image_generation = body["tools"]
      .as_array()
      .is_some_and(|tools| tools.iter().any(|tool| tool["type"] == "image_generation"));
    requirements
  }

  fn input(&mut self, kind: MediaKind) {
    if !self.input.contains(&kind) {
      self.input.push(kind);
    }
  }

  fn content(&mut self, content: &Value) {
    if content.as_str().is_some_and(|text| !text.is_empty()) {
      self.input(MediaKind::Text);
    }
    let Some(parts) = content.as_array() else { return };
    for part in parts {
      if matches!(
        part["type"].as_str(),
        Some(
          "image"
            | "image_url"
            | "input_image"
            | "output_image"
            | "audio"
            | "input_audio"
            | "output_audio"
            | "video"
            | "input_video"
            | "video_url"
            | "file"
            | "input_file"
            | "document"
        )
      ) {
        self.structured_media = true;
      }
      match part["type"].as_str() {
        Some("text" | "input_text" | "output_text") => self.input(MediaKind::Text),
        Some("image" | "image_url" | "input_image" | "output_image") => self.input(MediaKind::Image),
        Some("audio" | "input_audio" | "output_audio") => self.input(MediaKind::Audio),
        Some("video" | "input_video" | "video_url") => self.input(MediaKind::Video),
        Some("file" | "input_file" | "document") => self.input(document_kind(part)),
        Some("tool_result") => self.content(&part["content"]),
        _ => {}
      }
    }
  }

  pub(super) fn is_text_only(&self) -> bool {
    !self.image_generation
      && !self.structured_media
      && self
        .input
        .iter()
        .chain(&self.output)
        .all(|kind| *kind == MediaKind::Text)
  }

  pub(super) fn accepts(&self, capabilities: &ModelCapabilities) -> bool {
    capabilities
      .input_modalities
      .as_ref()
      .is_none_or(|supported| self.input.iter().all(|kind| supported.contains(kind)))
      && capabilities
        .output_modalities
        .as_ref()
        .is_none_or(|supported| self.output.iter().all(|kind| supported.contains(kind)))
  }
}

fn document_kind(part: &Value) -> MediaKind {
  let source = part.get("source").unwrap_or(part);
  let file = part.get("file").unwrap_or(part);
  if source["type"] == "text"
    || source["media_type"]
      .as_str()
      .is_some_and(|mime| mime.starts_with("text/"))
    || file["file_data"]
      .as_str()
      .is_some_and(|data| data.starts_with("data:text/"))
  {
    MediaKind::Text
  } else {
    MediaKind::Document
  }
}

pub(super) fn matches_asset(binding: &Binding, asset: &ProviderAssetReference) -> bool {
  asset.scope.provider == binding.provider.as_str()
    && asset.scope.account == binding.account.id.as_str()
    && asset.scope.backend == sha256_hex(binding.base_url.as_bytes())
    && asset.scope.upstream_account_digest.as_deref()
      == binding
        .account
        .provider_account_id
        .as_deref()
        .map(|id| sha256_hex(id.as_bytes()))
        .as_deref()
}

pub(super) fn validate_assets(assets: &[ProviderAssetReference], now: u64) -> Result<(), Error> {
  for asset in assets {
    asset.validate().map_err(|error| Error::new("resolve", error))?;
    if asset.expires_at.is_some_and(|expiry| expiry <= now) {
      return Err(Error::new(
        "resolve",
        "provider media reference has expired; supply portable source material",
      ));
    }
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::vec;
  use serde_json::json;

  #[test]
  fn nested_tool_media_and_output_requirements_use_known_model_evidence() {
    let request = json!({"messages":[{"role":"user","content":[
      {"type":"tool_result","content":[{"type":"image","source":{"type":"url","url":"https://example.invalid/a.png"}}]}
    ]}]});
    let requirements = Requirements::from_request(Endpoint::Messages, &request);
    assert!(!requirements.is_text_only());
    assert!(requirements.accepts(&ModelCapabilities::default()));
    let mut capabilities = ModelCapabilities {
      input_modalities: Some(vec![MediaKind::Text]),
      output_modalities: Some(vec![MediaKind::Text]),
      ..Default::default()
    };
    assert!(!requirements.accepts(&capabilities));
    capabilities.input_modalities.as_mut().unwrap().push(MediaKind::Image);
    assert!(requirements.accepts(&capabilities));
    capabilities.output_modalities = Some(Vec::new());
    assert!(!requirements.accepts(&capabilities));
  }

  #[test]
  fn hosted_image_tool_does_not_claim_direct_model_image_generation() {
    let requirements = Requirements::from_request(
      Endpoint::Responses,
      &json!({
        "input":"Draw a tree", "tools":[{"type":"image_generation"}]
      }),
    );
    assert!(!requirements.is_text_only());
    assert!(requirements.accepts(&ModelCapabilities {
      input_modalities: Some(vec![MediaKind::Text]),
      output_modalities: Some(vec![MediaKind::Text]),
      ..Default::default()
    }));
  }
}
