//! Portable text and media values shared by generation APIs and renderers.
//!
//! Content describes what a value is. Message roles, generation settings,
//! transport, resource resolution and display layout belong to their callers.

mod resource;

pub use resource::{
  decode_base64, encode_base64, AssetReference, MediaResource, MediaSource, ProviderAssetReference, ProviderAssetScope,
};

use alloc::{string::String, vec, vec::Vec};
use core::fmt;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
  Text,
  Image,
  Audio,
  Video,
  Document,
}

/// One semantic content part, independent of input/output direction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Content {
  Text(TextContent),
  Image(ImageContent),
  Audio(AudioContent),
  Video(VideoContent),
  Document(DocumentContent),
  Transcript(TranscriptContent),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextContent {
  pub text: String,
  #[serde(default, skip_serializing_if = "TextFormat::is_plain")]
  pub format: TextFormat,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextFormat {
  #[default]
  Plain,
  Markdown,
}

impl TextFormat {
  fn is_plain(&self) -> bool {
    *self == Self::Plain
  }
}

impl TextContent {
  pub fn new(text: impl Into<String>) -> Self {
    Self {
      text: text.into(),
      format: TextFormat::Plain,
    }
  }
  pub fn markdown(text: impl Into<String>) -> Self {
    Self {
      text: text.into(),
      format: TextFormat::Markdown,
    }
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageDimensions {
  pub width: u32,
  pub height: u32,
}

/// A hint for interpreting an image, independent of display dimensions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageDetail {
  Auto,
  Low,
  High,
  Original,
}

impl ImageDetail {
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Auto => "auto",
      Self::Low => "low",
      Self::High => "high",
      Self::Original => "original",
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageContent {
  pub resource: MediaResource,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub dimensions: Option<ImageDimensions>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub detail: Option<ImageDetail>,
}

impl ImageContent {
  pub fn new(resource: MediaResource) -> Self {
    Self {
      resource,
      dimensions: None,
      detail: None,
    }
  }
  pub fn with_detail(mut self, detail: ImageDetail) -> Self {
    self.detail = Some(detail);
    self
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioFormat {
  Wav,
  Mp3,
  Flac,
  Opus,
  Pcm16,
  Aac,
}

impl AudioFormat {
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Wav => "wav",
      Self::Mp3 => "mp3",
      Self::Flac => "flac",
      Self::Opus => "opus",
      Self::Pcm16 => "pcm16",
      Self::Aac => "aac",
    }
  }

  pub fn media_type(self) -> &'static str {
    match self {
      Self::Wav => "audio/wav",
      Self::Mp3 => "audio/mpeg",
      Self::Flac => "audio/flac",
      Self::Opus => "audio/opus",
      Self::Pcm16 => "audio/pcm",
      Self::Aac => "audio/aac",
    }
  }

  pub fn from_media_type(media_type: &str) -> Option<Self> {
    match media_type {
      "audio/wav" | "audio/x-wav" | "audio/wave" => Some(Self::Wav),
      "audio/mpeg" | "audio/mp3" => Some(Self::Mp3),
      "audio/flac" | "audio/x-flac" => Some(Self::Flac),
      "audio/opus" => Some(Self::Opus),
      "audio/pcm" | "audio/pcm16" => Some(Self::Pcm16),
      "audio/aac" => Some(Self::Aac),
      _ => None,
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioContent {
  pub resource: MediaResource,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub format: Option<AudioFormat>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub duration_ms: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub sample_rate: Option<u32>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub channels: Option<u16>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub transcript: Option<TranscriptContent>,
}

impl AudioContent {
  pub fn new(resource: MediaResource) -> Self {
    let format = resource.media_type.as_deref().and_then(AudioFormat::from_media_type);
    Self {
      resource,
      format,
      duration_ms: None,
      sample_rate: None,
      channels: None,
      transcript: None,
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoContent {
  pub source: VideoSource,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub dimensions: Option<ImageDimensions>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub duration_ms: Option<u64>,
}

/// Encoded video and an explicitly sampled frame sequence have different
/// semantics. Adapters may not silently discard a resource's audio track.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum VideoSource {
  Resource(MediaResource),
  Frames(Vec<VideoFrame>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoFrame {
  pub timestamp_ms: u64,
  pub image: ImageContent,
}

impl VideoContent {
  pub fn new(resource: MediaResource) -> Self {
    Self {
      source: VideoSource::Resource(resource),
      dimensions: None,
      duration_ms: None,
    }
  }
  pub fn frames(frames: Vec<VideoFrame>) -> Self {
    Self {
      source: VideoSource::Frames(frames),
      dimensions: None,
      duration_ms: None,
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentContent {
  pub resource: MediaResource,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub filename: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub title: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub page_count: Option<u32>,
}

impl DocumentContent {
  pub fn new(resource: MediaResource) -> Self {
    Self {
      resource,
      filename: None,
      title: None,
      page_count: None,
    }
  }
  pub fn with_filename(mut self, filename: impl Into<String>) -> Self {
    self.filename = Some(filename.into());
    self
  }
}

/// Timed text is reusable display/input content; ASR execution metadata belongs
/// to its operation response. Times are integer milliseconds from source start.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptContent {
  pub text: String,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub segments: Vec<TranscriptSegment>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub language: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub source: Option<MediaResource>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptSegment {
  pub text: String,
  pub start_ms: u64,
  pub end_ms: u64,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub speaker: Option<String>,
}

impl TranscriptContent {
  pub fn new(text: impl Into<String>) -> Self {
    Self {
      text: text.into(),
      segments: Vec::new(),
      language: None,
      source: None,
    }
  }
}

/// Ordered message or tool-result content. Plain text keeps its original JSON
/// string representation so existing persisted text history is unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ContentBody {
  Text(String),
  Parts(Vec<Content>),
}

impl Default for ContentBody {
  fn default() -> Self {
    Self::Text(String::new())
  }
}

/// A borrowed view permits iterating legacy text and structured parts without
/// allocating a replacement `TextContent`.
#[derive(Clone, Copy, Debug)]
pub enum ContentRef<'a> {
  Text(&'a str),
  Part(&'a Content),
}

impl ContentRef<'_> {
  pub fn kind(self) -> MediaKind {
    match self {
      Self::Text(_) => MediaKind::Text,
      Self::Part(content) => content.kind(),
    }
  }
}

impl ContentBody {
  pub fn iter(&self) -> impl Iterator<Item = ContentRef<'_>> {
    let (text, parts) = match self {
      Self::Text(text) => (Some(text.as_str()), &[][..]),
      Self::Parts(parts) => (None, parts.as_slice()),
    };
    text
      .into_iter()
      .map(ContentRef::Text)
      .chain(parts.iter().map(ContentRef::Part))
  }

  /// Explicit text projection. Media is excluded; transcript text is included.
  pub fn text(&self) -> String {
    match self {
      Self::Text(text) => text.clone(),
      Self::Parts(parts) => parts.iter().filter_map(Content::text_value).collect(),
    }
  }

  pub fn is_empty(&self) -> bool {
    match self {
      Self::Text(text) => text.is_empty(),
      Self::Parts(parts) => parts.iter().all(Content::is_empty),
    }
  }

  pub fn is_blank(&self) -> bool {
    match self {
      Self::Text(text) => text.trim().is_empty(),
      Self::Parts(parts) => parts.iter().all(Content::is_blank),
    }
  }

  pub fn is_text_only(&self) -> bool {
    match self {
      Self::Text(_) => true,
      Self::Parts(parts) => parts.iter().all(|part| matches!(part, Content::Text(_))),
    }
  }

  pub fn into_parts(self) -> Vec<Content> {
    match self {
      Self::Text(text) => vec![Content::text(text)],
      Self::Parts(parts) => parts,
    }
  }

  pub fn validate(&self) -> Result<(), ContentError> {
    if let Self::Parts(parts) = self {
      for part in parts {
        part.validate()?;
      }
    }
    Ok(())
  }

  pub fn visit_resources(&self, visitor: &mut impl FnMut(&MediaResource)) {
    if let Self::Parts(parts) = self {
      for part in parts {
        part.visit_resources(visitor);
      }
    }
  }

  pub fn try_map_resources<E>(
    &self,
    mut mapper: impl FnMut(&MediaResource) -> Result<MediaResource, E>,
  ) -> Result<Self, E> {
    match self {
      Self::Text(text) => Ok(Self::Text(text.clone())),
      Self::Parts(parts) => parts
        .iter()
        .map(|part| part.try_map_resources(&mut mapper))
        .collect::<Result<Vec<_>, _>>()
        .map(Self::Parts),
    }
  }
}

impl Content {
  pub fn text(text: impl Into<String>) -> Self {
    Self::Text(TextContent::new(text))
  }

  pub fn kind(&self) -> MediaKind {
    match self {
      Self::Text(_) | Self::Transcript(_) => MediaKind::Text,
      Self::Image(_) => MediaKind::Image,
      Self::Audio(_) => MediaKind::Audio,
      Self::Video(_) => MediaKind::Video,
      Self::Document(_) => MediaKind::Document,
    }
  }

  pub fn text_value(&self) -> Option<&str> {
    match self {
      Self::Text(text) => Some(&text.text),
      Self::Transcript(transcript) => Some(&transcript.text),
      _ => None,
    }
  }

  pub fn is_empty(&self) -> bool {
    self.text_value().is_some_and(str::is_empty)
  }
  pub fn is_blank(&self) -> bool {
    self.text_value().is_some_and(|text| text.trim().is_empty())
  }

  pub fn validate(&self) -> Result<(), ContentError> {
    match self {
      Self::Text(_) => Ok(()),
      Self::Image(image) => image.validate(),
      Self::Audio(audio) => audio.validate(),
      Self::Video(video) => video.validate(),
      Self::Document(document) => document.validate(),
      Self::Transcript(transcript) => transcript.validate(),
    }
  }

  pub fn visit_resources(&self, visitor: &mut impl FnMut(&MediaResource)) {
    match self {
      Self::Text(_) => {}
      Self::Image(image) => visitor(&image.resource),
      Self::Audio(audio) => {
        visitor(&audio.resource);
        if let Some(transcript) = &audio.transcript {
          if let Some(source) = &transcript.source {
            visitor(source);
          }
        }
      }
      Self::Video(video) => match &video.source {
        VideoSource::Resource(resource) => visitor(resource),
        VideoSource::Frames(frames) => {
          for frame in frames {
            visitor(&frame.image.resource);
          }
        }
      },
      Self::Document(document) => visitor(&document.resource),
      Self::Transcript(transcript) => {
        if let Some(source) = &transcript.source {
          visitor(source);
        }
      }
    }
  }

  pub fn try_map_resources<E>(
    &self,
    mut mapper: impl FnMut(&MediaResource) -> Result<MediaResource, E>,
  ) -> Result<Self, E> {
    let mut content = self.clone();
    match &mut content {
      Self::Text(_) => {}
      Self::Image(image) => image.resource = mapper(&image.resource)?,
      Self::Audio(audio) => {
        audio.resource = mapper(&audio.resource)?;
        if let Some(transcript) = &mut audio.transcript {
          map_transcript(transcript, &mut mapper)?;
        }
      }
      Self::Video(video) => match &mut video.source {
        VideoSource::Resource(resource) => *resource = mapper(resource)?,
        VideoSource::Frames(frames) => {
          for frame in frames {
            frame.image.resource = mapper(&frame.image.resource)?;
          }
        }
      },
      Self::Document(document) => document.resource = mapper(&document.resource)?,
      Self::Transcript(transcript) => map_transcript(transcript, &mut mapper)?,
    }
    Ok(content)
  }
}

fn map_transcript<E>(
  transcript: &mut TranscriptContent,
  mapper: &mut impl FnMut(&MediaResource) -> Result<MediaResource, E>,
) -> Result<(), E> {
  if let Some(source) = &mut transcript.source {
    *source = mapper(source)?;
  }
  Ok(())
}

impl ImageContent {
  pub fn validate(&self) -> Result<(), ContentError> {
    validate_resource_kind(&self.resource, "image/")?;
    validate_dimensions(self.dimensions)
  }
}

impl AudioContent {
  pub fn validate(&self) -> Result<(), ContentError> {
    validate_resource_kind(&self.resource, "audio/")?;
    if self.sample_rate == Some(0) || self.channels == Some(0) {
      return Err(ContentError::InvalidAudioMetadata);
    }
    if let (Some(format), Some(media_type)) = (self.format, self.resource.media_type.as_deref()) {
      if AudioFormat::from_media_type(media_type).is_some_and(|actual| actual != format) {
        return Err(ContentError::AudioFormatMismatch);
      }
    }
    if let Some(transcript) = &self.transcript {
      transcript.validate()?;
    }
    Ok(())
  }
}

impl VideoContent {
  pub fn validate(&self) -> Result<(), ContentError> {
    validate_dimensions(self.dimensions)?;
    match &self.source {
      VideoSource::Resource(resource) => validate_resource_kind(resource, "video/")?,
      VideoSource::Frames(frames) => {
        if frames.is_empty() {
          return Err(ContentError::EmptyVideoFrames);
        }
        if frames
          .windows(2)
          .any(|pair| pair[0].timestamp_ms >= pair[1].timestamp_ms)
        {
          return Err(ContentError::InvalidFrameOrder);
        }
        for frame in frames {
          frame.image.validate()?;
          if self.duration_ms.is_some_and(|duration| frame.timestamp_ms > duration) {
            return Err(ContentError::InvalidFrameOrder);
          }
        }
      }
    }
    Ok(())
  }
}

impl DocumentContent {
  pub fn validate(&self) -> Result<(), ContentError> {
    self.resource.validate()?;
    if self.page_count == Some(0) {
      return Err(ContentError::InvalidDocumentMetadata);
    }
    if let Some(filename) = &self.filename {
      nonempty(filename, "document filename")?;
    }
    Ok(())
  }
}

impl TranscriptContent {
  pub fn validate(&self) -> Result<(), ContentError> {
    if let Some(source) = &self.source {
      validate_resource_kind(source, "audio/")?;
    }
    if self.segments.iter().any(|segment| segment.end_ms < segment.start_ms) {
      return Err(ContentError::InvalidTranscriptTiming);
    }
    if self.segments.windows(2).any(|pair| pair[1].start_ms < pair[0].start_ms) {
      return Err(ContentError::InvalidTranscriptTiming);
    }
    Ok(())
  }
}

fn validate_resource_kind(resource: &MediaResource, prefix: &'static str) -> Result<(), ContentError> {
  resource.validate()?;
  if resource
    .media_type
    .as_ref()
    .is_some_and(|media_type| !media_type.starts_with(prefix))
  {
    return Err(ContentError::WrongMediaType { expected: prefix });
  }
  Ok(())
}

fn validate_dimensions(dimensions: Option<ImageDimensions>) -> Result<(), ContentError> {
  if dimensions.is_some_and(|dimensions| dimensions.width == 0 || dimensions.height == 0) {
    return Err(ContentError::InvalidDimensions);
  }
  Ok(())
}

fn nonempty(value: &str, field: &'static str) -> Result<(), ContentError> {
  if value.trim().is_empty() {
    Err(ContentError::EmptyField { field })
  } else {
    Ok(())
  }
}

impl From<String> for ContentBody {
  fn from(text: String) -> Self {
    Self::Text(text)
  }
}
impl From<&String> for ContentBody {
  fn from(text: &String) -> Self {
    Self::Text(text.clone())
  }
}
impl From<&str> for ContentBody {
  fn from(text: &str) -> Self {
    Self::Text(text.into())
  }
}
impl From<Vec<Content>> for ContentBody {
  fn from(parts: Vec<Content>) -> Self {
    Self::Parts(parts)
  }
}
impl From<Content> for ContentBody {
  fn from(content: Content) -> Self {
    Self::Parts(vec![content])
  }
}
impl From<String> for Content {
  fn from(text: String) -> Self {
    Self::text(text)
  }
}
impl From<&str> for Content {
  fn from(text: &str) -> Self {
    Self::text(text)
  }
}

macro_rules! content_conversions {
  ($($ty:ident => $variant:ident),* $(,)?) => {$ (
    impl From<$ty> for Content { fn from(value: $ty) -> Self { Self::$variant(value) } }
    impl From<$ty> for ContentBody { fn from(value: $ty) -> Self { Content::$variant(value).into() } }
  )*};
}
content_conversions! { TextContent => Text, ImageContent => Image, AudioContent => Audio, VideoContent => Video, DocumentContent => Document, TranscriptContent => Transcript }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentError {
  EmptyField { field: &'static str },
  EmptyResource,
  MissingMediaType,
  InvalidMediaType,
  WrongMediaType { expected: &'static str },
  InvalidBase64,
  InvalidDataUrl,
  InvalidUrl,
  InvalidDigest,
  DigestMismatch,
  UnresolvedResource,
  InvalidDimensions,
  InvalidAudioMetadata,
  AudioFormatMismatch,
  EmptyVideoFrames,
  InvalidFrameOrder,
  InvalidDocumentMetadata,
  InvalidTranscriptTiming,
}

impl fmt::Display for ContentError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let message = match self {
      Self::EmptyField { field } => return write!(f, "{field} must not be empty"),
      Self::WrongMediaType { expected } => return write!(f, "resource requires a {expected} media type"),
      Self::EmptyResource => "inline media must contain bytes",
      Self::MissingMediaType => "inline media requires a media type",
      Self::InvalidMediaType => "invalid media type",
      Self::InvalidBase64 => "invalid base64 media data",
      Self::InvalidDataUrl => "media data URL must contain a media type and base64 bytes",
      Self::InvalidUrl => "remote media requires an HTTP or HTTPS URL without embedded credentials",
      Self::InvalidDigest => "digest must be a lowercase SHA-256 hexadecimal value",
      Self::DigestMismatch => "media bytes do not match their digest",
      Self::UnresolvedResource => "resource must be resolved to inline bytes",
      Self::InvalidDimensions => "image dimensions must be greater than zero",
      Self::InvalidAudioMetadata => "audio sample rate and channel count must be greater than zero",
      Self::AudioFormatMismatch => "audio format does not match its media type",
      Self::EmptyVideoFrames => "sampled video requires at least one frame",
      Self::InvalidFrameOrder => "video frame timestamps must increase within the video duration",
      Self::InvalidDocumentMetadata => "document page count must be greater than zero",
      Self::InvalidTranscriptTiming => "transcript segments must have valid ranges ordered by start time",
    };
    f.write_str(message)
  }
}
impl core::error::Error for ContentError {}

#[cfg(test)]
mod tests;
