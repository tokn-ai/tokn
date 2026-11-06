use alloc::{boxed::Box, format, string::String};
use core::fmt;

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{types::Bytes, util::digest::sha256_hex};

use super::{nonempty, ContentError};

/// Encode a complete byte buffer or one independently encoded stream chunk.
pub fn encode_base64(bytes: &[u8]) -> String {
  STANDARD.encode(bytes)
}

/// Decode one complete base64 value. Independently encoded stream chunks must
/// each be decoded before concatenating their bytes.
pub fn decode_base64(encoded: &str) -> Result<Bytes, ContentError> {
  STANDARD
    .decode(encoded)
    .map(Bytes::from)
    .map_err(|_| ContentError::InvalidBase64)
}

/// A resource locator and facts about its bytes. This type never performs I/O.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaResource {
  pub source: MediaSource,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub media_type: Option<String>,
  /// A lowercase SHA-256 digest of the decoded bytes, when known.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub digest: Option<String>,
}

/// Where resource bytes can be obtained. Application assets and remote resources
/// must be resolved explicitly when an immutable retry snapshot is required.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MediaSource {
  Inline {
    #[serde(with = "base64_bytes")]
    data: Bytes,
  },
  Remote {
    url: String,
  },
  Asset(AssetReference),
  Provider(Box<ProviderAssetReference>),
}

impl fmt::Debug for MediaSource {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Inline { data } => f.debug_struct("Inline").field("byte_length", &data.len()).finish(),
      Self::Remote { .. } => f.debug_struct("Remote").finish_non_exhaustive(),
      Self::Asset(reference) => reference.fmt(f),
      Self::Provider(reference) => reference.fmt(f),
    }
  }
}

/// An application-owned asset. A version identifies the requested revision;
/// resolvers must retain or verify the corresponding bytes for exact retries.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetReference {
  pub id: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub version: Option<String>,
}

impl fmt::Debug for AssetReference {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("AssetReference")
      .field("versioned", &self.version.is_some())
      .finish_non_exhaustive()
  }
}

/// Scope of an uploaded resource. Unlike conversation state, asset scope is not
/// model-bound. `backend` is a digest of the canonical backend identity, never
/// a URL containing credentials. The optional upstream account digest detects
/// replacement credentials that refer to a different upstream account.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAssetScope {
  pub provider: String,
  pub account: String,
  pub backend: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub upstream_account_digest: Option<String>,
}

impl fmt::Debug for ProviderAssetScope {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ProviderAssetScope").finish_non_exhaustive()
  }
}

/// A provider-owned handle. `namespace` distinguishes otherwise similar file,
/// audio and output-item identifiers. Expiry is Unix time in seconds.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAssetReference {
  pub id: String,
  pub namespace: String,
  pub scope: ProviderAssetScope,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub expires_at: Option<u64>,
}

impl fmt::Debug for ProviderAssetReference {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ProviderAssetReference")
      .field("expires_at", &self.expires_at)
      .finish_non_exhaustive()
  }
}

impl MediaResource {
  pub fn inline(media_type: impl Into<String>, data: impl Into<Bytes>) -> Self {
    Self::from_bytes(data).with_media_type(media_type)
  }

  /// Retain bytes whose media type will be supplied by an operation's settings.
  pub fn from_bytes(data: impl Into<Bytes>) -> Self {
    let data = data.into();
    Self {
      digest: Some(sha256_hex(&data)),
      source: MediaSource::Inline { data },
      media_type: None,
    }
  }

  pub fn remote(url: impl Into<String>) -> Self {
    Self {
      source: MediaSource::Remote { url: url.into() },
      media_type: None,
      digest: None,
    }
  }

  pub fn asset(id: impl Into<String>, version: Option<String>) -> Self {
    Self {
      source: MediaSource::Asset(AssetReference { id: id.into(), version }),
      media_type: None,
      digest: None,
    }
  }

  pub fn provider(reference: ProviderAssetReference) -> Self {
    Self {
      source: MediaSource::Provider(Box::new(reference)),
      media_type: None,
      digest: None,
    }
  }

  pub fn with_media_type(mut self, media_type: impl Into<String>) -> Self {
    self.media_type = Some(media_type.into());
    self
  }

  pub fn with_digest(mut self, digest: impl Into<String>) -> Self {
    self.digest = Some(digest.into());
    self
  }

  pub fn bytes(&self) -> Option<&Bytes> {
    match &self.source {
      MediaSource::Inline { data } => Some(data),
      _ => None,
    }
  }

  /// Encode inline bytes for a wire adapter. No other source is fetched.
  pub fn encode_base64(&self) -> Option<String> {
    self.bytes().map(|bytes| STANDARD.encode(bytes))
  }

  pub fn data_url(&self) -> Result<String, ContentError> {
    self.validate()?;
    let media_type = self.media_type.as_deref().ok_or(ContentError::MissingMediaType)?;
    let data = self.encode_base64().ok_or(ContentError::UnresolvedResource)?;
    Ok(format!("data:{media_type};base64,{data}"))
  }

  pub fn from_base64(media_type: impl Into<String>, encoded: &str) -> Result<Self, ContentError> {
    let data = STANDARD.decode(encoded).map_err(|_| ContentError::InvalidBase64)?;
    let resource = Self::inline(media_type, data);
    resource.validate()?;
    Ok(resource)
  }

  pub fn from_data_url(value: &str) -> Result<Self, ContentError> {
    let (header, encoded) = value
      .strip_prefix("data:")
      .and_then(|s| s.split_once(','))
      .ok_or(ContentError::InvalidDataUrl)?;
    let media_type = header.strip_suffix(";base64").ok_or(ContentError::InvalidDataUrl)?;
    Self::from_base64(media_type, encoded)
  }

  pub fn validate(&self) -> Result<(), ContentError> {
    if let Some(media_type) = &self.media_type {
      let Some((kind, subtype)) = media_type.split_once('/') else {
        return Err(ContentError::InvalidMediaType);
      };
      if kind.is_empty() || subtype.is_empty() || !kind.bytes().chain(subtype.bytes()).all(is_mime_token) {
        return Err(ContentError::InvalidMediaType);
      }
    }
    if let Some(digest) = &self.digest {
      validate_digest(digest)?;
    }
    match &self.source {
      MediaSource::Inline { data } => {
        if data.is_empty() {
          return Err(ContentError::EmptyResource);
        }
        if self.digest.as_ref().is_some_and(|digest| *digest != sha256_hex(data)) {
          return Err(ContentError::DigestMismatch);
        }
      }
      MediaSource::Remote { url } => {
        let authority_and_path = url
          .strip_prefix("https://")
          .or_else(|| url.strip_prefix("http://"))
          .ok_or(ContentError::InvalidUrl)?;
        let authority = authority_and_path.split(['/', '?', '#']).next().unwrap_or_default();
        if authority.is_empty() || authority.contains('@') || url.chars().any(|c| c.is_whitespace() || c.is_control()) {
          return Err(ContentError::InvalidUrl);
        }
      }
      MediaSource::Asset(reference) => {
        nonempty(&reference.id, "asset id")?;
        if let Some(version) = &reference.version {
          nonempty(version, "asset version")?;
        }
      }
      MediaSource::Provider(reference) => reference.validate()?,
    }
    Ok(())
  }
}

impl ProviderAssetReference {
  pub fn validate(&self) -> Result<(), ContentError> {
    nonempty(&self.id, "provider asset id")?;
    nonempty(&self.namespace, "provider asset namespace")?;
    nonempty(&self.scope.provider, "provider asset provider")?;
    nonempty(&self.scope.account, "provider asset account")?;
    validate_digest(&self.scope.backend)?;
    if let Some(digest) = &self.scope.upstream_account_digest {
      validate_digest(digest)?;
    }
    Ok(())
  }
}

fn is_mime_token(byte: u8) -> bool {
  byte.is_ascii_alphanumeric() || matches!(byte, b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-')
}

fn validate_digest(digest: &str) -> Result<(), ContentError> {
  if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f')) {
    return Err(ContentError::InvalidDigest);
  }
  Ok(())
}

mod base64_bytes {
  use super::*;

  pub fn serialize<S: Serializer>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&STANDARD.encode(bytes))
  }

  pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
    let encoded = String::deserialize(deserializer)?;
    STANDARD
      .decode(encoded)
      .map(Bytes::from)
      .map_err(serde::de::Error::custom)
  }
}
