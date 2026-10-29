use alloc::string::{String, ToString};
#[cfg(feature = "std")]
use std::path::PathBuf;
use tokn_core::Endpoint;
use tokn_requests::session::{self, ErrorKind};

/// SDK request construction, generation, and execution errors.
///
/// Standard builds retain the standard request error source; alloc-only builds
/// expose the shared session error directly through [`Error::Request`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[cfg(feature = "std")]
  #[error("failed to load configuration")]
  LoadConfig {
    #[source]
    source: tokn_config::Error,
  },
  #[cfg(feature = "std")]
  #[error("failed to project legacy configuration from {path}")]
  ProjectConfig {
    path: PathBuf,
    #[source]
    source: Box<tokn_router_legacy_config::v2::V2ProjectionError>,
  },
  #[cfg(feature = "std")]
  #[error("failed to load credentials from {path}")]
  LoadCredentials {
    path: PathBuf,
    #[source]
    source: anyhow::Error,
  },
  #[cfg(feature = "std")]
  #[error("failed to configure the request runtime")]
  BuildEngine {
    #[source]
    source: anyhow::Error,
  },
  #[error("unknown SDK profile '{profile}'")]
  UnknownProfile { profile: String },
  #[cfg(feature = "std")]
  #[error("this SDK client was constructed in memory and has no configuration files to reload")]
  ReloadUnavailable,
  #[error("failed to serialize request")]
  SerializeRequest {
    #[source]
    source: serde_json::Error,
  },
  #[error("failed to deserialize response")]
  DeserializeResponse {
    #[source]
    source: serde_json::Error,
  },
  #[error("invalid generation request: {message}")]
  InvalidGenerateRequest { message: String },
  #[error("failed to build provider-neutral generation request")]
  BuildGenerateRequest {
    #[source]
    source: tokn_convert::error::ConvertError,
  },
  #[error("generation request failed with HTTP status {status}: {body}")]
  GenerateResponseStatus { status: u16, body: String },
  #[error("generation stream failed: {message}")]
  GenerateStream { message: String },
  #[error("failed to deserialize generation stream event")]
  DeserializeStreamEvent {
    #[source]
    source: serde_json::Error,
  },
  #[error("request execution failed")]
  Request {
    #[cfg(feature = "std")]
    #[source]
    source: tokn_requests::RequestError,
    #[cfg(not(feature = "std"))]
    #[source]
    source: session::Error,
  },
  #[error("expected a buffered response but received a stream; use the endpoint's stream method")]
  UnexpectedStream,
  #[error("expected a streaming response but received a buffered body")]
  UnexpectedBuffered,
}

pub type Result<T> = core::result::Result<T, Error>;

impl Error {
  pub(crate) fn from_execution(error: session::Error, endpoint: Endpoint) -> Self {
    if let ErrorKind::UnknownProfile { profile } = &error.kind {
      return Self::UnknownProfile {
        profile: profile.to_string(),
      };
    }
    if error.stage == "generation" {
      return Self::InvalidGenerateRequest {
        message: error.message.to_string(),
      };
    }
    #[cfg(feature = "std")]
    {
      if error.stage == "config" {
        return Self::BuildEngine {
          source: anyhow::Error::new(error),
        };
      }
      Self::Request {
        source: tokn_requests::v2::adapters::pipeline_error(error, endpoint).into(),
      }
    }
    #[cfg(not(feature = "std"))]
    {
      let _ = endpoint;
      Self::Request { source: error }
    }
  }

  pub(crate) fn from_generation(error: session::Error, endpoint: Endpoint) -> Self {
    use tokn_requests::preparation::PreparationError;
    match &error.kind {
      ErrorKind::UpstreamStatus { status, body } => Self::GenerateResponseStatus {
        status: *status,
        body: body.to_string(),
      },
      ErrorKind::Preparation(
        source @ (PreparationError::InvalidGenerationOptions { .. }
        | PreparationError::UnsupportedGenerationControl { .. }),
      ) => Self::InvalidGenerateRequest {
        message: source.to_string(),
      },
      _ => Self::from_execution(error, endpoint),
    }
  }
}
