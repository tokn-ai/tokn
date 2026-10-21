use alloc::string::String;
use thiserror::Error;

pub type Result<T, E = ConvertError> = core::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum ConvertError {
  #[error("missing required field `{field}`")]
  MissingField { field: &'static str },

  #[error("bad `{field}` shape: {message}")]
  BadShape { field: &'static str, message: String },

  #[error("unsupported conversion feature: {message}")]
  UnsupportedFeature { message: String },

  #[error("json conversion failed")]
  Json { source: serde_json::Error },

  #[error("sse conversion failed: {message}")]
  Sse { message: String },
}

impl ConvertError {
  pub fn bad_shape(field: &'static str, message: impl Into<String>) -> Self {
    Self::BadShape {
      field,
      message: message.into(),
    }
  }

  pub fn sse(message: impl Into<String>) -> Self {
    Self::Sse {
      message: message.into(),
    }
  }
}

impl From<serde_json::Error> for ConvertError {
  fn from(source: serde_json::Error) -> Self {
    Self::Json { source }
  }
}
