use core::fmt;

use super::Response;

/// Failures from a buffered direct request.
///
/// HTTP failures retain the original status, headers, and response bytes.
/// Display/debug output omits that body; inspect it explicitly when needed.
#[derive(Debug)]
pub enum Error<E> {
  Transport(E),
  Serialize(serde_json::Error),
  Deserialize(serde_json::Error),
  HttpStatus { response: Response },
  StreamingUnsupported,
  UnexpectedStream,
}

impl<E: fmt::Display> fmt::Display for Error<E> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Transport(error) => write!(f, "transport failed: {error}"),
      Self::Serialize(error) => write!(f, "failed to serialize request: {error}"),
      Self::Deserialize(error) => write!(f, "failed to deserialize response: {error}"),
      Self::HttpStatus { response } => write!(f, "OpenAI request failed with HTTP status {}", response.status),
      Self::StreamingUnsupported => {
        f.write_str("buffered requests require stream=false; use a streaming method instead")
      }
      Self::UnexpectedStream => f.write_str("received an event stream for a buffered request"),
    }
  }
}

impl<E: core::error::Error + 'static> core::error::Error for Error<E> {
  fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
    match self {
      Self::Transport(error) => Some(error),
      Self::Serialize(error) | Self::Deserialize(error) => Some(error),
      _ => None,
    }
  }
}
