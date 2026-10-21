//! Platform-independent ordering and retry control for the request pipeline.
//!
//! A session owns one request's adapters. Its final stage consumes the session,
//! allowing a local response body to retain a borrowed transport without making
//! the server's concurrent request sessions borrow a global mutable client.

use core::time::Duration;

/// Default handling of a received upstream status. Opaque adapters may forward
/// failures verbatim, but managed execution shares this replay classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusDisposition {
  Success,
  PermanentFailure,
  RecoverableFailure,
}

/// Preserve the established managed behavior: client errors are permanent,
/// server errors are recoverable, and informational/success/redirect responses
/// proceed to the response stage. Replay safety and limits are checked separately.
pub const fn classify_status(status: u16) -> StatusDisposition {
  if status >= 500 {
    StatusDisposition::RecoverableFailure
  } else if status >= 400 {
    StatusDisposition::PermanentFailure
  } else {
    StatusDisposition::Success
  }
}

/// Saturating exponential delay used by every pipeline platform.
pub fn backoff(initial: Duration, attempt: u32) -> Duration {
  initial.saturating_mul(1u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX))
}

/// The six stages of one request, supplied by a platform session.
#[allow(async_fn_in_trait)]
pub trait Session: Sized {
  type Extracted;
  type Resolved;
  type Headers;
  type Converted;
  type Sent;
  type Output;
  type Error;

  fn start(&mut self, attempt: u32);
  async fn extract(&mut self) -> Result<Self::Extracted, Self::Error>;
  async fn resolve(&mut self, extracted: &Self::Extracted) -> Result<Self::Resolved, Self::Error>;
  async fn build_headers(
    &mut self,
    extracted: &Self::Extracted,
    resolved: &Self::Resolved,
  ) -> Result<Self::Headers, Self::Error>;
  async fn convert_request(
    &mut self,
    extracted: &Self::Extracted,
    resolved: &Self::Resolved,
    headers: &Self::Headers,
  ) -> Result<Self::Converted, Self::Error>;
  async fn send(
    &mut self,
    extracted: &Self::Extracted,
    resolved: &Self::Resolved,
    headers: &Self::Headers,
    converted: &Self::Converted,
  ) -> Result<Self::Sent, Self::Error>;
  async fn convert_response(self, sent: Self::Sent) -> Result<Self::Output, Self::Error>;
  /// Publish a failed attempt and decide whether its send may be replayed.
  fn failed(&mut self, attempt: u32, error: &Self::Error) -> Option<Duration>;
  async fn sleep(&mut self, delay: Duration);
}

/// Execute the same stage sequence for standard and local sessions.
pub async fn run<S: Session>(mut session: S) -> Result<S::Output, S::Error> {
  let mut attempt = 0;
  loop {
    session.start(attempt);
    // Only the send stage is eligible for retry. Re-entering the sequence
    // deliberately resolves accounts again after recording a send failure.
    macro_rules! permanent {
      ($operation:expr) => {
        match $operation {
          Ok(value) => value,
          Err(error) => {
            session.failed(attempt, &error);
            return Err(error);
          }
        }
      };
    }
    let extracted = permanent!(session.extract().await);
    let resolved = permanent!(session.resolve(&extracted).await);
    let headers = permanent!(session.build_headers(&extracted, &resolved).await);
    let converted = permanent!(session.convert_request(&extracted, &resolved, &headers).await);
    match session.send(&extracted, &resolved, &headers, &converted).await {
      Ok(sent) => return session.convert_response(sent).await,
      Err(error) => match session.failed(attempt, &error) {
        Some(delay) => {
          if !delay.is_zero() {
            session.sleep(delay).await;
          }
          attempt = attempt.saturating_add(1);
        }
        None => return Err(error),
      },
    }
  }
}
