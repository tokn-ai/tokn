use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokn_core::{headers::HeaderMap, Endpoint};

pub use tokn_requests::configured::RequestOptions;

/// The response operations common to standard and injected platforms.
pub trait PlatformResponse: Sized {
  type Error;
  type Stream;
  fn into_json<T: DeserializeOwned>(self) -> Result<BufferedResponse<T>, Self::Error>;
  fn into_stream(self) -> Result<Self::Stream, Self::Error>;
}

#[derive(Debug)]
pub struct BufferedResponse<T> {
  pub status: u16,
  pub headers: HeaderMap,
  pub data: T,
}

/// Platform services used by the single SDK client.
#[allow(async_fn_in_trait)]
pub trait Platform {
  type Error;
  type Stream<'a>
  where
    Self: 'a;
  type Response<'a>: PlatformResponse<Error = Self::Error, Stream = Self::Stream<'a>>
  where
    Self: 'a;
  fn encode_error(source: serde_json::Error) -> Self::Error;
  async fn execute(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
  ) -> Result<Self::Response<'_>, Self::Error>;
}

/// Configured LLM execution through shared routing, provider and pipeline code.
#[cfg(feature = "std")]
pub struct Client<P = crate::StdPlatform> {
  pub(crate) platform: P,
}
#[cfg(not(feature = "std"))]
pub struct Client<P> {
  pub(crate) platform: P,
}

impl<P> Client<P> {
  pub fn from_platform(platform: P) -> Self {
    Self { platform }
  }
  pub fn into_platform(self) -> P {
    self.platform
  }
}

impl<P: Platform> Client<P> {
  pub fn responses(&self) -> crate::Responses<'_, P> {
    crate::Responses::new(self)
  }
  pub fn chat_completions(&self) -> crate::ChatCompletions<'_, P> {
    crate::ChatCompletions::new(self)
  }
  pub fn messages(&self) -> crate::Messages<'_, P> {
    crate::Messages::new(self)
  }

  pub async fn execute(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
  ) -> Result<P::Response<'_>, P::Error> {
    self.platform.execute(endpoint, body, options).await
  }

  pub(crate) async fn execute_typed<T: Serialize>(
    &self,
    endpoint: Endpoint,
    request: &T,
    stream: bool,
    options: RequestOptions,
  ) -> Result<P::Response<'_>, P::Error> {
    let mut body = serde_json::to_value(request).map_err(P::encode_error)?;
    body["stream"] = Value::Bool(stream);
    self.execute(endpoint, body, options).await
  }
}

impl<T: tokn_service::transport::Transport, R: tokn_requests::configured::Runtime> Platform
  for tokn_requests::configured::Injected<T, R>
{
  type Error = tokn_requests::configured::Error;
  type Response<'a>
    = tokn_requests::configured::Response<'a, T, R>
  where
    Self: 'a;
  type Stream<'a>
    = tokn_requests::configured::EventStream<'a, T, R>
  where
    Self: 'a;
  fn encode_error(source: serde_json::Error) -> Self::Error {
    Self::Error::new("request", source)
  }
  async fn execute(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
  ) -> Result<Self::Response<'_>, Self::Error> {
    self.execute(endpoint, body, options).await
  }
}

impl<'a, T: tokn_service::transport::Transport, R: tokn_requests::configured::Runtime> PlatformResponse
  for tokn_requests::configured::Response<'a, T, R>
{
  type Error = tokn_requests::configured::Error;
  type Stream = tokn_requests::configured::EventStream<'a, T, R>;
  fn into_json<D: DeserializeOwned>(self) -> Result<BufferedResponse<D>, Self::Error> {
    let (status, headers, data) = self.into_json()?;
    Ok(BufferedResponse { status, headers, data })
  }
  fn into_stream(self) -> Result<Self::Stream, Self::Error> {
    match self.body {
      tokn_requests::configured::ResponseBody::Stream(stream) => Ok(*stream),
      _ => Err(Self::Error::new("response", "expected a streaming response")),
    }
  }
}
