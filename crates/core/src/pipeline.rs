use crate::provider::Endpoint;
use async_trait::async_trait;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use tokn_headers::HeaderMap;

pub type SendFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug)]
pub struct RequestMeta {
  pub endpoint: Endpoint,
  pub upstream_endpoint: Endpoint,
  pub model: String,
  pub upstream_model: String,
  pub stream: bool,
  pub session_id: Option<String>,
  pub request_id: Option<String>,
  /// Retry attempt number (0 = first attempt, 1 = first retry, ...).
  pub attempt: u32,
  pub project_id: Option<String>,
  /// Persistable initiator when the request carried a concrete signal.
  /// `None` means the runtime may still fall back internally, but we
  /// should not treat that default as ground truth in storage.
  pub initiator: Option<String>,
  /// Raw initiator from x-initiator header, if valid.
  pub header_initiator: Option<String>,
  pub inbound_headers: HeaderMap,
}

#[derive(Clone, Debug)]
pub struct ParsedRequest {
  pub meta: RequestMeta,
  pub body: Value,
}

pub trait InputTransformer: Send + Sync {
  fn transform_input(&self, endpoint: Endpoint, body: Value) -> crate::provider::Result<Value>;
}

pub trait RequestResolver: Send + Sync {
  type State;
  type Resolved;
  type Error;

  fn resolve(&self, state: &Self::State, parsed: ParsedRequest, attempt: usize) -> Result<Self::Resolved, Self::Error>;
}

pub trait RequestSender: Send + Sync {
  type State;
  type Request;
  type Response;
  type Error;

  fn send<'a>(&'a self, state: &'a Self::State, req: &'a Self::Request) -> SendFuture<'a, Self::Response, Self::Error>;
}

#[async_trait]
#[allow(
  clippy::double_must_use,
  reason = "async_trait adds must_use to methods returning must-use futures"
)]
pub trait OutputTransformer: Send + Sync {
  type State;
  type Upstream;
  type Output;

  async fn transform_result(&self, state: Self::State, upstream: Self::Upstream) -> Self::Output;

  async fn transform_sse(&self, state: Self::State, upstream: Self::Upstream) -> Self::Output;
}
