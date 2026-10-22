use crate::{BufferedResponse, Client, Platform, PlatformResponse, RequestOptions};
use tokn_core::provider::Endpoint;
use tokn_endpoint_chat_completions::{ChatRequest, ChatResponse};
use tokn_endpoint_messages::{MessagesRequest, MessagesResponse};
use tokn_endpoint_responses::{ResponsesRequest, ResponsesResponse};

macro_rules! endpoint_client {
  (
    $name:ident,
    $request:ty,
    $response:ty,
    $endpoint:expr
  ) => {
    #[cfg(feature = "std")]
    pub struct $name<'client, P: Platform = crate::StdPlatform> {
      client: &'client Client<P>,
    }
    #[cfg(not(feature = "std"))]
    pub struct $name<'client, P: Platform> {
      client: &'client Client<P>,
    }

    impl<'client, P: Platform> $name<'client, P> {
      pub(crate) fn new(client: &'client Client<P>) -> Self {
        Self { client }
      }

      pub async fn create(&self, request: &$request) -> Result<BufferedResponse<$response>, P::Error> {
        self.create_with(request, RequestOptions::default()).await
      }

      pub async fn create_with(
        &self,
        request: &$request,
        options: RequestOptions,
      ) -> Result<BufferedResponse<$response>, P::Error> {
        self
          .client
          .execute_typed($endpoint, request, false, options)
          .await?
          .into_json()
      }

      pub async fn stream(&self, request: &$request) -> Result<P::Stream<'_>, P::Error> {
        self.stream_with(request, RequestOptions::default()).await
      }

      pub async fn stream_with(&self, request: &$request, options: RequestOptions) -> Result<P::Stream<'_>, P::Error> {
        self
          .client
          .execute_typed($endpoint, request, true, options)
          .await?
          .into_stream()
      }
    }
  };
}

endpoint_client!(Responses, ResponsesRequest, ResponsesResponse, Endpoint::Responses);
endpoint_client!(ChatCompletions, ChatRequest, ChatResponse, Endpoint::ChatCompletions);
endpoint_client!(Messages, MessagesRequest, MessagesResponse, Endpoint::Messages);
