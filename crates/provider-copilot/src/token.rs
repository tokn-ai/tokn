use crate::config::CopilotHeaders;
use crate::provider::Result;
use crate::util::redact::token_fingerprint;
use tracing::{debug, instrument};

pub use crate::native::CopilotTokenResponse as CopilotTokenResp;

/// Exchange a long-lived GitHub OAuth token for a short-lived Copilot API token.
#[instrument(
  name = "copilot_token_exchange",
  skip_all,
  fields(
    github_token_fp = %token_fingerprint(github_token),
    api_token_fp = tracing::field::Empty,
    expires_at = tracing::field::Empty,
    status = tracing::field::Empty,
  ),
)]
pub async fn exchange(
  client: &reqwest::Client,
  github_token: &str,
  headers: &CopilotHeaders,
) -> Result<CopilotTokenResp> {
  let h = crate::headers::token_exchange_headers(github_token, headers)?;
  debug!("posting token exchange");
  let resp = crate::util::http::send(
    client,
    reqwest::Method::GET,
    crate::COPILOT_TOKEN_EXCHANGE_URL,
    h,
    None,
    None,
    "token exchange",
  )
  .await?;
  let status = resp.status();
  tracing::Span::current().record("status", status.as_u16());
  let parsed: CopilotTokenResp = crate::util::http::read_json(resp, "token exchange").await?;
  let span = tracing::Span::current();
  span.record(
    "api_token_fp",
    tracing::field::display(token_fingerprint(&parsed.token)),
  );
  span.record("expires_at", parsed.expires_at);
  debug!("token exchange ok");
  Ok(parsed)
}
