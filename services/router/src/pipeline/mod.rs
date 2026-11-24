pub(crate) mod parse;
pub(crate) mod request;

pub(crate) use parse::{request_header_extract, ChatParser, MessagesParser, RequestParser, ResponsesParser};

pub use request::{dry_run_request, DryRunEndpoint, DryRunOutput};

#[cfg(test)]
mod tests {
  use super::*;
  use axum::http::{HeaderMap, HeaderValue};
  use serde_json::json;
  use tokn_provider_zai::Endpoint;

  #[test]
  fn chat_parser_reads_request_metadata() {
    let mut headers = HeaderMap::new();
    headers.insert("x-session-id", HeaderValue::from_static("session-1"));
    headers.insert("x-request-id", HeaderValue::from_static("request-1"));
    headers.insert("x-opencode-project", HeaderValue::from_static("project-1"));
    let body = json!({
      "model": "gpt-4.1",
      "stream": true,
      "messages": [{"role": "user", "content": "hi"}]
    });

    let parsed = ChatParser.parse(headers.clone(), body.clone());

    assert_eq!(parsed.meta.endpoint, Endpoint::ChatCompletions);
    assert_eq!(parsed.meta.upstream_endpoint, Endpoint::ChatCompletions);
    assert_eq!(parsed.meta.model, "gpt-4.1");
    assert_eq!(parsed.meta.upstream_model, "gpt-4.1");
    assert!(parsed.meta.stream);
    assert_eq!(parsed.meta.session_id.as_deref(), Some("session-1"));
    assert_eq!(parsed.meta.request_id.as_deref(), Some("request-1"));
    assert_eq!(parsed.meta.project_id.as_deref(), Some("project-1"));
    assert_eq!(parsed.meta.initiator.as_deref(), Some("user"));
    assert_eq!(parsed.body, body);
    assert_eq!(
      parsed.meta.inbound_headers.get("x-session-id").map(|v| v.as_str()),
      headers.get("x-session-id").and_then(|v| v.to_str().ok())
    );
  }

  #[test]
  fn parser_uses_codex_turn_metadata_as_session_fallback() {
    let mut headers = HeaderMap::new();
    headers.insert(
      "x-codex-turn-metadata",
      HeaderValue::from_static(r#"{"session_id":"session-meta","thread_id":"thread-meta","turn_id":"turn-meta"}"#),
    );

    let parsed = ResponsesParser.parse(headers, json!({"model": "gpt-5", "input": "hi"}));

    assert_eq!(parsed.meta.session_id.as_deref(), Some("session-meta"));
  }

  #[test]
  fn infers_stream_from_accept_header_when_body_omits_it() {
    let mut headers = HeaderMap::new();
    headers.insert(
      axum::http::header::ACCEPT,
      HeaderValue::from_static("text/event-stream"),
    );
    let body = json!({
      "model": "gpt-5",
      "input": "hi"
    });

    let parsed = ResponsesParser.parse(headers, body);
    assert!(parsed.meta.stream);
  }

  #[test]
  fn explicit_stream_flag_overrides_accept_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
      axum::http::header::ACCEPT,
      HeaderValue::from_static("text/event-stream"),
    );
    let body = json!({
      "model": "gpt-5",
      "stream": false,
      "input": "hi"
    });

    let parsed = ResponsesParser.parse(headers, body);
    assert!(!parsed.meta.stream);
  }

  #[test]
  fn tool_followup_is_persisted_as_agent() {
    let body = json!({
      "model": "gpt-5",
      "messages": [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "working"}
      ]
    });

    let parsed = ChatParser.parse(HeaderMap::new(), body);
    assert_eq!(parsed.meta.initiator.as_deref(), Some("agent"));
  }
}
