use crate::keys::X_CODEX_TURN_METADATA;
use crate::{HeaderMap, TemplateVars};
#[cfg(test)]
use alloc::string::ToString;
use serde::Deserialize;
use tokn_core::types::SmolStr;

pub const SESSION_ID_HEADERS: &[&str] = &[
  "x-session-id",
  "x-client-session-id",
  "session-id",
  "session_id",
  "x-session-affinity",
  "x-opencode-session",
];

pub const THREAD_ID_HEADERS: &[&str] = &["thread-id", "thread_id"];

pub const PARENT_THREAD_ID_HEADERS: &[&str] = &[
  "x-codex-parent-thread-id",
  "x-parent-thread-id",
  "parent-thread-id",
  "parent_thread_id",
];

pub const REQUEST_ID_HEADERS: &[&str] = &["x-request-id", "x-interaction-id", "x-opencode-request"];

pub const PROJECT_ID_HEADERS: &[&str] = &["x-opencode-project", "x-project-cwd"];

pub const INTERACTION_ID_HEADERS: &[&str] = &["x-interaction-id"];

pub const ACCOUNT_ID_HEADERS: &[&str] = &["chatgpt-account-id"];

/// Correlation identifiers normalized from direct headers and structured
/// client metadata. Direct headers are authoritative, while metadata may fill
/// missing descendants only when its parent identifiers are consistent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InboundCorrelation {
  pub session_id: Option<SmolStr>,
  pub thread_id: Option<SmolStr>,
  pub parent_thread_id: Option<SmolStr>,
  pub turn_id: Option<SmolStr>,
}

#[derive(Debug, Default, Deserialize)]
struct CodexTurnMetadata {
  session_id: Option<SmolStr>,
  thread_id: Option<SmolStr>,
  parent_thread_id: Option<SmolStr>,
  turn_id: Option<SmolStr>,
}

pub fn first_present<'a>(headers: &'a HeaderMap, names: &[&str]) -> Option<&'a str> {
  names.iter().find_map(|name| {
    headers
      .get(*name)
      .map(|value| value.as_str().trim())
      .filter(|value| !value.is_empty())
  })
}

pub fn first_present_smol(headers: &HeaderMap, names: &[&str]) -> Option<SmolStr> {
  first_present(headers, names).map(SmolStr::new)
}

/// Resolve inbound correlation without treating structured metadata headers
/// as literal identifier headers. Malformed metadata is ignored so request
/// handling remains best-effort.
pub fn inbound_correlation(headers: &HeaderMap) -> InboundCorrelation {
  let metadata = headers
    .get(&X_CODEX_TURN_METADATA)
    .and_then(|value| serde_json::from_str::<CodexTurnMetadata>(value.as_str()).ok())
    .map(|metadata| InboundCorrelation {
      session_id: non_empty(metadata.session_id),
      thread_id: non_empty(metadata.thread_id),
      parent_thread_id: non_empty(metadata.parent_thread_id),
      turn_id: non_empty(metadata.turn_id),
    })
    .unwrap_or_default();

  reconcile_correlation(
    InboundCorrelation {
      session_id: first_present_smol(headers, SESSION_ID_HEADERS),
      thread_id: first_present_smol(headers, THREAD_ID_HEADERS),
      parent_thread_id: first_present_smol(headers, PARENT_THREAD_ID_HEADERS),
      turn_id: None,
    },
    metadata,
  )
}

fn reconcile_correlation(direct: InboundCorrelation, metadata: InboundCorrelation) -> InboundCorrelation {
  let session_is_compatible = metadata_scope_is_compatible(&direct.session_id, &metadata.session_id);
  let thread_is_compatible =
    session_is_compatible && metadata_scope_is_compatible(&direct.thread_id, &metadata.thread_id);

  InboundCorrelation {
    session_id: direct.session_id.or_else(|| metadata.session_id.clone()),
    thread_id: direct
      .thread_id
      .or_else(|| session_is_compatible.then_some(metadata.thread_id).flatten()),
    parent_thread_id: direct
      .parent_thread_id
      .or_else(|| thread_is_compatible.then_some(metadata.parent_thread_id).flatten()),
    turn_id: thread_is_compatible.then_some(metadata.turn_id).flatten(),
  }
}

fn metadata_scope_is_compatible(direct: &Option<SmolStr>, metadata: &Option<SmolStr>) -> bool {
  match metadata {
    Some(metadata) => direct.as_ref().is_none_or(|direct| direct == metadata),
    None => false,
  }
}

fn non_empty(value: Option<SmolStr>) -> Option<SmolStr> {
  value.and_then(|value| {
    let value = value.trim();
    (!value.is_empty()).then(|| SmolStr::new(value))
  })
}

pub fn build_template_vars(headers: &HeaderMap) -> TemplateVars {
  let correlation = inbound_correlation(headers);
  TemplateVars {
    session_id: correlation.session_id,
    request_id: first_present_smol(headers, REQUEST_ID_HEADERS),
    project_cwd: first_present_smol(headers, PROJECT_ID_HEADERS),
    interaction_id: first_present_smol(headers, INTERACTION_ID_HEADERS),
    account_id: first_present_smol(headers, ACCOUNT_ID_HEADERS),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{HeaderName, HeaderValue};

  fn header_map(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
      headers.insert(HeaderName::new(*name), HeaderValue::from_string((*value).to_string()));
    }
    headers
  }

  #[test]
  fn first_present_uses_priority_and_trims() {
    let headers = header_map(&[
      ("x-session-id", "   "),
      ("x-client-session-id", "   "),
      ("session-id", " sess-2 "),
      ("x-opencode-session", "sess-3"),
    ]);

    assert_eq!(first_present(&headers, SESSION_ID_HEADERS), Some("sess-2"));
  }

  #[test]
  fn template_vars_cover_all_correlation_headers() {
    let headers = header_map(&[
      ("x-session-id", "ses_abc"),
      ("x-request-id", "req_xyz"),
      ("x-project-cwd", "/tmp/work"),
      ("x-interaction-id", "int_9"),
      ("chatgpt-account-id", "acct_42"),
    ]);

    let vars = build_template_vars(&headers);
    assert_eq!(vars.session_id.as_deref(), Some("ses_abc"));
    assert_eq!(vars.request_id.as_deref(), Some("req_xyz"));
    assert_eq!(vars.project_cwd.as_deref(), Some("/tmp/work"));
    assert_eq!(vars.interaction_id.as_deref(), Some("int_9"));
    assert_eq!(vars.account_id.as_deref(), Some("acct_42"));
  }

  #[test]
  fn thread_headers_keep_thread_topology_separate_from_session_identity() {
    let headers = header_map(&[
      ("session-id", "session-root"),
      ("thread-id", "thread-child"),
      ("x-codex-parent-thread-id", "thread-root"),
    ]);

    assert_eq!(first_present(&headers, SESSION_ID_HEADERS), Some("session-root"));
    assert_eq!(first_present(&headers, THREAD_ID_HEADERS), Some("thread-child"));
    assert_eq!(first_present(&headers, PARENT_THREAD_ID_HEADERS), Some("thread-root"));
  }

  #[test]
  fn correlation_falls_back_to_codex_turn_metadata() {
    let headers = header_map(&[(
      "x-codex-turn-metadata",
      r#"{"session_id":" session-meta ","thread_id":"thread-meta","parent_thread_id":"thread-parent","turn_id":"turn-meta","workspaces":{}}"#,
    )]);

    assert_eq!(
      inbound_correlation(&headers),
      InboundCorrelation {
        session_id: Some(SmolStr::new("session-meta")),
        thread_id: Some(SmolStr::new("thread-meta")),
        parent_thread_id: Some(SmolStr::new("thread-parent")),
        turn_id: Some(SmolStr::new("turn-meta")),
      }
    );
    assert_eq!(
      build_template_vars(&headers).session_id.as_deref(),
      Some("session-meta")
    );
  }

  #[test]
  fn direct_headers_take_precedence_when_metadata_matches_their_scope() {
    let headers = header_map(&[
      ("session-id", "session-direct"),
      ("thread-id", "thread-direct"),
      ("parent-thread-id", "parent-direct"),
      (
        "x-codex-turn-metadata",
        r#"{"session_id":"session-direct","thread_id":"thread-direct","parent_thread_id":"parent-meta","turn_id":"turn-meta"}"#,
      ),
    ]);

    let correlation = inbound_correlation(&headers);
    assert_eq!(correlation.session_id.as_deref(), Some("session-direct"));
    assert_eq!(correlation.thread_id.as_deref(), Some("thread-direct"));
    assert_eq!(correlation.parent_thread_id.as_deref(), Some("parent-direct"));
    assert_eq!(correlation.turn_id.as_deref(), Some("turn-meta"));
  }

  #[test]
  fn matching_metadata_fills_missing_descendants() {
    let headers = header_map(&[
      ("session-id", "session-direct"),
      (
        "x-codex-turn-metadata",
        r#"{"session_id":"session-direct","thread_id":"thread-meta","parent_thread_id":"parent-meta","turn_id":"turn-meta"}"#,
      ),
    ]);

    assert_eq!(
      inbound_correlation(&headers),
      InboundCorrelation {
        session_id: Some(SmolStr::new("session-direct")),
        thread_id: Some(SmolStr::new("thread-meta")),
        parent_thread_id: Some(SmolStr::new("parent-meta")),
        turn_id: Some(SmolStr::new("turn-meta")),
      }
    );
  }

  #[test]
  fn conflicting_metadata_session_does_not_create_a_hybrid_path() {
    let headers = header_map(&[
      ("session-id", "session-direct"),
      (
        "x-codex-turn-metadata",
        r#"{"session_id":"session-meta","thread_id":"thread-meta","parent_thread_id":"parent-meta","turn_id":"turn-meta"}"#,
      ),
    ]);

    assert_eq!(
      inbound_correlation(&headers),
      InboundCorrelation {
        session_id: Some(SmolStr::new("session-direct")),
        ..InboundCorrelation::default()
      }
    );
  }

  #[test]
  fn conflicting_metadata_thread_does_not_fill_thread_descendants() {
    let headers = header_map(&[
      ("session-id", "session-direct"),
      ("thread-id", "thread-direct"),
      ("parent-thread-id", "parent-direct"),
      (
        "x-codex-turn-metadata",
        r#"{"session_id":"session-direct","thread_id":"thread-meta","parent_thread_id":"parent-meta","turn_id":"turn-meta"}"#,
      ),
    ]);

    assert_eq!(
      inbound_correlation(&headers),
      InboundCorrelation {
        session_id: Some(SmolStr::new("session-direct")),
        thread_id: Some(SmolStr::new("thread-direct")),
        parent_thread_id: Some(SmolStr::new("parent-direct")),
        ..InboundCorrelation::default()
      }
    );
  }

  #[test]
  fn metadata_descendants_require_their_parent_identifiers() {
    let headers = header_map(&[
      ("session-id", "session-direct"),
      (
        "x-codex-turn-metadata",
        r#"{"session_id":"session-direct","parent_thread_id":"parent-meta","turn_id":"turn-meta"}"#,
      ),
    ]);

    assert_eq!(
      inbound_correlation(&headers),
      InboundCorrelation {
        session_id: Some(SmolStr::new("session-direct")),
        ..InboundCorrelation::default()
      }
    );
  }

  #[test]
  fn malformed_or_empty_metadata_does_not_create_correlation() {
    let malformed = header_map(&[("x-codex-turn-metadata", "not-json")]);
    assert_eq!(inbound_correlation(&malformed), InboundCorrelation::default());

    let empty = header_map(&[(
      "x-codex-turn-metadata",
      r#"{"session_id":" ","thread_id":"","turn_id":null}"#,
    )]);
    assert_eq!(inbound_correlation(&empty), InboundCorrelation::default());
  }
}
