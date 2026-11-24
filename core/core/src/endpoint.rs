use crate::types::SmolStr;
use serde::Serialize;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Endpoint {
  ChatCompletions,
  Responses,
  Messages,
}

impl Endpoint {
  pub fn as_str(self) -> &'static str {
    match self {
      Endpoint::ChatCompletions => "chat_completions",
      Endpoint::Responses => "responses",
      Endpoint::Messages => "messages",
    }
  }

  /// Best-effort guess at which [`Endpoint`] variant a given path
  /// represents. Used only to populate [`tokn_requests::RawInbound::endpoint`];
  /// the proxy passthrough pipeline never branches on it.
  pub fn infer_from(path: impl AsRef<str>) -> Option<Self> {
    let path = path.as_ref();
    if path.ends_with("/chat/completions") {
      Some(Endpoint::ChatCompletions)
    } else if path.ends_with("/responses") {
      Some(Endpoint::Responses)
    } else if path.ends_with("/messages") {
      Some(Endpoint::Messages)
    } else {
      None
    }
  }
}

impl core::fmt::Display for Endpoint {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str(self.as_str())
  }
}

/// The requested operation or an opaque path retained by a passthrough request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(untagged)]
pub enum RequestEndpoint {
  Known(Endpoint),
  CustomPath(SmolStr),
}

impl RequestEndpoint {
  pub fn as_str(&self) -> &str {
    match self {
      RequestEndpoint::Known(endpoint) => endpoint.as_str(),
      RequestEndpoint::CustomPath(label) => label.as_str(),
    }
  }

  pub fn custom(label: impl Into<SmolStr>) -> Self {
    Self::CustomPath(label.into())
  }

  pub fn infer_from_path(path: impl AsRef<str>) -> Self {
    let path = path.as_ref();
    if let Some(endpoint) = Endpoint::infer_from(path) {
      Self::Known(endpoint)
    } else {
      Self::CustomPath(SmolStr::new(path))
    }
  }

  pub fn resolved(&self) -> Option<Endpoint> {
    match self {
      RequestEndpoint::Known(endpoint) => Some(*endpoint),
      RequestEndpoint::CustomPath(_) => None,
    }
  }
}

impl From<Endpoint> for RequestEndpoint {
  fn from(value: Endpoint) -> Self {
    Self::Known(value)
  }
}

impl PartialEq<Endpoint> for RequestEndpoint {
  fn eq(&self, other: &Endpoint) -> bool {
    match self {
      RequestEndpoint::Known(endpoint) => endpoint == other,
      RequestEndpoint::CustomPath(_) => false,
    }
  }
}

impl core::fmt::Display for RequestEndpoint {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str(self.as_str())
  }
}

/// Per-provider declarative rule mapping a model id glob pattern to the
/// set of endpoints that model is allowed to be served on.
///
/// Patterns use a tiny `*`-only glob (no character classes, no `?`).
/// Examples: `"claude-*"`, `"gpt-5*"`, `"o4-mini"`.
///
/// Lives in `tokn-core` so both [`ProviderInfo`] and the descriptor type
/// in `tokn-auth` can reference it without a dependency cycle.
#[derive(Copy, Clone, Debug)]
pub struct EndpointRule {
  pub pattern: &'static str,
  pub endpoints: &'static [Endpoint],
}

/// Walk `rules` in order; the first pattern that matches `model` wins.
/// Returns `Some(true)` if that rule allows `endpoint`, `Some(false)` if
/// it explicitly does not, and `None` if no rule matched.
pub fn match_endpoint_rule(rules: &[EndpointRule], model: &str, endpoint: Endpoint) -> Option<bool> {
  for rule in rules {
    if glob_match(rule.pattern, model) {
      return Some(rule.endpoints.contains(&endpoint));
    }
  }
  None
}

/// Tiny `*`-only glob matcher. `*` matches any (possibly empty) run of
/// characters; everything else matches literally. ASCII case-sensitive.
pub fn glob_match(pattern: &str, input: &str) -> bool {
  let p = pattern.as_bytes();
  let s = input.as_bytes();
  fn rec(p: &[u8], s: &[u8]) -> bool {
    let mut pi = 0;
    let mut si = 0;
    while pi < p.len() {
      if p[pi] == b'*' {
        // Collapse consecutive '*'.
        while pi < p.len() && p[pi] == b'*' {
          pi += 1;
        }
        if pi == p.len() {
          return true;
        }
        let rest = &p[pi..];
        while si <= s.len() {
          if rec(rest, &s[si..]) {
            return true;
          }
          si += 1;
        }
        return false;
      } else {
        if si >= s.len() || p[pi] != s[si] {
          return false;
        }
        pi += 1;
        si += 1;
      }
    }
    si == s.len()
  }
  rec(p, s)
}
