//! Local-environment credential importers for github-copilot.
//!
//! These two helpers replace what used to live in the CLI's
//! `onboarding.rs`. They acquire a long-lived OAuth token from the
//! user's local machine without contacting any GitHub endpoint:
//!
//! * [`from_gh`] shells out to `gh auth token` (the GitHub CLI).
//! * [`from_copilot_plugin`] scrapes the Copilot editor plugin's
//!   credential cache under `~/.config/github-copilot/`.
//!
//! Both return a raw OAuth token string ready to be fed into a
//! [`crate::CopilotAuth`] account; the caller is responsible for
//! exchanging it for a short-lived access token via the normal refresh
//! path.
//!
//! Errors are reported as `String` to keep the public surface free of
//! crate-local error types — the trait method that wraps these
//! converts them to [`tokn_auth::AuthError`].

use std::path::PathBuf;
use std::process::Command;

/// Run `gh auth token` and return the resulting OAuth token. Fails if
/// the GitHub CLI is not installed, the user is not logged in, or the
/// command produces an empty string.
pub fn from_gh() -> Result<String, String> {
  let out = Command::new("gh")
    .args(["auth", "token"])
    .output()
    .map_err(|e| format!("running `gh auth token` (is the GitHub CLI installed?): {e}"))?;
  if !out.status.success() {
    return Err(format!(
      "`gh auth token` failed: {}",
      String::from_utf8_lossy(&out.stderr).trim()
    ));
  }
  let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
  if token.is_empty() {
    return Err("`gh auth token` returned an empty token".into());
  }
  Ok(token)
}

/// Scrape the Copilot editor plugin's credential cache.
///
/// The plugin writes one of `~/.config/github-copilot/apps.json` or
/// `~/.config/github-copilot/hosts.json` (depending on plugin version)
/// containing nested JSON with an `oauth_token` (or `token`) field. We
/// walk the JSON tree and return the first non-empty value we find.
pub fn from_copilot_plugin() -> Result<String, String> {
  let home: PathBuf = directories::BaseDirs::new()
    .ok_or_else(|| "cannot resolve home dir".to_string())?
    .home_dir()
    .to_path_buf();
  let candidates = [
    home.join(".config/github-copilot/apps.json"),
    home.join(".config/github-copilot/hosts.json"),
  ];
  for path in &candidates {
    if !path.exists() {
      continue;
    }
    let raw = std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let v: serde_json::Value = serde_json::from_str(&raw).map_err(|e| format!("parse {}: {e}", path.display()))?;
    if let Some(t) = scan_token(&v) {
      return Ok(t);
    }
  }
  Err("no Copilot plugin token found in ~/.config/github-copilot/".into())
}

/// Recursively walk a JSON value, returning the first non-empty
/// `oauth_token` or `token` string field encountered.
fn scan_token(v: &serde_json::Value) -> Option<String> {
  match v {
    serde_json::Value::Object(m) => {
      for (k, val) in m {
        if (k == "oauth_token" || k == "token") && val.as_str().filter(|s| !s.is_empty()).is_some() {
          return val.as_str().map(|s| s.to_string());
        }
        if let Some(found) = scan_token(val) {
          return Some(found);
        }
      }
      None
    }
    serde_json::Value::Array(a) => a.iter().find_map(scan_token),
    _ => None,
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn scan_token_finds_oauth_token_at_root() {
    let v = json!({"oauth_token": "abc"});
    assert_eq!(scan_token(&v).as_deref(), Some("abc"));
  }

  #[test]
  fn scan_token_finds_nested_token() {
    let v = json!({"github.com": {"user": "x", "oauth_token": "deep"}});
    assert_eq!(scan_token(&v).as_deref(), Some("deep"));
  }

  #[test]
  fn scan_token_skips_empty_strings() {
    let v = json!({"oauth_token": "", "child": {"token": "found"}});
    assert_eq!(scan_token(&v).as_deref(), Some("found"));
  }

  #[test]
  fn scan_token_walks_arrays() {
    let v = json!([{"unrelated": 1}, {"token": "from_array"}]);
    assert_eq!(scan_token(&v).as_deref(), Some("from_array"));
  }

  #[test]
  fn scan_token_returns_none_when_absent() {
    let v = json!({"a": {"b": [1, 2, "x"]}});
    assert_eq!(scan_token(&v), None);
  }
}
