//! Minimal JWT *claim parser*. Does not verify signatures — codex's
//! ChatGPT OAuth flow trusts the issuer over TLS and only uses the
//! `id_token` to surface the `chatgpt_account_id` for outbound headers.
//!
//! Mirrors `parseJwtClaims` / `extractAccountIdFromClaims` from
//! `opencode/src/plugin/codex.ts`.

use alloc::{
  string::{String, ToString},
  vec::Vec,
};
use base64::Engine;
use serde::Deserialize;
use tokn_core::account::AccountConfig;

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct IdTokenClaims {
  pub exp: Option<i64>,
  pub chatgpt_account_id: Option<String>,
  pub email: Option<String>,
  pub organizations: Option<Vec<Organization>>,
  #[serde(rename = "https://api.openai.com/auth")]
  pub openai_auth: Option<OpenAiAuthClaim>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct OpenAiAuthClaim {
  pub chatgpt_account_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Organization {
  pub id: String,
}

/// Parse the unverified payload of a JWT. Returns `None` for malformed
/// inputs (wrong number of segments, base64 decode failure, JSON decode
/// failure).
pub fn parse_jwt_claims(token: &str) -> Option<IdTokenClaims> {
  let mut parts = token.split('.');
  let _header = parts.next()?;
  let payload = parts.next()?;
  let _sig = parts.next();
  // No fourth segment.
  if parts.next().is_some() {
    return None;
  }
  let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
    .decode(payload)
    .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(payload))
    .ok()?;
  serde_json::from_slice(&decoded).ok()
}

/// Pick the first available account id, mirroring opencode's precedence:
/// top-level `chatgpt_account_id` → namespaced
/// `https://api.openai.com/auth.chatgpt_account_id` → `organizations[0].id`.
pub fn extract_account_id(claims: &IdTokenClaims) -> Option<String> {
  claims
    .chatgpt_account_id
    .as_deref()
    .filter(|id| !id.trim().is_empty())
    .or_else(|| {
      claims
        .openai_auth
        .as_ref()
        .and_then(|auth| auth.chatgpt_account_id.as_deref())
        .filter(|id| !id.trim().is_empty())
    })
    .or_else(|| {
      claims
        .organizations
        .as_ref()
        .and_then(|organizations| organizations.first().map(|organization| organization.id.as_str()))
        .filter(|id| !id.trim().is_empty())
    })
    .map(str::to_string)
}

/// Resolve the account context for imported credentials that may only carry
/// their ChatGPT account id inside a token. Explicit configuration wins.
pub fn account_id(account: &AccountConfig) -> Option<String> {
  account
    .provider_account_id
    .as_ref()
    .filter(|id| !id.trim().is_empty())
    .cloned()
    .or_else(|| {
      [account.id_token.as_ref(), account.access_token.as_ref()]
        .into_iter()
        .flatten()
        .filter_map(|token| parse_jwt_claims(token.expose()))
        .find_map(|claims| extract_account_id(&claims))
    })
}

#[cfg(test)]
mod tests {
  use super::*;
  use base64::Engine;

  fn jwt(payload: &serde_json::Value) -> String {
    let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
    let body = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    format!("{header}.{body}.")
  }

  #[test]
  fn parses_top_level_account_id() {
    let token = jwt(&serde_json::json!({"chatgpt_account_id": "acc-1", "email": "a@b"}));
    let claims = parse_jwt_claims(&token).unwrap();
    assert_eq!(extract_account_id(&claims).as_deref(), Some("acc-1"));
  }

  #[test]
  fn parses_namespaced_account_id() {
    let token = jwt(&serde_json::json!({
      "https://api.openai.com/auth": {"chatgpt_account_id": "acc-2"},
    }));
    let claims = parse_jwt_claims(&token).unwrap();
    assert_eq!(extract_account_id(&claims).as_deref(), Some("acc-2"));
  }

  #[test]
  fn falls_back_to_first_organization() {
    let token = jwt(&serde_json::json!({"organizations": [{"id": "org-x"}, {"id": "org-y"}]}));
    let claims = parse_jwt_claims(&token).unwrap();
    assert_eq!(extract_account_id(&claims).as_deref(), Some("org-x"));
  }

  #[test]
  fn missing_account_id_returns_none() {
    let token = jwt(&serde_json::json!({"email": "x"}));
    let claims = parse_jwt_claims(&token).unwrap();
    assert_eq!(extract_account_id(&claims), None);
  }

  #[test]
  fn account_context_prefers_explicit_id_then_identity_token_then_access_token() {
    let mut account: AccountConfig = serde_json::from_value(serde_json::json!({
      "id": "imported-codex",
      "provider": "codex",
      "provider_account_id": "explicit-account",
      "id_token": jwt(&serde_json::json!({"chatgpt_account_id": "identity-account"})),
      "access_token": jwt(&serde_json::json!({
        "https://api.openai.com/auth": {"chatgpt_account_id": "access-account"}
      })),
    }))
    .unwrap();
    assert_eq!(account_id(&account).as_deref(), Some("explicit-account"));

    account.provider_account_id = Some("  ".into());
    assert_eq!(account_id(&account).as_deref(), Some("identity-account"));

    account.id_token = Some(tokn_core::account::Secret::new(jwt(&serde_json::json!({
      "chatgpt_account_id": " ",
      "email": "user@example.test"
    }))));
    assert_eq!(account_id(&account).as_deref(), Some("access-account"));

    account.id_token = Some(tokn_core::account::Secret::new("malformed".into()));
    assert_eq!(account_id(&account).as_deref(), Some("access-account"));

    account.access_token = None;
    assert!(account_id(&account).is_none());
  }

  #[test]
  fn blank_claim_does_not_hide_a_namespaced_account_id() {
    let token = jwt(&serde_json::json!({
      "chatgpt_account_id": " ",
      "https://api.openai.com/auth": {"chatgpt_account_id": "namespaced-account"}
    }));
    assert_eq!(
      extract_account_id(&parse_jwt_claims(&token).unwrap()).as_deref(),
      Some("namespaced-account")
    );
  }

  #[test]
  fn malformed_jwt_returns_none() {
    assert!(parse_jwt_claims("not-a-jwt").is_none());
    assert!(parse_jwt_claims("a.b.c.d").is_none());
    assert!(parse_jwt_claims("a.!notbase64.c").is_none());
  }
}
