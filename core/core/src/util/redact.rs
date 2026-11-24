//! Logging-safe redactors / fingerprints.
//!
//! Logs and span fields must never expose raw credentials. Use these helpers
//! whenever a log site needs to identify *which* token / key was in play
//! without revealing the secret itself:
//!
//! - [`token_fingerprint`] — first 8 hex chars of SHA-256, prefix-tagged.
//!
//! A header-preview helper was deliberately omitted: per project policy we
//! never log inbound or upstream headers, request bodies, or response bodies
//! — even truncated, even at trace.

/// Return `sha256[..8]` of `secret` as a lowercase hex string with a
/// stable prefix so log greppers can spot fingerprints at a glance.
///
/// Two refreshes of the same secret yield identical fingerprints, which is
/// useful for verifying that token caching / rotation actually rotated.
pub fn token_fingerprint(secret: &str) -> String {
  crate::util::secret::fingerprint_str(secret)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn fingerprint_is_stable_and_short() {
    let a = token_fingerprint("hunter2");
    let b = token_fingerprint("hunter2");
    let c = token_fingerprint("hunter3");
    assert_eq!(a, b);
    assert_ne!(a, c);
    assert!(a.starts_with("fp:"));
    assert_eq!(a.len(), 3 + 16);
  }

  #[test]
  fn fingerprint_tags_empty() {
    assert_eq!(token_fingerprint(""), "fp:<empty>");
  }
}
