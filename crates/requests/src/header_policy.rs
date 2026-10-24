//! Pure relay header policy shared by standard and injected sessions.

use tokn_core::headers::HeaderMap;

// Connection / Keep-Alive remain observable for replay compatibility with the
// standard relay. Transport implementations own final HTTP framing.
const HOP_BY_HOP: &[&str] = &[
  "host",
  "proxy-authorization",
  "proxy-connection",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
];

pub(crate) fn passthrough_headers(inbound: &HeaderMap, preserve_host: bool, preserve_client_auth: bool) -> HeaderMap {
  let mut headers = HeaderMap::new();
  for (name, value) in inbound.iter() {
    let lower = name.as_str().to_ascii_lowercase();
    if lower.starts_with("x-tokn-router-") || matches!(lower.as_str(), "x-route-mode" | "x-behave-as") {
      continue;
    }
    if !preserve_client_auth && matches!(lower.as_str(), "authorization" | "x-api-key") {
      continue;
    }
    if HOP_BY_HOP.contains(&lower.as_str()) && !(preserve_host && lower == "host") {
      continue;
    }
    headers.insert(name.clone(), value.clone());
  }
  headers
}

pub(crate) const BODY_DIGEST_HEADERS: &[&str] = &["content-md5", "digest", "content-digest", "repr-digest"];

/// Body digests cease to describe a transformed request payload.
pub(crate) fn remove_body_digests(headers: &mut HeaderMap) {
  for name in BODY_DIGEST_HEADERS {
    headers.remove(*name);
  }
}
