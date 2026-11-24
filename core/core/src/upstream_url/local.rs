//! Alloc-only HTTP URL storage following the shared strict URL admission rules.
//!
//! The general-purpose URL crate requires atomics for opaque-origin identities.
//! Embedded execution only needs canonical HTTP(S) URLs and encoded operation
//! segments, so it uses this restricted backend. Authority and dot-segment
//! validation remain shared with the standard implementation.

use super::{admit_http_url, InvalidUpstreamUrl};
use crate::authority::{CanonicalAuthority, CanonicalHost};
use alloc::string::{String, ToString};
use core::{
  fmt::{self, Write},
  net::Ipv6Addr,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Url {
  serialized: String,
  scheme: &'static str,
  authority: CanonicalAuthority,
  origin: String,
  path: String,
  query: Option<String>,
  fragment: Option<String>,
}

impl Url {
  pub fn parse(raw: &str) -> Result<Self, InvalidUpstreamUrl> {
    let mut authority = admit_http_url(raw)?;
    let scheme = if raw.starts_with("https://") { "https" } else { "http" };
    if authority.port() == Some(if scheme == "https" { 443 } else { 80 }) {
      authority = CanonicalAuthority::new(authority.host().clone(), None);
    }
    let remainder = &raw[scheme.len() + 3..];
    let end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let suffix = &remainder[end..];
    let (path_and_query, fragment) = suffix
      .split_once('#')
      .map_or((suffix, None), |(left, right)| (left, Some(right)));
    let (path, query) = path_and_query
      .split_once('?')
      .map_or((path_and_query, None), |(left, right)| (left, Some(right)));
    let path = if path.is_empty() {
      String::from("/")
    } else {
      encode(path, Encoding::Path)
    };
    let origin = format!("{scheme}://{}", format_authority(&authority));
    let mut url = Self {
      serialized: String::new(),
      scheme,
      authority,
      origin,
      path,
      query: query.map(|value| encode(value, Encoding::Query)),
      fragment: fragment.map(|value| encode(value, Encoding::Fragment)),
    };
    url.rebuild();
    Ok(url)
  }
  pub fn as_str(&self) -> &str {
    &self.serialized
  }
  pub fn scheme(&self) -> &str {
    self.scheme
  }
  pub fn host(&self) -> Option<&CanonicalHost> {
    Some(self.authority.host())
  }
  pub fn host_str(&self) -> Option<&str> {
    Some(self.authority.host().as_str())
  }
  pub fn username(&self) -> &str {
    ""
  }
  pub fn password(&self) -> Option<&str> {
    None
  }
  pub fn port(&self) -> Option<u16> {
    self.authority.port()
  }
  pub fn path(&self) -> &str {
    &self.path
  }
  pub fn query(&self) -> Option<&str> {
    self.query.as_deref()
  }
  pub fn fragment(&self) -> Option<&str> {
    self.fragment.as_deref()
  }
  pub fn origin(&self) -> Origin<'_> {
    Origin(&self.origin)
  }
  pub fn path_segments_mut(&mut self) -> Result<PathSegments<'_>, InvalidUpstreamUrl> {
    Ok(PathSegments(self))
  }
  pub fn query_pairs_mut(&mut self) -> QueryPairs<'_> {
    QueryPairs(self)
  }
  fn rebuild(&mut self) {
    self.serialized.clear();
    self.serialized.push_str(&self.origin);
    self.serialized.push_str(&self.path);
    if let Some(query) = &self.query {
      self.serialized.push('?');
      self.serialized.push_str(query);
    }
    if let Some(fragment) = &self.fragment {
      self.serialized.push('#');
      self.serialized.push_str(fragment);
    }
  }
}
impl fmt::Display for Url {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(&self.serialized)
  }
}

pub struct Origin<'a>(&'a str);
impl Origin<'_> {
  pub fn ascii_serialization(&self) -> String {
    self.0.to_string()
  }
}

pub struct PathSegments<'a>(&'a mut Url);
impl PathSegments<'_> {
  pub fn pop_if_empty(&mut self) -> &mut Self {
    if self.0.path.len() > 1 && self.0.path.ends_with('/') {
      self.0.path.pop();
    }
    self
  }
  pub fn push(&mut self, value: &str) -> &mut Self {
    if self.0.path != "/" {
      self.0.path.push('/');
    }
    self.0.path.push_str(&encode(value, Encoding::Segment));
    self
  }
}
impl Drop for PathSegments<'_> {
  fn drop(&mut self) {
    self.0.rebuild();
  }
}

pub struct QueryPairs<'a>(&'a mut Url);
impl QueryPairs<'_> {
  pub fn append_pair(&mut self, name: &str, value: &str) -> &mut Self {
    let query = self.0.query.get_or_insert_with(String::new);
    if !query.is_empty() {
      query.push('&');
    }
    query.push_str(&encode(name, Encoding::Form));
    query.push('=');
    query.push_str(&encode(value, Encoding::Form));
    self.0.rebuild();
    self
  }
}

// WHATWG URLs render IPv4-mapped IPv6 tails as hexadecimal groups; the
// standard library's IPv6 Display implementation uses dotted decimal instead.
fn format_authority(authority: &CanonicalAuthority) -> String {
  if !authority.host().is_ipv6() {
    return authority.to_string();
  }
  let address: Ipv6Addr = authority.host().as_str().parse().expect("canonical IPv6 host");
  let segments = address.segments();
  let mut best_start = 0;
  let mut best_length = 0;
  let mut index = 0;
  while index < segments.len() {
    if segments[index] != 0 {
      index += 1;
      continue;
    }
    let start = index;
    while index < segments.len() && segments[index] == 0 {
      index += 1;
    }
    if index - start > best_length {
      best_start = start;
      best_length = index - start;
    }
  }
  let mut rendered = String::from("[");
  index = 0;
  while index < segments.len() {
    if best_length > 1 && index == best_start {
      rendered.push_str("::");
      index += best_length;
    } else {
      if index > 0 && !(best_length > 1 && index == best_start + best_length) {
        rendered.push(':');
      }
      write!(rendered, "{:x}", segments[index]).expect("format into String");
      index += 1;
    }
  }
  rendered.push(']');
  if let Some(port) = authority.port() {
    write!(rendered, ":{port}").expect("format into String");
  }
  rendered
}

enum Encoding {
  Path,
  Segment,
  Query,
  Fragment,
  Form,
}
fn encode(value: &str, mode: Encoding) -> String {
  let mut encoded = String::with_capacity(value.len());
  const HEX: &[u8; 16] = b"0123456789ABCDEF";
  for byte in value.bytes() {
    if matches!(mode, Encoding::Form) && byte == b' ' {
      encoded.push('+');
      continue;
    }
    let escape = if matches!(mode, Encoding::Form) {
      !(byte.is_ascii_alphanumeric() || matches!(byte, b'*' | b'-' | b'.' | b'_'))
    } else {
      byte <= 0x20
        || byte >= 0x7f
        || matches!(byte, b'"' | b'<' | b'>')
        || match mode {
          Encoding::Path => matches!(byte, b'`' | b'#' | b'?' | b'{' | b'}'),
          Encoding::Segment => matches!(byte, b'`' | b'#' | b'?' | b'{' | b'}' | b'/' | b'%' | b'\\'),
          Encoding::Query => matches!(byte, b'#' | b'\''),
          Encoding::Fragment => byte == b'`',
          Encoding::Form => unreachable!(),
        }
    };
    if escape {
      encoded.push('%');
      encoded.push(HEX[(byte >> 4) as usize] as char);
      encoded.push(HEX[(byte & 0xf) as usize] as char);
    } else {
      encoded.push(byte as char);
    }
  }
  encoded
}

#[cfg(all(test, feature = "std"))]
mod tests {
  use super::*;
  #[test]
  fn admitted_http_urls_and_segments_match_standard_backend() {
    for base in [
      "https://API.Example.com:443",
      "http://[::ffff:192.0.2.1]:80/v1",
      "https://[0:0:1:0:0:1:0:0]/",
      "https://[1:2:3:4:5:6:7:8]/",
      "http://127.0.0.1:8080/v1",
      "https://[2001:0DB8::1]:443/a//b/",
      "https://api.example/v1/%2f{}<>`",
      "https://api.example:8443/v1?old=x'y#abc`",
    ] {
      let mut local = Url::parse(base).unwrap();
      let mut standard = url::Url::parse(base).unwrap();
      assert_eq!(local.as_str(), standard.as_str(), "base {base}");
      assert_eq!(local.to_string(), standard.to_string());
      assert_eq!(local.scheme(), standard.scheme());
      assert!(local.host().is_some());
      assert_eq!(local.username(), standard.username());
      assert_eq!(local.password(), standard.password());
      assert_eq!(local.port(), standard.port());
      assert_eq!(local.path(), standard.path());
      assert_eq!(local.query(), standard.query());
      assert_eq!(local.fragment(), standard.fragment());
      assert_eq!(
        local.host_str().map(CanonicalHost::parse).transpose().unwrap(),
        standard.host_str().map(CanonicalHost::parse).transpose().unwrap(),
      );
      assert_eq!(
        local.origin().ascii_serialization(),
        standard.origin().ascii_serialization()
      );
      for segment in [
        "chat",
        "model/id?x#y%2f",
        "héllo 🌍",
        "a\\b",
        "{}<>`[]^|",
        "a:b@c;d=e",
        "",
      ] {
        local.path_segments_mut().unwrap().pop_if_empty().push(segment);
        standard.path_segments_mut().unwrap().pop_if_empty().push(segment);
        assert_eq!(local.as_str(), standard.as_str(), "segment {segment:?}");
      }
      local.query_pairs_mut().append_pair("client version", "0.1+&?=é");
      standard.query_pairs_mut().append_pair("client version", "0.1+&?=é");
      assert_eq!(local.as_str(), standard.as_str());
    }
  }
}
