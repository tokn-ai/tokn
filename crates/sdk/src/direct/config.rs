use alloc::{format, string::String};
use core::{fmt, net::Ipv6Addr};

pub const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

/// In-memory configuration for direct OpenAI API-key authentication.
///
/// The API key is omitted from debug output and never included in config
/// errors. It is sent only to the configured base URL.
#[derive(Clone)]
pub struct OpenAi {
  api_key: String,
  base_url: String,
}

impl OpenAi {
  /// Configure OpenAI's public API using a non-empty, visible ASCII API key.
  pub fn new(api_key: impl Into<String>) -> Result<Self, ConfigError> {
    let api_key = api_key.into();
    if api_key.is_empty() || !api_key.bytes().all(|byte| byte.is_ascii_graphic()) {
      return Err(ConfigError::InvalidApiKey);
    }
    Ok(Self {
      api_key,
      base_url: OPENAI_BASE_URL.into(),
    })
  }

  /// Use an explicit OpenAI-compatible HTTPS base URL, including its API path.
  ///
  /// For example, `https://example.com/api/v1` produces requests to
  /// `https://example.com/api/v1/responses`. Trailing slashes are removed.
  /// Supported URLs have an ASCII DNS/IP host, an optional nonzero port, and an
  /// unescaped path. Userinfo, queries, fragments, percent escapes, backslashes,
  /// and `.`/`..` path segments are rejected. This deliberately small URL
  /// contract avoids depending on a networking library in `no_std` builds.
  pub fn with_base_url(mut self, base_url: impl Into<String>) -> Result<Self, ConfigError> {
    let base_url = base_url.into();
    validate_base_url(&base_url)?;
    self.base_url = base_url.trim_end_matches('/').into();
    Ok(self)
  }

  pub fn base_url(&self) -> &str {
    &self.base_url
  }

  pub(super) fn authorization(&self) -> String {
    format!("Bearer {}", self.api_key)
  }
}

impl fmt::Debug for OpenAi {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("OpenAi")
      .field("api_key", &"<redacted>")
      .field("base_url", &self.base_url)
      .finish()
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
  InvalidApiKey,
  InvalidBaseUrl,
}

impl fmt::Display for ConfigError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(match self {
      Self::InvalidApiKey => "API key must contain non-empty visible ASCII characters without whitespace",
      Self::InvalidBaseUrl => "base URL must be HTTPS with a valid host and an unescaped path, without userinfo, query, fragment, or dot segments",
    })
  }
}

impl core::error::Error for ConfigError {}

fn validate_base_url(url: &str) -> Result<(), ConfigError> {
  let remainder = url.strip_prefix("https://").ok_or(ConfigError::InvalidBaseUrl)?;
  let (authority, path) = remainder.split_once('/').unwrap_or((remainder, ""));
  if !valid_authority(authority)
    || !path
      .bytes()
      .all(|byte| byte.is_ascii_alphanumeric() || b"-._~/!$&'()*+,;=:@".contains(&byte))
    || path.split('/').any(|segment| matches!(segment, "." | ".."))
  {
    return Err(ConfigError::InvalidBaseUrl);
  }
  Ok(())
}

fn valid_authority(authority: &str) -> bool {
  let (host, port) = if let Some(ipv6) = authority.strip_prefix('[') {
    let Some((host, suffix)) = ipv6.split_once(']') else {
      return false;
    };
    if host.parse::<Ipv6Addr>().is_err() {
      return false;
    }
    return suffix.is_empty() || suffix.strip_prefix(':').is_some_and(valid_port);
  } else {
    match authority.split_once(':') {
      Some((host, port)) => (host, Some(port)),
      None => (authority, None),
    }
  };
  let host = host.strip_suffix('.').unwrap_or(host);
  !host.is_empty()
    && host.len() <= 253
    && host.split('.').all(|label| {
      !label.is_empty()
        && label.len() <= 63
        && !label.starts_with('-')
        && !label.ends_with('-')
        && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
    && port.is_none_or(valid_port)
}

fn valid_port(port: &str) -> bool {
  !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) && port.parse::<u16>().is_ok_and(|port| port != 0)
}
