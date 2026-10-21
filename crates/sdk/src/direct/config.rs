use alloc::{format, string::String, vec, vec::Vec};
use core::{fmt, net::Ipv6Addr};

use super::{Endpoint, Header};

pub const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
pub const ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/v1";
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Native API routes with independently scoped URLs and credentials.
///
/// Only configured endpoints can be called. This configuration does not perform
/// model routing, protocol conversion, provider-specific body transformations,
/// or credential refresh. Callers supply native requests and current credentials.
#[derive(Clone, Debug, Default)]
pub struct ProviderConfig {
  chat_completions: Option<EndpointConfig>,
  responses: Option<EndpointConfig>,
  messages: Option<EndpointConfig>,
}

impl ProviderConfig {
  /// Start with no configured endpoints.
  pub fn new() -> Self {
    Self::default()
  }

  /// Configure or replace one endpoint without changing other routes or headers.
  pub fn with_endpoint(mut self, endpoint: Endpoint, config: EndpointConfig) -> Self {
    *match endpoint {
      Endpoint::ChatCompletions => &mut self.chat_completions,
      Endpoint::Responses => &mut self.responses,
      Endpoint::Messages => &mut self.messages,
    } = Some(config);
    self
  }

  pub fn endpoint(&self, endpoint: Endpoint) -> Option<&EndpointConfig> {
    match endpoint {
      Endpoint::ChatCompletions => self.chat_completions.as_ref(),
      Endpoint::Responses => self.responses.as_ref(),
      Endpoint::Messages => self.messages.as_ref(),
    }
  }
}

/// An explicit native API URL and the headers sent only to that endpoint.
///
/// Header values are omitted from debug output. The SDK supplies `content-type`
/// and `accept`; the transport supplies HTTP framing. Authentication is optional
/// and must be configured explicitly, for example with [`Self::with_bearer_auth`]
/// or `with_header("x-api-key", key)`.
#[derive(Clone, Debug)]
pub struct EndpointConfig {
  url: String,
  headers: Vec<Header>,
}

impl EndpointConfig {
  /// Configure a complete HTTPS endpoint URL, including its operation path.
  ///
  /// Uses the same small URL grammar as [`OpenAi::with_base_url`]. The URL is
  /// preserved verbatim: no operation path is appended or trailing slash removed.
  pub fn new(url: impl Into<String>) -> Result<Self, ConfigError> {
    Self::with_scheme(url.into(), "https://")
  }

  /// Explicitly allow a complete cleartext HTTP endpoint, for example a local
  /// llama.cpp server. Prompts and any configured credentials travel unencrypted.
  pub fn new_insecure_http(url: impl Into<String>) -> Result<Self, ConfigError> {
    Self::with_scheme(url.into(), "http://")
  }

  fn with_scheme(url: String, scheme: &str) -> Result<Self, ConfigError> {
    if !valid_url(&url, scheme) {
      return Err(ConfigError::InvalidEndpointUrl);
    }
    Ok(Self {
      url,
      headers: Vec::new(),
    })
  }

  /// Add a header, replacing any previous value with the same case-insensitive
  /// name. Names must be HTTP tokens; values may contain ASCII spaces and visible
  /// characters, but no control bytes. SDK-owned and hop-by-hop headers are rejected.
  pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Result<Self, ConfigError> {
    let name = name.into();
    let value = value.into();
    if name.is_empty()
      || !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
    {
      return Err(ConfigError::InvalidHeaderName);
    }
    if !value.bytes().all(|byte| (b' '..=b'~').contains(&byte)) {
      return Err(ConfigError::InvalidHeaderValue);
    }
    let name = name.to_ascii_lowercase();
    if matches!(
      name.as_str(),
      "accept"
        | "content-type"
        | "host"
        | "content-length"
        | "transfer-encoding"
        | "connection"
        | "keep-alive"
        | "proxy-authenticate"
        | "proxy-authorization"
        | "te"
        | "trailer"
        | "upgrade"
    ) {
      return Err(ConfigError::ReservedHeader);
    }
    if let Some(header) = self.headers.iter_mut().find(|header| header.name == name) {
      header.value = value;
    } else {
      self.headers.push(Header::new(name, value));
    }
    Ok(self)
  }

  /// Set a Bearer API key or an already acquired access token.
  pub fn with_bearer_auth(self, token: impl Into<String>) -> Result<Self, ConfigError> {
    let token = token.into();
    validate_api_key(&token)?;
    self.with_header("authorization", format!("Bearer {token}"))
  }

  pub fn url(&self) -> &str {
    &self.url
  }

  pub fn headers(&self) -> &[Header] {
    &self.headers
  }
}

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
    validate_api_key(&api_key)?;
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
}

impl From<OpenAi> for ProviderConfig {
  fn from(provider: OpenAi) -> Self {
    let route = |operation| EndpointConfig {
      url: format!("{}/{operation}", provider.base_url),
      headers: vec![Header::new("authorization", format!("Bearer {}", provider.api_key))],
    };
    Self::new()
      .with_endpoint(Endpoint::ChatCompletions, route("chat/completions"))
      .with_endpoint(Endpoint::Responses, route("responses"))
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

/// Anthropic Messages using an explicit API key and version header.
///
/// Uses `x-api-key` authentication. For other authentication schemes or extra
/// headers, configure a Messages route with [`EndpointConfig`] directly.
#[derive(Clone)]
pub struct Anthropic {
  api_key: String,
  base_url: String,
  version: String,
}

impl Anthropic {
  pub fn new(api_key: impl Into<String>) -> Result<Self, ConfigError> {
    let api_key = api_key.into();
    validate_api_key(&api_key)?;
    Ok(Self {
      api_key,
      base_url: ANTHROPIC_BASE_URL.into(),
      version: ANTHROPIC_VERSION.into(),
    })
  }

  /// Set an HTTPS API base URL. `/messages` is appended when making requests.
  /// Uses the same URL grammar and trailing-slash handling as [`OpenAi::with_base_url`].
  pub fn with_base_url(mut self, base_url: impl Into<String>) -> Result<Self, ConfigError> {
    let base_url = base_url.into();
    validate_base_url(&base_url)?;
    self.base_url = base_url.trim_end_matches('/').into();
    Ok(self)
  }

  /// Override the API version sent in `anthropic-version`.
  pub fn with_version(mut self, version: impl Into<String>) -> Result<Self, ConfigError> {
    let version = version.into();
    if version.is_empty() || !version.bytes().all(|byte| byte.is_ascii_graphic()) {
      return Err(ConfigError::InvalidHeaderValue);
    }
    self.version = version;
    Ok(self)
  }

  pub fn base_url(&self) -> &str {
    &self.base_url
  }

  pub fn version(&self) -> &str {
    &self.version
  }
}

impl From<Anthropic> for ProviderConfig {
  fn from(provider: Anthropic) -> Self {
    Self::new().with_endpoint(
      Endpoint::Messages,
      EndpointConfig {
        url: format!("{}/messages", provider.base_url),
        headers: vec![
          Header::new("x-api-key", provider.api_key),
          Header::new("anthropic-version", provider.version),
        ],
      },
    )
  }
}

impl fmt::Debug for Anthropic {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Anthropic")
      .field("api_key", &"<redacted>")
      .field("base_url", &self.base_url)
      .field("version", &self.version)
      .finish()
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
  InvalidApiKey,
  InvalidBaseUrl,
  InvalidEndpointUrl,
  InvalidHeaderName,
  InvalidHeaderValue,
  ReservedHeader,
}

impl fmt::Display for ConfigError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(match self {
      Self::InvalidApiKey => "API key must contain non-empty visible ASCII characters without whitespace",
      Self::InvalidBaseUrl => "base URL must be HTTPS with a valid host and an unescaped path, without userinfo, query, fragment, or dot segments",
      Self::InvalidEndpointUrl => "endpoint URL must use the selected scheme with a valid host and an unescaped path, without userinfo, query, fragment, or dot segments",
      Self::InvalidHeaderName => "header name must be a non-empty HTTP token",
      Self::InvalidHeaderValue => "header value must contain only ASCII spaces and visible characters",
      Self::ReservedHeader => "header is managed by the SDK or HTTP transport",
    })
  }
}

impl core::error::Error for ConfigError {}

fn validate_base_url(url: &str) -> Result<(), ConfigError> {
  if valid_url(url, "https://") {
    Ok(())
  } else {
    Err(ConfigError::InvalidBaseUrl)
  }
}

fn validate_api_key(api_key: &str) -> Result<(), ConfigError> {
  if api_key.is_empty() || !api_key.bytes().all(|byte| byte.is_ascii_graphic()) {
    Err(ConfigError::InvalidApiKey)
  } else {
    Ok(())
  }
}

fn valid_url(url: &str, scheme: &str) -> bool {
  let Some(remainder) = url.strip_prefix(scheme) else {
    return false;
  };
  let (authority, path) = remainder.split_once('/').unwrap_or((remainder, ""));
  if !valid_authority(authority)
    || !path
      .bytes()
      .all(|byte| byte.is_ascii_alphanumeric() || b"-._~/!$&'()*+,;=:@".contains(&byte))
    || path.split('/').any(|segment| matches!(segment, "." | ".."))
  {
    return false;
  }
  true
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
