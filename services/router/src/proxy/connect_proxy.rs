use anyhow::{Context, Result};
use ipnet::IpNet;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokn_core::util::http::HttpClientOptions;

#[derive(Clone, Debug)]
pub(super) struct ConnectProxy {
  explicit: Option<ProxyRoute>,
  system: Option<ProxyRoute>,
  no_proxy: Vec<String>,
}

impl ConnectProxy {
  pub(super) fn from_options(options: &HttpClientOptions) -> Self {
    Self {
      explicit: options.url.as_deref().and_then(parse_proxy_route),
      system: options.system.then(system_proxy_route).flatten(),
      no_proxy: options
        .no_proxy
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect(),
    }
  }

  fn route_for(&self, host: &str) -> Option<&ProxyRoute> {
    if should_bypass_proxy(host, &self.no_proxy) {
      return None;
    }
    self.explicit.as_ref().or(self.system.as_ref())
  }
}

pub(super) async fn connect_upstream(host: &str, port: u16, outbound_proxy: &ConnectProxy) -> Result<TcpStream> {
  match outbound_proxy.route_for(host) {
    Some(ProxyRoute::Http(proxy)) => connect_via_http_proxy(proxy, host, port).await,
    Some(ProxyRoute::Https(proxy)) => connect_via_https_proxy(proxy, host, port).await,
    Some(ProxyRoute::Socks5(proxy)) => connect_via_socks5_proxy(proxy, host, port).await,
    None => TcpStream::connect((host, port))
      .await
      .with_context(|| format!("connect upstream {host}:{port}")),
  }
}

async fn connect_via_http_proxy(proxy: &ProxyEndpoint, host: &str, port: u16) -> Result<TcpStream> {
  let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port))
    .await
    .with_context(|| format!("connect outbound proxy {}:{}", proxy.host, proxy.port))?;
  send_http_connect(&mut stream, proxy, host, port).await?;
  Ok(stream)
}

async fn connect_via_https_proxy(proxy: &ProxyEndpoint, host: &str, port: u16) -> Result<TcpStream> {
  let stream = TcpStream::connect((proxy.host.as_str(), proxy.port))
    .await
    .with_context(|| format!("connect outbound proxy {}:{}", proxy.host, proxy.port))?;
  let connector = TlsConnector::from(Arc::new(client_tls_config()?));
  let server_name = rustls::pki_types::ServerName::try_from(proxy.host.clone())
    .map_err(|_| anyhow::anyhow!("invalid outbound proxy host {}", proxy.host))?;
  let mut stream = connector
    .connect(server_name, stream)
    .await
    .with_context(|| format!("tls handshake outbound proxy {}:{}", proxy.host, proxy.port))?;
  send_http_connect(&mut stream, proxy, host, port).await?;
  let (stream, _) = stream.into_inner();
  Ok(stream)
}

async fn send_http_connect<S>(stream: &mut S, proxy: &ProxyEndpoint, host: &str, port: u16) -> Result<()>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
  let authority = format!("{host}:{port}");
  let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: Keep-Alive\r\n");
  if let Some(value) = &proxy.authorization {
    request.push_str("Proxy-Authorization: ");
    request.push_str(value);
    request.push_str("\r\n");
  }
  request.push_str("\r\n");
  stream.write_all(request.as_bytes()).await?;
  stream.flush().await?;

  let mut reader = BufReader::new(stream);
  let mut status_line = String::new();
  if reader.read_line(&mut status_line).await? == 0 {
    anyhow::bail!("outbound proxy closed CONNECT for {host}:{port}");
  }
  let status_line = status_line.trim_end_matches(['\r', '\n']);
  let mut parts = status_line.split_whitespace();
  let _version = parts.next().unwrap_or_default();
  let status = parts.next().unwrap_or_default();
  if status != "200" {
    anyhow::bail!("outbound proxy CONNECT failed for {host}:{port}: {status_line}");
  }
  loop {
    let mut header_line = String::new();
    if reader.read_line(&mut header_line).await? == 0 {
      anyhow::bail!("outbound proxy closed CONNECT headers for {host}:{port}");
    }
    if header_line == "\r\n" || header_line == "\n" {
      break;
    }
  }
  Ok(())
}

async fn connect_via_socks5_proxy(proxy: &ProxyEndpoint, host: &str, port: u16) -> Result<TcpStream> {
  let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port))
    .await
    .with_context(|| format!("connect outbound proxy {}:{}", proxy.host, proxy.port))?;

  if let Some((username, password)) = &proxy.user_pass {
    stream.write_all(&[0x05, 0x01, 0x02]).await?;
    stream.flush().await?;
    let mut method = [0u8; 2];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut method).await?;
    if method != [0x05, 0x02] {
      anyhow::bail!("outbound SOCKS5 proxy does not accept username/password auth");
    }
    if username.len() > u8::MAX as usize || password.len() > u8::MAX as usize {
      anyhow::bail!("outbound SOCKS5 credentials exceed protocol limits");
    }
    let mut auth = Vec::with_capacity(3 + username.len() + password.len());
    auth.push(0x01);
    auth.push(username.len() as u8);
    auth.extend_from_slice(username.as_bytes());
    auth.push(password.len() as u8);
    auth.extend_from_slice(password.as_bytes());
    stream.write_all(&auth).await?;
    stream.flush().await?;
    let mut auth_resp = [0u8; 2];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut auth_resp).await?;
    if auth_resp != [0x01, 0x00] {
      anyhow::bail!("outbound SOCKS5 proxy rejected credentials");
    }
  } else {
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    stream.flush().await?;
    let mut method = [0u8; 2];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut method).await?;
    if method != [0x05, 0x00] {
      anyhow::bail!("outbound SOCKS5 proxy requires unsupported authentication");
    }
  }

  let host_bytes = host.as_bytes();
  if host_bytes.len() > u8::MAX as usize {
    anyhow::bail!("target hostname too long for SOCKS5: {host}");
  }
  let mut request = Vec::with_capacity(7 + host_bytes.len());
  request.extend_from_slice(&[0x05, 0x01, 0x00, 0x03, host_bytes.len() as u8]);
  request.extend_from_slice(host_bytes);
  request.extend_from_slice(&port.to_be_bytes());
  stream.write_all(&request).await?;
  stream.flush().await?;

  let mut header = [0u8; 4];
  tokio::io::AsyncReadExt::read_exact(&mut stream, &mut header).await?;
  if header[0] != 0x05 {
    anyhow::bail!("invalid SOCKS5 version from outbound proxy");
  }
  if header[1] != 0x00 {
    anyhow::bail!("outbound SOCKS5 CONNECT failed with code 0x{:02x}", header[1]);
  }
  let addr_len = match header[3] {
    0x01 => 4,
    0x03 => {
      let mut len = [0u8; 1];
      tokio::io::AsyncReadExt::read_exact(&mut stream, &mut len).await?;
      len[0] as usize
    }
    0x04 => 16,
    atyp => anyhow::bail!("unsupported SOCKS5 address type 0x{atyp:02x}"),
  };
  let mut discard = vec![0u8; addr_len + 2];
  tokio::io::AsyncReadExt::read_exact(&mut stream, &mut discard).await?;
  Ok(stream)
}

#[derive(Clone, Debug)]
enum ProxyRoute {
  Http(ProxyEndpoint),
  Https(ProxyEndpoint),
  Socks5(ProxyEndpoint),
}

#[derive(Clone, Debug)]
struct ProxyEndpoint {
  host: String,
  port: u16,
  authorization: Option<String>,
  user_pass: Option<(String, String)>,
}

fn parse_proxy_route(value: &str) -> Option<ProxyRoute> {
  let url = reqwest::Url::parse(value).ok()?;
  let host = url.host_str()?.to_string();
  let port = url.port_or_known_default()?;
  let username = url.username();
  let password = url.password().unwrap_or_default();
  let has_credentials = !username.is_empty() || url.password().is_some();
  let endpoint = ProxyEndpoint {
    host,
    port,
    authorization: has_credentials.then(|| {
      use base64::Engine;
      let raw = format!("{username}:{password}");
      format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(raw))
    }),
    user_pass: has_credentials.then(|| (username.to_string(), password.to_string())),
  };
  match url.scheme() {
    "http" => Some(ProxyRoute::Http(endpoint)),
    "https" => Some(ProxyRoute::Https(endpoint)),
    "socks5" | "socks5h" => Some(ProxyRoute::Socks5(endpoint)),
    _ => None,
  }
}

fn system_proxy_route() -> Option<ProxyRoute> {
  let https = std::env::var("HTTPS_PROXY")
    .ok()
    .or_else(|| std::env::var("https_proxy").ok());
  let http = std::env::var("HTTP_PROXY")
    .ok()
    .or_else(|| std::env::var("http_proxy").ok());
  https.or(http).as_deref().and_then(parse_proxy_route)
}

fn should_bypass_proxy(host: &str, entries: &[String]) -> bool {
  let host = host.trim().trim_matches('.').to_ascii_lowercase();
  if host.is_empty() {
    return false;
  }
  entries.iter().any(|entry| no_proxy_matches(&host, entry))
}

fn no_proxy_matches(host: &str, entry: &str) -> bool {
  let entry = entry.trim().trim_matches('.');
  if entry.is_empty() {
    return false;
  }
  if entry == "*" {
    return true;
  }
  if let (Ok(ip), Ok(net)) = (host.parse::<std::net::IpAddr>(), entry.parse::<IpNet>()) {
    return net.contains(&ip);
  }
  host == entry || host.ends_with(&format!(".{entry}"))
}

fn client_tls_config() -> Result<rustls::ClientConfig> {
  let mut roots = rustls::RootCertStore::empty();
  let certs = rustls_native_certs::load_native_certs();
  let mut loaded = 0usize;
  for cert in certs.certs {
    if roots.add(cert).is_ok() {
      loaded += 1;
    }
  }
  if let Some(err) = certs.errors.into_iter().next() {
    tracing::warn!(error = %err, "failed to load some native root certificates");
  }
  if loaded == 0 {
    anyhow::bail!("no native root certificates available for outbound HTTPS proxy");
  }
  Ok(
    rustls::ClientConfig::builder()
      .with_root_certificates(roots)
      .with_no_client_auth(),
  )
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn connect_proxy_prefers_explicit_proxy() {
    let proxy = ConnectProxy::from_options(&HttpClientOptions {
      url: Some("http://user:pass@proxy.example:8080".into()),
      no_proxy: Vec::new(),
      system: false,
    });

    match proxy.route_for("api.example.com") {
      Some(ProxyRoute::Http(endpoint)) => {
        assert_eq!(endpoint.host, "proxy.example");
        assert_eq!(endpoint.port, 8080);
        assert!(endpoint.authorization.is_some());
      }
      other => panic!("unexpected route: {other:?}"),
    }
  }

  #[test]
  fn connect_proxy_respects_no_proxy_suffixes() {
    let proxy = ConnectProxy::from_options(&HttpClientOptions {
      url: Some("http://proxy.example:8080".into()),
      no_proxy: vec!["example.com".into(), "internal.local".into()],
      system: false,
    });

    assert!(proxy.route_for("api.example.com").is_none());
    assert!(proxy.route_for("internal.local").is_none());
    assert!(proxy.route_for("foo.internal.local").is_none());
    assert!(proxy.route_for("example.net").is_some());
  }

  #[test]
  fn connect_proxy_respects_no_proxy_cidr() {
    let proxy = ConnectProxy::from_options(&HttpClientOptions {
      url: Some("http://proxy.example:8080".into()),
      no_proxy: vec!["192.168.0.0/24".into(), "fd00::/8".into()],
      system: false,
    });

    assert!(proxy.route_for("192.168.0.10").is_none());
    assert!(proxy.route_for("192.168.1.10").is_some());
    assert!(proxy.route_for("fd00::1").is_none());
    assert!(proxy.route_for("fe80::1").is_some());
  }

  #[test]
  fn parse_proxy_route_supports_socks5() {
    match parse_proxy_route("socks5h://user:pass@proxy.example:1080") {
      Some(ProxyRoute::Socks5(endpoint)) => {
        assert_eq!(endpoint.host, "proxy.example");
        assert_eq!(endpoint.port, 1080);
        assert_eq!(endpoint.user_pass, Some(("user".into(), "pass".into())));
      }
      other => panic!("unexpected route: {other:?}"),
    }
  }
}
