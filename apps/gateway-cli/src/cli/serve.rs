use crate::cli::config_cmd::RouteModeArg;
use crate::cli::config_context::{compile_effective_v2_config, EffectiveV2Config};
#[cfg(test)]
use crate::config::Config;
use anyhow::{Context, Result};
use clap::Args;
use std::future::Future;
use std::path::{Path, PathBuf};
use tokn_core::util::shutdown::ShutdownSignal;
use tokn_gateway::runtime::{self, BindOptions, ReloadCandidate, ReloadRequest, RuntimeConfig, RuntimeMode};
use tokn_router::v2::ReloadError;
use tokn_router_legacy_config::v2::{V2ProjectionOptions, V2ProjectionWarning};

#[derive(Args, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ServeArgs {
  /// Execute requests over one Unix socket instead of binding public listeners.
  #[arg(long, hide = true, conflicts_with_all = ["host", "port", "insecure_allow_remote"])]
  pub worker_socket: Option<PathBuf>,
  #[arg(long)]
  pub host: Option<String>,
  #[arg(long)]
  pub port: Option<u16>,
  /// Also expose the legacy forward proxy.
  #[arg(long)]
  pub with_proxy: bool,
  #[command(flatten)]
  #[serde(flatten)]
  pub rollout: super::rollout::RolloutArgs,
  /// Override the projected proxy listener's static route mode.
  #[arg(long, value_enum, requires = "with_proxy")]
  pub proxy_route_mode: Option<RouteModeArg>,
  /// Allow non-loopback binding. Projected v2 listeners also require [api_key].
  #[arg(long)]
  pub insecure_allow_remote: bool,
  /// Skip outbound proxy for this run.
  #[arg(long)]
  pub no_proxy: bool,
}

impl ServeArgs {
  pub(super) fn bind_options(&self) -> BindOptions {
    BindOptions {
      host: self.host.clone(),
      port: self.port,
      insecure_allow_remote: self.insecure_allow_remote,
    }
  }
}

pub(super) async fn run_direct(cfg_path: Option<PathBuf>, args: ServeArgs) -> Result<()> {
  anyhow::ensure!(
    !args.rollout.ab_test,
    "--ab-test requires the Unix frontend/worker runtime"
  );
  args.rollout.policy()?;
  let config = tokn_config::load_config(cfg_path.as_deref())?;
  let resolved_cfg_path = config.path().to_path_buf();
  match config.schema() {
    tokn_config::ConfigSchema::Legacy => run_projected_legacy(resolved_cfg_path, config, args).await,
    tokn_config::ConfigSchema::V2 => run_v2(resolved_cfg_path, config, args).await,
  }
}

#[cfg(unix)]
pub(super) fn load_runtime(cfg_path: Option<PathBuf>, args: ServeArgs) -> Result<(RuntimeSource, LoadedRuntime)> {
  let config = tokn_config::load_config(cfg_path.as_deref())?;
  let config_path = config.path().to_path_buf();
  let auth_path = tokn_auth::default_auth_path()?;
  let source = match config.schema() {
    tokn_config::ConfigSchema::Legacy => RuntimeSource::ProjectedLegacy {
      config_path,
      auth_path,
      args,
    },
    tokn_config::ConfigSchema::V2 => {
      anyhow::ensure!(
        !args.with_proxy && args.proxy_route_mode.is_none(),
        "v2 listeners are declared in config; remove --with-proxy/--proxy-route-mode"
      );
      RuntimeSource::NativeV2 {
        config_path,
        auth_path,
        args,
      }
    }
  };
  let loaded = source.load_matching_schema(config)?;
  Ok((source, loaded))
}

#[derive(Clone)]
pub(super) enum RuntimeSource {
  NativeV2 {
    config_path: PathBuf,
    auth_path: PathBuf,
    args: ServeArgs,
  },
  ProjectedLegacy {
    config_path: PathBuf,
    auth_path: PathBuf,
    args: ServeArgs,
  },
}

pub(super) struct LoadedRuntime {
  pub(super) compiled: tokn_config::v2::CompiledConfig,
  accounts: Vec<tokn_core::account::AccountConfig>,
  pub(super) args: ServeArgs,
  warnings: Vec<V2ProjectionWarning>,
  config_path: PathBuf,
}

impl RuntimeSource {
  #[cfg(test)]
  fn load(&self) -> Result<LoadedRuntime> {
    let config = tokn_config::load_config(Some(self.config_path()))?;
    if config.schema() != self.expected_schema() {
      anyhow::bail!("config schema changed; restart the gateway to switch runtime sources");
    }
    self.load_matching_schema(config)
  }

  fn checked_candidate(&self, config: tokn_config::SchemaConfig) -> Result<LoadedRuntime, ReloadError> {
    if config.schema() != self.expected_schema() {
      return Err(ReloadError::RestartRequired(
        "config schema changed; restart the gateway to switch runtime sources".into(),
      ));
    }
    self.load_matching_schema(config).map_err(invalid_reload)
  }

  fn reload_candidate(&self, request: ReloadRequest) -> Result<ReloadCandidate, ReloadError> {
    let (loaded, revision) = match request {
      ReloadRequest::InProcess => {
        let config = tokn_config::load_config(Some(self.config_path())).map_err(invalid_reload)?;
        (self.checked_candidate(config)?, None)
      }
      #[cfg(unix)]
      ReloadRequest::Coordinated => {
        let (loaded, revision) = self.split_candidate()?;
        (loaded, Some(revision))
      }
    };
    if !loaded.warnings.is_empty() {
      log_projection_warnings(&loaded.config_path, &loaded.warnings);
    }
    Ok(ReloadCandidate {
      config: RuntimeConfig {
        compiled: loaded.compiled,
        accounts: loaded.accounts,
      },
      revision,
    })
  }

  #[cfg(unix)]
  pub(super) fn split_candidate(&self) -> Result<(LoadedRuntime, String), ReloadError> {
    let path = self.config_path();
    let input = tokn_config::read_gateway_config(Some(path)).map_err(invalid_reload)?;
    // Each part is self-delimiting JSON. Hash the same decoded input we compile,
    // so concurrent rewrites cannot make two different generations agree.
    let mut source = match &input {
      tokn_config::GatewayConfigInput::Legacy(loaded) => serde_json::to_vec(&loaded.config),
      tokn_config::GatewayConfigInput::V2 { raw, .. } => serde_json::to_vec(raw),
    }
    .map_err(invalid_reload)?;
    let loaded = self.checked_candidate(input.compile().map_err(invalid_reload)?)?;
    let (auth_path, args) = match self {
      RuntimeSource::NativeV2 { auth_path, args, .. } | RuntimeSource::ProjectedLegacy { auth_path, args, .. } => {
        (auth_path, args)
      }
    };
    let config_identity = path.canonicalize().map_err(invalid_reload)?;
    let auth_identity = std::path::absolute(auth_path).map_err(invalid_reload)?;
    serde_json::to_writer(
      &mut source,
      &(
        config_identity.as_os_str().as_encoded_bytes(),
        auth_identity.as_os_str().as_encoded_bytes(),
        args.with_proxy,
        &args.proxy_route_mode,
        &args.host,
        args.port,
        args.insecure_allow_remote,
        args.no_proxy,
        &loaded.accounts,
      ),
    )
    .map_err(invalid_reload)?;
    Ok((loaded, tokn_core::util::digest::sha256_hex(&source)))
  }

  pub(super) fn load_matching_schema(&self, config: tokn_config::SchemaConfig) -> Result<LoadedRuntime> {
    match (self, config) {
      (
        Self::NativeV2 {
          config_path,
          auth_path,
          args,
        },
        config @ tokn_config::SchemaConfig::V2 { .. },
      ) => {
        let accounts = tokn_auth::AuthStore::load(Some(auth_path), Some(config_path))?.accounts;
        let effective = compile_effective_v2_config(config, accounts, V2ProjectionOptions::default())?;
        Ok(LoadedRuntime {
          compiled: effective.compiled,
          accounts: effective.accounts,
          args: args.clone(),
          warnings: effective.warnings,
          config_path: effective.config_path,
        })
      }
      (Self::ProjectedLegacy { auth_path, args, .. }, config @ tokn_config::SchemaConfig::Legacy(_)) => {
        let accounts = tokn_auth::AuthStore::load(Some(auth_path), Some(config.path()))?.accounts;
        let (effective, args) = prepare_projected_legacy_runtime(config, accounts, args.clone())?;
        Ok(LoadedRuntime {
          compiled: effective.compiled,
          accounts: effective.accounts,
          args,
          warnings: effective.warnings,
          config_path: effective.config_path,
        })
      }
      _ => unreachable!("schema was checked before loading its runtime source"),
    }
  }

  fn config_path(&self) -> &std::path::Path {
    match self {
      Self::NativeV2 { config_path, .. } | Self::ProjectedLegacy { config_path, .. } => config_path,
    }
  }

  const fn expected_schema(&self) -> tokn_config::ConfigSchema {
    match self {
      Self::NativeV2 { .. } => tokn_config::ConfigSchema::V2,
      Self::ProjectedLegacy { .. } => tokn_config::ConfigSchema::Legacy,
    }
  }
}

async fn run_projected_legacy(config_path: PathBuf, config: tokn_config::SchemaConfig, args: ServeArgs) -> Result<()> {
  let source = RuntimeSource::ProjectedLegacy {
    config_path,
    auth_path: tokn_auth::default_auth_path()?,
    args,
  };
  let loaded = source.load_matching_schema(config)?;
  log_projection_warnings(&loaded.config_path, &loaded.warnings);
  run_v2_runtime(source, loaded).await
}

async fn run_v2(config_path: PathBuf, config: tokn_config::SchemaConfig, args: ServeArgs) -> Result<()> {
  if args.with_proxy || args.proxy_route_mode.is_some() {
    anyhow::bail!(
      "v2 listeners are declared in config; remove --with-proxy/--proxy-route-mode and configure a forward_proxy listener"
    );
  }

  let source = RuntimeSource::NativeV2 {
    config_path,
    auth_path: tokn_auth::default_auth_path()?,
    args,
  };
  let loaded = source.load_matching_schema(config)?;
  run_v2_runtime(source, loaded).await
}

fn prepare_projected_legacy_runtime(
  config: tokn_config::SchemaConfig,
  accounts: Vec<tokn_core::account::AccountConfig>,
  mut args: ServeArgs,
) -> Result<(EffectiveV2Config, ServeArgs)> {
  let tokn_config::SchemaConfig::Legacy(mut loaded) = config else {
    unreachable!("projected legacy runtime requires a legacy config")
  };
  let legacy = &mut loaded.config;
  if !args.with_proxy && args.proxy_route_mode.is_some() {
    anyhow::bail!("--proxy-route-mode requires --with-proxy");
  }
  if let Some(host) = args.host.take() {
    legacy.server.host = host;
  }
  if let Some(port) = args.port.take() {
    legacy.server.port = port;
  }
  if args.no_proxy {
    legacy.proxy = crate::config::ProxyConfig::default();
  }
  if args.with_proxy && args.insecure_allow_remote && legacy.proxy_mode.host == tokn_config::DEFAULT_HOST {
    legacy.proxy_mode.host = legacy.server.host.clone();
  }
  let forward_proxy = if args.with_proxy {
    let route_mode = args
      .proxy_route_mode
      .take()
      .map(Into::into)
      .unwrap_or(legacy.proxy_mode.route_mode);
    Some(crate::cli::v2_projection::forward_proxy_options(route_mode))
  } else {
    None
  };
  args.with_proxy = false;

  let effective = compile_effective_v2_config(
    tokn_config::SchemaConfig::Legacy(loaded),
    accounts,
    V2ProjectionOptions {
      allow_insecure_public_listener: args.insecure_allow_remote,
      forward_proxy,
      ..V2ProjectionOptions::default()
    },
  )
  .context("project legacy config into the in-memory v2 runtime")?;
  Ok((effective, args))
}

fn log_projection_warnings(config_path: &std::path::Path, warnings: &[V2ProjectionWarning]) {
  tracing::warn!(
    config = %config_path.display(),
    warning_count = warnings.len(),
    "legacy config is running through the in-memory v2 runtime"
  );
  for warning in warnings {
    tracing::warn!(config = %config_path.display(), warning = %warning, "legacy-to-v2 projection warning");
  }
}

async fn run_v2_runtime(source: RuntimeSource, loaded: LoadedRuntime) -> Result<()> {
  let shutdown = ShutdownSignal::new().context("install shutdown signal handlers")?;
  run_loaded(source, loaded, None, async {
    shutdown.wait().await.map_err(Into::into)
  })
  .await
}

pub(super) async fn run_loaded<F>(
  source: RuntimeSource,
  loaded: LoadedRuntime,
  frontend_maintenance_dir: Option<&Path>,
  shutdown: F,
) -> Result<()>
where
  F: Future<Output = Result<()>> + Send,
{
  let LoadedRuntime {
    compiled,
    accounts,
    args,
    ..
  } = loaded;
  let mode = runtime_mode(args, frontend_maintenance_dir)?;
  runtime::serve(
    RuntimeConfig { compiled, accounts },
    mode,
    move |request| source.reload_candidate(request),
    crate::server_runtime::event_observers,
    shutdown,
  )
  .await
}

fn runtime_mode(args: ServeArgs, frontend_maintenance_dir: Option<&Path>) -> Result<RuntimeMode> {
  #[cfg(not(unix))]
  let _ = frontend_maintenance_dir;
  if let Some(socket_path) = args.worker_socket {
    if args.host.is_some() || args.port.is_some() || args.insecure_allow_remote {
      anyhow::bail!("worker mode cannot override public listener addresses");
    }
    #[cfg(unix)]
    return Ok(RuntimeMode::Worker {
      socket_path,
      frontend_maintenance_dir: frontend_maintenance_dir.map(Path::to_path_buf),
    });
    #[cfg(not(unix))]
    {
      let _ = socket_path;
      anyhow::bail!("Unix-socket worker mode is only supported on Unix");
    }
  }
  Ok(RuntimeMode::InProcess(args.bind_options()))
}

fn invalid_reload(error: impl std::fmt::Display) -> ReloadError {
  ReloadError::Invalid(format!("{error:#}"))
}

#[cfg(test)]
mod cors_tests;
#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
  use super::{test_support::*, *};
  use reqwest::{Client, StatusCode};
  use std::path::Path;
  use std::sync::Arc;
  use tokio::io::{AsyncReadExt, AsyncWriteExt};
  use tokio::net::{TcpListener, TcpStream};
  use tokio::sync::watch;
  use tokn_core::event::EventBus;

  #[cfg(unix)]
  #[tokio::test]
  async fn worker_socket_serves_without_binding_configured_tcp_ports() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = directory.path().join("worker.sock");
    let address = unused_loopback_addr();
    let compiled = tokn_config::v2::parse_config(
      &format!(
        r#"
schema_version = 2

[service.persistence]
enabled = false

[service.models]
enabled = false

[listeners.api]
kind = "llm_api"
bind = "{address}"
client_auth = "none"
"#
      ),
      Path::new("worker.toml"),
    )
    .unwrap();
    let mut args = v2_serve_args();
    args.worker_socket = Some(socket.clone());
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(serve_fixture(compiled, Vec::new(), args, None, stopped));
    let mut client = tokio::time::timeout(std::time::Duration::from_secs(5), async {
      loop {
        if let Ok(client) = tokio::net::UnixStream::connect(&socket).await {
          break client;
        }
        tokio::task::yield_now().await;
      }
    })
    .await
    .unwrap();
    let public = TcpListener::bind(address)
      .await
      .expect("worker mode must not claim the public API port");
    client
      .write_all(b"GET /_tokn/ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
      .await
      .unwrap();
    let mut response = String::new();
    client.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    let (_, body) = response.split_once("\r\n\r\n").unwrap();
    let info: tokn_router::ipc::WorkerInfo = serde_json::from_str(body).unwrap();
    assert_eq!(info.listeners[0].listener_id, "api");
    drop(public);
    stop.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
      .await
      .unwrap()
      .unwrap()
      .unwrap();
    assert!(!socket.exists());
  }

  #[test]
  fn worker_cli_rejects_public_address_overrides() {
    use clap::Parser;
    let parsed = crate::cli::Cli::try_parse_from([
      "tokn-gateway",
      "serve",
      "--worker-socket",
      "/tmp/private/worker.sock",
      "--with-proxy",
    ])
    .unwrap();
    let crate::cli::Cmd::Serve(args) = parsed.cmd else {
      panic!("expected serve command");
    };
    assert!(args.with_proxy);
    assert_eq!(
      args.worker_socket.as_deref(),
      Some(Path::new("/tmp/private/worker.sock"))
    );
    for option in ["--host", "--port", "--insecure-allow-remote"] {
      let mut argv = vec![
        "tokn-gateway",
        "serve",
        "--worker-socket",
        "/tmp/private/worker.sock",
        option,
      ];
      if option == "--host" {
        argv.push("127.0.0.1");
      }
      if option == "--port" {
        argv.push("4141");
      }
      assert!(crate::cli::Cli::try_parse_from(argv).is_err());
    }
  }

  fn v2_serve_args() -> ServeArgs {
    ServeArgs {
      worker_socket: None,
      host: None,
      port: None,
      with_proxy: false,
      rollout: Default::default(),
      proxy_route_mode: None,
      insecure_allow_remote: false,
      no_proxy: false,
    }
  }

  fn account(id: &str, provider: &str) -> tokn_core::account::AccountConfig {
    toml::from_str(&format!(
      "id = {id:?}\nprovider = {provider:?}\nenabled = true\napi_key = \"test-key\"\n"
    ))
    .unwrap()
  }

  fn prepare_projected_legacy_for_test(
    config: Config,
    accounts: Vec<tokn_core::account::AccountConfig>,
    args: ServeArgs,
  ) -> Result<(
    tokn_config::v2::CompiledConfig,
    Vec<tokn_core::account::AccountConfig>,
    Vec<V2ProjectionWarning>,
    ServeArgs,
  )> {
    let loaded = tokn_config::SchemaConfig::Legacy(Box::new(tokn_config::LoadedConfig {
      config,
      sources: tokn_config::ConfigSources {
        root: "test-config.toml".into(),
        fragment_dir: "test-config.d".into(),
        fragments: Vec::new(),
      },
    }));
    let (effective, args) = prepare_projected_legacy_runtime(loaded, accounts, args)?;
    Ok((effective.compiled, effective.accounts, effective.warnings, args))
  }

  fn v2_listener_config(
    api_addr: std::net::SocketAddr,
    proxy_addr: std::net::SocketAddr,
  ) -> tokn_config::v2::CompiledConfig {
    let config = format!(
      r#"
schema_version = 2

[service.persistence]
enabled = false

[service.models]
enabled = false

[listeners.api]
kind = "llm_api"
bind = "{api_addr}"
client_auth = "none"

[listeners.proxy]
kind = "forward_proxy"
bind = "{proxy_addr}"
client_auth = "none"
default_http_action = {{ kind = "reject" }}
default_connect = "reject"
"#
    );
    tokn_config::v2::parse_config(&config, Path::new("v2-serve-test.toml")).unwrap()
  }

  fn v2_reload_config(
    api_addr: std::net::SocketAddr,
    proxy_addr: std::net::SocketAddr,
    default_connect: &str,
    max_wire_bytes: usize,
  ) -> String {
    format!(
      r#"
schema_version = 2

[service.persistence]
enabled = false

[service.models]
enabled = false

[service.request_limits]
max_wire_bytes = {max_wire_bytes}

[listeners.api]
kind = "llm_api"
bind = "{api_addr}"
client_auth = "none"

[listeners.proxy]
kind = "forward_proxy"
bind = "{proxy_addr}"
client_auth = "none"
default_http_action = {{ kind = "reject" }}
default_connect = "{default_connect}"
"#
    )
  }

  async fn assert_connect_rejected(addr: std::net::SocketAddr) {
    let mut proxy = TcpStream::connect(addr).await.unwrap();
    proxy
      .write_all(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
      .await
      .unwrap();
    let mut response = Vec::new();
    proxy.read_to_end(&mut response).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 403 Forbidden"));
  }

  #[test]
  fn detects_v2_config_without_treating_legacy_as_v2() {
    let directory = tempfile::tempdir().unwrap();
    let v2_path = directory.path().join("v2.toml");
    let legacy_path = directory.path().join("legacy.toml");
    std::fs::write(&v2_path, "schema_version = 2\n").unwrap();
    std::fs::write(&legacy_path, "[server]\nport = 4141\n").unwrap();

    assert_eq!(
      tokn_config::detect_config_schema(&v2_path).unwrap(),
      tokn_config::ConfigSchema::V2
    );
    assert_eq!(
      tokn_config::detect_config_schema(&legacy_path).unwrap(),
      tokn_config::ConfigSchema::Legacy
    );
    assert_eq!(
      tokn_config::detect_config_schema(&directory.path().join("missing.toml")).unwrap(),
      tokn_config::ConfigSchema::Legacy
    );
  }

  #[test]
  fn invalid_toml_is_not_silently_treated_as_legacy() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("invalid.toml");
    std::fs::write(&path, "schema_version = [").unwrap();

    assert!(tokn_config::detect_config_schema(&path).is_err());
  }

  #[test]
  fn legacy_serve_prepares_a_linkable_v2_runtime_without_proxy_settings() {
    let mut legacy = Config::default();
    legacy.server.port = 5151;
    legacy.proxy.url = Some("http://proxy.example:8080".into());
    let mut args = v2_serve_args();
    args.port = Some(5252);
    args.no_proxy = true;

    let (compiled, accounts, warnings, args) =
      prepare_projected_legacy_for_test(legacy, vec![account("primary", "openai")], args).unwrap();

    assert_eq!(compiled.gateway().listeners()["api"].bind().port(), 5252);
    assert!(compiled.service().outbound().proxy_url().is_none());
    assert_eq!(accounts.len(), 1);
    assert!(args.host.is_none());
    assert!(args.port.is_none());
    assert!(warnings.iter().any(|warning| matches!(
      warning,
      V2ProjectionWarning::BehaviorChange(tokn_router_legacy_config::v2::V2BehaviorChange::ManagedSelectionOrder)
    )));

    let (plan, service) = compiled.into_parts();
    let states = tokn_router::v2::build_runtime_states_with_service(
      plan,
      service,
      &accounts,
      Arc::new(tokn_access::AccessStore::disabled()),
      Arc::new(EventBus::noop()),
    )
    .unwrap();
    assert_eq!(states.llm_api.len(), 1);
    assert!(states.forward_proxy.is_empty());
  }

  #[test]
  fn projected_legacy_serve_requires_explicit_authenticated_remote_bind() {
    let mut legacy = Config::default();
    legacy.server.host = "0.0.0.0".into();
    let accounts = vec![account("primary", "openai")];

    let error = prepare_projected_legacy_for_test(legacy.clone(), accounts.clone(), v2_serve_args()).unwrap_err();
    assert!(format!("{error:#}").contains("requires an explicit public-listener review"));

    legacy.api_key.enabled = true;
    let mut args = v2_serve_args();
    args.insecure_allow_remote = true;
    let (compiled, _, warnings, _) = prepare_projected_legacy_for_test(legacy, accounts, args).unwrap();
    assert_eq!(
      compiled.gateway().listeners()["api"].bind(),
      "0.0.0.0:4141".parse().unwrap()
    );
    assert!(warnings
      .iter()
      .any(|warning| matches!(warning, V2ProjectionWarning::RemoteApiBindAllowed { .. })));
  }

  #[test]
  fn projected_legacy_serve_builds_both_v2_listeners() {
    let ca = tempfile::tempdir().unwrap();
    let mut legacy = Config::default();
    legacy.proxy_mode.ca_dir = Some(ca.path().to_path_buf());
    let mut args = v2_serve_args();
    args.with_proxy = true;
    args.proxy_route_mode = Some(RouteModeArg::Passthrough);
    let (compiled, accounts, warnings, args) =
      prepare_projected_legacy_for_test(legacy, vec![account("primary", "openai")], args).unwrap();

    assert_eq!(compiled.gateway().listeners().len(), 2);
    assert!(warnings.iter().any(|warning| matches!(
      warning,
      V2ProjectionWarning::BehaviorChange(tokn_router_legacy_config::v2::V2BehaviorChange::ProxyRequestModeOverrides)
    )));
    assert!(!args.with_proxy);
    assert!(args.proxy_route_mode.is_none());

    let (plan, service) = compiled.into_parts();
    let states = tokn_router::v2::build_runtime_states_with_service(
      plan,
      service,
      &accounts,
      Arc::new(tokn_access::AccessStore::disabled()),
      Arc::new(EventBus::noop()),
    )
    .unwrap();
    assert_eq!(states.llm_api.len(), 1);
    assert_eq!(states.forward_proxy.len(), 1);
    assert!(ca.path().join("ca.crt").exists());
  }

  #[test]
  fn projected_remote_legacy_proxy_follows_api_host_and_requires_authentication() {
    let mut legacy = Config::default();
    legacy.server.host = "0.0.0.0".into();
    let mut args = v2_serve_args();
    args.with_proxy = true;
    args.insecure_allow_remote = true;

    let error =
      prepare_projected_legacy_for_test(legacy.clone(), vec![account("primary", "openai")], args).unwrap_err();
    assert!(format!("{error:#}").contains("unauthenticated listeners must bind to a loopback address"));

    legacy.api_key.enabled = true;
    let mut args = v2_serve_args();
    args.with_proxy = true;
    args.insecure_allow_remote = true;
    let (compiled, _, warnings, _) =
      prepare_projected_legacy_for_test(legacy, vec![account("primary", "openai")], args).unwrap();
    assert_eq!(
      compiled.gateway().listeners()["proxy"].bind(),
      "0.0.0.0:4142".parse().unwrap()
    );
    assert!(warnings
      .iter()
      .any(|warning| matches!(warning, V2ProjectionWarning::RemoteForwardProxyBindAllowed { .. })));
  }

  #[tokio::test]
  async fn projected_legacy_proxy_serves_passthrough_http() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    let received = tokio::spawn(async move {
      let (mut stream, _) = upstream.accept().await.unwrap();
      let mut request = Vec::new();
      let mut buffer = [0_u8; 1024];
      loop {
        let read = stream.read(&mut buffer).await.unwrap();
        assert!(read > 0);
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") && request.ends_with(b"hello") {
          break;
        }
      }
      stream
        .write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 5\r\nConnection: close\r\n\r\nworld")
        .await
        .unwrap();
      request
    });

    let ca = tempfile::tempdir().unwrap();
    let api_addr = unused_loopback_addr();
    let proxy_addr = unused_loopback_addr();
    let mut legacy = Config::default();
    legacy.db.enabled = false;
    let _outbound_proxy = isolate_legacy_outbound(&mut legacy).await;
    legacy.server.host = api_addr.ip().to_string();
    legacy.server.port = api_addr.port();
    legacy.proxy_mode.host = proxy_addr.ip().to_string();
    legacy.proxy_mode.port = proxy_addr.port();
    legacy.proxy_mode.ca_dir = Some(ca.path().to_path_buf());
    let mut args = v2_serve_args();
    args.with_proxy = true;
    args.proxy_route_mode = Some(RouteModeArg::Passthrough);
    let (compiled, accounts, _, args) =
      prepare_projected_legacy_for_test(legacy, vec![account("primary", "openai")], args).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(serve_fixture(compiled, accounts, args, None, shutdown_rx));
    wait_for_listener(api_addr).await;
    wait_for_listener(proxy_addr).await;

    let mut proxy = TcpStream::connect(proxy_addr).await.unwrap();
    proxy
      .write_all(
        format!(
          "POST http://{upstream_addr}/custom HTTP/1.1\r\nHost: {upstream_addr}\r\nAuthorization: Bearer client-secret\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello"
        )
        .as_bytes(),
      )
      .await
      .unwrap();
    let mut response = Vec::new();
    proxy.read_to_end(&mut response).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 201 Created"));

    let request = String::from_utf8(received.await.unwrap()).unwrap();
    assert!(request.starts_with("POST /custom HTTP/1.1\r\n"));
    assert!(request
      .to_ascii_lowercase()
      .contains("authorization: bearer client-secret\r\n"));
    shutdown_tx.send(true).unwrap();
    server.await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn native_v2_admin_reload_reads_disk_and_preserves_generation_on_restart_only_changes() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    let auth_path = directory.path().join("auth.yaml");
    let api_addr = unused_loopback_addr();
    let proxy_addr = unused_loopback_addr();
    std::fs::write(
      &config_path,
      v2_reload_config(api_addr, proxy_addr, "reject", 1_048_576),
    )
    .unwrap();
    let source = RuntimeSource::NativeV2 {
      config_path: config_path.clone(),
      auth_path,
      args: v2_serve_args(),
    };
    let loaded = source.load().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(serve_fixture(
      loaded.compiled,
      loaded.accounts,
      loaded.args,
      Some(source),
      shutdown_rx,
    ));
    wait_for_listener(api_addr).await;
    let client = Client::builder()
      .no_proxy()
      .timeout(std::time::Duration::from_secs(5))
      .build()
      .unwrap();

    std::fs::write(
      &config_path,
      v2_reload_config(api_addr, proxy_addr, "tunnel", 1_048_576),
    )
    .unwrap();
    let (status, body) = reload_config(&client, api_addr).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["generation"], 2);

    std::fs::write(
      &config_path,
      v2_reload_config(api_addr, proxy_addr, "reject", 2_097_152),
    )
    .unwrap();
    let (status, body) = reload_config(&client, api_addr).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["type"], "restart_required");
    assert!(body["error"]["message"]
      .as_str()
      .unwrap()
      .contains("request limits changed"));

    for setting in [
      "level = 'debug'",
      "format = 'json'",
      "target = 'stderr'",
      "dir = 'custom-logs'",
      "ansi = false",
      "include_spans = true",
    ] {
      std::fs::write(
        &config_path,
        format!(
          "{}\n[service.logging]\n{setting}\n",
          v2_reload_config(api_addr, proxy_addr, "tunnel", 1_048_576)
        ),
      )
      .unwrap();
      let (status, body) = reload_config(&client, api_addr).await;
      assert_eq!(status, StatusCode::CONFLICT, "{setting}");
      assert_eq!(body["error"]["type"], "restart_required");
      assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("logging settings changed"));
    }

    std::fs::write(&config_path, "[server]\nport = 4141\n").unwrap();
    let (status, body) = reload_config(&client, api_addr).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(body["error"]["message"]
      .as_str()
      .unwrap()
      .contains("config schema changed"));
    std::fs::write(
      &config_path,
      v2_reload_config(api_addr, proxy_addr, "reject", 1_048_576),
    )
    .unwrap();
    let (status, body) = reload_config(&client, api_addr).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["generation"], 3);
    shutdown_tx.send(true).unwrap();
    server.await.unwrap().unwrap();
  }

  #[cfg(unix)]
  #[test]
  fn split_revision_matches_only_the_same_config_and_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    let alternate = directory.path().join("alternate.toml");
    let auth_path = directory.path().join("auth.yaml");
    let mut legacy = Config::default();
    legacy.save(&config_path).unwrap();
    legacy.save(&alternate).unwrap();
    let revision = |path: &std::path::Path| {
      RuntimeSource::ProjectedLegacy {
        config_path: path.to_owned(),
        auth_path: auth_path.clone(),
        args: v2_serve_args(),
      }
      .split_candidate()
      .unwrap()
      .1
    };
    let mut auth = tokn_auth::AuthStore::load(Some(&auth_path), None).unwrap();
    let mut credential = account("primary", "openai");
    credential.api_key = Some("first-test-key".to_owned().into());
    auth.upsert(credential.clone());
    auth.save().unwrap();
    let before_rotation = revision(&config_path);
    assert_eq!(before_rotation, revision(&config_path));
    assert_ne!(before_rotation, revision(&alternate));
    credential.api_key = Some("second-test-key".to_owned().into());
    auth.upsert(credential);
    auth.save().unwrap();
    let after_rotation = revision(&config_path);
    assert_ne!(before_rotation, after_rotation);
    legacy.logging.level = "debug".to_string();
    legacy.save(&config_path).unwrap();
    assert_ne!(after_rotation, revision(&config_path));
  }

  #[tokio::test]
  async fn projected_legacy_admin_reload_reprojects_config_and_accounts() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("legacy.toml");
    let auth_path = directory.path().join("auth.yaml");
    let ca_path = directory.path().join("ca");
    let api_addr = unused_loopback_addr();
    let proxy_addr = unused_loopback_addr();
    let mut legacy = Config::default();
    legacy.db.enabled = false;
    let _outbound_proxy = isolate_legacy_outbound(&mut legacy).await;
    legacy.server.host = api_addr.ip().to_string();
    legacy.server.port = api_addr.port();
    legacy.proxy_mode.host = proxy_addr.ip().to_string();
    legacy.proxy_mode.port = proxy_addr.port();
    legacy.proxy_mode.ca_dir = Some(ca_path);
    legacy.save(&config_path).unwrap();
    let mut auth = tokn_auth::AuthStore::load(Some(&auth_path), None).unwrap();
    auth.upsert(account("primary", "openai"));
    auth.save().unwrap();
    let mut args = v2_serve_args();
    args.with_proxy = true;
    let source = RuntimeSource::ProjectedLegacy {
      config_path: config_path.clone(),
      auth_path: auth_path.clone(),
      args,
    };
    let loaded = source.load().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(serve_fixture(
      loaded.compiled,
      loaded.accounts,
      loaded.args,
      Some(source),
      shutdown_rx,
    ));
    wait_for_listener(api_addr).await;
    let client = Client::builder()
      .no_proxy()
      .timeout(std::time::Duration::from_secs(5))
      .build()
      .unwrap();

    legacy.proxy_mode.route_mode = tokn_config::RouteMode::Fuzzy;
    legacy.save(&config_path).unwrap();
    let mut auth = tokn_auth::AuthStore::load(Some(&auth_path), None).unwrap();
    auth.upsert(account("secondary", "openai"));
    auth.save().unwrap();
    let (status, body) = reload_config(&client, api_addr).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["generation"], 2);
    assert_eq!(body["accounts"], 2);

    legacy.logging.target = tokn_config::LogTarget::Stderr;
    legacy.save(&config_path).unwrap();
    let (status, body) = reload_config(&client, api_addr).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["type"], "restart_required");
    assert!(body["error"]["message"]
      .as_str()
      .unwrap()
      .contains("logging settings changed"));
    legacy.logging.target = Config::default().logging.target;
    legacy.save(&config_path).unwrap();
    let (status, body) = reload_config(&client, api_addr).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["generation"], 3);
    assert_eq!(body["accounts"], 2);
    shutdown_tx.send(true).unwrap();
    server.await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn v2_rejects_legacy_proxy_flags_before_loading_config() {
    let args = ServeArgs {
      worker_socket: None,
      host: None,
      port: None,
      with_proxy: true,
      rollout: Default::default(),
      proxy_route_mode: None,
      insecure_allow_remote: false,
      no_proxy: false,
    };

    let path = PathBuf::from("unused.toml");
    let compiled = tokn_config::v2::parse_config(
      r#"schema_version = 2

[listeners.local]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"
"#,
      &path,
    )
    .unwrap();
    let config = tokn_config::SchemaConfig::V2 {
      config: Box::new(compiled),
      path: path.clone(),
    };
    let error = run_v2(path, config, args).await.unwrap_err();
    assert!(error.to_string().contains("v2 listeners are declared in config"));
  }

  #[tokio::test]
  async fn v2_serves_llm_api_and_forward_proxy_listeners_together() {
    let api_addr = unused_loopback_addr();
    let proxy_addr = unused_loopback_addr();
    let compiled = v2_listener_config(api_addr, proxy_addr);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(serve_fixture(compiled, Vec::new(), v2_serve_args(), None, shutdown_rx));

    wait_for_listener(api_addr).await;
    wait_for_listener(proxy_addr).await;

    let mut api = TcpStream::connect(api_addr).await.unwrap();
    api
      .write_all(
        b"POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
      )
      .await
      .unwrap();
    let mut api_response = Vec::new();
    api.read_to_end(&mut api_response).await.unwrap();
    assert!(api_response.starts_with(b"HTTP/1.1 404 Not Found"));

    assert_connect_rejected(proxy_addr).await;

    shutdown_tx.send(true).unwrap();
    server.await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn v2_listener_override_requires_exactly_one_listener() {
    let compiled = v2_listener_config(unused_loopback_addr(), unused_loopback_addr());
    let mut args = v2_serve_args();
    args.port = Some(4141);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let error = serve_fixture(compiled, Vec::new(), args, None, shutdown_rx)
      .await
      .unwrap_err();
    assert!(error.to_string().contains("exactly one listener"));
  }

  #[tokio::test]
  async fn v2_applies_bind_override_to_single_forward_proxy_listener() {
    let configured_addr = unused_loopback_addr();
    let override_addr = unused_loopback_addr();
    let config = format!(
      r#"
schema_version = 2

[service.persistence]
enabled = false

[service.models]
enabled = false

[listeners.proxy]
kind = "forward_proxy"
bind = "{configured_addr}"
client_auth = "none"
default_http_action = {{ kind = "reject" }}
default_connect = "reject"
"#
    );
    let compiled = tokn_config::v2::parse_config(&config, Path::new("v2-proxy-override.toml")).unwrap();
    let mut args = v2_serve_args();
    args.host = Some(override_addr.ip().to_string());
    args.port = Some(override_addr.port());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(serve_fixture(compiled, Vec::new(), args, None, shutdown_rx));

    wait_for_listener(override_addr).await;
    assert_connect_rejected(override_addr).await;

    shutdown_tx.send(true).unwrap();
    server.await.unwrap().unwrap();
  }
}
