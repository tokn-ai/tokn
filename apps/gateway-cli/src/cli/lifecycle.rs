//! CLI adapters for the Unix gateway frontend and worker lifecycle.
use super::rollout::{default_duration, default_policy, RolloutArgs};
use super::serve::{self, ServeArgs};
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use std::path::{Path, PathBuf};
use tokio::process::Command;
use tokio::sync::watch;
use tokn_core::util::shutdown::ShutdownSignal;
use tokn_gateway::split::{self, ForceShutdown, WorkerRegistration, WorkerSettings};

#[derive(Subcommand, Debug)]
pub enum WorkerCmd {
  /// Start a worker, replacing the current worker or joining an A/B experiment.
  Start(WorkerArgs),
}

#[derive(Args, Debug, Default)]
pub struct WorkerArgs {
  /// Discover a frontend started with a different configuration file.
  #[arg(long)]
  frontend_config: Option<PathBuf>,
  /// Register without promoting, for an A/B experiment using /admin/workers.
  #[arg(long, conflicts_with = "ab_test")]
  candidate: bool,
  #[command(flatten)]
  rollout: RolloutArgs,
  /// Skip outbound proxy for this worker.
  #[arg(long)]
  no_proxy: bool,
}

fn runtime_dir(config: Option<&Path>) -> Result<PathBuf> {
  let config = tokn_config::load_config(config)?;
  tokn_gateway::supervision::runtime_dir(config.path())
}

pub async fn frontend(config: Option<PathBuf>, args: ServeArgs) -> Result<()> {
  let shutdown = ShutdownSignal::new()?;
  anyhow::ensure!(args.worker_socket.is_none(), "frontend does not accept --worker-socket");
  anyhow::ensure!(
    !args.rollout.ab_test,
    "--ab-test belongs to serve or worker start, not frontend"
  );
  let dir = runtime_dir(config.as_deref())?;
  let (source, loaded) = serve::load_runtime(config, args.clone())?;
  let bind = loaded.args.bind_options();
  let gateway = tokn_gateway::frontend::Gateway::bind(&dir, loaded.compiled, bind, move || {
    let (loaded, revision) = source.split_candidate()?;
    Ok((loaded.compiled, revision))
  })
  .await?;
  gateway
    .serve(
      args,
      default_policy(default_duration()),
      crate::server_runtime::maintenance_handlers(),
      shutdown.wait(),
    )
    .await
}

pub async fn worker(config: Option<PathBuf>, command: WorkerCmd) -> Result<()> {
  let WorkerCmd::Start(args) = command;
  let discovery = args.frontend_config.as_deref().or(config.as_deref());
  let result = split::join(
    || runtime_dir(discovery),
    || registration(args.candidate, &args.rollout),
    async |settings, stop| run_worker(config.clone(), args.no_proxy, settings, stop).await,
    crate::progress::set_ab_test_status,
  )
  .await;
  worker_exit(result)
}

async fn run_worker(
  config: Option<PathBuf>,
  no_proxy: bool,
  settings: WorkerSettings<ServeArgs>,
  mut stop: watch::Receiver<bool>,
) -> Result<()> {
  let mut args = settings.frontend;
  args.worker_socket = Some(settings.socket_path);
  args.no_proxy |= no_proxy;
  let (source, mut loaded) = serve::load_runtime(config, args)?;
  // Public binding consent is consumed during projection; this process only binds IPC.
  loaded.args.insecure_allow_remote = false;
  loaded.args.host = None;
  loaded.args.port = None;
  serve::run_loaded(
    source,
    loaded,
    settings.frontend_maintenance_dir.as_deref(),
    async move {
      let _ = stop.changed().await;
      Ok(())
    },
  )
  .await
}

pub async fn serve(config: Option<PathBuf>, args: ServeArgs) -> Result<()> {
  let result = split::serve(
    || runtime_dir(config.as_deref()),
    args.rollout.ab_test,
    || frontend_command(config.as_deref(), &args),
    |frontend: &ServeArgs| {
      anyhow::ensure!(
        !args.with_proxy || frontend.with_proxy,
        "the existing frontend was started without --with-proxy; restart the frontend to enable its legacy proxy listener"
      );
      registration(false, &args.rollout)
    },
    async |settings, stop| run_worker(config.clone(), args.no_proxy, settings, stop).await,
    crate::progress::set_ab_test_status,
  )
  .await;
  worker_exit(result)
}

fn frontend_command(config: Option<&Path>, args: &ServeArgs) -> Result<Command> {
  // Validate all configuration before spawning an independently owned process.
  let _ = serve::load_runtime(config.map(Path::to_path_buf), args.clone())?;
  let mut command = Command::new(std::env::current_exe()?);
  let loaded = tokn_config::load_config(config)?;
  command
    .arg("--config")
    .arg(std::fs::canonicalize(loaded.path())?)
    .arg("frontend");
  if args.with_proxy {
    command.arg("--with-proxy");
  }
  if let Some(host) = &args.host {
    command.arg("--host").arg(host);
  }
  if let Some(port) = args.port {
    command.arg("--port").arg(port.to_string());
  }
  if let Some(mode) = args.proxy_route_mode {
    use clap::ValueEnum;
    command
      .arg("--proxy-route-mode")
      .arg(mode.to_possible_value().context("invalid route mode")?.get_name());
  }
  if args.insecure_allow_remote {
    command.arg("--insecure-allow-remote");
  }
  if args.no_proxy {
    command.arg("--no-proxy");
  }
  Ok(command)
}

fn registration(candidate: bool, rollout: &RolloutArgs) -> Result<WorkerRegistration> {
  Ok(WorkerRegistration {
    candidate,
    rollout_policy: rollout.policy()?,
    default_rollout: default_policy(default_duration()),
  })
}

fn worker_exit(result: Result<()>) -> Result<()> {
  if let Err(error) = &result {
    if error.is::<ForceShutdown>() {
      eprintln!("{error}");
      std::process::exit(130);
    }
  }
  result
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cli::{Cli, Cmd};
  use clap::Parser;
  use tokn_gateway::control::FrontendInfo;

  #[test]
  fn serving_settings_from_older_frontends_default_rollout_options() {
    let mut args = serde_json::to_value(ServeArgs::default()).unwrap();
    args.as_object_mut().unwrap().remove("ab_test");
    let info: FrontendInfo<ServeArgs> = serde_json::from_value(serde_json::json!({
      "protocol_version": 2, "frontend_pid": 42, "args": args,
    }))
    .unwrap();
    assert!(!info.args.rollout.ab_test);
    assert!(info.maintenance_requests_dir.is_none());
  }

  #[test]
  fn rollout_duration_flags_are_shared_and_require_an_experiment() {
    for argv in [
      vec!["tokn-gateway", "serve", "--ab-test", "--ab-test-duration", "72h"],
      vec![
        "tokn-gateway",
        "worker",
        "start",
        "--ab-test",
        "--ab-test-duration",
        "72h",
      ],
    ] {
      let cli = Cli::try_parse_from(argv).unwrap();
      let args = match cli.cmd {
        Cmd::Serve(args) => args.rollout,
        Cmd::Worker(WorkerCmd::Start(args)) => args.rollout,
        _ => panic!("worker startup command"),
      };
      assert_eq!(args.policy().unwrap().unwrap().validate().unwrap(), 72 * 3600);
    }
    assert!(Cli::try_parse_from(["tokn-gateway", "serve", "--ab-test-duration", "72h"]).is_err());
    assert!(Cli::try_parse_from(["tokn-gateway", "serve", "--ab-test", "--ab-test-duration", "0s"]).is_err());
    assert!(Cli::try_parse_from([
      "tokn-gateway",
      "serve",
      "--ab-test",
      "--ab-test-duration",
      "72h",
      "--ab-test-policy",
      "policy.toml"
    ])
    .is_err());
  }

  #[test]
  fn ab_test_flags_select_automatic_registration() {
    let cli = Cli::try_parse_from(["tokn-gateway", "serve", "--with-proxy", "--ab-test"]).unwrap();
    let Cmd::Serve(args) = cli.cmd else {
      panic!("serve command");
    };
    assert!(args.rollout.ab_test && args.with_proxy);
    let cli = Cli::try_parse_from(["tokn-gateway", "worker", "start", "--ab-test"]).unwrap();
    let Cmd::Worker(WorkerCmd::Start(args)) = cli.cmd else {
      panic!("worker command");
    };
    assert!(args.rollout.ab_test && !args.candidate);
    assert!(Cli::try_parse_from(["tokn-gateway", "worker", "start", "--ab-test", "--candidate"]).is_err());
  }
}
