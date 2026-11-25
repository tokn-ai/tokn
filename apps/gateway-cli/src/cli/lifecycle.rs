//! CLI adapters for the Unix gateway frontend and worker lifecycle.
use super::rollout::{default_duration, default_policy, RolloutArgs};
use super::serve::{self, ServeArgs};
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use std::path::{Path, PathBuf};
use tokio::process::Command;
use tokio::sync::{oneshot, watch};
use tokn_core::util::shutdown::ShutdownSignal;
use tokn_gateway::control::{self, FrontendInfo};
use tokn_router::routing::RolloutPolicy;

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
  let plan = loaded.compiled.gateway();
  let needs_access = plan
    .listeners()
    .values()
    .any(|listener| listener.client_auth() == tokn_policy::ClientAuthPlan::LocalKeys);
  let access = crate::server_runtime::load_access_store(needs_access)?;
  let bind_override = if loaded.args.host.is_some() || loaded.args.port.is_some() {
    anyhow::ensure!(
      plan.listeners().len() == 1,
      "--host and --port can only override a config with exactly one listener"
    );
    let listener = plan.listeners().values().next().context("missing listener")?;
    Some(serve::listener_bind(
      listener.bind(),
      listener.client_auth(),
      &loaded.args,
    )?)
  } else {
    None
  };
  let gateway = tokn_gateway::frontend::Gateway::bind(&dir, loaded.compiled, bind_override, access, move || {
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
  let mut shutdown = ShutdownSignal::new()?;
  let WorkerCmd::Start(args) = command;
  let discovery = args.frontend_config.as_deref().or(config.as_deref());
  let dir = runtime_dir(discovery)?;
  let control_path = control::control_socket_path(&dir);
  let info = tokio::select! {
    biased;
    result = shutdown.recv() => return result.map_err(Into::into),
    result = control::inspect(&control_path) => result.context(
      "no compatible frontend; run `tokn-gateway frontend --with-proxy` or `tokn-gateway serve --with-proxy` first",
    )?,
  };
  let registration = WorkerRegistration {
    candidate: args.candidate,
    rollout_policy: args.rollout.policy()?,
    no_proxy: args.no_proxy,
  };
  start_worker(config, dir, info, registration, shutdown, None).await
}

struct WorkerRegistration {
  candidate: bool,
  rollout_policy: Option<RolloutPolicy>,
  no_proxy: bool,
}

async fn start_worker(
  config: Option<PathBuf>,
  dir: PathBuf,
  info: FrontendInfo<ServeArgs>,
  registration: WorkerRegistration,
  mut shutdown: ShutdownSignal,
  supervisor: Option<oneshot::Receiver<()>>,
) -> Result<()> {
  let WorkerRegistration {
    candidate,
    rollout_policy,
    no_proxy,
  } = registration;
  let supervised = supervisor.is_some();
  let attachment = info.worker_attachment(
    &dir,
    candidate,
    rollout_policy,
    &default_policy(default_duration()),
    supervised,
  )?;
  let worker_id = attachment.worker_id().to_owned();
  let mut args = info.args;
  args.worker_socket = Some(attachment.socket_path().to_owned());
  args.no_proxy |= no_proxy;
  let (source, mut loaded) = serve::load_runtime(config, args)?;
  // Public binding consent is consumed during projection; this process only binds IPC.
  loaded.args.insecure_allow_remote = false;
  loaded.args.host = None;
  loaded.args.port = None;
  let (stop_tx, mut stop_rx) = watch::channel(false);
  let server = serve::run_loaded(source, loaded, info.maintenance_requests_dir.as_deref(), async move {
    let _ = stop_rx.changed().await;
    Ok(())
  });
  tokio::pin!(server);
  let (retire_tx, retire_rx) = watch::channel(false);
  let attach = attachment.attach(retire_rx, crate::progress::set_ab_test_status);
  tokio::pin!(attach);
  let mut attachment_done = false;
  let mut attachment_result = Ok(());
  let mut interrupted = false;
  let released = async {
    match supervisor {
      Some(lease) => {
        let _ = lease.await;
      }
      None => std::future::pending().await,
    }
  };
  tokio::pin!(released);
  loop {
    tokio::select! {
      biased;
      // The supervisor coordinates termination with frontend drain. An
      // independently launched worker handles its own shutdown signals.
      result = shutdown.recv(), if !supervised => {
        result?;
        if interrupted {
          eprintln!("Second shutdown signal received; exiting immediately.");
          std::process::exit(130);
        }
        interrupted = true;
        tracing::info!(%worker_id, "retiring worker; waiting for existing requests (Ctrl+C again forces exit)");
        let _ = retire_tx.send(true);
      }
      _ = &mut released, if !interrupted => {
        interrupted = true;
        let _ = retire_tx.send(true);
      }
      result = &mut server => {
        crate::progress::set_ab_test_status(None);
        return attachment_result.and(result);
      }
      result = &mut attach, if !attachment_done => {
        attachment_result = result;
        attachment_done = true;
        crate::progress::set_ab_test_status(None);
        let _ = stop_tx.send(true);
      }
    }
  }
}

pub async fn serve(config: Option<PathBuf>, args: ServeArgs) -> Result<()> {
  let mut shutdown = ShutdownSignal::new()?;
  let dir = runtime_dir(config.as_deref())?;
  let launch = control::discover_or_launch::<ServeArgs>(&dir, args.rollout.ab_test, shutdown.recv(), || {
    // Validate all configuration before spawning an independently owned process.
    let _ = serve::load_runtime(config.clone(), args.clone())?;
    let mut command = Command::new(std::env::current_exe()?);
    let loaded = tokn_config::load_config(config.as_deref())?;
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
  })
  .await?;
  let Some(control::FrontendLaunch { info, owned }) = launch else {
    return Ok(());
  };
  anyhow::ensure!(
    !args.with_proxy || info.args.with_proxy,
    "the existing frontend was started without --with-proxy; restart the frontend to enable its legacy proxy listener"
  );
  tracing::info!(frontend_pid = info.frontend_pid, "using frontend");
  let registration = WorkerRegistration {
    candidate: false,
    rollout_policy: args.rollout.policy()?,
    no_proxy: args.no_proxy,
  };
  if let Some((frontend, parent)) = owned {
    let stop = ShutdownSignal::new()?;
    return tokn_gateway::supervision::serve_owned(
      frontend,
      parent,
      move |release| start_worker(config, dir, info, registration, shutdown, Some(release)),
      stop.wait(),
    )
    .await;
  }
  start_worker(config, dir, info, registration, shutdown, None).await
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cli::{Cli, Cmd};
  use clap::Parser;

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
