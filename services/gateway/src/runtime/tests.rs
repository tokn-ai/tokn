use super::*;

#[tokio::test]
async fn listener_failure_signals_and_awaits_sibling_cleanup() {
  let (shutdown_tx, shutdown_rx) = watch::channel(false);
  let (cleaned_tx, cleaned_rx) = tokio::sync::oneshot::channel();
  let servers: Vec<BoxFuture<'static, Result<()>>> = vec![
    Box::pin(async { anyhow::bail!("fixture bind failure") }),
    Box::pin(async {
      wait_for_shutdown(shutdown_rx).await;
      tokio::task::yield_now().await;
      cleaned_tx.send(()).unwrap();
      Ok(())
    }),
  ];
  let error = supervise_listeners(servers, shutdown_tx, std::future::pending())
    .await
    .unwrap_err();
  assert!(error.to_string().contains("fixture bind failure"));
  cleaned_rx.await.unwrap();
}

#[test]
fn remote_bind_override_cannot_bypass_compiled_authentication() {
  let configured = "127.0.0.1:4141".parse().unwrap();
  let mut args = BindOptions {
    host: Some("0.0.0.0".into()),
    insecure_allow_remote: true,
    ..BindOptions::default()
  };
  let error = listener_bind(configured, tokn_policy::ClientAuthPlan::None, &args).unwrap_err();
  assert!(error.to_string().contains("require client_auth"));
  assert_eq!(
    listener_bind(configured, tokn_policy::ClientAuthPlan::LocalKeys, &args).unwrap(),
    "0.0.0.0:4141".parse::<std::net::SocketAddr>().unwrap()
  );
  args.insecure_allow_remote = false;
  assert!(listener_bind(configured, tokn_policy::ClientAuthPlan::LocalKeys, &args).is_err());
}

#[test]
fn model_refresh_changes_require_restart() {
  let replacement = tokn_config::v2::parse_config(
    "schema_version = 2\n[defaults]\n[listeners.api]\nkind = 'llm_api'\nbind = '127.0.0.1:4141'\nclient_auth = 'none'\n[service.models]\nupstream_refresh_seconds = 60",
    std::path::Path::new("refresh.toml"),
  )
  .unwrap();
  assert!(matches!(
    v2::ensure_service_reload_compatible(&tokn_config::v2::ServiceSettings::default(), replacement.service()),
    Err(tokn_router::v2::ReloadError::RestartRequired(message)) if message == "model refresh settings changed"
  ));
}
