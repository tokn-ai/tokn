use super::{test_support::*, *};
use reqwest::header;
use reqwest::{Client, Method, StatusCode};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use tokio::sync::watch;

const FIRST: &str = "https://first.example.com";
const SECOND: &str = "https://second.example.com";

/// Write equivalent disk configs to exercise both runtime-source reload paths.
fn write_config(path: &Path, legacy: Option<&Config>, address: SocketAddr, cors: &tokn_config::CorsConfig) {
  if let Some(config) = legacy {
    let mut config = config.clone();
    config.server.cors = cors.clone();
    config.save(path).unwrap();
  } else {
    let mut raw = tokn_config::v2::decode(
      &format!(
        r#"
schema_version = 2
[service.persistence]
enabled = false
[service.models]
enabled = false
[defaults]
[listeners.api]
kind = "llm_api"
bind = "{address}"
client_auth = "none"
"#
      ),
      path,
    )
    .unwrap();
    let tokn_config::v2::RawListener::LlmApi { cors: target, .. } = raw.listeners.get_mut("api").unwrap() else {
      panic!("expected API listener");
    };
    *target = cors.into();
    std::fs::write(path, toml::to_string(&raw).unwrap()).unwrap();
  }
}

async fn preflight_allowed(client: &Client, address: SocketAddr, origin: &str) -> bool {
  client
    .request(Method::OPTIONS, format!("http://{address}/v1/models"))
    .header(header::ORIGIN, origin)
    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
    .send()
    .await
    .unwrap()
    .headers()
    .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN)
}

#[tokio::test]
async fn cors_reload_reads_both_schemas_and_preserves_permissions_after_invalid_edits() {
  for legacy in [false, true] {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    let auth_path = directory.path().join("auth.yaml");
    let address = unused_loopback_addr();
    let (legacy_config, _outbound_proxy) = if legacy {
      let mut config = Config::default();
      config.db.enabled = false;
      config.server.host = address.ip().to_string();
      config.server.port = address.port();
      let proxy = isolate_legacy_outbound(&mut config).await;
      (Some(config), Some(proxy))
    } else {
      (None, None)
    };
    let mut auth = tokn_auth::AuthStore::load(Some(&auth_path), None).unwrap();
    auth.upsert(toml::from_str("id = 'primary'\nprovider = 'openai'\nenabled = true\napi_key = 'test-key'").unwrap());
    auth.save().unwrap();
    let mut cors = tokn_config::CorsConfig {
      enabled: true,
      allow_localhost: false,
      allowed_origins: vec![FIRST.into()],
    };
    write_config(&config_path, legacy_config.as_ref(), address, &cors);
    let args = ServeArgs {
      worker_socket: None,
      host: None,
      port: None,
      with_proxy: false,
      rollout: Default::default(),
      proxy_route_mode: None,
      insecure_allow_remote: false,
      no_proxy: false,
    };
    let source = if legacy {
      RuntimeSource::ProjectedLegacy {
        config_path: config_path.clone(),
        auth_path,
        args,
      }
    } else {
      RuntimeSource::NativeV2 {
        config_path: config_path.clone(),
        auth_path,
        args,
      }
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
    wait_for_listener(address).await;
    let client = Client::builder()
      .no_proxy()
      .timeout(Duration::from_secs(5))
      .build()
      .unwrap();
    assert!(preflight_allowed(&client, address, FIRST).await);
    assert!(!preflight_allowed(&client, address, SECOND).await);

    cors.allowed_origins = vec![SECOND.into()];
    write_config(&config_path, legacy_config.as_ref(), address, &cors);
    let (status, body) = reload_config(&client, address).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["generation"], 2);
    assert!(!preflight_allowed(&client, address, FIRST).await);
    assert!(preflight_allowed(&client, address, SECOND).await);

    // Bypass config writers' validation to simulate an invalid manual edit.
    let valid = std::fs::read_to_string(&config_path).unwrap();
    let invalid = valid.replace(SECOND, "https://second.example.com/not-an-origin");
    std::fs::write(&config_path, invalid).unwrap();
    let (status, body) = reload_config(&client, address).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["type"], "reload_failed");
    assert!(!preflight_allowed(&client, address, FIRST).await);
    assert!(preflight_allowed(&client, address, SECOND).await);

    cors.enabled = false;
    write_config(&config_path, legacy_config.as_ref(), address, &cors);
    let (status, body) = reload_config(&client, address).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["generation"], 3);
    assert!(!preflight_allowed(&client, address, SECOND).await);

    cors.enabled = true;
    cors.allowed_origins.clear();
    cors.allow_localhost = true;
    write_config(&config_path, legacy_config.as_ref(), address, &cors);
    let (status, body) = reload_config(&client, address).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["generation"], 4);
    assert!(!preflight_allowed(&client, address, SECOND).await);
    assert!(preflight_allowed(&client, address, "http://localhost:3000").await);
    shutdown_tx.send(true).unwrap();
    server.await.unwrap().unwrap();
  }
}
