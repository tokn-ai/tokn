use crate::{
  config, data,
  gateway::{self, Gateway, GatewayStatus},
};

pub(crate) async fn blocking<T: Send + 'static>(
  f: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> Result<T, String> {
  tokio::task::spawn_blocking(f)
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| format!("{error:#}"))
}

#[tauri::command]
pub async fn gateway_status(state: tauri::State<'_, Gateway>) -> Result<GatewayStatus, String> {
  state
    .status(blocking(config::address).await?)
    .await
    .map_err(|error| format!("{error:#}"))
}
#[tauri::command]
pub async fn start_gateway(state: tauri::State<'_, Gateway>) -> Result<(), String> {
  let (executable, path, address) =
    blocking(|| Ok((gateway::executable()?, config::path()?, config::address()?))).await?;
  state
    .start(tokio::process::Command::new(executable), path, address)
    .await
    .map_err(|error| format!("{error:#}"))
}
#[tauri::command]
pub async fn stop_gateway(state: tauri::State<'_, Gateway>) -> Result<(), String> {
  state.stop().await.map_err(|error| format!("{error:#}"))
}
#[tauri::command]
pub async fn reload_gateway() -> Result<String, String> {
  gateway::reload(blocking(config::address).await?)
    .await
    .map_err(|error| format!("{error:#}"))
}
#[tauri::command]
pub async fn read_routing() -> Result<config::RoutingDocument, String> {
  blocking(|| config::read_at(&config::path()?)).await
}
#[tauri::command(rename_all = "snake_case")]
pub async fn save_routing(revision: String, routing_toml: String) -> Result<config::RoutingDocument, String> {
  blocking(move || config::save_at(&config::path()?, &revision, &routing_toml)).await
}
#[tauri::command]
pub async fn read_usage() -> Result<Vec<data::UsageSummary>, String> {
  blocking(data::usage).await
}
