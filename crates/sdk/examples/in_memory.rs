//! Configure the shared client in memory and select standard HTTP services.
use serde_json::json;
use tokn_sdk::{account::AccountConfig, Client, StdPlatform};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let raw = serde_json::from_value(json!({
    "schema_version": 2,
    "defaults": {
      "provider": {"kind": "fixed", "provider": "openai"},
      "model": {"kind": "qualified", "namespace": "provider"}
    },
    "providers": {"openai": {"driver": "openai", "base_url": "https://api.openai.com"}}
  }))?;
  let rules = tokn_sdk::config::v2::compile_execution(&raw)?;
  let account: AccountConfig = serde_json::from_value(json!({
    "id": "application-key",
    "provider": "openai",
    "api_key": std::env::var("OPENAI_API_KEY")?
  }))?;
  // Adapter::new(transport, runtime) can replace the services here without
  // changing the rules, accounts, or shared request session.
  let client = Client::new(rules, vec![account], StdPlatform::new()?)?;
  let response = client.generate("openai/gpt-5").prompt("Say hello.").send().await?;
  println!("{}", response.text);
  Ok(())
}
