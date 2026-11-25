use anyhow::Result;
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize)]
pub struct UsageSummary {
  pub account: Option<String>,
  pub provider: Option<String>,
  pub model: String,
  pub requests: u64,
  pub input_tokens: u64,
  pub output_tokens: u64,
  pub cached_tokens: u64,
}

pub fn usage() -> Result<Vec<UsageSummary>> {
  let path = crate::config::load()?.persistence().resolve_paths()?.usage_db;
  if !path.exists() {
    return Ok(Vec::new());
  }
  Ok(
    tokn_persistence::UsageDb::open_readonly(&path)?
      .summary_for_window(Duration::from_secs(24 * 60 * 60), None, None)?
      .into_iter()
      .map(|row| UsageSummary {
        account: row.account,
        provider: row.provider,
        model: row.model,
        requests: row.count,
        input_tokens: row.input_tokens,
        output_tokens: row.output_tokens,
        cached_tokens: row.cached_tokens,
      })
      .collect(),
  )
}
