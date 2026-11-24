//! `tokn-router update` — refresh the on-disk models.dev catalogue cache.
//!
//! The cache (when present) is preferred over the snapshot embedded at
//! build time. Without this command the binary still works — it just runs
//! against whatever was current when the binary was compiled.

use anyhow::Result;
use clap::Args;
use std::time::Duration;

use tokn_catalogue::loader::{self, Source};

const DEFAULT_URL: &str = "https://models.dev/api.json";

#[derive(Args, Debug)]
pub struct UpdateArgs {
  /// Override the source URL.
  #[arg(long, default_value = DEFAULT_URL)]
  url: String,

  /// HTTP timeout in seconds.
  #[arg(long, default_value_t = 30)]
  timeout: u64,

  /// Don't fetch — just report where the cache lives and which source is
  /// active.
  #[arg(long)]
  status: bool,
}

pub async fn run(args: UpdateArgs) -> Result<()> {
  if args.status {
    return print_status();
  }

  let http = reqwest::Client::builder()
    .timeout(Duration::from_secs(args.timeout))
    .user_agent(tokn_core::util::version::tokn_router_user_agent())
    .build()?;

  println!("fetching {} ...", args.url);
  let r = loader::fetch_and_persist(&http, &args.url).await?;
  println!(
    "ok: {} providers, {} models, {} bytes -> {} ({}ms)",
    r.providers,
    r.models,
    r.bytes,
    r.path.display(),
    r.elapsed.as_millis()
  );
  println!("(restart the server for the new catalogue to take effect)");
  Ok(())
}

fn print_status() -> Result<()> {
  let cache = loader::cache_path();
  println!("source url   : {DEFAULT_URL}");
  match &cache {
    Some(p) => {
      println!("cache path   : {}", p.display());
      match std::fs::metadata(p) {
        Ok(meta) => {
          println!("cache size   : {} bytes", meta.len());
          if let Ok(modified) = meta.modified() {
            let dur = modified
              .elapsed()
              .ok()
              .map(|d| crate::util::timefmt::relative_delta(-(d.as_secs() as i64)))
              .unwrap_or_else(|| "unknown".into());
            println!("cache age    : {dur}");
          }
        }
        Err(_) => println!("cache size   : (no file — run `tokn-router update`)"),
      }
    }
    None => println!("cache path   : (no XDG cache dir resolvable)"),
  }

  // Trigger lazy load to learn the active source.
  let (cat, src) = loader::global_with_source();
  let active = match src {
    Source::Embedded => "embedded snapshot (compiled in)".to_string(),
    Source::DiskCache(p) => format!("disk cache at {}", p.display()),
  };
  println!("active source: {active}");
  let total_models: usize = cat.values().map(|p| p.models.len()).sum();
  println!("loaded       : {} providers, {} models", cat.len(), total_models);
  Ok(())
}
