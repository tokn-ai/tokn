//! Extras-stats harness.
//!
//! Samples up to N passthrough rows per endpoint from the local request
//! databases, deserializes them through the typed endpoint crates, and
//! prints which `extras` keys (i.e. fields the typed schemas don't model)
//! were captured.
//!
//! Run with:
//!   cargo run --example extras_stats -p tokn-convert
//!
//! Optional env vars:
//!   EXTRAS_LIMIT=1000    rows per endpoint (default 1000)
//!   EXTRAS_DB_DIR=...    override request DB directory

#![cfg(debug_assertions)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use tokn_endpoint_chat_completions::{ChatEvent, ChatRequest, ChatResponse};
use tokn_endpoint_core::ExtraKeys;
use tokn_endpoint_messages::{MessagesEvent, MessagesRequest, MessagesResponse};
use tokn_endpoint_responses::{ResponsesEvent, ResponsesRequest, ResponsesResponse};

const DEFAULT_LIMIT: usize = 1000;
const ENDPOINTS: &[&str] = &["chat_completions", "responses", "messages"];

#[derive(Default)]
struct Bucket {
  parsed: usize,
  failed: usize,
  keys: BTreeMap<String, usize>,
  errors: BTreeMap<String, usize>,
}

impl Bucket {
  fn record_keys(&mut self, keys: Vec<String>) {
    self.parsed += 1;
    for k in keys {
      *self.keys.entry(k).or_default() += 1;
    }
  }

  fn record_error(&mut self, err: impl ToString) {
    self.failed += 1;
    let e = err.to_string();
    let short: String = e.chars().take(120).collect();
    *self.errors.entry(short).or_default() += 1;
  }
}

#[derive(Default)]
struct Report {
  // (endpoint, direction) -> bucket. direction in {request, response.{kind}}
  buckets: BTreeMap<(String, String), Bucket>,
  rows_seen: BTreeMap<String, usize>,
}

impl Report {
  fn bucket(&mut self, endpoint: &str, direction: &str) -> &mut Bucket {
    self
      .buckets
      .entry((endpoint.to_string(), direction.to_string()))
      .or_default()
  }
}

fn main() -> Result<()> {
  let limit: usize = std::env::var("EXTRAS_LIMIT")
    .ok()
    .and_then(|s| s.parse().ok())
    .unwrap_or(DEFAULT_LIMIT);

  let dir = match std::env::var("EXTRAS_DB_DIR") {
    Ok(s) => PathBuf::from(s),
    Err(_) => tokn_config::paths::default_requests_dir().context("resolve default requests dir")?,
  };

  if !dir.exists() {
    anyhow::bail!("requests dir does not exist: {}", dir.display());
  }

  let mut db_files = list_db_files(&dir)?;
  db_files.sort();
  db_files.reverse(); // newest first

  eprintln!("scanning {} db file(s) in {}", db_files.len(), dir.display());

  let mut report = Report::default();
  let mut remaining: BTreeMap<&str, usize> = ENDPOINTS.iter().map(|e| (*e, limit)).collect();

  for path in &db_files {
    if remaining.values().all(|n| *n == 0) {
      break;
    }
    if let Err(e) = scan_db(path, &mut remaining, &mut report) {
      eprintln!("  {}: skipped ({e})", path.display());
    }
  }

  print_report(&report);
  Ok(())
}

fn list_db_files(dir: &Path) -> Result<Vec<PathBuf>> {
  let mut out = Vec::new();
  for entry in fs::read_dir(dir)? {
    let entry = entry?;
    let path = entry.path();
    if path.extension().and_then(|s| s.to_str()) == Some("db") {
      out.push(path);
    }
  }
  Ok(out)
}

fn scan_db(path: &Path, remaining: &mut BTreeMap<&'static str, usize>, report: &mut Report) -> Result<()> {
  let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
  let has_mode = table_has_column(&conn, "requests", "mode")?;
  for endpoint in ENDPOINTS {
    let want = remaining[endpoint];
    if want == 0 {
      continue;
    }
    let rows = fetch_rows(&conn, endpoint, want, has_mode)?;
    let got = rows.len();
    *report.rows_seen.entry((*endpoint).into()).or_default() += got;
    for (req_body, resp_body, stream) in rows {
      process_request(endpoint, &req_body, report);
      process_response(endpoint, &resp_body, stream, report);
    }
    *remaining.get_mut(*endpoint).unwrap() = want.saturating_sub(got);
  }
  Ok(())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
  let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
  let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
  for r in rows {
    if r? == column {
      return Ok(true);
    }
  }
  Ok(false)
}

type RequestRow = (Vec<u8>, Vec<u8>, i64);

fn fetch_rows(conn: &Connection, endpoint: &str, limit: usize, has_mode: bool) -> Result<Vec<RequestRow>> {
  let where_clause = if has_mode {
    "mode = 'passthrough' AND endpoint = ?1"
  } else {
    // Older schemas predate the `mode` column; they only ever recorded
    // passthrough-style traffic so we can sample everything.
    "endpoint = ?1"
  };
  let sql = format!(
    "SELECT inbound_req_body, COALESCE(outbound_resp_body, X''), stream \
     FROM requests \
     WHERE {where_clause} \
     LIMIT ?2"
  );
  let mut stmt = conn.prepare(&sql)?;
  let rows = stmt.query_map(rusqlite::params![endpoint, limit as i64], |row| {
    Ok((
      row.get::<_, Vec<u8>>(0)?,
      row.get::<_, Vec<u8>>(1)?,
      row.get::<_, i64>(2)?,
    ))
  })?;
  let mut out = Vec::new();
  for r in rows {
    out.push(r?);
  }
  Ok(out)
}

fn process_request(endpoint: &str, body: &[u8], report: &mut Report) {
  if body.is_empty() {
    return;
  }
  match endpoint {
    "chat_completions" => match serde_json::from_slice::<ChatRequest>(body) {
      Ok(v) => report.bucket(endpoint, "request").record_keys(v.extra_keys()),
      Err(e) => report.bucket(endpoint, "request").record_error(e),
    },
    "responses" => match serde_json::from_slice::<ResponsesRequest>(body) {
      Ok(v) => report.bucket(endpoint, "request").record_keys(v.extra_keys()),
      Err(e) => report.bucket(endpoint, "request").record_error(e),
    },
    "messages" => match serde_json::from_slice::<MessagesRequest>(body) {
      Ok(v) => report.bucket(endpoint, "request").record_keys(v.extra_keys()),
      Err(e) => report.bucket(endpoint, "request").record_error(e),
    },
    _ => {}
  }
}

fn process_response(endpoint: &str, body: &[u8], stream: i64, report: &mut Report) {
  if body.is_empty() {
    return;
  }
  if stream != 0 {
    process_sse_response(endpoint, body, report);
    return;
  }
  match endpoint {
    "chat_completions" => match serde_json::from_slice::<ChatResponse>(body) {
      Ok(v) => report.bucket(endpoint, "response").record_keys(v.extra_keys()),
      Err(e) => report.bucket(endpoint, "response").record_error(e),
    },
    "responses" => match serde_json::from_slice::<ResponsesResponse>(body) {
      Ok(v) => report.bucket(endpoint, "response").record_keys(v.extra_keys()),
      Err(e) => report.bucket(endpoint, "response").record_error(e),
    },
    "messages" => match serde_json::from_slice::<MessagesResponse>(body) {
      Ok(v) => report.bucket(endpoint, "response").record_keys(v.extra_keys()),
      Err(e) => report.bucket(endpoint, "response").record_error(e),
    },
    _ => {}
  }
}

fn process_sse_response(endpoint: &str, body: &[u8], report: &mut Report) {
  let text = match std::str::from_utf8(body) {
    Ok(s) => s,
    Err(_) => {
      report
        .bucket(endpoint, "response.stream")
        .record_error("non-utf8 sse body");
      return;
    }
  };
  for line in text.lines() {
    let payload = match line.strip_prefix("data:") {
      Some(rest) => rest.trim(),
      None => continue,
    };
    if payload.is_empty() || payload == "[DONE]" {
      continue;
    }
    match endpoint {
      "chat_completions" => match serde_json::from_str::<ChatEvent>(payload) {
        Ok(ev) => {
          let kind = match &ev {
            ChatEvent::Chunk(_) => "chunk",
            ChatEvent::Done => "done",
          };
          let dir = format!("response.stream.{kind}");
          report.bucket(endpoint, &dir).record_keys(ev.extra_keys());
        }
        Err(e) => report.bucket(endpoint, "response.stream").record_error(e),
      },
      "responses" => match serde_json::from_str::<ResponsesEvent>(payload) {
        Ok(ev) => {
          let dir = format!("response.stream.{}", ev.kind());
          report.bucket(endpoint, &dir).record_keys(ev.extra_keys());
        }
        Err(e) => report.bucket(endpoint, "response.stream").record_error(e),
      },
      "messages" => match serde_json::from_str::<MessagesEvent>(payload) {
        Ok(ev) => {
          let dir = format!("response.stream.{}", ev.kind());
          report.bucket(endpoint, &dir).record_keys(ev.extra_keys());
        }
        Err(e) => report.bucket(endpoint, "response.stream").record_error(e),
      },
      _ => {}
    }
  }
}

fn print_report(report: &Report) {
  for ep in ENDPOINTS {
    let seen = report.rows_seen.get(*ep).copied().unwrap_or(0);
    println!("\n########## {ep} ({seen} rows sampled) ##########");
    for ((endpoint, direction), bucket) in &report.buckets {
      if endpoint != ep {
        continue;
      }
      println!(
        "\n--- {ep} / {direction} (parsed {}, failed {}) ---",
        bucket.parsed, bucket.failed
      );
      let mut keys: Vec<(&String, &usize)> = bucket.keys.iter().collect();
      keys.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
      for (k, n) in keys.iter().take(50) {
        println!("  {n:>6}  {k}");
      }
      if keys.len() > 50 {
        println!("  ... ({} more)", keys.len() - 50);
      }
      if !bucket.errors.is_empty() {
        let mut errs: Vec<(&String, &usize)> = bucket.errors.iter().collect();
        errs.sort_by(|a, b| b.1.cmp(a.1));
        println!("  parse errors:");
        for (e, n) in errs.iter().take(5) {
          println!("    {n:>6}  {e}");
        }
        if errs.len() > 5 {
          println!("    ... ({} more)", errs.len() - 5);
        }
      }
    }
  }
}
