//! TTY progress display for in-flight requests.
//!
//! Implements [`EventHandler`] driving an [`indicatif::MultiProgress`].
//! One bar per request_id; updated on lifecycle events; finalised on
//! `RequestCompleted` and persisted in the scrollback. A persistent
//! footer bar (last line of the MultiProgress) shows live session
//! counters.
//!
//! Only registered when stdout is a TTY (see `server_runtime.rs`).

use crate::db::archive::{ArchiveEvent, ArchiveEventHandler};
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use time::{macros::format_description, OffsetDateTime};
use tokn_core::db::Usage;
use tokn_core::event::{Event, EventHandler};
use tokn_core::request_classification::RequestClassification;
use tokn_core::request_event::{ConvertedRequestSummary, RecordEvent, RequestEvent, RequestEventPayload, StageEvent};
use tokn_router::routing::AbTestStatus;

/// Process-wide [`MultiProgress`] shared between [`ProgressEventHandler`]
/// and the tracing log writer (so log lines suspend the bars during
/// emission instead of garbling them).
static MULTI: OnceLock<MultiProgress> = OnceLock::new();
const USAGE_GRACE_PERIOD: Duration = Duration::from_secs(3);

/// Returns the shared [`MultiProgress`]. Lazily initialized on first call;
/// safe to call from any context (logging init or event handler).
pub fn multi() -> &'static MultiProgress {
  MULTI.get_or_init(|| MultiProgress::with_draw_target(ProgressDrawTarget::stdout()))
}

struct RequestState {
  started: Instant,
  provider: String,
  model: String,
  priority: bool,
  account: String,
  endpoint: String,
  request_classification: Option<RequestClassification>,
  attempt: u32,
  sent_bytes: u64,
  recv_bytes: u64,
  usage: Usage,
  final_status: Option<u16>,
  error: Option<String>,
}

impl RequestState {
  fn new(endpoint: String) -> Self {
    Self {
      started: Instant::now(),
      provider: String::new(),
      model: String::new(),
      priority: false,
      account: String::new(),
      endpoint,
      request_classification: None,
      attempt: 0,
      sent_bytes: 0,
      recv_bytes: 0,
      usage: Usage::default(),
      final_status: None,
      error: None,
    }
  }

  fn id_short(request_id: &str) -> String {
    request_id.chars().take(8).collect()
  }

  fn render_endpoint(&self) -> String {
    let classification = self
      .request_classification
      .map(|classification| format!(" {}", style(format!("[{}]", classification.purpose.as_str())).yellow()))
      .unwrap_or_default();
    format!("{}{}", style(&self.endpoint).dim(), classification)
  }

  fn update_priority(&mut self, request: &ConvertedRequestSummary) {
    if request.upstream_body.is_object() {
      self.priority = request
        .upstream_body
        .get("service_tier")
        .and_then(serde_json::Value::as_str)
        == Some("priority");
    } else {
      // Relay summaries keep a null body sentinel; the debug bytes are
      // decoded JSON even when the wire request uses gzip or zstd.
      #[derive(serde::Deserialize)]
      struct ServiceTierPeek {
        service_tier: Option<String>,
      }
      self.priority = serde_json::from_slice::<ServiceTierPeek>(&request.debug_outbound_body)
        .is_ok_and(|body| body.service_tier.as_deref() == Some("priority"));
    }
  }

  fn render_model(&self) -> String {
    let model = style(truncate(&self.model, 28)).cyan();
    if self.priority {
      format!("{model} {}", style("⚡").yellow())
    } else {
      model.to_string()
    }
  }

  fn render_in_flight(&self, request_id: &str) -> String {
    let elapsed = self.started.elapsed().as_secs_f64();
    let speed_kbs = if elapsed > 0.05 {
      (self.recv_bytes as f64) / 1024.0 / elapsed
    } else {
      0.0
    };
    let attempt_part = if self.attempt > 0 {
      format!(" {}", style(format!("a={}", self.attempt)).yellow())
    } else {
      String::new()
    };
    format!(
      "[{}] {} {} {}{} {} sent={:.1}kB recv={:.1}kB {:.1}kB/s elapsed={:.1}s",
      style(Self::id_short(request_id)).dim(),
      style(&self.provider).blue(),
      self.render_model(),
      style(truncate(&self.account, 16)).magenta(),
      attempt_part,
      self.render_endpoint(),
      (self.sent_bytes as f64) / 1024.0,
      (self.recv_bytes as f64) / 1024.0,
      speed_kbs,
      elapsed,
    )
  }

  fn merge_usage(&mut self, usage: &Usage) {
    if usage.input_tokens.is_some() {
      self.usage.input_tokens = usage.input_tokens;
    }
    if usage.output_tokens.is_some() {
      self.usage.output_tokens = usage.output_tokens;
    }
    if usage.total_tokens.is_some() {
      self.usage.total_tokens = usage.total_tokens;
    }
    if usage.details.cache_read.is_some() {
      self.usage.details.cache_read = usage.details.cache_read;
    }
    if usage.details.cache_write.is_some() {
      self.usage.details.cache_write = usage.details.cache_write;
    }
    if usage.details.reasoning.is_some() {
      self.usage.details.reasoning = usage.details.reasoning;
    }
  }

  fn render_completed(
    &self,
    request_id: &str,
    success: bool,
    total_attempts: u32,
    final_status: Option<u16>,
    total_latency_ms: u64,
    error: Option<&str>,
  ) -> String {
    let id_short = Self::id_short(request_id);
    let latency_s = (total_latency_ms as f64) / 1000.0;
    let attempts_part = if total_attempts > 1 {
      format!(" attempts={total_attempts}")
    } else {
      String::new()
    };
    if success {
      let status_part = final_status
        .map(|status| format!(" {}", style_status(status)))
        .unwrap_or_default();
      format!(
        "[{}] {}{} {} {} {} {} sent={:.1}kB recv={:.1}kB{} latency={:.1}s{}",
        style(&id_short).dim(),
        style("✓").green().bold(),
        status_part,
        style(&self.provider).blue(),
        self.render_model(),
        style(truncate(&self.account, 16)).magenta(),
        self.render_endpoint(),
        (self.sent_bytes as f64) / 1024.0,
        (self.recv_bytes as f64) / 1024.0,
        format_usage(&self.usage),
        latency_s,
        attempts_part,
      )
    } else {
      let err = error.unwrap_or("failed");
      let status_part = match final_status {
        Some(s) => format!(" {}", style_status(s)),
        None => String::new(),
      };
      format!(
        "[{}] {}{} {} {} {} {} sent={:.1}kB recv={:.1}kB latency={:.1}s{} error={}",
        style(&id_short).dim(),
        style("✗").red().bold(),
        status_part,
        style(&self.provider).blue(),
        self.render_model(),
        style(truncate(&self.account, 16)).magenta(),
        self.render_endpoint(),
        (self.sent_bytes as f64) / 1024.0,
        (self.recv_bytes as f64) / 1024.0,
        latency_s,
        attempts_part,
        style(truncate(err, 80)).red(),
      )
    }
  }

  fn render_interrupted(&self, request_id: &str) -> String {
    let id_short = Self::id_short(request_id);
    let elapsed = self.started.elapsed().as_secs_f64();
    let model_part = if self.model.is_empty() {
      String::new()
    } else {
      format!(" {}", self.render_model())
    };
    let account_part = if self.account.is_empty() {
      String::new()
    } else {
      format!(" {}", style(truncate(&self.account, 16)).magenta())
    };
    format!(
      "[{}] {}{}{} {} sent={:.1}kB recv={:.1}kB elapsed={:.1}s",
      style(&id_short).dim(),
      style("⚠ interrupted").yellow().bold(),
      model_part,
      account_part,
      self.render_endpoint(),
      (self.sent_bytes as f64) / 1024.0,
      (self.recv_bytes as f64) / 1024.0,
      elapsed,
    )
  }

  fn render_waiting_for_usage(&self, request_id: &str) -> String {
    format!(
      "{} {}",
      self.render_in_flight(request_id),
      style("waiting for usage...").yellow()
    )
  }
}

struct BarState {
  bar: ProgressBar,
  request: RequestState,
}

struct PendingCompletion {
  bar: ProgressBar,
  request: RequestState,
  success: bool,
  attempts: u32,
  done: bool,
}

fn truncate(s: &str, max: usize) -> &str {
  if s.len() <= max {
    s
  } else {
    &s[..max]
  }
}

fn file_label(path: &Path) -> String {
  path
    .file_name()
    .and_then(|v| v.to_str())
    .unwrap_or_else(|| path.to_str().unwrap_or("unknown"))
    .to_string()
}

fn style_status(status: u16) -> console::StyledObject<u16> {
  match status {
    200..=299 => style(status).green(),
    300..=399 => style(status).cyan(),
    400..=499 => style(status).yellow(),
    500..=599 => style(status).red(),
    _ => style(status),
  }
}

/// Format a [`Usage`] for the success line: ` in=N out=M cache=K reason=R`,
/// omitting any field whose value is None or 0. Returns an empty string if no
/// fields are set; otherwise the result starts with a leading space.
fn format_usage(u: &Usage) -> String {
  let mut parts = Vec::with_capacity(4);
  if let Some(v) = u.input_tokens {
    if v > 0 {
      parts.push(format!("in={v}"));
    }
  }
  if let Some(v) = u.output_tokens {
    if v > 0 {
      parts.push(format!("out={v}"));
    }
  }
  if let Some(v) = u.total_tokens {
    if v > 0 {
      parts.push(format!("total={v}"));
    }
  }
  if let Some(v) = u.details.cache_read {
    if v > 0 {
      parts.push(format!("cache_read={v}"));
    }
  }
  if let Some(v) = u.details.cache_write {
    if v > 0 {
      parts.push(format!("cache_write={v}"));
    }
  }
  if let Some(v) = u.details.reasoning {
    if v > 0 {
      parts.push(format!("reason={v}"));
    }
  }
  if parts.is_empty() {
    String::new()
  } else {
    format!(" {}", parts.join(" "))
  }
}

// The worker control loop updates this display even when request events are idle.
// Keep a weak reference so shutdown does not retain or resurrect a finished bar.
static LIVE_FOOTER: OnceLock<Mutex<Weak<Mutex<SessionFooter>>>> = OnceLock::new();

struct SessionFooter {
  bar: ProgressBar,
  in_flight: u64,
  completed: u64,
  errors: u64,
  ab_test: Option<AbTestStatus>,
}

impl SessionFooter {
  fn message(&self) -> String {
    let errors_part = if self.errors > 0 {
      format!("errors={}", style(self.errors).red())
    } else {
      format!("errors={}", self.errors)
    };
    let ab_test_part = self
      .ab_test
      .as_ref()
      .map(|status| {
        let elapsed = status.elapsed_seconds.min(status.duration_seconds) / 60;
        let duration = status.duration_seconds / 60;
        format!(
          " ab={}%/{}% elapsed={:02}:{:02}/{:02}:{:02}",
          status.traffic_percent,
          100u32.saturating_sub(status.traffic_percent),
          elapsed / 60,
          elapsed % 60,
          duration / 60,
          duration % 60
        )
      })
      .unwrap_or_default();
    format!(
      "─── in-flight={} completed={} {}{} ───",
      style(self.in_flight).bold(),
      style(self.completed).green(),
      errors_part,
      ab_test_part
    )
  }

  fn refresh(&self) {
    if self.bar.is_finished() {
      return;
    }
    self.bar.set_message(self.message());
    self.bar.tick();
  }
}

pub fn set_ab_test_status(status: Option<AbTestStatus>) {
  let footer = LIVE_FOOTER.get().and_then(|slot| slot.lock().ok()?.upgrade());
  if let Some(footer) = footer {
    if let Ok(mut footer) = footer.lock() {
      footer.ab_test = status;
      footer.refresh();
    }
  }
}

pub struct ProgressEventHandler {
  multi: MultiProgress,
  bars: HashMap<String, BarState>,
  pending: HashMap<String, Arc<Mutex<PendingCompletion>>>,
  /// Style for in-flight bars (spinner + dynamic message).
  style: ProgressStyle,
  /// Persistent footer bar (last line) showing session counters.
  footer: ProgressBar,
  footer_display: Arc<Mutex<SessionFooter>>,
  /// Session counters.
  in_flight: u64,
  completed: u64,
  errors: u64,
}

impl ProgressEventHandler {
  pub fn new() -> Self {
    let multi = multi().clone();
    let style = ProgressStyle::with_template("{spinner:.cyan} {msg}")
      .unwrap_or_else(|_| ProgressStyle::default_spinner())
      .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ");

    // Footer bar: a borderless line that never finishes, always at the bottom.
    let footer = multi.add(ProgressBar::new_spinner());
    let footer_style = ProgressStyle::with_template("{msg}").unwrap_or_else(|_| ProgressStyle::default_spinner());
    footer.set_style(footer_style);
    let footer_display = Arc::new(Mutex::new(SessionFooter {
      bar: footer.clone(),
      in_flight: 0,
      completed: 0,
      errors: 0,
      ab_test: None,
    }));
    if let Ok(mut slot) = LIVE_FOOTER.get_or_init(|| Mutex::new(Weak::new())).lock() {
      *slot = Arc::downgrade(&footer_display);
    }

    let handler = Self {
      multi,
      bars: HashMap::new(),
      pending: HashMap::new(),
      style,
      footer,
      footer_display,
      in_flight: 0,
      completed: 0,
      errors: 0,
    };
    handler.refresh_footer();
    handler
  }

  fn refresh(&mut self, request_id: &str) {
    if let Some(state) = self.bars.get(request_id) {
      let msg = state.request.render_in_flight(request_id);
      state.bar.set_message(msg);
      state.bar.tick();
    }
  }

  fn prune_finished_pending(&mut self) {
    self.pending.retain(|_, pending| {
      let Ok(pending) = pending.lock() else {
        return false;
      };
      !pending.done
    });
  }

  fn finalize_pending(multi: &MultiProgress, request_id: &str, pending: &Arc<Mutex<PendingCompletion>>) {
    let Ok(mut pending) = pending.lock() else {
      return;
    };
    if pending.done {
      return;
    }
    let latency_ms = pending.request.started.elapsed().as_millis() as u64;
    let final_msg = pending.request.render_completed(
      request_id,
      pending.success,
      pending.attempts,
      pending.request.final_status,
      latency_ms,
      pending.request.error.as_deref(),
    );
    pending.bar.disable_steady_tick();
    let _ = multi.println(final_msg);
    pending.bar.finish_and_clear();
    pending.done = true;
  }

  fn queue_pending_completion(&mut self, request_id: String, state: BarState, attempts: u32) {
    state
      .bar
      .set_message(state.request.render_waiting_for_usage(&request_id));
    let pending = Arc::new(Mutex::new(PendingCompletion {
      bar: state.bar,
      request: state.request,
      success: true,
      attempts,
      done: false,
    }));
    self.pending.insert(request_id.clone(), pending.clone());

    let multi = self.multi.clone();
    std::thread::spawn(move || {
      std::thread::sleep(USAGE_GRACE_PERIOD);
      Self::finalize_pending(&multi, &request_id, &pending);
    });
  }

  fn finalize_pending_if_waiting(&mut self, request_id: &str) {
    if let Some(pending) = self.pending.remove(request_id) {
      Self::finalize_pending(&self.multi, request_id, &pending);
    }
  }

  fn refresh_footer(&self) {
    if let Ok(mut footer) = self.footer_display.lock() {
      footer.in_flight = self.in_flight;
      footer.completed = self.completed;
      footer.errors = self.errors;
      footer.refresh();
    }
  }
}

impl Default for ProgressEventHandler {
  fn default() -> Self {
    Self::new()
  }
}

impl ProgressEventHandler {
  fn handle_request(&mut self, event: &RequestEvent) {
    self.prune_finished_pending();
    let request_id = event.request_id.as_str();
    let composite_id = if event.attempt == 0 {
      request_id.to_string()
    } else {
      format!("{}:{}", request_id, event.attempt)
    };
    match &event.payload {
      RequestEventPayload::Stage(StageEvent::Started { request_endpoint }) => {
        let bar = self.multi.insert_before(&self.footer, ProgressBar::new_spinner());
        bar.set_style(self.style.clone());
        bar.enable_steady_tick(std::time::Duration::from_millis(120));
        let state = BarState {
          bar,
          request: RequestState::new(request_endpoint.as_str().to_string()),
        };
        self.bars.insert(composite_id.clone(), state);
        self.in_flight = self.in_flight.saturating_add(1);
        self.refresh(&composite_id);
        self.refresh_footer();
      }
      RequestEventPayload::Stage(StageEvent::Extract(s)) => {
        if let Some(state) = self.bars.get_mut(&composite_id) {
          state.request.model = s.model.to_string();
          state.request.request_classification = s.request_classification;
        }
        self.refresh(&composite_id);
      }
      RequestEventPayload::Stage(StageEvent::Resolve(s)) => {
        if let Some(state) = self.bars.get_mut(&composite_id) {
          state.request.provider = s.provider_id.to_string();
          state.request.account = s.account_id.to_string();
        }
        self.refresh(&composite_id);
      }
      RequestEventPayload::Stage(StageEvent::ConvertRequest(s)) => {
        if let Some(state) = self.bars.get_mut(&composite_id) {
          state.request.update_priority(s);
        }
        self.refresh(&composite_id);
      }
      RequestEventPayload::Record(RecordEvent::UpstreamReq { body, .. }) => {
        if let Some(state) = self.bars.get_mut(&composite_id) {
          state.request.sent_bytes = body.len() as u64;
        }
        self.refresh(&composite_id);
      }
      RequestEventPayload::Record(RecordEvent::Usage(usage)) => {
        if let Some(state) = self.bars.get_mut(&composite_id) {
          state.request.merge_usage(usage);
          self.refresh(&composite_id);
          return;
        }
        if let Some(pending) = self.pending.get(&composite_id).cloned() {
          if let Ok(mut pending) = pending.lock() {
            pending.request.merge_usage(usage);
          }
          self.finalize_pending_if_waiting(&composite_id);
        }
      }
      RequestEventPayload::Record(RecordEvent::UpstreamResp { status, .. }) => {
        if let Some(state) = self.bars.get_mut(&composite_id) {
          state.request.final_status = Some(*status);
        } else if let Some(pending) = self.pending.get(&composite_id).cloned() {
          if let Ok(mut pending) = pending.lock() {
            pending.request.final_status = Some(*status);
          }
        }
      }
      RequestEventPayload::Stage(StageEvent::Error { message, .. }) => {
        if let Some(state) = self.bars.get_mut(&composite_id) {
          state.request.error = Some(message.to_string());
        } else if let Some(pending) = self.pending.get(&composite_id).cloned() {
          if let Ok(mut pending) = pending.lock() {
            pending.request.error = Some(message.to_string());
          }
        }
      }
      RequestEventPayload::Stage(StageEvent::Completed { success, attempts }) => {
        if let Some(state) = self.bars.remove(&composite_id) {
          if *success {
            self.queue_pending_completion(composite_id.clone(), state, *attempts);
          } else {
            let latency_ms = state.request.started.elapsed().as_millis() as u64;
            let final_msg = state.request.render_completed(
              &composite_id,
              *success,
              *attempts,
              state.request.final_status,
              latency_ms,
              state.request.error.as_deref(),
            );
            state.bar.disable_steady_tick();
            let _ = self.multi.println(final_msg);
            state.bar.finish_and_clear();
          }
        }
        self.in_flight = self.in_flight.saturating_sub(1);
        self.completed = self.completed.saturating_add(1);
        if !success {
          self.errors = self.errors.saturating_add(1);
        }
        self.refresh_footer();
      }
      _ => {}
    }
  }
}

impl EventHandler for ProgressEventHandler {
  fn handle(&mut self, event: &Event) {
    self.prune_finished_pending();
    match event {
      Event::Requests(e) => self.handle_request(e),
      Event::StreamProgress {
        request_id,
        bytes_streamed,
        usage,
        ..
      } => {
        if let Some(state) = self.bars.get_mut(request_id) {
          state.request.recv_bytes = *bytes_streamed;
          // Merge any non-None usage fields seen so far.
          state.request.merge_usage(usage);
        }
        self.refresh(request_id);
      }
      _ => {}
    }
  }

  fn flush(&mut self) {
    // For each in-flight straggler: emit a one-line interrupted summary
    // via multi.println (suspends bars during emit) then clear the bar
    // so the live region shrinks. The println line lands in scrollback.
    let stragglers: Vec<(String, BarState)> = self.bars.drain().collect();
    for (request_id, state) in stragglers {
      let line = state.request.render_interrupted(&request_id);
      let _ = self.multi.println(line);
      state.bar.disable_steady_tick();
      state.bar.finish_and_clear();
    }
    let waiting: Vec<(String, Arc<Mutex<PendingCompletion>>)> = self.pending.drain().collect();
    for (request_id, pending) in waiting {
      Self::finalize_pending(&self.multi, &request_id, &pending);
    }

    // Footer: print final session summary, then clear the live footer bar.
    let interrupted_part = if self.in_flight > 0 {
      format!(" interrupted={}", style(self.in_flight).yellow())
    } else {
      String::new()
    };
    let errors_part = if self.errors > 0 {
      format!("errors={}", style(self.errors).red())
    } else {
      format!("errors={}", self.errors)
    };
    let summary = format!(
      "─── session ended: completed={} {}{} ───",
      style(self.completed).green(),
      errors_part,
      interrupted_part,
    );
    let _ = self.multi.println(summary);
    // Stop status updates from redrawing this footer during persistence cleanup.
    if let Ok(footer) = self.footer_display.lock() {
      footer.bar.finish_and_clear();
    }
  }
}

pub struct ProgressLogEventHandler {
  writer: BufWriter<File>,
  requests: HashMap<String, RequestState>,
  in_flight: u64,
  completed: u64,
  errors: u64,
  write_failed: bool,
}

struct ArchiveBarState {
  bar: ProgressBar,
  started: Instant,
  path: PathBuf,
  archive: PathBuf,
  total_bytes: u64,
}

pub struct ArchiveProgressEventHandler {
  multi: MultiProgress,
  bars: HashMap<String, ArchiveBarState>,
  style: ProgressStyle,
}

impl ArchiveProgressEventHandler {
  pub fn new() -> Self {
    let multi = multi().clone();
    let style = ProgressStyle::with_template("{spinner:.yellow} {msg}")
      .unwrap_or_else(|_| ProgressStyle::default_spinner())
      .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ");
    Self {
      multi,
      bars: HashMap::new(),
      style,
    }
  }

  fn refresh(&self, id: &str, bytes_read: u64, total_bytes: u64) {
    if let Some(state) = self.bars.get(id) {
      let percent = if total_bytes > 0 {
        (bytes_read as f64 * 100.0) / total_bytes as f64
      } else {
        100.0
      };
      let elapsed = state.started.elapsed().as_secs_f64();
      let speed_kbs = if elapsed > 0.05 {
        (bytes_read as f64) / 1024.0 / elapsed
      } else {
        0.0
      };
      state.bar.set_message(format!(
        "archive {} {:.1}% {:.1}/{:.1}MB {:.1}kB/s -> {}",
        style(file_label(&state.path)).yellow(),
        percent.min(100.0),
        bytes_read as f64 / 1024.0 / 1024.0,
        state.total_bytes as f64 / 1024.0 / 1024.0,
        speed_kbs,
        style(file_label(&state.archive)).dim(),
      ));
      state.bar.tick();
    }
  }
}

impl Default for ArchiveProgressEventHandler {
  fn default() -> Self {
    Self::new()
  }
}

impl ArchiveEventHandler for ArchiveProgressEventHandler {
  fn handle(&mut self, event: &ArchiveEvent) {
    match event {
      ArchiveEvent::ScanStarted { dir } => {
        tracing::debug!(path = %dir.display(), "request db archival progress scan started");
      }
      ArchiveEvent::FileStarted {
        id,
        path,
        archive,
        total_bytes,
      } => {
        let bar = self.multi.add(ProgressBar::new_spinner());
        bar.set_style(self.style.clone());
        bar.enable_steady_tick(std::time::Duration::from_millis(120));
        self.bars.insert(
          id.clone(),
          ArchiveBarState {
            bar,
            started: Instant::now(),
            path: path.clone(),
            archive: archive.clone(),
            total_bytes: *total_bytes,
          },
        );
        self.refresh(id, 0, *total_bytes);
      }
      ArchiveEvent::FileProgress {
        id,
        bytes_read,
        total_bytes,
      } => self.refresh(id, *bytes_read, *total_bytes),
      ArchiveEvent::FileCompleted {
        id,
        path,
        archive,
        bytes_in,
        bytes_out,
      } => {
        if let Some(state) = self.bars.remove(id) {
          state.bar.disable_steady_tick();
          state.bar.finish_and_clear();
        }
        let ratio = if *bytes_in > 0 {
          (*bytes_out as f64 * 100.0) / *bytes_in as f64
        } else {
          0.0
        };
        let _ = self.multi.println(format!(
          "{} archived {} -> {} {:.1}MB to {:.1}MB ({:.1}%)",
          style("✓").green().bold(),
          style(file_label(path)).yellow(),
          style(file_label(archive)).dim(),
          *bytes_in as f64 / 1024.0 / 1024.0,
          *bytes_out as f64 / 1024.0 / 1024.0,
          ratio,
        ));
      }
      ArchiveEvent::FileSkipped { path, archive } => {
        tracing::debug!(path = %path.display(), archive = %archive.display(), "request db archive already exists");
      }
      ArchiveEvent::FileFailed {
        id,
        path,
        archive,
        error,
      } => {
        if let Some(state) = self.bars.remove(id) {
          state.bar.disable_steady_tick();
          state.bar.finish_and_clear();
        }
        let _ = self.multi.println(format!(
          "{} archive {} -> {} failed: {}",
          style("✗").red().bold(),
          style(file_label(path)).yellow(),
          style(file_label(archive)).dim(),
          style(truncate(error, 120)).red(),
        ));
      }
      ArchiveEvent::ScanCompleted { dir, stats } => {
        tracing::debug!(path = %dir.display(), archived = stats.archived, skipped_existing = stats.skipped_existing, failed = stats.failed, "request db archival progress scan completed");
      }
    }
  }

  fn flush(&mut self) {
    let bars: Vec<ArchiveBarState> = self.bars.drain().map(|(_, state)| state).collect();
    for state in bars {
      let _ = self.multi.println(format!(
        "{} archive {} interrupted",
        style("⚠").yellow().bold(),
        style(file_label(&state.path)).yellow(),
      ));
      state.bar.disable_steady_tick();
      state.bar.finish_and_clear();
    }
  }
}

impl ProgressLogEventHandler {
  pub fn new(log_dir: &Path) -> io::Result<Self> {
    std::fs::create_dir_all(log_dir)?;
    let file = OpenOptions::new()
      .create(true)
      .append(true)
      .open(progress_log_path(log_dir))?;
    Ok(Self {
      writer: BufWriter::new(file),
      requests: HashMap::new(),
      in_flight: 0,
      completed: 0,
      errors: 0,
      write_failed: false,
    })
  }

  fn write_line(&mut self, line: &str) {
    if self.write_failed {
      return;
    }
    if let Err(e) = writeln!(self.writer, "{line}").and_then(|_| self.writer.flush()) {
      self.write_failed = true;
      tracing::warn!(error = %e, "failed to write progress log");
    }
  }

  fn summary(&self) -> String {
    let interrupted_part = if self.in_flight > 0 {
      format!(" interrupted={}", style(self.in_flight).yellow())
    } else {
      String::new()
    };
    let errors_part = if self.errors > 0 {
      format!("errors={}", style(self.errors).red())
    } else {
      format!("errors={}", self.errors)
    };
    format!(
      "─── session ended: completed={} {}{} ───",
      style(self.completed).green(),
      errors_part,
      interrupted_part,
    )
  }
}

impl ProgressLogEventHandler {
  fn handle_request(&mut self, event: &RequestEvent) {
    let request_id = event.request_id.as_str();
    let composite_id = if event.attempt == 0 {
      request_id.to_string()
    } else {
      format!("{}:{}", request_id, event.attempt)
    };
    match &event.payload {
      RequestEventPayload::Stage(StageEvent::Started { request_endpoint }) => {
        self
          .requests
          .insert(composite_id, RequestState::new(request_endpoint.as_str().to_string()));
        self.in_flight = self.in_flight.saturating_add(1);
      }
      RequestEventPayload::Stage(StageEvent::Extract(s)) => {
        if let Some(state) = self.requests.get_mut(&composite_id) {
          state.model = s.model.to_string();
          state.request_classification = s.request_classification;
        }
      }
      RequestEventPayload::Stage(StageEvent::Resolve(s)) => {
        if let Some(state) = self.requests.get_mut(&composite_id) {
          state.provider = s.provider_id.to_string();
          state.account = s.account_id.to_string();
        }
      }
      RequestEventPayload::Stage(StageEvent::ConvertRequest(s)) => {
        if let Some(state) = self.requests.get_mut(&composite_id) {
          state.update_priority(s);
        }
      }
      RequestEventPayload::Record(RecordEvent::UpstreamReq { body, .. }) => {
        if let Some(state) = self.requests.get_mut(&composite_id) {
          state.sent_bytes = body.len() as u64;
        }
      }
      RequestEventPayload::Record(RecordEvent::Usage(usage)) => {
        if let Some(state) = self.requests.get_mut(&composite_id) {
          state.merge_usage(usage);
        }
      }
      RequestEventPayload::Record(RecordEvent::UpstreamResp { status, .. }) => {
        if let Some(state) = self.requests.get_mut(&composite_id) {
          state.final_status = Some(*status);
        }
      }
      RequestEventPayload::Stage(StageEvent::Error { message, .. }) => {
        if let Some(state) = self.requests.get_mut(&composite_id) {
          state.error = Some(message.to_string());
        }
      }
      RequestEventPayload::Stage(StageEvent::Completed { success, .. }) => {
        if let Some(state) = self.requests.remove(&composite_id) {
          let latency_ms = state.started.elapsed().as_millis() as u64;
          let line = state.render_completed(
            &composite_id,
            *success,
            1,
            state.final_status,
            latency_ms,
            state.error.as_deref(),
          );
          self.write_line(&line);
        }
        self.in_flight = self.in_flight.saturating_sub(1);
        self.completed = self.completed.saturating_add(1);
        if !success {
          self.errors = self.errors.saturating_add(1);
        }
      }
      _ => {}
    }
  }
}

impl EventHandler for ProgressLogEventHandler {
  fn handle(&mut self, event: &Event) {
    match event {
      Event::Requests(e) => self.handle_request(e),
      Event::StreamProgress {
        request_id,
        bytes_streamed,
        usage,
        ..
      } => {
        if let Some(state) = self.requests.get_mut(request_id) {
          state.recv_bytes = *bytes_streamed;
          state.merge_usage(usage);
        }
      }
      _ => {}
    }
  }

  fn flush(&mut self) {
    let stragglers: Vec<(String, RequestState)> = self.requests.drain().collect();
    for (request_id, state) in stragglers {
      let line = state.render_interrupted(&request_id);
      self.write_line(&line);
    }
    let summary = self.summary();
    self.write_line(&summary);
  }
}

fn progress_log_path(log_dir: &Path) -> PathBuf {
  let date = OffsetDateTime::now_utc()
    .format(format_description!("[year]-[month]-[day]"))
    .unwrap_or_else(|_| "unknown-date".to_string());
  log_dir.join(format!("tokn-router-progress.log.{date}"))
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Bytes;
  use tokn_core::db::UsageDetails;
  use tokn_core::request_classification::{RequestClassificationSource, RequestPurpose};
  use tokn_core::request_event::{ExtractedSummary, RequestEndpoint, RequestEventPayload, StageEvent};
  use tokn_requests::pipeline::stages::ExtractStage;
  use tokn_requests::stages::PassthroughExtract;
  use tokn_requests::{EventBus, PipelineCtx, RawInbound, RunConfig};

  #[test]
  fn ab_footer_updates_while_idle_and_clears_without_changing_counters() {
    let mut footer = SessionFooter {
      bar: ProgressBar::hidden(),
      in_flight: 2,
      completed: 255,
      errors: 3,
      ab_test: None,
    };
    footer.refresh();
    let counters = console::strip_ansi_codes(&footer.bar.message()).into_owned();
    assert_eq!(counters, "─── in-flight=2 completed=255 errors=3 ───");
    footer.ab_test = Some(AbTestStatus {
      baseline_worker_id: "old".into(),
      worker_id: "new".into(),
      elapsed_seconds: 60,
      duration_seconds: 24 * 3600,
      traffic_percent: 10,
    });
    footer.refresh();
    assert_eq!(
      console::strip_ansi_codes(&footer.bar.message()),
      "─── in-flight=2 completed=255 errors=3 ab=10%/90% elapsed=00:01/24:00 ───"
    );
    let status = footer.ab_test.as_mut().unwrap();
    status.elapsed_seconds = 12 * 3600;
    status.traffic_percent = 50;
    footer.refresh();
    assert!(footer.bar.message().contains("ab=50%/50% elapsed=12:00/24:00"));
    footer.ab_test = None;
    footer.refresh();
    assert_eq!(console::strip_ansi_codes(&footer.bar.message()), counters);
    footer.bar.finish_and_clear();
    footer.ab_test = Some(AbTestStatus {
      baseline_worker_id: "old".into(),
      worker_id: "new".into(),
      elapsed_seconds: 0,
      duration_seconds: 24 * 3600,
      traffic_percent: 10,
    });
    footer.refresh();
    assert_eq!(
      console::strip_ansi_codes(&footer.bar.message()),
      counters,
      "finished footer must not redraw"
    );
  }

  fn req(payload: RequestEventPayload) -> RequestEvent {
    req_with_id("req-1", payload)
  }

  fn req_with_id(request_id: &str, payload: RequestEventPayload) -> RequestEvent {
    RequestEvent {
      request_id: request_id.into(),
      attempt: 0,
      ts: 0,
      payload,
    }
  }

  fn extract_event(classification: Option<RequestClassification>) -> RequestEventPayload {
    RequestEventPayload::Stage(StageEvent::Extract(ExtractedSummary {
      agent_id: None,
      request_classification: classification,
      model: "gpt-test".into(),
      stream: false,
      session_id: None,
      project_id: None,
      initiator: None,
      header_initiator: None,
      route_mode_hint: None,
      headers: tokn_headers::HeaderMap::new(),
      raw_body: Bytes::new(),
      decoded_body: Bytes::new(),
      body_json: Arc::new(serde_json::Value::Null),
    }))
  }

  fn compaction() -> RequestClassification {
    RequestClassification {
      purpose: RequestPurpose::Compaction,
      source: RequestClassificationSource::CodexPrompt,
    }
  }

  #[test]
  fn progress_handlers_follow_outbound_priority_without_matching_history() {
    let dir = std::env::temp_dir().join(format!("tokn-router-progress-test-{}", uuid::Uuid::new_v4()));
    let mut tty = ProgressEventHandler::new();
    let mut log = ProgressLogEventHandler::new(&dir).unwrap();
    let cases: &[(&[u8], bool)] = &[
      (br#"{"service_tier":"priority","input":"hello"}"#, true),
      (br#"{"service_tier":"flex"}"#, false),
      (br#"{"service_tier":"default"}"#, false),
      (br#"{"input":[{"service_tier":"priority"}]}"#, false),
      (br#"{"service_tier":true}"#, false),
      (br#"{"service_tier":"priority""#, false),
      (br#"{}"#, false),
    ];
    for &(body, priority) in cases {
      for parsed in [false, true] {
        let upstream_body = if parsed {
          serde_json::from_slice(body).unwrap_or(serde_json::Value::Null)
        } else {
          serde_json::Value::Null
        };
        for payload in [
          RequestEventPayload::Stage(StageEvent::Started {
            request_endpoint: RequestEndpoint::custom("responses"),
          }),
          extract_event(None),
          RequestEventPayload::Stage(StageEvent::ConvertRequest(ConvertedRequestSummary {
            upstream_body: Arc::new(upstream_body),
            upstream_wire_body: Bytes::copy_from_slice(body),
            debug_outbound_body: Bytes::copy_from_slice(body),
            content_encoding: None,
          })),
        ] {
          let event = req(payload);
          tty.handle_request(&event);
          log.handle_request(&event);
        }
        let state = &tty.bars.get("req-1").unwrap().request;
        assert_eq!(state.priority, priority);
        assert_eq!(log.requests.get("req-1").unwrap().priority, priority);
        assert_eq!(tty.bars.get("req-1").unwrap().bar.message().contains("⚡"), priority);
        for rendered in [
          state.render_in_flight("req-1"),
          state.render_completed("req-1", true, 1, Some(200), 0, None),
          state.render_completed("req-1", false, 1, Some(500), 0, Some("failed")),
          state.render_interrupted("req-1"),
          state.render_waiting_for_usage("req-1"),
        ] {
          assert_eq!(rendered.contains("⚡"), priority);
          if priority {
            assert!(console::strip_ansi_codes(&rendered).contains("gpt-test ⚡"));
          }
        }
        let complete = req(RequestEventPayload::Stage(StageEvent::Completed {
          success: false,
          attempts: 1,
        }));
        tty.handle_request(&complete);
        log.handle_request(&complete);
        let content = std::fs::read_to_string(progress_log_path(&dir)).unwrap();
        assert_eq!(content.lines().last().unwrap().contains("⚡"), priority);
      }
    }
    drop(log);
    std::fs::remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn tty_extract_event_labels_all_request_states() {
    let mut handler = ProgressEventHandler::new();
    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Started {
      request_endpoint: RequestEndpoint::custom("responses"),
    })));
    handler.handle_request(&req(extract_event(Some(compaction()))));

    let state = &handler.bars.get("req-1").unwrap().request;
    assert_eq!(state.request_classification, Some(compaction()));
    assert!(state.render_in_flight("req-1").contains("responses [compaction] sent="));
    assert!(state
      .render_completed("req-1", true, 1, Some(200), 0, None)
      .contains("responses [compaction] sent="));
    assert!(state
      .render_completed("req-1", false, 1, Some(500), 0, Some("failed"))
      .contains("responses [compaction] sent="));
    assert!(state
      .render_interrupted("req-1")
      .contains("responses [compaction] sent="));

    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Completed {
      success: true,
      attempts: 1,
    })));
    let pending = handler.pending.get("req-1").unwrap().lock().unwrap();
    assert!(pending
      .request
      .render_waiting_for_usage("req-1")
      .contains("responses [compaction] sent="));
    drop(pending);
    handler.handle_request(&req(RequestEventPayload::Record(RecordEvent::Usage(Usage::default()))));

    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Started {
      request_endpoint: RequestEndpoint::custom("responses"),
    })));
    handler.handle_request(&req(extract_event(None)));
    assert!(!handler
      .bars
      .get("req-1")
      .unwrap()
      .request
      .render_in_flight("req-1")
      .contains("[compaction]"));
  }

  #[test]
  fn progress_log_extract_event_labels_completed_and_failed_rows_only_when_known() {
    let dir = std::env::temp_dir().join(format!("tokn-router-progress-test-{}", uuid::Uuid::new_v4()));
    let mut handler = ProgressLogEventHandler::new(&dir).unwrap();
    for (id, classification, success) in [
      ("req-success", Some(compaction()), true),
      ("req-failure", Some(compaction()), false),
      ("req-ordinary", None, true),
    ] {
      handler.handle_request(&req_with_id(
        id,
        RequestEventPayload::Stage(StageEvent::Started {
          request_endpoint: RequestEndpoint::custom("responses"),
        }),
      ));
      handler.handle_request(&req_with_id(id, extract_event(classification)));
      handler.handle_request(&req_with_id(
        id,
        RequestEventPayload::Stage(StageEvent::Completed { success, attempts: 1 }),
      ));
    }
    let content = std::fs::read_to_string(progress_log_path(&dir)).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert!(lines[0].contains("responses [compaction] sent="));
    assert!(lines[1].contains("responses [compaction] sent="));
    assert!(lines[1].contains("error=failed"));
    assert!(lines[2].contains("responses sent="));
    assert!(!lines[2].contains("[compaction]"));

    drop(handler);
    std::fs::remove_dir_all(&dir).unwrap();
  }

  #[tokio::test]
  async fn passthrough_compaction_trigger_reaches_progress_handlers_on_retry_attempt() {
    use tokn_requests::pipeline::stages::{ConvertRequestStage, ResolveStage};
    use tokn_requests::stages::resolve::proxy::keys;
    use tokn_requests::stages::{PassthroughConvertRequest, ProxyResolve};
    use tokn_requests::utils::codec::{encode_body_bytes, ContentEncodingKind};
    let dir = std::env::temp_dir().join(format!("tokn-router-progress-test-{}", uuid::Uuid::new_v4()));
    let mut tty = ProgressEventHandler::new();
    let mut log = ProgressLogEventHandler::new(&dir).unwrap();
    let bus = Arc::new(EventBus::new(8));
    let mut events = bus.subscribe();
    let endpoint = RequestEndpoint::custom("/backend-api/codex/responses");
    let ctx = PipelineCtx::new_with_attempt_and_config(
      "req-compaction-trigger",
      1,
      endpoint.clone(),
      bus,
      Arc::new(
        RunConfig::builder()
          .with_str(keys::HOST, "chatgpt.com")
          .with_str(keys::PROVIDER_ID, "codex")
          .with_str(keys::PATH, endpoint.as_str())
          .build(),
      ),
    );
    let body = Bytes::from_static(
      br#"{"model":"gpt-test","stream":true,"input":[{"role":"user","content":"Continue the task."},{"type":"compaction_trigger"}]}"#,
    );
    let raw_body = encode_body_bytes(&body, Some(ContentEncodingKind::Zstd)).unwrap();
    let mut headers = tokn_headers::HeaderMap::new();
    headers.insert("content-encoding", "zstd");
    ctx.emit_stage(StageEvent::Started {
      request_endpoint: endpoint.clone(),
    });
    let extracted = PassthroughExtract
      .extract(
        &ctx,
        RawInbound {
          request_endpoint: endpoint,
          headers,
          raw_body: raw_body.clone(),
          decoded_body: body.clone(),
          body_json: serde_json::Value::Null,
          request_id: Some(ctx.request_id.clone()),
        },
      )
      .await
      .unwrap();
    assert_eq!(extracted.raw_body, raw_body);
    assert_eq!(*extracted.body_json, serde_json::Value::Null);
    ctx.emit_stage(StageEvent::Extract(ExtractedSummary::from(&extracted)));
    let resolved = ProxyResolve.resolve(&ctx, &extracted).await.unwrap();
    let converted = PassthroughConvertRequest
      .convert_request(&ctx, &extracted, &resolved)
      .await
      .unwrap();
    assert_eq!(converted.upstream_body["service_tier"], "priority");
    assert_eq!(converted.content_encoding, Some(ContentEncodingKind::Zstd));
    ctx.emit_stage(StageEvent::ConvertRequest(ConvertedRequestSummary::from(&converted)));

    for _ in 0..3 {
      let event = events.try_recv().unwrap();
      tty.handle(event.as_ref());
      log.handle(event.as_ref());
    }
    let attempt_id = "req-compaction-trigger:1";
    let state = &tty.bars.get(attempt_id).unwrap().request;
    assert!(state.priority);
    assert!(console::strip_ansi_codes(&state.render_in_flight(attempt_id)).contains("gpt-test ⚡"));
    assert!(log.requests.get(attempt_id).unwrap().priority);
    assert_eq!(
      state.request_classification,
      Some(RequestClassification {
        purpose: RequestPurpose::Compaction,
        source: RequestClassificationSource::RequestField,
      })
    );
    assert!(state
      .render_in_flight(attempt_id)
      .contains("/backend-api/codex/responses [compaction] sent="));
    assert!(tty.bars.get(attempt_id).unwrap().bar.message().contains("[compaction]"));
    assert_eq!(
      log.requests.get(attempt_id).unwrap().request_classification,
      state.request_classification
    );

    ctx.emit_stage(StageEvent::Completed {
      success: false,
      attempts: 2,
    });
    let event = events.try_recv().unwrap();
    tty.handle(event.as_ref());
    log.handle(event.as_ref());
    let content = std::fs::read_to_string(progress_log_path(&dir)).unwrap();
    assert!(content.contains("/backend-api/codex/responses [compaction] sent="));
    assert!(content.contains("error=failed"));
    assert!(content.contains("⚡"));

    drop(log);
    std::fs::remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn tty_handler_tracks_sent_bytes_and_usage_from_records() {
    let mut handler = ProgressEventHandler::new();
    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Started {
      request_endpoint: tokn_core::request_event::RequestEndpoint::custom("responses"),
    })));
    handler.handle_request(&req(RequestEventPayload::Record(RecordEvent::UpstreamReq {
      method: "POST".into(),
      url: "https://example.test".into(),
      headers: tokn_headers::HeaderMap::new(),
      body: Bytes::from_static(b"123456"),
    })));
    handler.handle_request(&req(RequestEventPayload::Record(RecordEvent::Usage(Usage {
      input_tokens: Some(11),
      output_tokens: Some(13),
      total_tokens: Some(24),
      usage_type: None,
      details: UsageDetails {
        cache_read: Some(17),
        cache_write: Some(18),
        reasoning: Some(19),
      },
    }))));

    let state = handler.bars.get("req-1").expect("request state must exist");
    assert_eq!(state.request.sent_bytes, 6);
    assert_eq!(state.request.usage.input_tokens, Some(11));
    assert_eq!(state.request.usage.output_tokens, Some(13));
    assert_eq!(state.request.usage.total_tokens, Some(24));
    assert_eq!(state.request.usage.details.cache_read, Some(17));
    assert_eq!(state.request.usage.details.cache_write, Some(18));
    assert_eq!(state.request.usage.details.reasoning, Some(19));
  }

  #[test]
  fn tty_handler_waits_for_usage_then_finalizes() {
    let mut handler = ProgressEventHandler::new();
    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Started {
      request_endpoint: tokn_core::request_event::RequestEndpoint::custom("responses"),
    })));
    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Completed {
      success: true,
      attempts: 1,
    })));

    assert!(!handler.bars.contains_key("req-1"));
    assert!(handler.pending.contains_key("req-1"));

    handler.handle_request(&req(RequestEventPayload::Record(RecordEvent::Usage(Usage {
      input_tokens: Some(7),
      output_tokens: Some(9),
      total_tokens: None,
      usage_type: None,
      details: UsageDetails::default(),
    }))));

    assert!(!handler.pending.contains_key("req-1"));
  }

  #[test]
  fn log_handler_tracks_sent_bytes_and_usage_from_records() {
    let dir = std::env::temp_dir().join(format!("tokn-router-progress-test-{}", uuid::Uuid::new_v4()));
    let mut handler = ProgressLogEventHandler::new(&dir).unwrap();
    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Started {
      request_endpoint: tokn_core::request_event::RequestEndpoint::custom("responses"),
    })));
    handler.handle_request(&req(RequestEventPayload::Record(RecordEvent::UpstreamReq {
      method: "POST".into(),
      url: "https://example.test".into(),
      headers: tokn_headers::HeaderMap::new(),
      body: Bytes::from_static(b"123456789"),
    })));
    handler.handle_request(&req(RequestEventPayload::Record(RecordEvent::Usage(Usage {
      input_tokens: Some(3),
      output_tokens: Some(5),
      total_tokens: None,
      usage_type: None,
      details: UsageDetails::default(),
    }))));

    let state = handler.requests.get("req-1").expect("request state must exist");
    assert_eq!(state.sent_bytes, 9);
    assert_eq!(state.usage.input_tokens, Some(3));
    assert_eq!(state.usage.output_tokens, Some(5));

    drop(handler);
    std::fs::remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn tty_handler_tracks_status_and_error_for_completion() {
    let mut handler = ProgressEventHandler::new();
    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Started {
      request_endpoint: tokn_core::request_event::RequestEndpoint::custom("responses"),
    })));
    handler.handle_request(&req(RequestEventPayload::Record(RecordEvent::UpstreamResp {
      status: 502,
      headers: tokn_headers::HeaderMap::new(),
    })));
    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Error {
      stage: tokn_core::request_event::Stage::Send,
      message: "upstream 502: boom".into(),
      recoverable: true,
      stop: false,
    })));

    let state = handler.bars.get("req-1").expect("request state must exist");
    assert_eq!(state.request.final_status, Some(502));
    assert_eq!(state.request.error.as_deref(), Some("upstream 502: boom"));
  }

  #[test]
  fn log_handler_tracks_status_and_error_for_completion() {
    let dir = std::env::temp_dir().join(format!("tokn-router-progress-test-{}", uuid::Uuid::new_v4()));
    let mut handler = ProgressLogEventHandler::new(&dir).unwrap();
    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Started {
      request_endpoint: tokn_core::request_event::RequestEndpoint::custom("responses"),
    })));
    handler.handle_request(&req(RequestEventPayload::Record(RecordEvent::UpstreamResp {
      status: 429,
      headers: tokn_headers::HeaderMap::new(),
    })));
    handler.handle_request(&req(RequestEventPayload::Stage(StageEvent::Error {
      stage: tokn_core::request_event::Stage::Send,
      message: "upstream 429: rate limited".into(),
      recoverable: false,
      stop: false,
    })));

    let state = handler.requests.get("req-1").expect("request state must exist");
    assert_eq!(state.final_status, Some(429));
    assert_eq!(state.error.as_deref(), Some("upstream 429: rate limited"));

    drop(handler);
    std::fs::remove_dir_all(&dir).unwrap();
  }
}
