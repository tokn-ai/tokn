//! Live request assignment, readiness checks, and response-lifetime accounting.

use super::protocol::{WireContext, CONNECT_TIMEOUT, CONTEXT_HEADER, MAX_CONTEXT_BYTES, PROTOCOL_VERSION, READY_PATH};
use super::transport::{exchange, strip_connection_headers, strip_internal_headers};
use super::WorkerInfo;
use crate::dispatch::{DispatchContext, RequestDispatcher};
use crate::routing::{AbTestStatus, RoutingControl, RoutingReport, WorkerState, WorkerStatus};
use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::Request;
use axum::http::HeaderValue;
use axum::response::Response;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use http_body::{Frame, SizeHint};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};
use tokn_policy::GatewayPlan;

#[derive(Clone, Debug)]
pub struct WorkerEndpoint {
  pub worker_id: String,
  pub socket_path: PathBuf,
  pub weight: u32,
}

/// Deterministic weighted request assignment. A request is selected once and
/// never replayed on another worker after a transport failure.
pub struct WorkerPool {
  routing: parking_lot::RwLock<RoutingTable>,
  expected: parking_lot::RwLock<Option<WorkerInfo>>,
  update_lock: tokio::sync::Mutex<()>,
  next: AtomicU64,
}

struct RoutingTable {
  ab_test: Option<AbTest>,
  main_worker_id: Option<String>,
  workers: Vec<WorkerEndpoint>,
  stats: Vec<Arc<WorkerStats>>,
  weights: Vec<u32>,
  total_weight: u64,
  generation: u64,
}

const AB_TEST_DURATION: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Copy, PartialEq, Eq)]
enum RegistrationMode {
  Current,
  Candidate,
  AbTest,
}

struct AbTest {
  baseline_worker_id: String,
  worker_id: String,
  started: Instant,
}

impl AbTest {
  fn status(&self, now: Instant) -> AbTestStatus {
    let elapsed = now.saturating_duration_since(self.started).min(AB_TEST_DURATION);
    AbTestStatus {
      baseline_worker_id: self.baseline_worker_id.clone(),
      worker_id: self.worker_id.clone(),
      elapsed_seconds: elapsed.as_secs(),
      duration_seconds: AB_TEST_DURATION.as_secs(),
      traffic_percent: 10 + (elapsed.as_secs() * 80 / AB_TEST_DURATION.as_secs()) as u32,
    }
  }
}

impl RoutingTable {
  fn advance_ab_test(&mut self, now: Instant) {
    let Some(experiment) = &self.ab_test else {
      return;
    };
    let baseline = self
      .workers
      .iter()
      .position(|worker| worker.worker_id == experiment.baseline_worker_id);
    let candidate = self
      .workers
      .iter()
      .position(|worker| worker.worker_id == experiment.worker_id);
    let (Some(baseline), Some(candidate)) = (baseline, candidate) else {
      self.ab_test = None;
      return;
    };
    let status = experiment.status(now);
    if status.elapsed_seconds == status.duration_seconds {
      self.weights[baseline] = 0;
      self.weights[candidate] = 1;
      self.total_weight = 1;
      self.stats[baseline].keep_alive.store(false, Ordering::Release);
      self.stats[candidate].keep_alive.store(false, Ordering::Release);
      self.stats[baseline].changed.notify_waiters();
      self.ab_test = None;
      self.generation += 1;
      tracing::info!("A/B ramp complete; new worker receives all traffic and baseline drains");
      return;
    }
    let percent = status.traffic_percent;
    if self.weights[candidate] != percent {
      self.weights[candidate] = percent;
      self.weights[baseline] = 100 - percent;
      self.generation += 1;
    }
  }

  fn cancel_ab_test_for(&mut self, worker_id: &str) {
    if self
      .ab_test
      .as_ref()
      .is_some_and(|experiment| experiment.worker_id == worker_id || experiment.baseline_worker_id == worker_id)
    {
      self.ab_test = None;
    }
  }

  fn advance_main(&mut self) {
    let eligible = |index: usize| *self.stats[index].state.read() != WorkerState::Exiting;
    if self.workers.iter().enumerate().any(|(index, worker)| {
      self.main_worker_id.as_ref() == Some(&worker.worker_id) && eligible(index) && self.weights[index] > 0
    }) {
      return;
    }
    let replacement = (0..self.workers.len())
      .rev()
      .find(|&index| eligible(index) && self.weights[index] > 0)
      .or_else(|| (0..self.workers.len()).rev().find(|&index| eligible(index)));
    for stats in &self.stats {
      if *stats.state.read() == WorkerState::Current {
        *stats.state.write() = WorkerState::Stale;
        stats.changed.notify_waiters();
      }
    }
    self.main_worker_id = replacement.map(|index| {
      *self.stats[index].state.write() = WorkerState::Current;
      self.stats[index].changed.notify_waiters();
      if self.weights[index] == 0 {
        self.weights[index] = 1;
        self.total_weight += 1;
      }
      self.workers[index].worker_id.clone()
    });
  }
}

#[derive(Default)]
struct WorkerStats {
  state: parking_lot::RwLock<WorkerState>,
  keep_alive: std::sync::atomic::AtomicBool,
  changed: tokio::sync::Notify,
  version: parking_lot::RwLock<Option<String>>,
  in_flight: AtomicU64,
  requests: AtomicU64,
  completed: AtomicU64,
  cancelled: AtomicU64,
  transport_errors: AtomicU64,
  http_errors: AtomicU64,
  response_headers_ms_total: AtomicU64,
  duration_ms_total: AtomicU64,
}

impl WorkerPool {
  pub fn new(workers: Vec<WorkerEndpoint>) -> Result<Self> {
    let mut ids = BTreeSet::new();
    let mut paths = BTreeSet::new();
    for worker in &workers {
      anyhow::ensure!(!worker.worker_id.trim().is_empty(), "worker id must not be empty");
      anyhow::ensure!(ids.insert(&worker.worker_id), "duplicate worker id");
      anyhow::ensure!(paths.insert(&worker.socket_path), "duplicate worker socket");
    }
    let total_weight = workers.iter().map(|worker| u64::from(worker.weight)).sum();
    anyhow::ensure!(total_weight > 0, "at least one worker must have a positive weight");
    let main_worker_id = workers
      .iter()
      .rev()
      .find(|worker| worker.weight > 0)
      .map(|worker| worker.worker_id.clone());
    let weights = workers.iter().map(|worker| worker.weight).collect();
    let stats = workers
      .iter()
      .map(|worker| {
        let stats = Arc::new(WorkerStats::default());
        stats.keep_alive.store(true, Ordering::Relaxed);
        if main_worker_id.as_ref() == Some(&worker.worker_id) {
          *stats.state.write() = WorkerState::Current;
        }
        stats
      })
      .collect();
    Ok(Self {
      routing: parking_lot::RwLock::new(RoutingTable {
        ab_test: None,
        main_worker_id,
        workers,
        stats,
        weights,
        total_weight,
        generation: 1,
      }),
      expected: parking_lot::RwLock::new(None),
      update_lock: tokio::sync::Mutex::new(()),
      next: AtomicU64::new(0),
    })
  }

  /// Verify every configured worker, including zero-weight candidates, before binding the frontend.
  pub async fn check_ready(&self, plan: &GatewayPlan) -> Result<()> {
    let expected = WorkerInfo::new(plan, "frontend")?;
    let (workers, stats) = {
      let routing = self.routing.read();
      (routing.workers.clone(), routing.stats.clone())
    };
    for (index, worker) in workers.iter().enumerate() {
      let info = verify_worker(worker, &expected).await?;
      *stats[index].version.write() = Some(info.version);
    }
    *self.expected.write() = Some(expected);
    Ok(())
  }

  /// A frontend may bind before its first worker arrives.
  pub fn empty(plan: &GatewayPlan) -> Result<Self> {
    Ok(Self {
      routing: parking_lot::RwLock::new(RoutingTable {
        ab_test: None,
        main_worker_id: None,
        workers: Vec::new(),
        stats: Vec::new(),
        weights: Vec::new(),
        total_weight: 0,
        generation: 0,
      }),
      expected: parking_lot::RwLock::new(Some(WorkerInfo::new(plan, "frontend")?)),
      update_lock: tokio::sync::Mutex::new(()),
      next: AtomicU64::new(0),
    })
  }

  /// Validate before atomically promoting the newcomer. Previous workers become stale and
  /// may take over again until they claim automatic exit after draining.
  pub async fn register(&self, worker: WorkerEndpoint) -> Result<()> {
    self.register_inner(worker, RegistrationMode::Current).await
  }

  pub async fn register_candidate(&self, worker: WorkerEndpoint) -> Result<()> {
    self.register_inner(worker, RegistrationMode::Candidate).await
  }

  /// Start a frontend-owned linear ramp against the sole active baseline worker.
  pub async fn register_ab_test(&self, worker: WorkerEndpoint) -> Result<()> {
    self.register_inner(worker, RegistrationMode::AbTest).await
  }

  /// Called by the frontend clock independently of request arrivals.
  pub fn advance_ab_test(&self) {
    self.routing.write().advance_ab_test(Instant::now());
  }

  async fn register_inner(&self, worker: WorkerEndpoint, mode: RegistrationMode) -> Result<()> {
    let candidate = mode == RegistrationMode::Candidate;
    let _update = self.update_lock.lock().await;
    anyhow::ensure!(!worker.worker_id.trim().is_empty(), "worker id must not be empty");
    let expected = self.expected.read().clone().context("missing frontend manifest")?;
    let info = verify_worker(&worker, &expected).await?;
    let mut routing = self.routing.write();
    anyhow::ensure!(
      !routing
        .workers
        .iter()
        .any(|old| old.worker_id == worker.worker_id || old.socket_path == worker.socket_path),
      "worker is already registered"
    );
    anyhow::ensure!(
      !candidate || routing.total_weight > 0,
      "start a primary worker before registering a candidate"
    );
    let baseline = if mode == RegistrationMode::AbTest {
      anyhow::ensure!(
        routing.ab_test.is_none(),
        "an A/B test is already active; cancel it with a manual weight update first"
      );
      let active = routing
        .weights
        .iter()
        .enumerate()
        .filter(|(_, weight)| **weight > 0)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
      anyhow::ensure!(
        active.len() == 1,
        "--ab-test requires exactly one active baseline worker"
      );
      Some(active[0])
    } else {
      None
    };
    if mode == RegistrationMode::Current {
      routing.ab_test = None;
      for stats in &routing.stats {
        if *stats.state.read() != WorkerState::Exiting {
          *stats.state.write() = WorkerState::Stale;
        }
        stats.keep_alive.store(false, Ordering::Release);
        stats.changed.notify_waiters();
      }
      routing.weights.fill(0);
    }
    if let Some(baseline) = baseline {
      *routing.stats[baseline].state.write() = WorkerState::Stale;
      routing.stats[baseline].keep_alive.store(false, Ordering::Release);
      routing.weights[baseline] = 90;
      routing.ab_test = Some(AbTest {
        baseline_worker_id: routing.workers[baseline].worker_id.clone(),
        worker_id: worker.worker_id.clone(),
        started: Instant::now(),
      });
      routing.stats[baseline].changed.notify_waiters();
    }
    let stats = Arc::new(WorkerStats::default());
    *stats.version.write() = Some(info.version);
    *stats.state.write() = if candidate {
      WorkerState::Stale
    } else {
      WorkerState::Current
    };
    stats.keep_alive.store(candidate, Ordering::Release);
    if !candidate {
      routing.main_worker_id = Some(worker.worker_id.clone());
    }
    routing.workers.push(worker);
    routing.stats.push(stats);
    routing.weights.push(match mode {
      RegistrationMode::Current => 1,
      RegistrationMode::Candidate => 0,
      RegistrationMode::AbTest => 10,
    });
    if !candidate {
      routing.total_weight = if baseline.is_some() { 100 } else { 1 };
    }
    routing.generation += 1;
    Ok(())
  }

  /// Stop new admissions while retaining response leases and the worker record.
  pub fn retire(&self, worker_id: &str) -> Result<()> {
    let mut routing = self.routing.write();
    let index = routing
      .workers
      .iter()
      .position(|worker| worker.worker_id == worker_id)
      .context("unknown worker")?;
    if *routing.stats[index].state.read() != WorkerState::Exiting {
      routing.cancel_ab_test_for(worker_id);
      *routing.stats[index].state.write() = WorkerState::Exiting;
      routing.total_weight -= u64::from(routing.weights[index]);
      routing.weights[index] = 0;
      routing.advance_main();
      routing.generation += 1;
      routing.stats[index].changed.notify_waiters();
    }
    Ok(())
  }

  pub fn disconnect(&self, worker_id: &str) {
    let mut routing = self.routing.write();
    if let Some(index) = routing.workers.iter().position(|worker| worker.worker_id == worker_id) {
      routing.cancel_ab_test_for(worker_id);
      routing.total_weight -= u64::from(routing.weights[index]);
      routing.weights[index] = 0;
      *routing.stats[index].state.write() = WorkerState::Exiting;
      routing.stats[index].changed.notify_waiters();
      routing.workers.remove(index);
      routing.stats.remove(index);
      routing.weights.remove(index);
      routing.advance_main();
      routing.generation += 1;
    }
  }

  pub async fn wait_retired(&self, worker_id: &str) -> Result<()> {
    let stats = {
      let routing = self.routing.read();
      let index = routing
        .workers
        .iter()
        .position(|worker| worker.worker_id == worker_id)
        .context("unknown worker")?;
      routing.stats[index].clone()
    };
    loop {
      let changed = stats.changed.notified();
      tokio::pin!(changed);
      changed.as_mut().enable();
      {
        // Claim automatic exit under the admission lock. A stale worker may be
        // promoted again until this transition, but never after exit is sent.
        let mut routing = self.routing.write();
        let index = routing
          .workers
          .iter()
          .position(|worker| worker.worker_id == worker_id)
          .context("worker disconnected")?;
        let state = *stats.state.read();
        let idle_stale =
          state == WorkerState::Stale && routing.weights[index] == 0 && !stats.keep_alive.load(Ordering::Acquire);
        if (state == WorkerState::Exiting || idle_stale) && stats.in_flight.load(Ordering::Acquire) == 0 {
          if idle_stale {
            *stats.state.write() = WorkerState::Exiting;
            routing.generation += 1;
          }
          return Ok(());
        }
      }
      changed.await;
    }
  }

  fn select(&self) -> Result<(WorkerEndpoint, RequestLease)> {
    // Count admission while holding the routing lock. Once a zero weight is
    // published, status cannot report drained ahead of an older admission.
    let routing = self.routing.read();
    anyhow::ensure!(
      routing.total_weight > 0,
      "frontend has no active worker; run tokn-gateway worker start"
    );
    let mut slot = self.next.fetch_add(1, Ordering::Relaxed) % routing.total_weight;
    for (index, weight) in routing.weights.iter().enumerate() {
      if slot < u64::from(*weight) {
        let stats = routing.stats[index].clone();
        stats.in_flight.fetch_add(1, Ordering::Relaxed);
        stats.requests.fetch_add(1, Ordering::Relaxed);
        return Ok((
          routing.workers[index].clone(),
          RequestLease {
            stats,
            started: Instant::now(),
            finished: false,
          },
        ));
      }
      slot -= u64::from(*weight);
    }
    unreachable!("validated worker weights cover every slot")
  }
}

async fn verify_worker(worker: &WorkerEndpoint, expected: &WorkerInfo) -> Result<WorkerInfo> {
  let request = Request::get(READY_PATH).body(Body::empty())?;
  let response = tokio::time::timeout(CONNECT_TIMEOUT, exchange(&worker.socket_path, request))
    .await
    .context("worker readiness timed out")??;
  anyhow::ensure!(
    response.status().is_success(),
    "worker '{}' is not ready",
    worker.worker_id
  );
  let body = tokio::time::timeout(CONNECT_TIMEOUT, axum::body::to_bytes(response.into_body(), 64 * 1024)).await??;
  let info: WorkerInfo = serde_json::from_slice(&body)?;
  anyhow::ensure!(
    info.protocol_version == PROTOCOL_VERSION,
    "worker '{}' has incompatible IPC protocol",
    worker.worker_id
  );
  for listener in &expected.listeners {
    anyhow::ensure!(
      info.listeners.contains(listener),
      "worker '{}' is missing compatible listener '{}'",
      worker.worker_id,
      listener.listener_id
    );
  }
  anyhow::ensure!(
    info.api_admission == expected.api_admission,
    "worker '{}' has incompatible API admission policy",
    worker.worker_id
  );
  tracing::info!(worker_id = %worker.worker_id, version = %info.version, "gateway worker ready");
  Ok(info)
}

#[async_trait]
impl RoutingControl for WorkerPool {
  fn status(&self) -> RoutingReport {
    let mut routing = self.routing.write();
    let now = Instant::now();
    routing.advance_ab_test(now);
    RoutingReport {
      generation: routing.generation,
      main_worker_id: routing.main_worker_id.clone(),
      ab_test: routing.ab_test.as_ref().map(|experiment| experiment.status(now)),
      workers: routing
        .workers
        .iter()
        .enumerate()
        .map(|(index, worker)| {
          let stats = &routing.stats[index];
          WorkerStatus {
            worker_id: worker.worker_id.clone(),
            version: stats.version.read().clone(),
            weight: routing.weights[index],
            state: *stats.state.read(),
            in_flight: stats.in_flight.load(Ordering::Relaxed),
            requests: stats.requests.load(Ordering::Relaxed),
            completed: stats.completed.load(Ordering::Relaxed),
            cancelled: stats.cancelled.load(Ordering::Relaxed),
            transport_errors: stats.transport_errors.load(Ordering::Relaxed),
            http_errors: stats.http_errors.load(Ordering::Relaxed),
            response_headers_ms_total: stats.response_headers_ms_total.load(Ordering::Relaxed),
            duration_ms_total: stats.duration_ms_total.load(Ordering::Relaxed),
          }
        })
        .collect(),
    }
  }

  async fn update_weights(&self, weights: BTreeMap<String, u32>) -> Result<RoutingReport> {
    let _update = self.update_lock.lock().await;
    let (workers, stats, generation) = {
      let routing = self.routing.read();
      (routing.workers.clone(), routing.stats.clone(), routing.generation)
    };
    anyhow::ensure!(
      weights.len() == workers.len(),
      "weights must include every configured worker exactly once"
    );
    let values = workers
      .iter()
      .map(|worker| {
        weights
          .get(&worker.worker_id)
          .copied()
          .with_context(|| format!("missing weight for worker '{}'", worker.worker_id))
      })
      .collect::<Result<Vec<_>>>()?;
    let total_weight = values.iter().map(|value| u64::from(*value)).sum::<u64>();
    anyhow::ensure!(total_weight > 0, "at least one worker must have a positive weight");
    let expected = self
      .expected
      .read()
      .clone()
      .context("initial worker readiness has not been checked")?;
    for (index, (worker, weight)) in workers.iter().zip(&values).enumerate() {
      if *weight > 0 {
        anyhow::ensure!(*stats[index].state.read() != WorkerState::Exiting, "worker is exiting");
        let info = verify_worker(worker, &expected).await?;
        *stats[index].version.write() = Some(info.version);
      }
    }
    {
      let mut routing = self.routing.write();
      anyhow::ensure!(
        routing.generation == generation,
        "workers changed during readiness checks; retry the weight update"
      );
      routing.ab_test = None;
      routing.weights = values;
      routing.total_weight = total_weight;
      routing.advance_main();
      routing.generation += 1;
      for stats in &routing.stats {
        stats.changed.notify_waiters();
      }
    }
    let report = self.status();
    tracing::info!(
      generation = report.generation,
      ?weights,
      "worker request weights updated"
    );
    Ok(report)
  }
}

struct RequestLease {
  stats: Arc<WorkerStats>,
  started: Instant,
  finished: bool,
}

impl RequestLease {
  fn finish(&mut self) {
    if !self.finished {
      self.finished = true;
      self.stats.completed.fetch_add(1, Ordering::Relaxed);
      self
        .stats
        .duration_ms_total
        .fetch_add(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
      self.stats.in_flight.fetch_sub(1, Ordering::Release);
      self.stats.changed.notify_waiters();
    }
  }
}

impl Drop for RequestLease {
  fn drop(&mut self) {
    if !self.finished {
      self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
      self
        .stats
        .duration_ms_total
        .fetch_add(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
      self.stats.in_flight.fetch_sub(1, Ordering::Release);
      self.stats.changed.notify_waiters();
    }
  }
}

struct TrackedBody {
  body: Body,
  lease: RequestLease,
}

impl http_body::Body for TrackedBody {
  type Data = bytes::Bytes;
  type Error = axum::Error;
  fn poll_frame(
    mut self: Pin<&mut Self>,
    cx: &mut TaskContext<'_>,
  ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
    let result = Pin::new(&mut self.body).poll_frame(cx);
    match &result {
      Poll::Ready(Some(Err(_))) => {
        self.lease.stats.transport_errors.fetch_add(1, Ordering::Relaxed);
      }
      Poll::Ready(None) => self.lease.finish(),
      Poll::Ready(Some(Ok(_))) if self.body.is_end_stream() => self.lease.finish(),
      _ => {}
    }
    result
  }
  fn is_end_stream(&self) -> bool {
    self.body.is_end_stream()
  }
  fn size_hint(&self) -> SizeHint {
    self.body.size_hint()
  }
}

#[async_trait]
impl RequestDispatcher for WorkerPool {
  async fn dispatch(&self, context: DispatchContext, mut request: Request) -> Result<Response> {
    let (worker, mut lease) = self.select()?;
    let request_id = request
      .headers()
      .get("x-request-id")
      .and_then(|value| value.to_str().ok())
      .unwrap_or("");
    tracing::debug!(worker_id = %worker.worker_id, %request_id, "dispatching request to worker");
    let wire = WireContext::new(context, request.uri().to_string());
    let metadata = STANDARD.encode(serde_json::to_vec(&wire)?);
    anyhow::ensure!(
      metadata.len() <= MAX_CONTEXT_BYTES,
      "IPC admission context exceeds limit"
    );
    strip_internal_headers(request.headers_mut());
    let request_id = request.headers().get("x-request-id").cloned();
    strip_connection_headers(request.headers_mut())?;
    if let Some(request_id) = request_id {
      request.headers_mut().insert("x-request-id", request_id);
    }
    request
      .headers_mut()
      .insert(CONTEXT_HEADER, HeaderValue::from_str(&metadata)?);
    *request.uri_mut() = "/_tokn/dispatch".parse()?;
    let mut response = match exchange(&worker.socket_path, request).await {
      Ok(response) => response,
      Err(error) => {
        lease.stats.transport_errors.fetch_add(1, Ordering::Relaxed);
        return Err(error).with_context(|| format!("dispatch to worker '{}'", worker.worker_id));
      }
    };
    lease
      .stats
      .response_headers_ms_total
      .fetch_add(lease.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    if response.status().is_client_error() || response.status().is_server_error() {
      lease.stats.http_errors.fetch_add(1, Ordering::Relaxed);
    }
    strip_internal_headers(response.headers_mut());
    strip_connection_headers(response.headers_mut())?;
    let (parts, body) = response.into_parts();
    if http_body::Body::is_end_stream(&body) {
      lease.finish();
    }
    Ok(Response::from_parts(parts, Body::new(TrackedBody { body, lease })))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn experiment(started: Instant) -> AbTest {
    AbTest {
      baseline_worker_id: "old".into(),
      worker_id: "new".into(),
      started,
    }
  }

  #[test]
  fn ramp_is_linear_and_caps_at_twenty_four_hours() {
    let start = Instant::now();
    let ramp = experiment(start);
    for (seconds, percent, complete) in [
      (0, 10, false),
      (6 * 3600, 30, false),
      (12 * 3600, 50, false),
      (24 * 3600 - 1, 89, false),
      (24 * 3600, 90, true),
      (48 * 3600, 90, true),
    ] {
      let status = ramp.status(start + Duration::from_secs(seconds));
      assert_eq!(status.traffic_percent, percent);
      assert_eq!(status.elapsed_seconds == status.duration_seconds, complete);
      assert!(status.elapsed_seconds <= status.duration_seconds);
    }
  }

  #[test]
  fn completion_routes_every_request_to_new_worker_and_preserves_old_lease() {
    let start = Instant::now();
    let pool = WorkerPool::new(vec![
      WorkerEndpoint {
        worker_id: "old".into(),
        socket_path: "/tmp/old.sock".into(),
        weight: 90,
      },
      WorkerEndpoint {
        worker_id: "new".into(),
        socket_path: "/tmp/new.sock".into(),
        weight: 10,
      },
    ])
    .unwrap();
    pool.routing.write().ab_test = Some(experiment(start));
    let (_, mut pending) = pool.select().unwrap();
    for (elapsed, expected) in [(Duration::ZERO, [90, 10]), (AB_TEST_DURATION / 2, [50, 50])] {
      pool.routing.write().advance_ab_test(start + elapsed);
      let mut counts = [0, 0];
      for _ in 0..100 {
        let (worker, mut lease) = pool.select().unwrap();
        counts[usize::from(worker.worker_id == "new")] += 1;
        lease.finish();
      }
      assert_eq!(counts, expected);
    }
    let generation = pool.routing.read().generation;
    pool.routing.write().advance_ab_test(start + AB_TEST_DURATION);
    assert!(pool.status().ab_test.is_none());
    assert_eq!(pool.status().workers[0].in_flight, 1);
    assert_eq!(pool.status().workers[0].weight, 0);
    assert!(!pool.routing.read().stats[0].keep_alive.load(Ordering::Acquire));
    for _ in 0..100 {
      let (worker, mut lease) = pool.select().unwrap();
      assert_eq!(worker.worker_id, "new");
      lease.finish();
    }
    pending.finish();
    assert_eq!(pool.status().workers[0].in_flight, 0);
    pool.routing.write().advance_ab_test(start + AB_TEST_DURATION * 2);
    assert_eq!(pool.status().generation, generation + 1);
  }
}
