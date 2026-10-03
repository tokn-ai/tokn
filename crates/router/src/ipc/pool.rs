//! Live request assignment, readiness checks, and response-lifetime accounting.

use super::protocol::{WireContext, CONNECT_TIMEOUT, CONTEXT_HEADER, MAX_CONTEXT_BYTES, PROTOCOL_VERSION, READY_PATH};
use super::transport::{exchange, strip_connection_headers, strip_internal_headers};
use super::WorkerInfo;
use crate::dispatch::{DispatchContext, RequestDispatcher};
use crate::routing::{RoutingControl, RoutingReport, WorkerState, WorkerStatus};
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
use std::time::Instant;
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
  main_worker_id: Option<String>,
  workers: Vec<WorkerEndpoint>,
  stats: Vec<Arc<WorkerStats>>,
  weights: Vec<u32>,
  total_weight: u64,
  generation: u64,
}

impl RoutingTable {
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
    self.register_inner(worker, false).await
  }

  pub async fn register_candidate(&self, worker: WorkerEndpoint) -> Result<()> {
    self.register_inner(worker, true).await
  }

  async fn register_inner(&self, worker: WorkerEndpoint, candidate: bool) -> Result<()> {
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
    if !candidate {
      for stats in &routing.stats {
        if *stats.state.read() != WorkerState::Exiting {
          *stats.state.write() = WorkerState::Stale;
        }
        stats.keep_alive.store(false, Ordering::Release);
        stats.changed.notify_waiters();
      }
      routing.weights.fill(0);
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
    routing.weights.push(if candidate { 0 } else { 1 });
    if !candidate {
      routing.total_weight = 1;
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
    let routing = self.routing.read();
    RoutingReport {
      generation: routing.generation,
      main_worker_id: routing.main_worker_id.clone(),
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
