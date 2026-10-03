//! Local control surface for request assignment and worker draining.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize)]
pub struct RoutingReport {
  pub generation: u64,
  pub main_worker_id: Option<String>,
  pub ab_test: Option<AbTestStatus>,
  pub workers: Vec<WorkerStatus>,
}

/// Frontend-owned linear traffic ramp, present until completion or cancellation.
#[derive(Clone, Debug, Serialize)]
pub struct AbTestStatus {
  pub baseline_worker_id: String,
  pub worker_id: String,
  pub elapsed_seconds: u64,
  pub duration_seconds: u64,
  pub traffic_percent: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
  Current,
  #[default]
  Stale,
  Exiting,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkerStatus {
  pub worker_id: String,
  pub version: Option<String>,
  pub weight: u32,
  pub state: WorkerState,
  pub in_flight: u64,
  pub requests: u64,
  pub completed: u64,
  pub cancelled: u64,
  pub transport_errors: u64,
  pub http_errors: u64,
  pub response_headers_ms_total: u64,
  pub duration_ms_total: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateWeights {
  pub weights: BTreeMap<String, u32>,
}

#[async_trait]
#[allow(
  clippy::double_must_use,
  reason = "async_trait adds must_use to methods returning must-use futures"
)]
pub trait RoutingControl: Send + Sync {
  fn status(&self) -> RoutingReport;
  async fn update_weights(&self, weights: BTreeMap<String, u32>) -> anyhow::Result<RoutingReport>;
}
