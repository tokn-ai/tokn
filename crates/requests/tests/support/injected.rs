//! Test fixture for exercising Session with borrowed services without an SDK dependency.
#![allow(dead_code)]

use serde_json::Value;
use std::{
  cell::Cell,
  collections::{BTreeMap, BTreeSet},
};
use tokn_core::{
  account::AccountConfig,
  request_event::RequestEventSink,
  types::{Shared, SmolStr},
  Endpoint,
};
use tokn_policy::{ExecutionRules, ProviderId};
use tokn_requests::{
  configured::{self, EventRuntime, RequestOptions, Runtime},
  session::{Error, ExecutionState, ModelCapabilities, RequestInput, Session},
};
use tokn_service::transport::Transport;

/// Only a test harness; production orchestration belongs to tokn-sdk::Client.
pub struct BorrowedSession<T, R> {
  state: Shared<ExecutionState>,
  services: configured::Injected<T, R>,
  request_sequence: Cell<u64>,
}

impl<T: Transport, R: Runtime> BorrowedSession<T, R> {
  pub fn new(rules: ExecutionRules, accounts: Vec<AccountConfig>, transport: T, runtime: R) -> Result<Self, Error> {
    Self::with_models(rules, accounts, BTreeMap::new(), transport, runtime)
  }

  pub fn with_models(
    rules: ExecutionRules,
    accounts: Vec<AccountConfig>,
    models: BTreeMap<ProviderId, BTreeSet<SmolStr>>,
    transport: T,
    runtime: R,
  ) -> Result<Self, Error> {
    let models = models
      .into_iter()
      .map(|(provider, models)| {
        (
          provider,
          models
            .into_iter()
            .map(|model| (model, ModelCapabilities::default()))
            .collect(),
        )
      })
      .collect();
    Ok(Self {
      state: Shared::new(ExecutionState::with_models(rules, accounts, models)?),
      services: configured::Injected::new(transport, runtime),
      request_sequence: Cell::new(0),
    })
  }

  pub fn with_event_sink<S: RequestEventSink>(self, sink: S) -> BorrowedSession<T, EventRuntime<R, S>> {
    BorrowedSession {
      state: self.state,
      services: self.services.with_event_sink(sink),
      request_sequence: self.request_sequence,
    }
  }

  pub async fn execute(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
  ) -> Result<configured::Response<'_, T, R>, Error> {
    let (transport, clock, events) = self.services.acquire()?;
    let sequence = self.request_sequence.get();
    self.request_sequence.set(sequence.wrapping_add(1));
    let request_id = options.request_id.clone().unwrap_or_else(|| format!("test-{sequence}"));
    let mut input = RequestInput::new(endpoint, body, options.inbound_headers(), request_id);
    input.profile = options.profile;
    Session::new(self.state.clone(), input, transport, clock, events)
      .run()
      .await
  }
}
