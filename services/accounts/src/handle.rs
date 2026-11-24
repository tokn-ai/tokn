use arc_swap::ArcSwap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokn_core::account::AccountConfig;
use tokn_core::provider::Provider;
use tracing::{debug, warn};

pub struct AccountHandle {
  pub config: ArcSwap<AccountConfig>,
  pub provider: Arc<dyn Provider>,
  inner: RwLock<AccountInner>,
}

struct AccountInner {
  cooldown_until: Option<Instant>,
  consecutive_failures: u32,
}

impl AccountHandle {
  /// Construct a fresh handle around the given config + provider. Public
  /// so requests stages and tests can synthesize handles without going
  /// through the full pool boot sequence; production code still goes
  /// through [`AccountPool`](crate::pool::AccountPool).
  pub fn new(config: Arc<AccountConfig>, provider: Arc<dyn Provider>) -> Self {
    Self {
      config: ArcSwap::from(config),
      provider,
      inner: RwLock::new(AccountInner {
        cooldown_until: None,
        consecutive_failures: 0,
      }),
    }
  }

  pub fn id(&self) -> String {
    self.config.load().id.clone()
  }

  pub(super) fn is_healthy(&self) -> bool {
    match self.inner.read().cooldown_until {
      None => true,
      Some(t) => Instant::now() >= t,
    }
  }

  pub(super) fn cooldown_until(&self) -> Option<Instant> {
    self.inner.read().cooldown_until
  }

  pub fn mark_failure(&self, cooldown_base: Duration) {
    let mut g = self.inner.write();
    g.consecutive_failures = g.consecutive_failures.saturating_add(1);
    let mult = 1u32 << (g.consecutive_failures.min(5) - 1);
    let cd = cooldown_base.saturating_mul(mult);
    g.cooldown_until = Some(Instant::now() + cd);
    warn!(
      account = %self.id(),
      retry_in_secs = cd.as_secs(),
      consecutive_failures = g.consecutive_failures,
      "account in cooldown"
    );
  }

  pub fn mark_success(&self) {
    let mut g = self.inner.write();
    if g.consecutive_failures > 0 {
      debug!(account = %self.id(), recovered_after = g.consecutive_failures, "account recovered");
    }
    g.consecutive_failures = 0;
    g.cooldown_until = None;
  }

  /// Notify the underlying provider that an upstream 401 happened so it can
  /// drop any cached short-lived credential.
  pub fn invalidate_credentials(&self) {
    debug!(account = %self.id(), "invalidating credentials due to upstream 401");
    self.provider.on_unauthorized();
  }
}
