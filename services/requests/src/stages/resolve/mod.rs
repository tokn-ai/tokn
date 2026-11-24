//! Resolve stage — picks the upstream account and upstream model for an
//! extracted request.
//!
//! * [`stage`] — the generic [`PoolResolve`] stage and the
//!   [`AccountSelector`] trait it consults.
//! * [`pool`] — [`PoolAccountSelector`], the production [`AccountSelector`]
//!   implementation backed by [`tokn_accounts::AccountPool`] +
//!   [`tokn_accounts::RouteResolver`].
//! * [`proxy`] — [`ProxyResolve`], the no-account variant used by the
//!   MITM proxy passthrough pipeline. Reads `proxy.host` from
//!   `PipelineCtx::config` and emits a `Resolved` with a stub
//!   `AccountHandle` so the proxy pipeline can reuse the standard
//!   stage shape without an actual account selection step.

mod pool;
pub mod proxy;
mod stage;

pub use pool::{PoolAccountSelector, ACCESS_ALLOWED_PROVIDERS_KEY};
pub use proxy::{ProxyProviderResolve, ProxyResolve, ProxyStubProvider};
pub use stage::{AccountSelector, PoolResolve, SelectorOutcome};
