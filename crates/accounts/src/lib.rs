//! Account management and request routing.
//!
//! `pool` owns provider-bucketed account acquisition; `affinity` keeps
//! session ids pinned to accounts across related requests; `routing`
//! resolves a requested model into a route mode + upstream selector for the
//! pool to consume.

#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;

pub mod native;
pub mod pool_state;
pub mod selection;

#[cfg(feature = "std")]
pub mod affinity;
#[cfg(feature = "std")]
pub mod inventory;
#[cfg(feature = "std")]
pub mod link;
#[cfg(feature = "std")]
pub mod registry;
#[cfg(feature = "std")]
pub mod routing;

#[cfg(feature = "std")]
mod handle;
#[cfg(feature = "std")]
mod pool;

#[cfg(feature = "std")]
pub use handle::AccountHandle;
#[cfg(feature = "std")]
pub use inventory::{AccountInventory, AccountPoolRuleset};
#[cfg(feature = "std")]
pub use pool::{AccountPool, EndpointAcquire, Error, Result, SessionAcquire};
#[cfg(feature = "std")]
pub use routing::{ResolveError, RouteResolution, RouteResolver, RouteSelector};

#[cfg(feature = "std")]
pub mod auth_registry;
#[cfg(feature = "std")]
pub mod context;
#[cfg(feature = "std")]
pub mod management;
