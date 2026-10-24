//! Shared values and contracts for standard and embedded Tokn runtimes.

#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;
#[cfg(test)]
extern crate std;

pub mod account;
pub mod authority;
#[cfg(feature = "std")]
pub mod db;
pub mod endpoint;
#[cfg(feature = "std")]
pub mod event;
pub mod generation;
pub mod headers;
#[cfg(feature = "std")]
pub mod pipeline;
#[cfg(feature = "std")]
pub mod provider;
#[cfg(not(feature = "std"))]
#[path = "provider/portable.rs"]
pub mod provider;
mod provider_values;
pub mod request_classification;
pub mod request_event;
pub mod settings;
pub mod time;
pub mod types;
pub mod upstream_url;
pub mod usage;
pub mod util;

pub use endpoint::Endpoint;
pub use headers::AgentId;
