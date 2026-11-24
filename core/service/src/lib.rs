//! Config-independent HTTP and Tower contracts for low-level tokn services.
//!
//! This crate deliberately has no dependency on tokn configuration, account
//! pools, persistence, provider implementations, or built-in header policy.
//! Higher-level crates compile those concerns into an [`HttpService`].

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod transport;

#[cfg(feature = "std")]
pub mod body;
#[cfg(feature = "std")]
mod service;

#[cfg(feature = "std")]
use std::error::Error as StdError;

#[cfg(feature = "std")]
pub use body::{Body, BodyError};
#[cfg(feature = "std")]
pub use service::{HttpService, ServiceError};

/// Type-erased thread-safe error used by streaming bodies.
#[cfg(feature = "std")]
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// Native low-level HTTP request.
#[cfg(feature = "std")]
pub type Request = http::Request<Body>;

/// Native low-level HTTP response.
#[cfg(feature = "std")]
pub type Response = http::Response<Body>;
