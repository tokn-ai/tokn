//! Provider preparation shared by standard and embedded execution.

#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;

pub mod native;
pub mod transform;

#[cfg(feature = "std")]
include!("runtime.rs");
