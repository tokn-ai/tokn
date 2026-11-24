//! Provider preparation shared by standard and embedded execution.

#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;

mod common;
pub mod jwt;
pub mod native;

#[cfg(feature = "std")]
include!("runtime.rs");
