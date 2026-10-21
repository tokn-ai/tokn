//! Shared engine logic for standard and local embedded runtimes.
//!
//! Disable default features for `no_std + alloc`. The embedded storage backend
//! uses non-atomic shared ownership; its string and buffer types are neither
//! `Send` nor `Sync`. Platform adapters own synchronization and provide time.
//!
//! Pool selection is the first extracted part of the existing execution engine.
//! Provider dispatch and protocol conversion remain in the standard runtime.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod pool;
pub mod time;
pub mod types;
