//! Instantiate the same routing scenarios as the standard engine tests using
//! only core/alloc, including a target without pointer atomics.

#![no_std]

extern crate alloc;

#[path = "../../scenarios/mod.rs"]
mod scenarios;

pub use scenarios::{pool_selection_transcript, storage_round_trip};

#[cfg(test)]
mod tests {
  #[test]
  fn shared_pool_selection() {
    super::pool_selection_transcript();
  }

  #[test]
  fn local_storage() {
    super::storage_round_trip();
  }
}
