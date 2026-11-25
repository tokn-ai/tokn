//! Gateway process supervision and coordination of public listeners, workers and persistence.

#[cfg(unix)]
pub mod supervision;

#[cfg(feature = "runtime")]
mod shutdown;
#[cfg(feature = "runtime")]
pub use shutdown::finish_events;

#[cfg(all(unix, feature = "runtime"))]
pub mod control;
#[cfg(all(unix, feature = "runtime"))]
pub mod frontend;
#[cfg(all(unix, feature = "runtime"))]
mod ingress;
