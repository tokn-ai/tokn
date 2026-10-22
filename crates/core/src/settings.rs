//! Provider-specific configuration values with TOML-compatible semantics.
//!
//! Standard builds retain the upstream TOML value types. Alloc-only builds use
//! a local representation because upstream TOML errors require atomic pointers.
//! Both representations preserve strings, signed integers, floats, booleans,
//! datetimes, arrays and tables; settings are never coerced through JSON.

#[cfg(feature = "std")]
pub use toml::{Table, Value};
pub use toml_datetime::{Date, Datetime, Offset, Time};

#[cfg(not(feature = "std"))]
mod local;
#[cfg(not(feature = "std"))]
pub use local::{Table, Value};
