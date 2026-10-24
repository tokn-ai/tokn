//! Concrete storage types used by the shared runtime.
//!
//! Standard builds re-export the existing upstream types. Alloc-only builds
//! provide the string and immutable-buffer operations needed by shared code,
//! using non-atomic reference counting. The local backend supports cheap
//! cloning and byte slicing, but does not reproduce upstream layout, mutable
//! buffers, or the complete `bytes` and `smol_str` extension APIs.
//!
//! Local values cannot cross threads. Dropping the final shared owner drops
//! its allocation normally; a nonempty slice retains its entire backing buffer.

/// Shared ownership for portable values. Standard builds retain atomic sharing;
/// alloc-only builds use local reference counting and cannot cross threads.
#[cfg(not(feature = "std"))]
pub use alloc::rc::Rc as Shared;
/// Shared ownership for portable values. Standard builds retain atomic sharing;
/// alloc-only builds use local reference counting and cannot cross threads.
#[cfg(feature = "std")]
pub use alloc::sync::Arc as Shared;

#[cfg(feature = "std")]
pub use bytes::Bytes;
#[cfg(feature = "std")]
pub use smol_str::SmolStr;

#[cfg(not(feature = "std"))]
#[path = "types/local_bytes.rs"]
mod local_bytes;
#[cfg(not(feature = "std"))]
#[path = "types/local_str.rs"]
mod local_str;

#[cfg(not(feature = "std"))]
pub use local_bytes::Bytes;
#[cfg(not(feature = "std"))]
pub use local_str::SmolStr;
