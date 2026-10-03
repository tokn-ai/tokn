//! Versioned HTTP-over-Unix-socket dispatch. Each IPC connection owns one
//! request, allowing cancellation independently of persistent client tunnels.

mod pool;
mod protocol;
mod transport;

pub use pool::{WorkerEndpoint, WorkerPool};
pub(crate) use protocol::auth_name;
pub use protocol::{ApiAdmission, WorkerInfo, WorkerListener, PROTOCOL_VERSION};
pub use transport::serve_worker;

#[cfg(test)]
mod tests;
