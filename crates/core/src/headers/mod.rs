//! Portable header values and correlation metadata shared by all runtimes.

pub mod agent_id;
pub mod keys;
pub mod map;
pub mod name;
#[cfg(feature = "std")]
pub mod reqwest_compat;
pub mod value;
pub mod vars;

pub use agent_id::AgentId;
pub use map::{AsHeaderName, HeaderMap};
pub use name::HeaderName;
pub use value::HeaderValue;
pub use vars::TemplateVars;
