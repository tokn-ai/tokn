//! Portable provider value contracts; runtime effects are supplied by callers.

pub use crate::endpoint::{glob_match, match_endpoint_rule, Endpoint, EndpointRule};
pub use crate::provider_values::*;

#[path = "prepare.rs"]
pub mod prepare;
pub use prepare::{prepare_request, prepare_request_with_policy, validate_credential, PrepareError};
