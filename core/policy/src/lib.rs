//! Dependency-light request policy types shared by configuration and runtime crates.
//!
//! This crate deliberately contains no configuration-file representation and no
//! request execution code. Configuration compilers produce these values, while
//! runtime crates consume them without depending on TOML shape or compatibility
//! aliases.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg_attr(test, macro_use)]
extern crate alloc;
#[cfg(test)]
extern crate std;

mod authority;
mod cors;
mod id;
mod route;
mod topology;

pub use authority::{
  AuthorityMismatch, CanonicalAuthority, CanonicalHost, IngressAuthority, IngressAuthoritySource, InvalidAuthority,
  InvalidHost, ResolvedAuthority,
};
pub use cors::CorsPlan;
pub use id::{
  AccountPoolId, BindingId, DriverId, HeaderPatchSetId, InvalidIdentifier, ListenerId, OperationId, ProfileId,
  ProviderId, RetryPolicyId, RouteId, WireIdentityId,
};
pub use route::{
  ApiBindingPlan, CredentialPolicy, DestinationPolicy, HeaderStrategy, ManagedRetry, ManagedRoute, ManagedTarget,
  ModelFamily, ModelSelector, OperationPolicy, PayloadTransform, ProfilePlan, ProviderSelector, QualificationNamespace,
  RelayCredentials, RelayDestination, RelayRetry, RelayRoute, RetryPolicyPlan, RouteKind, RoutePlan, WireIdentity,
};
pub use topology::{
  AccountPoolPlan, AccountSelectionStrategy, AccountSelector, ClientAuthPlan, ConnectAction, ConnectMatch,
  ConnectRulePlan, EmptyConnectMatch, EmptyHttpMatch, ExecutionRules, HostPattern, HttpAction, HttpBindingPlan,
  HttpMatch, InvalidSubdomainSuffix, ProviderOrigin, ProviderPlan, SessionAffinityPlan,
  DEFAULT_FORWARD_PROXY_REQUEST_BODY_MAX_BYTES,
};
#[cfg(feature = "std")]
pub use topology::{ForwardProxyListenerPlan, GatewayPlan, ListenerKind, ListenerPlan, LlmApiListenerPlan, TlsPlan};
