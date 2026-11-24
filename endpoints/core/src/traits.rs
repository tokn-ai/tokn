use serde::{de::DeserializeOwned, Serialize};

use crate::endpoint::Endpoint;

/// Marker trait for a request payload bound to a specific endpoint.
pub trait EndpointRequest: DeserializeOwned + Serialize {
  const ENDPOINT: Endpoint;
}

/// Marker trait for a response payload bound to a specific endpoint.
pub trait EndpointResponse: DeserializeOwned + Serialize {
  const ENDPOINT: Endpoint;
}

/// Marker trait for an input/output item type used by an endpoint.
///
/// "Item" here refers to the discrete units that compose a request input
/// or response output (e.g. a message, a function call, a content block).
pub trait EndpointItem: DeserializeOwned + Serialize {
  const ENDPOINT: Endpoint;
}

/// Marker trait for a streaming event payload bound to an endpoint.
pub trait EndpointEvent: DeserializeOwned + Serialize {
  const ENDPOINT: Endpoint;

  /// The wire `type` (or equivalent) of this event, used for routing
  /// and filtering. Returns an empty string for events that have no
  /// inherent type discriminator.
  fn event_name(&self) -> &str;
}
