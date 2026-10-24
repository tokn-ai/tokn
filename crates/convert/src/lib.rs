#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;

/// Endpoint identity shared with provider and request crates.
pub mod provider {
  pub use tokn_core::Endpoint;
}

pub mod buffered;
pub mod chat;
pub mod error;
pub mod ir;
pub mod messages;
pub mod responses;
pub mod sse;
pub mod tools;
pub mod usage;
pub mod value;

use crate::provider::Endpoint;
use serde_json::Value;

pub use error::Result;

pub fn convert_request(from: Endpoint, to: Endpoint, body: &Value) -> Result<Value> {
  if from == to {
    return Ok(body.clone());
  }
  let req = match from {
    Endpoint::ChatCompletions => value::chat::request_from_value(body)?,
    Endpoint::Responses => value::responses::request_from_value(body)?,
    Endpoint::Messages => value::messages::request_from_value(body)?,
  };
  match to {
    Endpoint::ChatCompletions => value::chat::request_to_value(&req),
    Endpoint::Responses => value::responses::request_to_value(&req),
    Endpoint::Messages => value::messages::request_to_value(&req),
  }
}

pub fn convert_response(from: Endpoint, to: Endpoint, body: &Value) -> Result<Value> {
  if from == to {
    return Ok(body.clone());
  }
  let resp = match from {
    Endpoint::ChatCompletions => value::chat::response_from_value(body)?,
    Endpoint::Responses => value::responses::response_from_value(body)?,
    Endpoint::Messages => value::messages::response_from_value(body)?,
  };
  render_response(to, &resp)
}

fn render_response(endpoint: Endpoint, response: &ir::IrResponse) -> Result<Value> {
  match endpoint {
    Endpoint::ChatCompletions => value::chat::response_to_value(response),
    Endpoint::Responses => value::responses::response_to_value(response),
    Endpoint::Messages => value::messages::response_to_value(response),
  }
}
