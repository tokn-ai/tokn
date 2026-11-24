//! Bind native replay to the exact portable history that produced it.

use super::{GenerateRequest, GenerateResponse, Message, Role};
use crate::{Error, Result};
use alloc::string::String;
use core::fmt;
use serde::{Deserialize, Serialize};
use tokn_requests::session::{self, ContinuationRequest};

/// Serializable provider continuation for a prefix ending in an assistant
/// message. Keep it alongside the full portable history for explicit fallback.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderState {
  native: session::ProviderState,
  covered_messages: usize,
  history_digest: String,
}

impl fmt::Debug for ProviderState {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ProviderState")
      .field("covered_messages", &self.covered_messages)
      .finish_non_exhaustive()
  }
}

pub(super) fn digest<T: Serialize + ?Sized>(value: &T) -> Result<String> {
  let mut json = serde_json::to_value(value).map_err(|source| Error::SerializeRequest { source })?;
  json.sort_all_objects();
  let bytes = serde_json::to_vec(&json).map_err(|source| Error::SerializeRequest { source })?;
  Ok(tokn_core::util::digest::sha256_hex(&bytes))
}

impl ProviderState {
  /// Scope for provider-owned media produced by this continuation's backend.
  /// Asset scope is independent of the model-bound continuation checkpoint.
  pub fn asset_scope(&self) -> tokn_core::content::ProviderAssetScope {
    self.native.origin().asset_scope()
  }

  pub(crate) fn capture(
    request: &GenerateRequest,
    reply: &GenerateResponse,
    native: session::ProviderState,
  ) -> Result<Self> {
    let assistant = reply.assistant_message();
    let history: alloc::vec::Vec<_> = request.messages.iter().chain(core::iter::once(&assistant)).collect();
    Ok(Self {
      native,
      covered_messages: history.len(),
      history_digest: digest(&history)?,
    })
  }

  fn suffix<'a>(&self, request: &'a GenerateRequest) -> Result<impl Iterator<Item = &'a Message>> {
    let prefix = request
      .messages
      .get(..self.covered_messages)
      .ok_or(Error::ProviderStateHistoryMismatch)?;
    if digest(prefix)? != self.history_digest {
      return Err(Error::ProviderStateHistoryMismatch);
    }
    Ok(
      prefix
        .iter()
        .filter(|m| m.role == Role::System)
        .chain(request.messages[self.covered_messages..].iter()),
    )
  }
}

impl GenerateRequest {
  pub(crate) fn validate_provider_prefix(&self) -> Result<()> {
    if let Some(state) = &self.provider_state {
      let _ = state.suffix(self)?;
    }
    Ok(())
  }

  pub(super) fn wants_checkpoint(&self) -> bool {
    self.generation_binding.is_some() || self.provider_state.is_some() || self.capture_provider_state
  }

  pub(super) fn continuation(&self, stream: bool) -> Result<ContinuationRequest> {
    if let Some(key) = [
      "previous_response_id",
      "conversation",
      "input",
      "messages",
      "instructions",
      "stream",
    ]
    .into_iter()
    .find(|key| self.extras.contains_key(*key))
    {
      return Err(Error::InvalidGenerateRequest {
        message: alloc::format!("extras['{key}'] conflicts with managed conversation input"),
      });
    }
    let suffix = self
      .provider_state
      .as_ref()
      .map(|state| self.render_messages(stream, state.suffix(self)?))
      .transpose()?;
    Ok(ContinuationRequest {
      state: self.provider_state.as_ref().map(|state| state.native.clone()),
      policy: self.continuation_policy,
      suffix,
      restart_unavailable: false,
    })
  }
}
