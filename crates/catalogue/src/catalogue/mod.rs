//! models.dev catalogue.
//!
//! At build time we embed `https://models.dev/api.json`; at runtime we
//! prefer a disk cache and publish validated refreshes without restarting. See
//! the standard loader for resolution order. See the standard mapping module
//! for conversion to internal provider metadata.

#[cfg(feature = "std")]
pub mod loader;
#[cfg(feature = "std")]
pub mod mapping;
pub mod schema;

#[cfg(feature = "std")]
use tokn_core::provider::{Endpoint, ModelInfo};

/// Build our internal model list for a given models.dev provider id.
///
/// Returns an empty Vec when the provider isn't in the catalogue (e.g. the
/// id changed in models.dev or the embedded snapshot is older than the
/// caller). Providers should treat this as best-effort metadata.
#[cfg(feature = "std")]
pub fn default_models_for(provider_id: &str) -> Vec<ModelInfo> {
  let cat = loader::global();
  let Some(p) = cat.get(provider_id) else {
    return Vec::new();
  };
  p.models.values().map(mapping::to_model_info).collect()
}

/// Look up one model by `(provider_id, model_id)` and convert. None if either
/// key is absent. Useful for enriching upstream model listings with catalogue
/// metadata. Neither source guarantees that a particular account can use a model.
#[allow(dead_code)]
#[cfg(feature = "std")]
pub fn model_info_for(provider_id: &str, model_id: &str) -> Option<ModelInfo> {
  loader::global()
    .get(provider_id)?
    .models
    .get(model_id)
    .map(mapping::to_model_info)
}

/// Return the endpoint implied by models.dev's provider adapter metadata.
/// This reads the current in-process snapshot, so a periodic catalogue refresh
/// can change routing without restarting the gateway.
#[cfg(feature = "std")]
pub fn endpoint_for_model(provider_id: &str, model_id: &str) -> Option<Endpoint> {
  loader::global().get(provider_id)?.endpoint_for_model(model_id)
}

#[cfg(all(test, feature = "std"))]
mod tests {
  use super::*;

  #[test]
  fn copilot_model_count_is_nontrivial() {
    let v = default_models_for("github-copilot");
    assert!(v.len() >= 5, "got only {} copilot models", v.len());
  }

  #[test]
  fn zai_coding_plan_models_present() {
    let v = default_models_for("zai-coding-plan");
    assert!(!v.is_empty(), "zai-coding-plan should expose models");
    assert!(v.iter().any(|m| m.id.starts_with("glm-")));
  }

  #[test]
  fn unknown_provider_yields_empty() {
    assert!(default_models_for("does-not-exist").is_empty());
  }

  #[test]
  fn lookup_specific_model() {
    // Pick a model id that's stable in models.dev: glm-4.5-air on zai.
    let mi = model_info_for("zai", "glm-4.5-air");
    assert!(mi.is_some(), "expected glm-4.5-air on zai");
  }
}
