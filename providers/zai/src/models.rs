//! Z.ai-specific overlays applied on top of the [models.dev] catalogue.
//!
//! [models.dev]: https://models.dev
//!
//! models.dev provides accurate cost / context / capability data for every
//! GLM model on all four Z.ai aliases (`zai`, `zai-coding-plan`, `zhipuai`,
//! `zhipuai-coding-plan`). The one Z.ai-specific quirk it can't express is
//! the wire-side "interleaved reasoning" channel: certain GLM models stream
//! reasoning text alongside tool calls in a `reasoning_content` side-field.
//! That signal is consumed by clients like opencode and Claude Code.
//!
//! This module owns the small id → field map and applies it as a
//! post-processing pass on the catalogue lookup.

use crate::catalogue;
use crate::provider::{Interleaved, ModelInfo};

/// GLM model ids that ship `reasoning_content` interleaved with content.
///
/// Membership matches the historical hand-curated catalogue (see git history
/// for `provider/zai/models.rs` pre-catalogue-refactor).
const INTERLEAVED_REASONING_IDS: &[&str] = &["glm-5", "glm-5-turbo", "glm-5.1", "glm-5v-turbo", "glm-4.7"];

/// Build the model list for one of the four Z.ai aliases by reading the
/// global catalogue and applying our overlays.
pub fn catalogue_for(alias: &str) -> Vec<ModelInfo> {
  let mut models = catalogue::default_models_for(alias);
  for m in &mut models {
    apply_interleaved_overlay(m);
  }
  models
}

/// Backwards-compat shim used by older callers (e.g. tests). Returns the
/// PAYG zai catalogue.
#[allow(dead_code)]
pub fn catalogue() -> Vec<ModelInfo> {
  catalogue_for(crate::provider::ID_ZAI)
}

fn apply_interleaved_overlay(m: &mut ModelInfo) {
  if INTERLEAVED_REASONING_IDS.contains(&m.id.as_str()) {
    m.capabilities.interleaved = Interleaved::Field {
      field: "reasoning_content".into(),
    };
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::provider::{ID_ZAI, ID_ZAI_CODING_PLAN, ID_ZHIPUAI, ID_ZHIPUAI_CODING_PLAN};

  #[test]
  fn coding_plan_is_free() {
    let v = catalogue_for(ID_ZAI_CODING_PLAN);
    assert!(!v.is_empty(), "coding plan should expose models");
    for m in v {
      let c = m.cost.expect("coding plan models declare cost");
      assert_eq!(c.input, 0.0, "{} input not free", m.id);
      assert_eq!(c.output, 0.0, "{} output not free", m.id);
    }
  }

  #[test]
  fn zhipuai_marks_up_glm51_and_5v_turbo() {
    let z = catalogue_for(ID_ZAI);
    let zh = catalogue_for(ID_ZHIPUAI);
    let z51 = z.iter().find(|m| m.id == "glm-5.1").unwrap().cost.as_ref().unwrap();
    let zh51 = zh.iter().find(|m| m.id == "glm-5.1").unwrap().cost.as_ref().unwrap();
    assert!(
      zh51.input > z51.input * 3.0,
      "zhipuai glm-5.1 should be markedly pricier"
    );
  }

  #[test]
  fn alias_dispatch_picks_right_catalogue() {
    for alias in [ID_ZAI_CODING_PLAN, ID_ZHIPUAI_CODING_PLAN] {
      let models = catalogue_for(alias);
      assert!(models.len() >= 3, "{alias} should expose coding-plan models");
      assert!(models.iter().any(|m| m.id == "glm-5.1"), "{alias} missing glm-5.1");
      assert!(models.iter().any(|m| m.id == "glm-4.7"), "{alias} missing glm-4.7");
    }
    assert!(catalogue_for(ID_ZAI).len() >= 12);
    assert!(catalogue_for(ID_ZHIPUAI).len() >= 11);
  }

  #[test]
  fn all_glm_models_advertise_reasoning() {
    for m in catalogue_for(ID_ZAI) {
      assert!(m.capabilities.reasoning, "{} should advertise reasoning", m.id);
    }
  }

  #[test]
  fn vision_models_have_attachment_and_image_input() {
    let v = catalogue_for(ID_ZAI);
    for id in ["glm-4.5v", "glm-4.6v", "glm-5v-turbo"] {
      let m = v.iter().find(|m| m.id == id).expect(id);
      assert!(m.capabilities.attachment, "{id} attachment");
      assert!(m.capabilities.input.image, "{id} image input");
    }
  }

  #[test]
  fn interleaved_overlay_applies_to_known_ids() {
    let v = catalogue_for(ID_ZAI);
    let glm47 = v.iter().find(|m| m.id == "glm-4.7").unwrap();
    assert!(matches!(
        glm47.capabilities.interleaved,
        Interleaved::Field { ref field } if field == "reasoning_content"
    ));
    let glm46 = v.iter().find(|m| m.id == "glm-4.6").unwrap();
    assert!(matches!(glm46.capabilities.interleaved, Interleaved::Disabled(false)));
  }
}
