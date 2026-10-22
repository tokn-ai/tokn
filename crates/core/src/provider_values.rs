use crate::{
  headers::{AgentId, HeaderMap, TemplateVars},
  Endpoint,
};
use serde_json::Value;

pub const ID_GITHUB_COPILOT: &str = "github-copilot";
pub const ID_DEEPSEEK: &str = "deepseek";
pub const ID_LLAMA_CPP: &str = "llama-cpp";
pub const ID_OPENAI: &str = "openai";
pub const ID_OPENCODE_GO: &str = "opencode-go";
pub const ID_CODEX: &str = "codex";
pub const ID_ZAI_CODING_PLAN: &str = "zai-coding-plan";
pub const ID_ZAI: &str = "zai";
pub const ID_ZHIPUAI_CODING_PLAN: &str = "zhipuai-coding-plan";
pub const ID_ZHIPUAI: &str = "zhipuai";
pub const ZAI_PROVIDERS: &[&str] = &[ID_ZAI_CODING_PLAN, ID_ZAI, ID_ZHIPUAI_CODING_PLAN, ID_ZHIPUAI];

/// One built-in provider destination made available by v2 without an
/// explicit `[providers.*]` table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfficialProviderPreset {
  pub id: &'static str,
  pub driver: &'static str,
}

pub const OFFICIAL_PROVIDER_PRESETS: &[OfficialProviderPreset] = &[
  OfficialProviderPreset {
    id: ID_CODEX,
    driver: ID_CODEX,
  },
  OfficialProviderPreset {
    id: ID_DEEPSEEK,
    driver: ID_DEEPSEEK,
  },
  OfficialProviderPreset {
    id: ID_GITHUB_COPILOT,
    driver: ID_GITHUB_COPILOT,
  },
  OfficialProviderPreset {
    id: ID_LLAMA_CPP,
    driver: ID_LLAMA_CPP,
  },
  OfficialProviderPreset {
    id: ID_OPENAI,
    driver: ID_OPENAI,
  },
  OfficialProviderPreset {
    id: ID_OPENCODE_GO,
    driver: ID_OPENCODE_GO,
  },
  OfficialProviderPreset {
    id: ID_ZAI,
    driver: ID_ZAI,
  },
  OfficialProviderPreset {
    id: ID_ZAI_CODING_PLAN,
    driver: ID_ZAI,
  },
  OfficialProviderPreset {
    id: ID_ZHIPUAI,
    driver: ID_ZAI,
  },
  OfficialProviderPreset {
    id: ID_ZHIPUAI_CODING_PLAN,
    driver: ID_ZAI,
  },
];

pub fn official_provider_preset(id: &str) -> Option<&'static OfficialProviderPreset> {
  OFFICIAL_PROVIDER_PRESETS.iter().find(|preset| preset.id == id)
}

/// A prepared provider request independent of the HTTP implementation.
#[derive(Clone, Debug)]
pub struct PreparedRequest {
  pub url: alloc::string::String,
  pub headers: crate::headers::HeaderMap,
  pub body: crate::types::Bytes,
  pub endpoint: crate::Endpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderRequestKind {
  Operation(Endpoint),
  Models,
  Opaque,
}

impl ProviderRequestKind {
  pub fn endpoint(self) -> Option<Endpoint> {
    match self {
      Self::Operation(endpoint) => Some(endpoint),
      Self::Models | Self::Opaque => None,
    }
  }

  pub fn from_provider_path(path: &str) -> Self {
    let path_only = path.split('?').next().unwrap_or(path).trim_end_matches('/');
    if path_only.ends_with("/models") {
      Self::Models
    } else {
      Self::Opaque
    }
  }
}

pub struct HeaderPatchCtx<'a> {
  pub request_kind: ProviderRequestKind,
  pub body: &'a Value,
  pub bearer_token: Option<&'a str>,
  pub content_encoding: Option<&'a str>,
  pub stream: bool,
  pub initiator: &'a str,
  pub inbound_headers: &'a HeaderMap,
  pub vars: &'a TemplateVars,
  pub agent_id: &'a AgentId,
}

impl HeaderPatchCtx<'_> {
  pub fn endpoint(&self) -> Option<Endpoint> {
    self.request_kind.endpoint()
  }

  pub fn operation_endpoint(&self) -> Endpoint {
    self
      .endpoint()
      .expect("generation endpoint required for operation header patching")
  }
}
