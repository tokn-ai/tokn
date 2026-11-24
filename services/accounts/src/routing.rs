use std::collections::HashMap;
use tokn_config::{ModelFamily, RouteMode};

const ROUTE_MODE_HEADER: &str = "x-route-mode";

#[derive(Clone, Debug)]
pub struct RouteResolver {
  default_mode: RouteMode,
  default_provider_id: Option<String>,
  families_by_name: HashMap<String, Vec<String>>,
  family_members: HashMap<String, Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteResolution {
  pub mode: RouteMode,
  pub requested_model: String,
  pub upstream_model: String,
  pub selector: RouteSelector,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteSelector {
  Any,
  Provider(String),
  Model,
  Fuzzy { candidates: Vec<String> },
}

impl RouteResolver {
  pub fn new(default_mode: RouteMode, families: &[ModelFamily]) -> Self {
    Self::with_default_provider(default_mode, None, families)
  }

  pub fn with_default_provider(
    default_mode: RouteMode,
    default_provider_id: Option<String>,
    families: &[ModelFamily],
  ) -> Self {
    let mut families_by_name = HashMap::new();
    let mut family_members = HashMap::new();
    for family in families {
      let members = family.members.clone();
      families_by_name.insert(family.name.clone(), members.clone());
      for member in &members {
        family_members.insert(member.clone(), members.clone());
      }
    }
    Self {
      default_mode,
      default_provider_id,
      families_by_name,
      family_members,
    }
  }

  pub fn mode_header() -> &'static str {
    ROUTE_MODE_HEADER
  }

  pub fn resolve_mode(&self, header_mode: Option<&str>) -> Result<RouteMode, ResolveError> {
    match header_mode {
      Some(raw) => parse_route_mode(raw),
      None => Ok(self.default_mode),
    }
  }

  pub fn resolve(&self, requested_model: &str, header_mode: Option<&str>) -> Result<RouteResolution, ResolveError> {
    let mode = self.resolve_mode(header_mode)?;
    match mode {
      RouteMode::Passthrough => Ok(RouteResolution {
        mode,
        requested_model: requested_model.to_string(),
        upstream_model: requested_model.to_string(),
        selector: self
          .default_provider_id
          .clone()
          .map(RouteSelector::Provider)
          .unwrap_or(RouteSelector::Any),
      }),
      RouteMode::Switch => Ok(RouteResolution {
        mode,
        requested_model: requested_model.to_string(),
        upstream_model: requested_model.to_string(),
        selector: self
          .default_provider_id
          .clone()
          .map(RouteSelector::Provider)
          .unwrap_or(RouteSelector::Any),
      }),
      RouteMode::Exact => {
        let (provider, model) = requested_model
          .split_once('/')
          .ok_or_else(|| ResolveError::InvalidExactModel {
            model: requested_model.to_string(),
          })?;
        if provider.trim().is_empty() || model.trim().is_empty() {
          return Err(ResolveError::InvalidExactModel {
            model: requested_model.to_string(),
          });
        }
        Ok(RouteResolution {
          mode,
          requested_model: requested_model.to_string(),
          upstream_model: model.to_string(),
          selector: RouteSelector::Provider(provider.to_string()),
        })
      }
      RouteMode::Route => Ok(RouteResolution {
        mode,
        requested_model: requested_model.to_string(),
        upstream_model: requested_model.to_string(),
        selector: RouteSelector::Model,
      }),
      RouteMode::Fuzzy => {
        let candidates = self
          .family_members
          .get(requested_model)
          .cloned()
          .or_else(|| self.families_by_name.get(requested_model).cloned())
          .unwrap_or_else(|| vec![requested_model.to_string()]);
        Ok(RouteResolution {
          mode,
          requested_model: requested_model.to_string(),
          upstream_model: requested_model.to_string(),
          selector: RouteSelector::Fuzzy { candidates },
        })
      }
    }
  }
}

pub fn route_mode_as_str(mode: RouteMode) -> &'static str {
  match mode {
    RouteMode::Passthrough => "passthrough",
    RouteMode::Switch => "switch",
    RouteMode::Exact => "exact",
    RouteMode::Route => "route",
    RouteMode::Fuzzy => "fuzzy",
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
  InvalidRouteMode { mode: String },
  InvalidExactModel { model: String },
}

impl std::fmt::Display for ResolveError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      ResolveError::InvalidRouteMode { mode } => {
        write!(f, "invalid route mode '{mode}'")
      }
      ResolveError::InvalidExactModel { model } => {
        write!(f, "exact mode requires model in 'provider/model' form, got '{model}'")
      }
    }
  }
}

impl std::error::Error for ResolveError {}

fn parse_route_mode(raw: &str) -> Result<RouteMode, ResolveError> {
  match raw.trim().to_ascii_lowercase().as_str() {
    "passthrough" => Ok(RouteMode::Passthrough),
    "switch" => Ok(RouteMode::Switch),
    "exact" => Ok(RouteMode::Exact),
    "route" => Ok(RouteMode::Route),
    "fuzzy" => Ok(RouteMode::Fuzzy),
    mode => Err(ResolveError::InvalidRouteMode { mode: mode.to_string() }),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn exact_strips_provider_prefix() {
    let resolver = RouteResolver::new(RouteMode::Route, &[]);
    let resolved = resolver.resolve("github-copilot/gpt-4o", Some("exact")).unwrap();
    assert_eq!(resolved.upstream_model, "gpt-4o");
    assert_eq!(resolved.selector, RouteSelector::Provider("github-copilot".into()));
  }

  #[test]
  fn fuzzy_uses_family_members() {
    let resolver = RouteResolver::new(
      RouteMode::Fuzzy,
      &[ModelFamily {
        name: "claude-sonnet".into(),
        members: vec!["claude-sonnet-4".into(), "claude-3-5-sonnet".into()],
      }],
    );
    let resolved = resolver.resolve("claude-sonnet", None).unwrap();
    assert_eq!(
      resolved.selector,
      RouteSelector::Fuzzy {
        candidates: vec!["claude-sonnet-4".into(), "claude-3-5-sonnet".into()]
      }
    );
  }

  #[test]
  fn parses_switch_mode() {
    let resolver = RouteResolver::new(RouteMode::Route, &[]);
    let resolved = resolver.resolve("gpt-4o", Some("switch")).unwrap();
    assert_eq!(resolved.mode, RouteMode::Switch);
    assert_eq!(resolved.upstream_model, "gpt-4o");
  }

  #[test]
  fn passthrough_and_switch_use_default_provider_when_configured() {
    let resolver = RouteResolver::with_default_provider(RouteMode::Passthrough, Some("openai".into()), &[]);
    let resolved = resolver.resolve("gpt-4o", None).unwrap();
    assert_eq!(resolved.selector, RouteSelector::Provider("openai".into()));

    let resolved = resolver.resolve("gpt-4o", Some("switch")).unwrap();
    assert_eq!(resolved.selector, RouteSelector::Provider("openai".into()));
  }
}
