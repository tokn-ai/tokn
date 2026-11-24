use core::time::Duration;
use serde::{Deserialize, Serialize};

/// Background model discovery while the gateway is serving requests.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RawModelRefresh {
  pub enabled: bool,
  pub upstream_refresh_seconds: u64,
  pub catalogue_refresh_seconds: u64,
  pub request_timeout_seconds: u64,
}

impl Default for RawModelRefresh {
  fn default() -> Self {
    Self {
      enabled: true,
      upstream_refresh_seconds: 300,
      catalogue_refresh_seconds: 86_400,
      request_timeout_seconds: 30,
    }
  }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelRefreshPlan {
  enabled: bool,
  upstream_interval: Duration,
  catalogue_interval: Duration,
  request_timeout: Duration,
}

impl Default for ModelRefreshPlan {
  fn default() -> Self {
    Self::compile(&RawModelRefresh::default()).expect("default model refresh settings are valid")
  }
}

impl ModelRefreshPlan {
  pub(super) fn compile(raw: &RawModelRefresh) -> Result<Self, super::CompileError> {
    fn duration(field: &str, seconds: u64) -> Result<Duration, super::CompileError> {
      let value = Duration::from_secs(seconds);
      #[cfg(feature = "std")]
      let representable = std::time::Instant::now().checked_add(value).is_some();
      #[cfg(not(feature = "std"))]
      let representable = seconds <= i64::MAX as u64;
      if seconds == 0 || !representable {
        return Err(super::CompileError::InvalidValue {
          location: format!("service.models.{field}"),
          message: "must be a positive, representable time duration".into(),
        });
      }
      Ok(value)
    }
    Ok(Self {
      enabled: raw.enabled,
      upstream_interval: duration("upstream_refresh_seconds", raw.upstream_refresh_seconds)?,
      catalogue_interval: duration("catalogue_refresh_seconds", raw.catalogue_refresh_seconds)?,
      request_timeout: duration("request_timeout_seconds", raw.request_timeout_seconds)?,
    })
  }

  pub const fn enabled(self) -> bool {
    self.enabled
  }

  pub const fn upstream_interval(self) -> Duration {
    self.upstream_interval
  }

  pub const fn catalogue_interval(self) -> Duration {
    self.catalogue_interval
  }

  pub const fn request_timeout(self) -> Duration {
    self.request_timeout
  }
}

#[cfg(all(test, feature = "std"))]
mod tests {
  use super::*;

  #[test]
  fn refresh_settings_default_and_round_trip() {
    let config = super::super::parse_config(
      "schema_version = 2\n[defaults]\n[listeners.api]\nkind = 'llm_api'\nbind = '127.0.0.1:4141'\nclient_auth = 'none'",
      std::path::Path::new("test.toml"),
    ).unwrap();
    assert_eq!(config.service().models(), ModelRefreshPlan::default());
    let raw: RawModelRefresh = toml::from_str("enabled = false\nupstream_refresh_seconds = 60").unwrap();
    let plan = ModelRefreshPlan::compile(&raw).unwrap();
    assert!(!plan.enabled());
    assert_eq!(plan.upstream_interval(), Duration::from_secs(60));
    assert_eq!(plan.catalogue_interval(), Duration::from_secs(86_400));
    assert_eq!(
      toml::from_str::<RawModelRefresh>(&toml::to_string(&raw).unwrap()).unwrap(),
      raw
    );
  }

  #[test]
  fn invalid_refresh_durations_and_unknown_fields_are_rejected() {
    for field in [
      "upstream_refresh_seconds",
      "catalogue_refresh_seconds",
      "request_timeout_seconds",
    ] {
      for value in [0, u64::MAX] {
        let mut raw = serde_json::to_value(RawModelRefresh::default()).unwrap();
        raw[field] = value.into();
        let raw = serde_json::from_value(raw).unwrap();
        assert!(ModelRefreshPlan::compile(&raw).is_err(), "{field}={value}");
      }
    }
    assert!(toml::from_str::<RawModelRefresh>("unknown = 5").is_err());
  }
}
