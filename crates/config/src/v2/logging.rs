#[cfg(feature = "std")]
use crate::{LogFormat, LogTarget, LoggingConfig};
use alloc::string::String;
use serde::{Deserialize, Serialize};
#[cfg(feature = "std")]
use std::path::PathBuf;
#[cfg(not(feature = "std"))]
type PathBuf = String;
#[cfg(not(feature = "std"))]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
  Pretty,
  #[default]
  Compact,
  Json,
}
#[cfg(not(feature = "std"))]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogTarget {
  Stderr,
  File,
  #[default]
  Both,
}

/// Strict v2 logging syntax with the same defaults as legacy logging.
///
/// Keep this boundary separate from `LoggingConfig` so v2 rejects unknown
/// fields without tightening the legacy configuration parser.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RawLogging {
  pub level: String,
  pub format: LogFormat,
  pub target: LogTarget,
  pub dir: Option<PathBuf>,
  pub ansi: bool,
  pub include_spans: bool,
}

impl Default for RawLogging {
  fn default() -> Self {
    #[cfg(feature = "std")]
    {
      LoggingConfig::default().into()
    }
    #[cfg(not(feature = "std"))]
    {
      Self {
        level: "info,tokn_router=info".into(),
        format: LogFormat::default(),
        target: LogTarget::default(),
        dir: None,
        ansi: true,
        include_spans: false,
      }
    }
  }
}

#[cfg(feature = "std")]
impl From<LoggingConfig> for RawLogging {
  fn from(config: LoggingConfig) -> Self {
    Self {
      level: config.level,
      format: config.format,
      target: config.target,
      dir: config.dir,
      ansi: config.ansi,
      include_spans: config.include_spans,
    }
  }
}

#[cfg(feature = "std")]
impl From<&RawLogging> for LoggingConfig {
  fn from(raw: &RawLogging) -> Self {
    Self {
      level: raw.level.clone(),
      format: raw.format,
      target: raw.target,
      dir: raw.dir.clone(),
      ansi: raw.ansi,
      include_spans: raw.include_spans,
    }
  }
}

#[cfg(all(test, feature = "std"))]
mod tests {
  use super::*;
  use crate::v2;
  use std::path::Path;

  const MINIMAL_CONFIG: &str = r#"
schema_version = 2
[listeners.local]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"
"#;

  #[test]
  fn omitted_and_partial_logging_keep_legacy_defaults() {
    for settings in ["", "[service.logging]"] {
      let source = format!("{MINIMAL_CONFIG}\n{settings}");
      let config = v2::parse_config(&source, Path::new("config.toml")).unwrap();
      assert_eq!(config.service().logging(), &LoggingConfig::default());
    }
    let config = v2::parse_config(
      &format!("{MINIMAL_CONFIG}\n[service.logging]\ntarget = 'stderr'"),
      Path::new("config.toml"),
    )
    .unwrap();
    assert_eq!(
      config.service().logging(),
      &LoggingConfig {
        target: LogTarget::Stderr,
        ..LoggingConfig::default()
      }
    );
  }

  #[test]
  fn logging_round_trip_preserves_every_setting() {
    let expected = LoggingConfig {
      level: "warn,tokn_router=debug".into(),
      format: LogFormat::Json,
      target: LogTarget::File,
      dir: Some(PathBuf::from("custom-logs")),
      ansi: false,
      include_spans: true,
    };
    let mut raw = v2::decode(MINIMAL_CONFIG, Path::new("config.toml")).unwrap();
    raw.service.logging = expected.clone().into();
    let rendered = toml::to_string_pretty(&raw).unwrap();
    let config = v2::parse_config(&rendered, Path::new("config.toml")).unwrap();
    assert_eq!(config.service().logging(), &expected);
  }

  #[test]
  fn v2_logging_rejects_unknown_fields_and_invalid_enums() {
    for field in [
      "levle = 'debug'",
      "format = 'invalid'",
      "target = 'invalid'",
      "ansi = 'false'",
    ] {
      let source = format!("{MINIMAL_CONFIG}\n[service.logging]\n{field}");
      assert!(v2::decode(&source, Path::new("config.toml")).is_err(), "{field}");
    }
  }
}
