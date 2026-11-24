//! CLI-owned rollout defaults and immutable policy loading.
use anyhow::{Context, Result};
use clap::Args;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokn_router::routing::{RolloutPolicy, RolloutStage};

#[derive(Args, Clone, Debug, Default, Serialize, Deserialize)]
pub struct RolloutArgs {
  /// Start this worker using a rollout policy (default: 10% to 90% over 24h, then 100%).
  #[arg(long)]
  #[serde(default)]
  pub ab_test: bool,
  /// Duration for the default linear policy, e.g. 36h or 72h. Set only at startup.
  #[arg(long, requires = "ab_test", conflicts_with = "ab_test_policy", value_parser = parse_duration)]
  pub ab_test_duration: Option<Duration>,
  /// TOML file defining initial_percent, stages, and completion_percent.
  #[arg(long, requires = "ab_test", conflicts_with = "ab_test_duration")]
  pub ab_test_policy: Option<PathBuf>,
}

pub fn default_policy(duration: Duration) -> RolloutPolicy {
  RolloutPolicy {
    initial_percent: 10,
    stages: vec![RolloutStage {
      duration_seconds: duration.as_secs(),
      traffic_percent: 90,
    }],
    completion_percent: 100,
  }
}

pub fn default_duration() -> Duration {
  Duration::from_secs(24 * 60 * 60)
}

fn parse_duration(value: &str) -> Result<Duration, String> {
  let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
  if duration.as_secs() == 0 || duration.subsec_nanos() != 0 {
    return Err("rollout duration must be positive whole seconds".into());
  }
  Ok(duration)
}

impl RolloutArgs {
  pub fn policy(&self) -> Result<Option<RolloutPolicy>> {
    if !self.ab_test {
      anyhow::ensure!(
        self.ab_test_duration.is_none() && self.ab_test_policy.is_none(),
        "rollout options require --ab-test"
      );
      return Ok(None);
    }
    anyhow::ensure!(
      self.ab_test_duration.is_none() || self.ab_test_policy.is_none(),
      "choose either --ab-test-duration or --ab-test-policy"
    );
    let policy = match &self.ab_test_policy {
      Some(path) => {
        let source =
          std::fs::read_to_string(path).with_context(|| format!("read rollout policy {}", path.display()))?;
        toml::from_str(&source).with_context(|| format!("parse rollout policy {}", path.display()))?
      }
      None => default_policy(self.ab_test_duration.unwrap_or_else(default_duration)),
    };
    policy.validate()?;
    Ok(Some(policy))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn policy_file_is_validated_and_loaded_as_an_immutable_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rollout.toml");
    let source = r#"
initial_percent = 5
completion_percent = 100
[[stages]]
duration_seconds = 21600
traffic_percent = 5
[[stages]]
duration_seconds = 237600
traffic_percent = 90
"#;
    std::fs::write(&path, source).unwrap();
    let args = RolloutArgs {
      ab_test: true,
      ab_test_policy: Some(path.clone()),
      ..Default::default()
    };
    let initialized = args.policy().unwrap().unwrap();
    assert_eq!(initialized.validate().unwrap(), 72 * 3600);
    assert_eq!(initialized.stages.len(), 2);
    std::fs::write(&path, source.replace("initial_percent = 5", "initial_percent = 25")).unwrap();
    assert_eq!(initialized.initial_percent, 5);
    assert_eq!(args.policy().unwrap().unwrap().initial_percent, 25);
    std::fs::write(&path, source.replace("traffic_percent = 90", "traffic_percent = 100")).unwrap();
    assert!(args.policy().is_err());
    std::fs::write(&path, source.replace("initial_percent = 5", "initial_percentage = 5")).unwrap();
    assert!(args.policy().is_err());
  }
}
