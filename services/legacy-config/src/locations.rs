use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyHomeKind {
  ProjectDirs,
  AccidentalToknRouter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyHome {
  pub kind: LegacyHomeKind,
  pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocationAction {
  CopyFile { from: PathBuf, to: PathBuf },
  CopyDir { from: PathBuf, to: PathBuf },
}

const LEGACY_PROJECT_QUALIFIER: &str = "dev";
const LEGACY_PROJECT_ORGANIZATION: &str = "tokn-router";
const LEGACY_PROJECT_APPLICATION: &str = "tokn-router";
const ACCIDENTAL_APPLICATION: &str = "tokn-router";

pub fn discover() -> Vec<LegacyHome> {
  let mut homes = Vec::new();
  if let Some(dirs) = directories::ProjectDirs::from(
    LEGACY_PROJECT_QUALIFIER,
    LEGACY_PROJECT_ORGANIZATION,
    LEGACY_PROJECT_APPLICATION,
  ) {
    homes.push(LegacyHome {
      kind: LegacyHomeKind::ProjectDirs,
      path: dirs.config_dir().to_path_buf(),
    });
  }
  if let Some(dirs) = directories::ProjectDirs::from("", "", ACCIDENTAL_APPLICATION) {
    homes.push(LegacyHome {
      kind: LegacyHomeKind::AccidentalToknRouter,
      path: dirs.config_dir().to_path_buf(),
    });
  }
  homes
}

pub fn migrate(target_home: &Path) -> Result<Vec<LocationAction>> {
  let mut actions = Vec::new();
  for home in discover() {
    migrate_home(&home, target_home, &mut actions)?;
  }
  Ok(actions)
}

fn migrate_home(home: &LegacyHome, target_home: &Path, actions: &mut Vec<LocationAction>) -> Result<()> {
  if home.path == target_home || !home.path.exists() {
    return Ok(());
  }
  for entry in fs::read_dir(&home.path).with_context(|| format!("reading {}", home.path.display()))? {
    let entry = entry?;
    let from = entry.path();
    let to = target_home.join(entry.file_name());
    let file_type = entry.file_type()?;
    if to.exists() {
      continue;
    }
    if file_type.is_dir() {
      copy_dir_all(&from, &to)?;
      actions.push(LocationAction::CopyDir { from, to });
    } else if file_type.is_file() {
      if let Some(parent) = to.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
      }
      fs::copy(&from, &to).with_context(|| format!("copying {} to {}", from.display(), to.display()))?;
      actions.push(LocationAction::CopyFile { from, to });
    }
  }
  Ok(())
}

fn copy_dir_all(from: &Path, to: &Path) -> Result<()> {
  fs::create_dir_all(to).with_context(|| format!("creating {}", to.display()))?;
  for entry in fs::read_dir(from).with_context(|| format!("reading {}", from.display()))? {
    let entry = entry?;
    let child_from = entry.path();
    let child_to = to.join(entry.file_name());
    let file_type = entry.file_type()?;
    if file_type.is_dir() {
      copy_dir_all(&child_from, &child_to)?;
    } else if file_type.is_file() && !child_to.exists() {
      fs::copy(&child_from, &child_to)
        .with_context(|| format!("copying {} to {}", child_from.display(), child_to.display()))?;
    }
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn discover_reports_legacy_home_candidates() {
    let homes = discover();

    assert!(homes
      .iter()
      .any(|home| matches!(home.kind, LegacyHomeKind::ProjectDirs)));
    assert!(homes
      .iter()
      .any(|home| matches!(home.kind, LegacyHomeKind::AccidentalToknRouter)));
  }

  #[test]
  fn migrate_copies_missing_files_and_skips_existing() {
    let old = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    fs::write(old.path().join("config.toml"), "old").unwrap();
    fs::write(target.path().join("auth.yaml"), "new").unwrap();
    fs::write(old.path().join("auth.yaml"), "old").unwrap();

    let home = LegacyHome {
      kind: LegacyHomeKind::ProjectDirs,
      path: old.path().to_path_buf(),
    };
    let mut actions = Vec::new();
    migrate_home(&home, target.path(), &mut actions).unwrap();

    assert_eq!(fs::read_to_string(target.path().join("config.toml")).unwrap(), "old");
    assert_eq!(fs::read_to_string(target.path().join("auth.yaml")).unwrap(), "new");
    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], LocationAction::CopyFile { .. }));
  }

  #[test]
  fn migrate_home_copies_nested_directories() {
    let old = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    fs::create_dir_all(old.path().join("requests/nested")).unwrap();
    fs::write(old.path().join("requests/nested/day.db"), "db").unwrap();

    let home = LegacyHome {
      kind: LegacyHomeKind::ProjectDirs,
      path: old.path().to_path_buf(),
    };
    let mut actions = Vec::new();
    migrate_home(&home, target.path(), &mut actions).unwrap();

    assert_eq!(
      fs::read_to_string(target.path().join("requests/nested/day.db")).unwrap(),
      "db"
    );
    assert!(matches!(actions.as_slice(), [LocationAction::CopyDir { .. }]));
  }

  #[test]
  fn migrate_home_noops_for_missing_or_target_home() {
    let missing = tempfile::tempdir().unwrap();
    let missing_path = missing.path().join("missing");
    let target = tempfile::tempdir().unwrap();
    let mut actions = Vec::new();

    migrate_home(
      &LegacyHome {
        kind: LegacyHomeKind::ProjectDirs,
        path: missing_path,
      },
      target.path(),
      &mut actions,
    )
    .unwrap();
    migrate_home(
      &LegacyHome {
        kind: LegacyHomeKind::ProjectDirs,
        path: target.path().to_path_buf(),
      },
      target.path(),
      &mut actions,
    )
    .unwrap();

    assert!(actions.is_empty());
  }
}
