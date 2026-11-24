//! Stamp only the executable with Git metadata. Shared crates receive this
//! metadata at runtime, so Git changes do not invalidate the dependency graph.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
  let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR missing"));
  let workspace_root = manifest_dir
    .join("../..")
    .canonicalize()
    .unwrap_or_else(|_| manifest_dir.join("../.."));
  let version_path = workspace_root.join("VERSION");

  println!("cargo:rerun-if-changed={}", version_path.display());

  // Resolve each Git path separately: linked worktrees keep HEAD/index locally
  // but share refs and packed-refs with the main repository.
  for name in ["HEAD", "index", "refs", "packed-refs"] {
    if let Some(path) = git_path(&workspace_root, name) {
      if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
      }
    }
  }

  let base_version = fs::read_to_string(&version_path)
    .ok()
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty())
    .unwrap_or_else(|| format!("v{}", env!("CARGO_PKG_VERSION")));
  let commit_id = git_output(&workspace_root, &["rev-parse", "--short=7", "HEAD"]).unwrap_or_else(|| "unknown".into());
  let is_dirty = git_output(&workspace_root, &["status", "--porcelain"]).is_some_and(|out| !out.trim().is_empty());
  let full_version = if is_dirty {
    format!("{base_version}+{commit_id}+dev")
  } else {
    format!("{base_version}+{commit_id}")
  };

  println!("cargo:rustc-env=tokn_ROUTER_BASE_VERSION={base_version}");
  println!("cargo:rustc-env=tokn_ROUTER_COMMIT_ID={commit_id}");
  println!("cargo:rustc-env=tokn_ROUTER_VERSION={full_version}");
  println!(
    "cargo:rustc-env=tokn_ROUTER_VERSION_DIRTY={}",
    if is_dirty { "1" } else { "0" }
  );
}

fn git_path(workspace_root: &Path, name: &str) -> Option<PathBuf> {
  git_output(workspace_root, &["rev-parse", "--git-path", name]).map(|path| {
    let path = PathBuf::from(path);
    if path.is_absolute() {
      path
    } else {
      workspace_root.join(path)
    }
  })
}

fn git_output(workspace_root: &Path, args: &[&str]) -> Option<String> {
  let output = Command::new("git")
    // In particular, `status` must not refresh the index that this script watches.
    .arg("--no-optional-locks")
    .args(args)
    .current_dir(workspace_root)
    .output()
    .ok()?;
  if !output.status.success() {
    return None;
  }
  let stdout = String::from_utf8(output.stdout).ok()?;
  let trimmed = stdout.trim();
  if trimmed.is_empty() {
    None
  } else {
    Some(trimmed.to_string())
  }
}
