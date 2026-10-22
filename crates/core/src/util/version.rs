//! Runtime build metadata supplied by the application, with a package-version
//! fallback for library users. Git state must not be a build input to core.

use alloc::string::String;
#[cfg(feature = "std")]
use std::sync::OnceLock;

/// Package-version defaults; use the accessor functions for application metadata.
pub const BASE: &str = concat!("v", env!("CARGO_PKG_VERSION"));
pub const COMMIT_ID: &str = "unknown";
pub const FULL: &str = BASE;

#[derive(Debug, Clone, Copy)]
pub struct BuildVersion {
  pub base: &'static str,
  pub commit_id: &'static str,
  pub full: &'static str,
  pub dirty: bool,
}

#[cfg(feature = "std")]
static APPLICATION_VERSION: OnceLock<BuildVersion> = OnceLock::new();

/// Install the application's build metadata before starting any workers.
/// A second installation is rejected so metadata stays consistent during a run.
#[cfg(feature = "std")]
pub fn install(version: BuildVersion) -> Result<(), BuildVersion> {
  APPLICATION_VERSION.set(version)
}

pub fn base() -> &'static str {
  application_version().map_or(BASE, |version| version.base)
}

pub fn commit_id() -> &'static str {
  application_version().map_or(COMMIT_ID, |version| version.commit_id)
}

pub fn full() -> &'static str {
  application_version().map_or(FULL, |version| version.full)
}

pub fn is_dirty() -> bool {
  application_version().is_some_and(|version| version.dirty)
}

pub fn tokn_router_user_agent() -> String {
  format!("tokn-router/{}", full())
}

#[cfg(feature = "std")]
fn application_version() -> Option<&'static BuildVersion> {
  APPLICATION_VERSION.get()
}
#[cfg(not(feature = "std"))]
fn application_version() -> Option<&'static BuildVersion> {
  None
}
