//! Catalogue source resolution.
//!
//! Start with the disk cache or embedded snapshot, then publish validated
//! refreshes atomically. Readers retain an immutable snapshot for the duration
//! of their work, and failed refreshes preserve the last good copy.

use snafu::{ResultExt, Snafu};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use super::schema::Catalogue;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
  #[snafu(display("HTTP GET {url} failed"))]
  Fetch { url: String, source: reqwest::Error },

  #[snafu(display("read response body from {url}"))]
  ReadBody { url: String, source: reqwest::Error },

  #[snafu(display("{url} returned HTTP {status}"))]
  HttpStatus { url: String, status: reqwest::StatusCode },

  #[snafu(display("parse {url} as models.dev catalogue"))]
  Parse { url: String, source: serde_json::Error },

  #[snafu(display("{url} returned an empty catalogue"))]
  EmptyCatalogue { url: String },

  #[snafu(display("could not resolve a cache directory"))]
  NoCacheDir,

  #[snafu(display("create cache dir `{}`", path.display()))]
  CreateCacheDir { path: PathBuf, source: std::io::Error },

  #[snafu(display("write `{}`", path.display()))]
  Write { path: PathBuf, source: std::io::Error },

  #[snafu(display("atomic rename to `{}`", path.display()))]
  Rename { path: PathBuf, source: std::io::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Where the loaded catalogue came from. Surfaced by `update --status`.
#[derive(Debug, Clone)]
pub enum Source {
  /// Compile-time `include_bytes!` of `models.dev/api.json`.
  Embedded,
  /// On-disk JSON, written by `tokn-router update`.
  DiskCache(PathBuf),
}

/// The embedded snapshot, baked in by `build.rs`.
const EMBEDDED: &[u8] = include_bytes!(env!("MODELS_DEV_SNAPSHOT_PATH"));

static GLOBAL: OnceLock<CatalogueStore> = OnceLock::new();

struct CatalogueStore {
  snapshot: RwLock<(Arc<Catalogue>, Source)>,
}

impl CatalogueStore {
  fn new(catalogue: Catalogue, source: Source) -> Self {
    Self {
      snapshot: RwLock::new((Arc::new(catalogue), source)),
    }
  }

  fn snapshot(&self) -> (Arc<Catalogue>, Source) {
    self
      .snapshot
      .read()
      .unwrap_or_else(|poisoned| poisoned.into_inner())
      .clone()
  }

  fn persist(&self, catalogue: Catalogue, body: &[u8], path: &Path) -> Result<()> {
    let parent = path
      .parent()
      .filter(|parent| !parent.as_os_str().is_empty())
      .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).context(CreateCacheDirSnafu {
      path: parent.to_path_buf(),
    })?;

    // Unique sibling files prevent concurrent refreshes (including another
    // process running `update`) from overwriting each other's staged bytes.
    let mut staged = tempfile::Builder::new()
      .prefix(".tokn-catalogue-")
      .suffix(".tmp")
      .tempfile_in(parent)
      .context(WriteSnafu {
        path: path.to_path_buf(),
      })?;
    staged.write_all(body).context(WriteSnafu {
      path: staged.path().to_path_buf(),
    })?;
    staged.as_file().sync_all().context(WriteSnafu {
      path: staged.path().to_path_buf(),
    })?;

    // Keep the on-disk rename and in-process publication ordered together.
    // No await or network work occurs under this lock.
    let mut snapshot = self.snapshot.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    staged.persist(path).map_err(|error| Error::Rename {
      path: path.to_path_buf(),
      source: error.error,
    })?;
    *snapshot = (Arc::new(catalogue), Source::DiskCache(path.to_path_buf()));
    Ok(())
  }
}

/// Path of the on-disk catalogue cache, if we can determine an XDG cache dir.
pub fn cache_path() -> Option<PathBuf> {
  tokn_core::util::paths::cache_dir().map(|dir| dir.join("catalogue.json"))
}

/// Read an immutable snapshot of the global catalogue, loading it on first call.
///
/// Lookup order:
///   1. On-disk cache at [`cache_path`] — the result of a successful
///      `tokn-router update`.
///   2. The embedded snapshot — always present, always parses.
///
/// If the disk cache is empty or fails to parse we log a warning and fall back
/// to the embedded copy. Subsequent successful refreshes replace the snapshot;
/// previously returned snapshots remain valid. Reading never performs network
/// requests.
pub fn global() -> Arc<Catalogue> {
  global_with_source().0
}

/// Same as [`global`] but also returns where the data came from.
pub fn global_with_source() -> (Arc<Catalogue>, Source) {
  load_global().snapshot()
}

fn load_global() -> &'static CatalogueStore {
  GLOBAL.get_or_init(|| {
    let (cat, src) = match try_disk_cache() {
      Some((c, p)) => (c, Source::DiskCache(p)),
      None => (parse_embedded(), Source::Embedded),
    };
    let providers = cat.len();
    let models: usize = cat.values().map(|p| p.models.len()).sum();
    match &src {
      Source::DiskCache(p) => {
        tracing::info!(source = "disk_cache", path = %p.display(), providers, models, "models.dev catalogue loaded");
      }
      Source::Embedded => {
        tracing::info!(source = "embedded", providers, models, "models.dev catalogue loaded");
      }
    }
    CatalogueStore::new(cat, src)
  })
}

fn try_disk_cache() -> Option<(Catalogue, PathBuf)> {
  let path = cache_path()?;
  let bytes = std::fs::read(&path).ok()?;
  match parse_catalogue(&bytes, &path.display().to_string()) {
    Ok(c) => Some((c, path)),
    Err(e) => {
      tracing::warn!(
          cache = %path.display(),
          error = %e,
          "models.dev cache is invalid; falling back to embedded snapshot"
      );
      None
    }
  }
}

fn parse_embedded() -> Catalogue {
  serde_json::from_slice(EMBEDDED).expect("embedded models.dev snapshot must parse — fix build.rs")
}

fn parse_catalogue(body: &[u8], url: &str) -> Result<Catalogue> {
  let parsed: Catalogue = serde_json::from_slice(body).context(ParseSnafu { url: url.to_string() })?;
  if parsed.is_empty() || parsed.values().all(|provider| provider.models.is_empty()) {
    return EmptyCatalogueSnafu { url: url.to_string() }.fail();
  }
  Ok(parsed)
}

/// Outcome of a successful `tokn-router update` run.
#[derive(Debug)]
pub struct UpdateReport {
  pub providers: usize,
  pub models: usize,
  pub bytes: u64,
  pub path: PathBuf,
  pub elapsed: std::time::Duration,
}

/// Fetch `url`, validate, atomically replace the disk and in-process snapshots.
///
/// The fetched bytes must:
///   * parse as our [`Catalogue`] schema, and
///   * contain at least one model (defends against silent empty payloads).
///
/// HTTP, validation, or persistence failures preserve the previous snapshot.
#[tracing::instrument(name = "catalogue_update", skip_all, fields(%url, status = tracing::field::Empty, providers = tracing::field::Empty, models = tracing::field::Empty, bytes = tracing::field::Empty))]
pub async fn fetch_and_persist(http: &reqwest::Client, url: &str) -> Result<UpdateReport> {
  let path = cache_path().ok_or(Error::NoCacheDir)?;
  fetch_and_persist_to(http, url, &path, load_global()).await
}

async fn fetch_and_persist_to(
  http: &reqwest::Client,
  url: &str,
  path: &Path,
  store: &CatalogueStore,
) -> Result<UpdateReport> {
  let started = Instant::now();
  tracing::debug!("fetching catalogue");
  let resp = http
    .get(url)
    .send()
    .await
    .context(FetchSnafu { url: url.to_string() })?;
  let status = resp.status();
  tracing::Span::current().record("status", status.as_u16());
  let body = resp.bytes().await.context(ReadBodySnafu { url: url.to_string() })?;
  if !status.is_success() {
    return HttpStatusSnafu {
      url: url.to_string(),
      status,
    }
    .fail();
  }
  let parsed = parse_catalogue(&body, url)?;
  let providers = parsed.len();
  let models: usize = parsed.values().map(|p| p.models.len()).sum();
  let span = tracing::Span::current();
  span.record("providers", parsed.len());
  span.record("models", models);
  span.record("bytes", body.len() as u64);

  store.persist(parsed, &body, path)?;
  tracing::info!(path = %path.display(), providers, models, "catalogue updated");

  Ok(UpdateReport {
    providers,
    models,
    bytes: body.len() as u64,
    path: path.to_path_buf(),
    elapsed: started.elapsed(),
  })
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;
  use tokn_mock_server::{MockEndpoint, MockLlmConfig, MockLlmServer, MockResponse, MockRoute};

  fn catalogue_body(id: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"test": {"id": "test", "name": "Test", "models": {
      id: {"id": id, "name": id}
    }}}))
    .unwrap()
  }

  fn store_with(id: &str) -> CatalogueStore {
    CatalogueStore::new(
      parse_catalogue(&catalogue_body(id), "fixture").unwrap(),
      Source::Embedded,
    )
  }

  async fn serve(response: MockResponse) -> MockLlmServer {
    MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::new(MockEndpoint::Models, response))).await
  }

  #[tokio::test]
  async fn successful_refresh_replaces_disk_and_memory_while_existing_readers_keep_their_snapshot() {
    let store = store_with("old");
    let original = store.snapshot().0;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("catalogue.json");
    let body = catalogue_body("new");
    let server = serve(MockResponse::json(
      serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
    ))
    .await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let report = fetch_and_persist_to(&http, &format!("{}/models", server.base_url()), &path, &store)
      .await
      .unwrap();

    let (current, source) = store.snapshot();
    assert!(original["test"].models.contains_key("old"));
    assert!(!original["test"].models.contains_key("new"));
    assert!(current["test"].models.contains_key("new"));
    assert!(!current["test"].models.contains_key("old"));
    assert!(matches!(source, Source::DiskCache(ref saved) if saved == &path));
    assert_eq!(report.providers, 1);
    assert_eq!(report.models, 1);
    let disk = parse_catalogue(&std::fs::read(&path).unwrap(), "disk").unwrap();
    assert!(disk["test"].models.contains_key("new"));
  }

  #[tokio::test]
  async fn invalid_or_failed_refresh_preserves_last_good_memory_and_disk() {
    let store = store_with("good");
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("catalogue.json");
    let good = catalogue_body("good");
    std::fs::write(&path, &good).unwrap();
    let original = store.snapshot().0;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut unavailable = MockResponse::json(json!({"error": "unavailable"}));
    unavailable.status = reqwest::StatusCode::SERVICE_UNAVAILABLE;

    for response in [
      MockResponse::json(json!({})),
      MockResponse::json(json!({"test": {"id": "test", "name": "Test", "models": {}}})),
      MockResponse::json(json!({"test": {"bad": "schema"}})),
      MockResponse::sse("not json"),
      unavailable,
    ] {
      let server = serve(response).await;
      let result = fetch_and_persist_to(&http, &format!("{}/models", server.base_url()), &path, &store).await;
      assert!(result.is_err());
      assert!(Arc::ptr_eq(&original, &store.snapshot().0));
      assert_eq!(std::fs::read(&path).unwrap(), good);
    }
  }

  #[test]
  fn persistence_failure_does_not_publish_or_leave_staged_files() {
    let store = store_with("old");
    let original = store.snapshot().0;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("existing-directory");
    std::fs::create_dir(&path).unwrap();
    let body = catalogue_body("new");

    assert!(store
      .persist(parse_catalogue(&body, "fixture").unwrap(), &body, &path)
      .is_err());
    assert!(Arc::ptr_eq(&original, &store.snapshot().0));
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
  }

  #[test]
  fn concurrent_publications_keep_disk_and_memory_consistent() {
    let store = store_with("old");
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("catalogue.json");
    std::thread::scope(|scope| {
      for id in ["first", "second"] {
        let store = &store;
        let path = &path;
        scope.spawn(move || {
          let body = catalogue_body(id);
          store
            .persist(parse_catalogue(&body, "fixture").unwrap(), &body, path)
            .unwrap();
        });
      }
    });

    let disk = parse_catalogue(&std::fs::read(&path).unwrap(), "disk").unwrap();
    let current = store.snapshot().0;
    assert_eq!(
      disk["test"].models.keys().collect::<Vec<_>>(),
      current["test"].models.keys().collect::<Vec<_>>()
    );
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
  }

  #[test]
  fn embedded_snapshot_parses() {
    let c = parse_embedded();
    assert!(!c.is_empty(), "embedded snapshot must contain providers");
    assert!(c.contains_key("github-copilot"), "missing github-copilot");
    for id in ["zai", "zai-coding-plan", "zhipuai", "zhipuai-coding-plan"] {
      assert!(c.contains_key(id), "missing provider {id}");
    }
  }

  #[test]
  fn copilot_has_models() {
    let c = parse_embedded();
    let p = c.get("github-copilot").unwrap();
    assert!(!p.models.is_empty(), "github-copilot has no models");
  }

  #[test]
  fn embedded_deepseek_efforts_are_model_specific() {
    let catalogue = parse_embedded();
    let models = &catalogue["deepseek"].models;
    assert_eq!(
      serde_json::to_value(models["deepseek-v4-flash"].reasoning_efforts()).unwrap(),
      serde_json::json!(["low", "high", "max"])
    );
    assert_eq!(
      serde_json::to_value(models["deepseek-v4-pro"].reasoning_efforts()).unwrap(),
      serde_json::json!(["high", "max"])
    );
    assert!(models["deepseek-reasoner"].reasoning_efforts().is_none());
  }
}
