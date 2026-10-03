//! Provider-agnostic authentication contracts.
//!
//! `tokn-auth` orchestrates account lifecycle (login, import, refresh,
//! status) but holds zero provider-specific HTTP code. Each provider crate
//! implements [`ProviderAuth`] and exposes a `provider_auth()` accessor;
//! `tokn-auth` looks up the impl by `AccountConfig::provider` and dispatches.
//!
//! Keeping the trait here (rather than in `tokn-auth`) avoids a circular
//! dep: provider crates already depend on `tokn-core`, and `tokn-auth` will
//! depend on both.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokn_core::account::{AccountConfig, Secret};

/// Outcome of a successful device-flow login (currently only used by
/// github-copilot). The caller is responsible for assembling these fields
/// into an [`AccountConfig`].
#[derive(Debug, Clone)]
pub struct DeviceFlowOutcome {
  /// Long-lived OAuth refresh token obtained from the upstream OAuth dance.
  pub refresh_token: String,
  /// Short-lived access token already exchanged from the refresh token.
  pub access_token: String,
  /// Unix timestamp at which `access_token` expires.
  pub access_token_expires_at: i64,
  /// Optional upstream username (used to suggest an account id).
  pub username: Option<String>,
  /// Optional provider-specific account identifier discovered during the
  /// OAuth dance (e.g. ChatGPT account id parsed from the codex `id_token`
  /// JWT). Persisted to [`AccountConfig::provider_account_id`] so providers
  /// can surface it in outbound headers.
  pub provider_account_id: Option<String>,
}

/// Opaque handle returned by [`ProviderAuth::request_device_code`] and
/// consumed by [`ProviderAuth::poll_device_code`]. The CLI uses
/// `verification_uri` + `user_code` to instruct the user, then hands the
/// whole struct back to the provider for polling.
///
/// `device_code` is the provider-internal identifier (opaque to the CLI);
/// it is sent back to the upstream authorization server during polling.
#[derive(Debug, Clone)]
pub struct DeviceCodeHandle {
  /// Provider-internal device code string. Opaque to the caller — but
  /// the CLI never needs to read it; the field is only public so that
  /// providers can construct the handle in their own crates.
  pub device_code: String,
  /// Short user-facing code to type at `verification_uri`.
  pub user_code: String,
  /// URL the user should visit in a browser.
  pub verification_uri: String,
  /// Seconds until the device code expires (display only).
  pub expires_in: u64,
  /// Minimum interval (seconds) between poll attempts.
  pub interval: u64,
}

/// What *shape* a credential is in. Determines whether the provider
/// treats it as a static API key (used as-is on every request) or a
/// long-lived refresh token (exchanged for short-lived access tokens).
///
/// Carried on every non-`Login` [`CredentialSource`] so the CLI can
/// say "this thing is a refresh token" without the provider having to
/// guess from the source.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CredentialFlavor {
  /// Static API key.
  ApiKey,
  /// Long-lived OAuth refresh token.
  RefreshToken,
}

/// Where the CLI is being told to fetch a credential from.
///
/// Each non-`Login` variant carries a [`CredentialFlavor`] — the caller
/// declares whether the bytes form an API key or a refresh token — and
/// `import_from` simply produces the matching [`CredentialResult`].
///
/// `Custom { key, value }` is the escape hatch for provider-specific
/// sources that don't fit the generic shape (e.g. Copilot's `gh` and
/// `copilot-plugin` scrapers). The `key` is `&'static str` — providers
/// advertise the legal set via [`ProviderAuth::custom_credential_sources`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialSource {
  /// Run the provider's interactive flow (device-flow OAuth or static-key
  /// prompt). Not dispatched through `import_from` — the CLI handles
  /// it via [`ProviderAuth::request_device_code`] /
  /// [`ProviderAuth::poll_device_code`] or the static-key prompt.
  Login,
  /// Read the credential from a named environment variable.
  Env { env_var: String, flavor: CredentialFlavor },
  /// Caller already has the credential bytes in hand (e.g. typed at a
  /// prompt, piped via stdin).
  String { value: String, flavor: CredentialFlavor },
  /// Read the credential from a file on disk.
  File {
    path: std::path::PathBuf,
    flavor: CredentialFlavor,
  },
  /// Provider-defined source. The provider chooses the resulting
  /// flavor itself; `value` is an optional payload the user typed at
  /// the prompt (most custom sources don't need one).
  Custom { key: &'static str, value: Option<String> },
}

/// The "kind" of a [`CredentialSource`] without its payload. Used by
/// providers to advertise capabilities up-front.
///
/// `Custom` carries the well-known key string so the CLI can build the
/// interactive picker and validate `--from <key>`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CredentialSourceKind {
  Login,
  Env,
  String,
  File,
  Custom(&'static str),
}

impl CredentialSource {
  pub fn kind(&self) -> CredentialSourceKind {
    match self {
      Self::Login => CredentialSourceKind::Login,
      Self::Env { .. } => CredentialSourceKind::Env,
      Self::String { .. } => CredentialSourceKind::String,
      Self::File { .. } => CredentialSourceKind::File,
      Self::Custom { key, .. } => CredentialSourceKind::Custom(key),
    }
  }

  /// Flavor declared on this source, if any. `Login` and `Custom` both
  /// return `None` (the provider chooses).
  pub fn flavor(&self) -> Option<CredentialFlavor> {
    match self {
      Self::Env { flavor, .. } | Self::String { flavor, .. } | Self::File { flavor, .. } => Some(*flavor),
      Self::Login | Self::Custom { .. } => None,
    }
  }
}

impl CredentialSourceKind {
  /// Stable string identifier, matches the CLI's `--from` value.
  /// `Custom("gh")` → `"gh"`.
  pub fn as_str(&self) -> &'static str {
    match self {
      Self::Login => "login",
      Self::Env => "env",
      Self::String => "string",
      Self::File => "file",
      Self::Custom(key) => key,
    }
  }
}

/// What the provider produced from a [`CredentialSource`]. The provider
/// chooses the variant — the CLI dispatches on it to decide whether to
/// build a refresh-token-shaped account (OAuth) or an api-key-shaped
/// account (static).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialResult {
  /// Long-lived OAuth refresh token. The next refresh exchanges it for
  /// a short-lived access token.
  Refresh(String),
  /// Static API key — used as-is on every request.
  ApiKey(String),
}

/// Outcome of a refresh-credential call. For OAuth providers this is a
/// fresh access token; for static-key providers it is a no-op (and
/// [`ProviderAuth::refresh_credential`] returns
/// [`RefreshOutcome::NotApplicable`]).
#[derive(Debug, Clone)]
pub enum RefreshOutcome {
  /// A new short-lived access token was issued.
  Refreshed {
    access_token: String,
    expires_at: i64,
    /// Rotated refresh token, when issued. Omission preserves the stored token.
    refresh_token: Option<String>,
    /// Updated identity token, when issued. Omission preserves the stored token.
    id_token: Option<String>,
    /// Optional upstream username/account handle discovered during refresh.
    username: Option<String>,
    /// Optional provider-specific account identifier (e.g. ChatGPT account
    /// id from a refreshed codex `id_token`). When `Some`, callers should
    /// overwrite [`AccountConfig::provider_account_id`].
    provider_account_id: Option<String>,
  },
  /// The cached OAuth credential is still current; no exchange was needed.
  Unchanged,
  /// The provider uses a static credential; nothing to refresh.
  NotApplicable,
}

impl RefreshOutcome {
  /// Apply a completed exchange, preserving optional fields the upstream did
  /// not return. Returns whether the account needs to be persisted.
  pub fn apply_to(&self, account: &mut AccountConfig) -> bool {
    let Self::Refreshed {
      access_token,
      expires_at,
      refresh_token,
      id_token,
      username,
      provider_account_id,
    } = self
    else {
      return false;
    };
    account.access_token = Some(Secret::new(access_token.clone()));
    account.access_token_expires_at = Some(*expires_at);
    if let Some(token) = refresh_token.as_ref().filter(|token| !token.trim().is_empty()) {
      account.refresh_token = Some(Secret::new(token.clone()));
    }
    if let Some(token) = id_token.as_ref().filter(|token| !token.trim().is_empty()) {
      account.id_token = Some(Secret::new(token.clone()));
    }
    if let Some(username) = username.as_ref().filter(|name| !name.trim().is_empty()) {
      account.username = Some(username.clone());
    }
    if let Some(id) = provider_account_id.as_ref().filter(|id| !id.trim().is_empty()) {
      account.provider_account_id = Some(id.clone());
    }
    account.last_refresh = Some(time::OffsetDateTime::now_utc().unix_timestamp());
    true
  }
}

/// Outcome of a successful credential verification.
#[derive(Debug, Clone, Default)]
pub struct VerifyOutcome {
  /// Optional upstream username/account handle discovered during verification.
  pub username: Option<String>,
}

/// Provider-agnostic snapshot of remote quota / plan state, returned by
/// [`ProviderAuth::probe_quota`]. Renderers (CLI status) interpret the
/// `provider_extra` blob for provider-specific detail.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaSnapshot {
  /// Human-readable plan name (e.g. `"copilot_pro"`, `"GLM Coding Plan"`).
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub plan: Option<String>,
  /// Short one-line headline for compact rendering (e.g. `"premium_interactions: 12/300"`).
  /// Used by `account status`; `account list` prefers `metered` for richer formatting.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub headline: Option<String>,
  /// ISO-8601 reset date if the upstream advertises one.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub reset_date: Option<String>,
  /// The primary metered request bucket — typically the visible
  /// "premium" / "headline" allowance the user cares about. Renderers
  /// display this with a percent gauge.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub metered: Option<MeteredBucket>,
  /// Additional usage buckets (e.g. Z.ai 5h tokens, weekly tokens, MCP
  /// monthly). Rendered as one row each by the CLI list command.
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub secondary: Vec<UsageBucket>,
  /// Provider-specific blob for extras the generic shape can't capture.
  #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
  pub provider_extra: serde_json::Value,
}

/// A metered request bucket — counts down as the user spends premium
/// requests. `entitlement = None` means the bucket is unlimited (some
/// Copilot plans expose `chat` as unmetered).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteredBucket {
  /// Display label, e.g. `"premium_interactions"`.
  pub label: String,
  /// Remaining count in the bucket.
  pub remaining: u64,
  /// Total entitlement; `None` = unlimited.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub entitlement: Option<u64>,
}

/// A used/total token (or request) bucket — counts up as usage accrues.
/// Z.ai exposes several of these (5-hour window, weekly, MCP monthly).
///
/// Either `used`/`total` or `percent_used` (or both) may be populated;
/// renderers should fall back gracefully. `total = 0` is treated as
/// "unknown total" for renderers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageBucket {
  /// Display label, e.g. `"5h tokens"`.
  pub label: String,
  /// Amount already used, when the upstream reports a discrete count.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub used: Option<u64>,
  /// Total cap for this window, when known.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub total: Option<u64>,
  /// Percent used (0.0–100.0), when the upstream only exposes a ratio.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub percent_used: Option<f64>,
  /// Optional epoch-ms timestamp at which the bucket resets.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub reset_at_ms: Option<i64>,
}

/// Errors surfaced by the auth layer. Kept lightweight (string payload)
/// because this trait crosses many crate boundaries; consumers can wrap
/// with `anyhow::Context` as needed.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
  #[error("provider '{0}' does not support this operation")]
  Unsupported(String),
  #[error("missing credential field '{field}' on account '{account}'")]
  MissingCredential { account: String, field: &'static str },
  #[error("upstream HTTP error: {0}")]
  Upstream(String),
  #[error("network error: {0}")]
  Network(String),
  #[error("malformed response: {0}")]
  Decode(String),
  #[error("{0}")]
  Other(String),
}

pub type Result<T> = std::result::Result<T, AuthError>;

/// All authentication-flow capabilities a provider can implement.
///
/// Static-key providers (e.g. Z.ai) leave [`Self::supports_device_flow`]
/// as `false` and return [`RefreshOutcome::NotApplicable`] from
/// [`Self::refresh_credential`]. OAuth providers (e.g. github-copilot)
/// implement everything.
///
/// Implementations must be cheap to construct (typically zero-sized) and
/// hold no state — all per-call inputs are passed as arguments. Each
/// provider crate exposes a `provider_auth() -> &'static dyn ProviderAuth`
/// accessor; `tokn-auth` builds a static dispatch table at startup.
#[async_trait]
#[allow(
  clippy::double_must_use,
  reason = "async_trait adds must_use to methods returning must-use futures"
)]
pub trait ProviderAuth: Send + Sync {
  /// Provider id this impl handles (e.g. `"github-copilot"`). Must match
  /// [`AccountConfig::provider`] exactly.
  fn id(&self) -> &'static str;

  /// True if [`Self::request_device_code`] / [`Self::poll_device_code`]
  /// (and the convenience [`Self::device_flow_login`] wrapper) are
  /// implemented.
  fn supports_device_flow(&self) -> bool {
    false
  }

  /// True if this provider authenticates with a static API key (no OAuth
  /// dance). Used to gate `--from env` / interactive key-paste prompts.
  fn supports_static_key(&self) -> bool {
    false
  }

  /// All credential acquisition sources this provider supports. The CLI
  /// uses this both to build its interactive picker and to validate
  /// `account import --from`.
  ///
  /// The generic sources (`Env` / `String` / `File`) are always
  /// available — `flavor` (carried on the source itself) is what gets
  /// validated against [`Self::supports_auth_flavor`]. `Custom` keys
  /// come from [`Self::custom_credential_sources`].
  fn credential_sources(&self) -> Vec<CredentialSourceKind> {
    let mut out = vec![
      CredentialSourceKind::Login,
      CredentialSourceKind::Env,
      CredentialSourceKind::String,
      CredentialSourceKind::File,
    ];
    for key in self.custom_credential_sources() {
      out.push(CredentialSourceKind::Custom(key));
    }
    out
  }

  /// Provider-specific [`CredentialSource::Custom`] keys this provider
  /// recognises. The CLI uses this list both for the interactive picker
  /// and for `--from <key>` validation.
  ///
  /// Example: github-copilot returns `&["gh", "copilot-plugin"]`.
  /// Default: empty.
  fn custom_credential_sources(&self) -> &'static [&'static str] {
    &[]
  }

  /// True if this provider accepts the given credential flavor.
  /// Default impl returns true for `RefreshToken` if
  /// [`Self::supports_device_flow`] and `ApiKey` if
  /// [`Self::supports_static_key`].
  fn supports_auth_flavor(&self, flavor: CredentialFlavor) -> bool {
    match flavor {
      CredentialFlavor::RefreshToken => self.supports_device_flow(),
      CredentialFlavor::ApiKey => self.supports_static_key(),
    }
  }

  /// The flavor to assume when the caller doesn't specify one (e.g. the
  /// CLI default for `--from env` without `--refresh-token`). Default:
  /// `RefreshToken` for OAuth providers, `ApiKey` otherwise.
  fn default_auth_flavor(&self) -> CredentialFlavor {
    if self.supports_device_flow() {
      CredentialFlavor::RefreshToken
    } else {
      CredentialFlavor::ApiKey
    }
  }

  /// True if a [`CredentialSource`] is supported by this provider.
  /// Default impl checks both that the source *kind* is advertised in
  /// [`Self::credential_sources`] and that the flavor (when present) is
  /// accepted by [`Self::supports_auth_flavor`].
  fn supports_credential_source(&self, src: &CredentialSource) -> bool {
    if !self.credential_sources().contains(&src.kind()) {
      return false;
    }
    src.flavor().map(|f| self.supports_auth_flavor(f)).unwrap_or(true)
  }

  /// Acquire a credential from the named source. The CLI has already
  /// gathered any user input (env-var name, literal token, file path)
  /// and packed it into [`CredentialSource`]; this method turns that
  /// into the actual credential bytes wrapped in a [`CredentialResult`].
  ///
  /// Default impl delegates to [`default_import_from`], which handles
  /// the generic `Env`/`String`/`File` variants and rejects `Custom` /
  /// `Login`. Providers override this to add `Custom` source handling
  /// (and typically delegate the generic cases back to
  /// [`default_import_from`]).
  async fn import_from(&self, source: &CredentialSource) -> Result<CredentialResult> {
    default_import_from(self.id(), source)
  }

  /// Suggested account id when the caller hasn't picked one and the
  /// flow can't infer one (e.g. failed username lookup, env-var import).
  /// Defaults to the provider id, which is fine for static-key providers.
  fn default_account_id(&self) -> &'static str {
    self.id()
  }

  /// Default upstream base URL to seed `AccountConfig::base_url` when
  /// onboarding a new account. `None` means "no override; let the
  /// provider's runtime choose".
  fn default_base_url(&self) -> Option<&'static str> {
    None
  }

  /// Default OAuth token-exchange URL to seed
  /// `AccountConfig::refresh_url`. Only meaningful for OAuth providers.
  fn default_refresh_url(&self) -> Option<&'static str> {
    None
  }

  /// Step 1 of device-flow OAuth: request a fresh device code from the
  /// upstream authorization server. Returns a handle to be passed back
  /// to [`Self::poll_device_code`].
  ///
  /// Default impl returns `Unsupported`; OAuth providers override.
  async fn request_device_code(&self, _client: &reqwest::Client) -> Result<DeviceCodeHandle> {
    Err(AuthError::Unsupported(self.id().to_string()))
  }

  /// Step 2 of device-flow OAuth: poll the upstream until the user has
  /// approved the device code in their browser, then exchange the
  /// resulting long-lived token for a short-lived access token and
  /// (best-effort) look up a username for id suggestion.
  ///
  /// Default impl returns `Unsupported`; OAuth providers override.
  async fn poll_device_code(&self, _client: &reqwest::Client, _handle: DeviceCodeHandle) -> Result<DeviceFlowOutcome> {
    Err(AuthError::Unsupported(self.id().to_string()))
  }

  /// Convenience wrapper that calls [`Self::request_device_code`] then
  /// [`Self::poll_device_code`] back-to-back. Callers that want to
  /// display the user code between request and poll (e.g. an interactive
  /// CLI) should call the two methods directly.
  async fn device_flow_login(&self, client: &reqwest::Client) -> Result<DeviceFlowOutcome> {
    let handle = self.request_device_code(client).await?;
    self.poll_device_code(client, handle).await
  }

  /// Force an exchange of the account's short-lived credential, even when
  /// its cached access token is still current. Static-key providers return
  /// [`RefreshOutcome::NotApplicable`].
  async fn refresh_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<RefreshOutcome>;

  /// Refresh before another authenticated operation. OAuth providers may
  /// return [`RefreshOutcome::Unchanged`] when their cached token is current.
  /// The default preserves providers that use an exchange to verify credentials.
  async fn refresh_credential_if_needed(
    &self,
    client: &reqwest::Client,
    account: &AccountConfig,
  ) -> Result<RefreshOutcome> {
    self.refresh_credential(client, account).await
  }

  /// Verify the account's stored credential is currently usable, without
  /// mutating it. Used by `account status` and the CLI smoke test.
  ///
  /// For OAuth providers this typically runs a token exchange to confirm
  /// the refresh token is still good; for static-key providers it hits a
  /// cheap upstream endpoint (e.g. `GET /models`).
  async fn verify_credential(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<VerifyOutcome>;

  /// Fetch a [`QuotaSnapshot`] for status display. May be a no-op
  /// (returning `Default::default()`) when the upstream offers no quota
  /// API.
  async fn probe_quota(&self, client: &reqwest::Client, account: &AccountConfig) -> Result<QuotaSnapshot>;

  /// Default outer-timeout to apply when running [`Self::probe_quota`]
  /// from the status command. Providers can shorten this for slow
  /// endpoints.
  fn quota_timeout(&self) -> Duration {
    Duration::from_secs(5)
  }
}

/// Trim `raw` and wrap it in the matching [`CredentialResult`] variant.
/// Errors with [`AuthError::Other`] when the trimmed bytes are empty,
/// using `source_label` to identify the origin in the message.
pub fn wrap_bytes(raw: String, flavor: CredentialFlavor, source_label: &str) -> Result<CredentialResult> {
  let trimmed = raw.trim().to_string();
  if trimmed.is_empty() {
    return Err(AuthError::Other(format!("{source_label} is empty")));
  }
  Ok(match flavor {
    CredentialFlavor::ApiKey => CredentialResult::ApiKey(trimmed),
    CredentialFlavor::RefreshToken => CredentialResult::Refresh(trimmed),
  })
}

/// The default `import_from` body, exposed as a free function so that
/// providers overriding [`ProviderAuth::import_from`] can delegate the
/// generic `Env`/`String`/`File` cases without duplicating the logic.
///
/// `provider_id` is used only for the error message when `source` is a
/// `Custom` variant the provider doesn't recognise (or `Login`).
pub fn default_import_from(provider_id: &str, source: &CredentialSource) -> Result<CredentialResult> {
  match source {
    CredentialSource::Env { env_var, flavor } => {
      let value =
        std::env::var(env_var).map_err(|_| AuthError::Other(format!("environment variable `{env_var}` is not set")))?;
      wrap_bytes(value, *flavor, &format!("environment variable `{env_var}`"))
    }
    CredentialSource::String { value, flavor } => wrap_bytes(value.clone(), *flavor, "credential value"),
    CredentialSource::File { path, flavor } => {
      let raw = std::fs::read_to_string(path).map_err(|e| AuthError::Other(format!("read {}: {e}", path.display())))?;
      wrap_bytes(raw, *flavor, &format!("file `{}`", path.display()))
    }
    CredentialSource::Custom { key, .. } => Err(AuthError::Unsupported(format!(
      "{provider_id} does not support custom credential source `{key}`"
    ))),
    CredentialSource::Login => Err(AuthError::Unsupported(
      "Login is dispatched via request_device_code / poll_device_code".into(),
    )),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn account() -> AccountConfig {
    serde_json::from_value(serde_json::json!({
      "id": "primary",
      "provider": "codex",
      "access_token": "old-access",
      "refresh_token": "old-refresh",
      "id_token": "old-id",
      "username": "old-user",
      "provider_account_id": "old-account",
      "last_refresh": 1
    }))
    .unwrap()
  }

  #[test]
  fn refreshed_credentials_preserve_omitted_fields() {
    let mut account = account();
    let refresh = RefreshOutcome::Refreshed {
      access_token: "new-access".into(),
      expires_at: 200,
      refresh_token: None,
      id_token: None,
      username: None,
      provider_account_id: None,
    };
    assert!(refresh.apply_to(&mut account));
    assert_eq!(account.access_token.as_ref().unwrap().expose(), "new-access");
    assert_eq!(account.access_token_expires_at, Some(200));
    assert_eq!(account.refresh_token.as_ref().unwrap().expose(), "old-refresh");
    assert_eq!(account.id_token.as_ref().unwrap().expose(), "old-id");
    assert_eq!(account.username.as_deref(), Some("old-user"));
    assert_eq!(account.provider_account_id.as_deref(), Some("old-account"));
    assert!(account.last_refresh.unwrap() > 1);
  }

  #[test]
  fn refreshed_credentials_apply_rotated_tokens_and_identity() {
    let mut account = account();
    let refresh = RefreshOutcome::Refreshed {
      access_token: "new-access".into(),
      expires_at: 200,
      refresh_token: Some("new-refresh".into()),
      id_token: Some("new-id".into()),
      username: Some("new-user".into()),
      provider_account_id: Some("new-account".into()),
    };
    assert!(refresh.apply_to(&mut account));
    assert_eq!(account.refresh_token.as_ref().unwrap().expose(), "new-refresh");
    assert_eq!(account.id_token.as_ref().unwrap().expose(), "new-id");
    assert_eq!(account.username.as_deref(), Some("new-user"));
    assert_eq!(account.provider_account_id.as_deref(), Some("new-account"));
  }

  #[test]
  fn unchanged_and_static_credentials_do_not_mutate_the_account() {
    let mut account = account();
    let before = serde_json::to_value(&account).unwrap();
    assert!(!RefreshOutcome::Unchanged.apply_to(&mut account));
    assert!(!RefreshOutcome::NotApplicable.apply_to(&mut account));
    assert_eq!(serde_json::to_value(account).unwrap(), before);
  }
}
