//! Catalogue of popular header names as compile-time `static` constants.
//!
//! Use these in place of ad-hoc `HeaderName::new(...)` for any header that
//! appears in this list — both for clarity and to avoid the (small) cost of
//! re-allocating the `SmolStr` on each construction.

use crate::name::HeaderName;

macro_rules! key {
  ($name:ident, $original:literal, $lower:literal) => {
    pub static $name: HeaderName = HeaderName::new_static($original, $lower);
  };
}

// Core HTTP transport
key!(AUTHORIZATION, "Authorization", "authorization");
key!(CONTENT_TYPE, "Content-Type", "content-type");
key!(CONTENT_ENCODING, "Content-Encoding", "content-encoding");
key!(CONTENT_LENGTH, "Content-Length", "content-length");
key!(ACCEPT, "Accept", "accept");
key!(ACCEPT_ENCODING, "Accept-Encoding", "accept-encoding");
key!(ACCEPT_LANGUAGE, "Accept-Language", "accept-language");
key!(CONNECTION, "Connection", "connection");
key!(UPGRADE, "Upgrade", "upgrade");
key!(COOKIE, "Cookie", "cookie");
key!(USER_AGENT, "User-Agent", "user-agent");
key!(HOST, "Host", "host");

// Tool / persona identity
key!(EDITOR_VERSION, "Editor-Version", "editor-version");
key!(EDITOR_PLUGIN_VERSION, "Editor-Plugin-Version", "editor-plugin-version");
key!(
  COPILOT_INTEGRATION_ID,
  "Copilot-Integration-Id",
  "copilot-integration-id"
);
key!(
  COPILOT_VISION_REQUEST,
  "Copilot-Vision-Request",
  "copilot-vision-request"
);
key!(OPENAI_INTENT, "OpenAI-Intent", "openai-intent");
key!(OPENAI_BETA, "OpenAI-Beta", "openai-beta");
key!(CHATGPT_ACCOUNT_ID, "chatgpt-account-id", "chatgpt-account-id");
key!(ANTHROPIC_BETA, "Anthropic-Beta", "anthropic-beta");
key!(ANTHROPIC_VERSION, "Anthropic-Version", "anthropic-version");
key!(X_API_KEY, "X-Api-Key", "x-api-key");

// Router-injected correlation
key!(X_SESSION_ID, "X-Session-Id", "x-session-id");
key!(X_SESSION_AFFINITY, "X-Session-Affinity", "x-session-affinity");
key!(X_PARENT_SESSION_ID, "X-Parent-Session-Id", "x-parent-session-id");
key!(X_REQUEST_ID, "X-Request-Id", "x-request-id");
key!(X_INITIATOR, "X-Initiator", "x-initiator");
key!(X_PROJECT_CWD, "X-Project-Cwd", "x-project-cwd");
key!(X_INTERACTION_ID, "X-Interaction-Id", "x-interaction-id");
key!(X_BEHAVE_AS, "X-Behave-As", "x-behave-as");
key!(X_OPENCODE_SESSION, "X-OpenCode-Session", "x-opencode-session");

// Codex CLI native (lowercase, no x- prefix in real captures)
key!(ORIGINATOR, "originator", "originator");
key!(VERSION, "version", "version");
key!(SESSION_ID_LOWER, "session_id", "session_id");
key!(THREAD_ID, "thread_id", "thread_id");
key!(X_CLIENT_REQUEST_ID, "x-client-request-id", "x-client-request-id");
key!(X_CODEX_BETA_FEATURES, "x-codex-beta-features", "x-codex-beta-features");
key!(X_CODEX_TURN_METADATA, "x-codex-turn-metadata", "x-codex-turn-metadata");
key!(X_CODEX_ROUTING_HINT, "x-codex-routing-hint", "x-codex-routing-hint");
key!(X_CODEX_WINDOW_ID, "x-codex-window-id", "x-codex-window-id");
key!(
  X_CODEX_RESPONSES_LITE,
  "x-openai-internal-codex-responses-lite",
  "x-openai-internal-codex-responses-lite"
);

// Copilot CLI / Stainless SDK family
key!(X_GITHUB_API_VERSION, "X-GitHub-Api-Version", "x-github-api-version");
key!(X_INTERACTION_TYPE, "X-Interaction-Type", "x-interaction-type");
key!(X_CLIENT_SESSION_ID, "X-Client-Session-Id", "x-client-session-id");
key!(X_AGENT_TASK_ID, "X-Agent-Task-Id", "x-agent-task-id");
key!(SEC_FETCH_MODE, "Sec-Fetch-Mode", "sec-fetch-mode");
key!(
  SEC_WEBSOCKET_EXTENSIONS,
  "Sec-WebSocket-Extensions",
  "sec-websocket-extensions"
);
key!(SEC_WEBSOCKET_KEY, "Sec-WebSocket-Key", "sec-websocket-key");
key!(SEC_WEBSOCKET_VERSION, "Sec-WebSocket-Version", "sec-websocket-version");
key!(
  X_STAINLESS_RETRY_COUNT,
  "X-Stainless-Retry-Count",
  "x-stainless-retry-count"
);
key!(X_STAINLESS_TIMEOUT, "X-Stainless-Timeout", "x-stainless-timeout");
key!(X_STAINLESS_LANG, "X-Stainless-Lang", "x-stainless-lang");
key!(
  X_STAINLESS_PACKAGE_VERSION,
  "X-Stainless-Package-Version",
  "x-stainless-package-version"
);
key!(X_STAINLESS_OS, "X-Stainless-OS", "x-stainless-os");
key!(X_STAINLESS_ARCH, "X-Stainless-Arch", "x-stainless-arch");
key!(X_STAINLESS_RUNTIME, "X-Stainless-Runtime", "x-stainless-runtime");
key!(
  X_STAINLESS_RUNTIME_VERSION,
  "X-Stainless-Runtime-Version",
  "x-stainless-runtime-version"
);
key!(
  X_STAINLESS_HELPER_METHOD,
  "x-stainless-helper-method",
  "x-stainless-helper-method"
);
key!(X_APP, "X-App", "x-app");
key!(
  ANTHROPIC_DANGEROUS_DIRECT_BROWSER_ACCESS,
  "Anthropic-Dangerous-Direct-Browser-Access",
  "anthropic-dangerous-direct-browser-access"
);
key!(X_GOOG_API_KEY, "x-goog-api-key", "x-goog-api-key");

#[cfg(test)]
mod tests {
  use super::*;

  /// Sanity test: every catalogued key's original form must lowercase to its
  /// declared lowercase form. Catches typos in `key!()` macro calls.
  #[test]
  fn original_lowercases_to_canonical() {
    macro_rules! check {
      ($($name:ident),* $(,)?) => {
        $({
          let n = &$name;
          assert_eq!(
            n.original().to_ascii_lowercase(),
            n.as_str(),
            "key {} declared lower form does not match", stringify!($name)
          );
        })*
      };
    }
    check!(
      AUTHORIZATION,
      CONTENT_TYPE,
      CONTENT_ENCODING,
      CONTENT_LENGTH,
      ACCEPT,
      ACCEPT_ENCODING,
      ACCEPT_LANGUAGE,
      CONNECTION,
      UPGRADE,
      COOKIE,
      USER_AGENT,
      HOST,
      EDITOR_VERSION,
      EDITOR_PLUGIN_VERSION,
      COPILOT_INTEGRATION_ID,
      COPILOT_VISION_REQUEST,
      OPENAI_INTENT,
      OPENAI_BETA,
      CHATGPT_ACCOUNT_ID,
      ANTHROPIC_BETA,
      ANTHROPIC_VERSION,
      X_API_KEY,
      X_SESSION_ID,
      X_SESSION_AFFINITY,
      X_PARENT_SESSION_ID,
      X_REQUEST_ID,
      X_INITIATOR,
      X_PROJECT_CWD,
      X_INTERACTION_ID,
      X_BEHAVE_AS,
      X_OPENCODE_SESSION,
      ORIGINATOR,
      VERSION,
      SESSION_ID_LOWER,
      THREAD_ID,
      X_CLIENT_REQUEST_ID,
      X_CODEX_BETA_FEATURES,
      X_CODEX_TURN_METADATA,
      X_CODEX_ROUTING_HINT,
      X_CODEX_WINDOW_ID,
      X_GITHUB_API_VERSION,
      X_INTERACTION_TYPE,
      X_CLIENT_SESSION_ID,
      X_AGENT_TASK_ID,
      SEC_FETCH_MODE,
      SEC_WEBSOCKET_EXTENSIONS,
      SEC_WEBSOCKET_KEY,
      SEC_WEBSOCKET_VERSION,
      X_STAINLESS_RETRY_COUNT,
      X_STAINLESS_TIMEOUT,
      X_STAINLESS_LANG,
      X_STAINLESS_PACKAGE_VERSION,
      X_STAINLESS_OS,
      X_STAINLESS_ARCH,
      X_STAINLESS_RUNTIME,
      X_STAINLESS_RUNTIME_VERSION,
      X_STAINLESS_HELPER_METHOD,
      X_APP,
      ANTHROPIC_DANGEROUS_DIRECT_BROWSER_ACCESS,
      X_GOOG_API_KEY,
    );
  }

  #[test]
  fn keys_are_case_insensitive_to_arbitrary_input() {
    use crate::HeaderName;
    assert_eq!(AUTHORIZATION, HeaderName::new("AUTHORIZATION"));
    assert_eq!(EDITOR_VERSION, HeaderName::new("editor-version"));
  }
}
