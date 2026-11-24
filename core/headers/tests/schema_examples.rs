//! Schema-targeted example fixtures extracted from the mined real-world
//! corpus. Keep these focused and stable; the large mined matrix remains for
//! corpus/inventory coverage only.

use std::path::PathBuf;

use tokn_headers::{HeaderMap, HeaderValue};

fn fixture_path(name: &str) -> PathBuf {
  PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("tests")
    .join("fixtures")
    .join("schema_examples")
    .join(format!("{name}.yaml"))
}

fn example_map(name: &str) -> HeaderMap {
  let path = fixture_path(name);
  let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
  let mut map = HeaderMap::new();
  for (lineno, raw_line) in text.lines().enumerate() {
    let line = raw_line.trim();
    if line.is_empty() {
      continue;
    }
    let body = line
      .strip_prefix("- ")
      .unwrap_or_else(|| panic!("{}:{} must start with `- `", path.display(), lineno + 1));
    let (name, value) = body
      .split_once(": ")
      .unwrap_or_else(|| panic!("{}:{} must contain `: `", path.display(), lineno + 1));
    let value =
      if (value.starts_with('\"') && value.ends_with('\"')) || (value.starts_with('\'') && value.ends_with('\'')) {
        &value[1..value.len() - 1]
      } else {
        value
      };
    map.insert(name, HeaderValue::from_string(value.to_string()));
  }
  map
}

#[test]
fn opencode_schema_parses_deepseek_example() {
  use tokn_headers::schemas::OpencodeHeaders;
  use tokn_headers::HeaderSchema;

  let key = "deepseek__chat_completions__opencode";
  let map = example_map(key);
  OpencodeHeaders::parse(&map).unwrap_or_else(|e| panic!("OpencodeHeaders::parse failed for `{key}`: {e}"));
}

#[test]
fn opencode_schema_parses_copilot_responses_example() {
  use tokn_headers::schemas::OpencodeHeaders;
  use tokn_headers::HeaderSchema;

  let key = "github-copilot__responses__opencode";
  let map = example_map(key);
  OpencodeHeaders::parse(&map).unwrap_or_else(|e| panic!("OpencodeHeaders::parse failed for `{key}`: {e}"));
}

#[test]
fn copilot_overlay_builds_from_opencode_copilot_example() {
  use tokn_headers::keys;
  use tokn_headers::schemas::CopilotOverlay;

  let map = example_map("github-copilot__responses__opencode");
  let overlay = CopilotOverlay::build(&Default::default(), &map);
  assert_eq!(overlay.editor_version.as_str(), "vscode/1.95.0");
  assert_eq!(overlay.editor_plugin_version.as_str(), "copilot-chat/0.23.0");
  assert_eq!(overlay.integration_id.as_str(), "vscode-chat");
  assert_eq!(
    overlay.initiator.as_deref(),
    map.get(&keys::X_INITIATOR).map(|v| v.as_str())
  );
  assert!(overlay.vision_request.is_none());
}

#[test]
fn codex_cli_schema_parses_router_sse_example() {
  use tokn_headers::schemas::CodexCliHeaders;
  use tokn_headers::HeaderSchema;

  let key = "deepseek__responses__codex-cli";
  let map = example_map(key);
  CodexCliHeaders::parse(&map).unwrap_or_else(|e| panic!("CodexCliHeaders::parse failed for `{key}`: {e}"));
}

#[test]
fn codex_cli_schema_parses_chatgpt_websocket_example() {
  use tokn_headers::schemas::CodexCliHeaders;
  use tokn_headers::HeaderSchema;

  let key = "codex__responses__codex-cli";
  let map = example_map(key);
  CodexCliHeaders::parse(&map).unwrap_or_else(|e| panic!("CodexCliHeaders::parse failed for `{key}`: {e}"));
}
