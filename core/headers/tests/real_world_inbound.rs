//! Real-world inbound header fixtures, captured from the production router's
//! request log database. Verifies that [`HeaderMap`] can ingest every observed
//! `(provider, endpoint, persona)` cell without information loss.
//!
//! Per the no-redaction policy, fixture values are stored verbatim — including
//! the literal `"<redacted>"` placeholder that the router substitutes for
//! credentials before persisting. No schema parsing/build is exercised here;
//! schema-targeted examples live in `schema_examples.rs`.
//!
//! Fixture source: `~/.local/share/tokn-router/requests/*.db`, mined via
//! `tmp/mine_inbound.py` and filtered to drop missing-persona, browser, and
//! OTel-exporter cells. See [`Phase 1.5 mining notes`] for methodology.

use std::collections::BTreeSet;

use serde_json::Value;
use tokn_headers::{HeaderMap, HeaderName, HeaderValue};

const FIXTURE_JSON: &str = include_str!("fixtures/inbound_real_world.json");
const USER_AGENTS_JSON: &str = include_str!("fixtures/user_agents.json");

/// Parse the fixture into `(cell_key, HeaderMap)` pairs.
fn load_cells() -> Vec<(String, HeaderMap)> {
  let root: serde_json::Map<String, Value> = serde_json::from_str(FIXTURE_JSON).expect("fixture is valid JSON object");
  let mut out = Vec::with_capacity(root.len());
  for (key, value) in root {
    let obj = value.as_object().expect("each cell is a JSON object");
    let mut map = HeaderMap::with_capacity(obj.len());
    for (n, v) in obj {
      let s = v.as_str().expect("header value is a string");
      map.insert(n.as_str(), HeaderValue::from_string(s.to_string()));
    }
    out.push((key, map));
  }
  out
}

#[test]
fn fixture_loads_with_expected_cell_count() {
  let cells = load_cells();
  // 41 = 50 mined cells minus 9 dropped (missing-persona, browser, OTel).
  assert_eq!(
    cells.len(),
    41,
    "fixture cell count drifted; re-run mine_inbound.py and update"
  );
}

#[test]
fn every_cell_is_non_empty() {
  for (key, map) in load_cells() {
    assert!(!map.is_empty(), "cell `{key}` parsed to an empty HeaderMap");
  }
}

#[test]
fn every_cell_has_user_agent_or_content_type() {
  // Either a client UA (most cells) or at least content-type (router-internal
  // pre-auth rejections that never saw a UA) must be present. Both is fine.
  let ua = HeaderName::new("user-agent");
  let ct = HeaderName::new("content-type");
  for (key, map) in load_cells() {
    assert!(
      map.contains_key(&ua) || map.contains_key(&ct),
      "cell `{key}` lacks both user-agent and content-type — fixture is malformed"
    );
  }
}

#[test]
fn case_insensitive_lookup_matches_lowercase_keys() {
  // The mined fixture stores all names lowercased (sqlite serialization
  // artifact). Verify HeaderName's case-insensitive lookup still finds them
  // when probed with mixed case.
  let cells = load_cells();
  let probe = HeaderName::new("Content-Type");
  let hits = cells.iter().filter(|(_, m)| m.contains_key(&probe)).count();
  assert!(hits > 30, "expected most cells to carry Content-Type; got {hits}");
}

#[test]
fn distinct_header_inventory_covers_known_keys() {
  // Build the union of every header name observed across all cells. The
  // foundational `keys` catalogue should cover the most common names; this
  // test asserts presence of the names we expect, and surfaces (via the
  // panic message) any new names that show up in future captures.
  let cells = load_cells();
  let mut all: BTreeSet<String> = BTreeSet::new();
  for (_, map) in &cells {
    for (name, _) in map.iter() {
      all.insert(name.as_str().to_string());
    }
  }
  // Sanity expectations — these are universally present across captures.
  for required in ["host", "user-agent", "content-type", "authorization", "accept"] {
    assert!(all.contains(required), "expected `{required}` to appear at least once");
  }
  // Diagnostic: print the inventory so future regressions are easy to read.
  // Using `println!` instead of `dbg!` keeps cargo output uncluttered when
  // the test passes (println! is suppressed unless --nocapture is set).
  println!("real-world inbound header inventory ({}): {:?}", all.len(), all);
}

#[test]
fn user_agents_fixture_covers_known_clients() {
  let uas: Vec<String> = serde_json::from_str(USER_AGENTS_JSON).expect("user_agents.json is a JSON array");
  assert!(!uas.is_empty(), "user_agents.json must not be empty");
  // Sorted, distinct invariant.
  let mut sorted = uas.clone();
  sorted.sort();
  sorted.dedup();
  assert_eq!(sorted, uas, "user_agents.json must be sorted and distinct");
  // Sanity: at least one of each major client family observed in mining.
  for needle in ["opencode/", "codex_exec/", "codex-tui/", "curl/", "OpenAI/JS"] {
    assert!(
      uas.iter().any(|ua| ua.contains(needle)),
      "expected at least one user-agent containing `{needle}`"
    );
  }
}
