use rusqlite::{params, Connection};

use super::super::{
  get_session, get_session_from_db, get_session_node_from_db, list_requests, list_sessions, list_sessions_from_db,
  RequestListOptions, SessionPartContent, SessionPartEncoding, SessionPartOmissionReason,
};
use super::support::{tempdir, write_request, write_session};
use crate::sessions::{SessionsDb, TreeRequestRecord};
use crate::{MessageRecord, PartRecord};
use bytes::Bytes;

#[test]
fn lists_sessions_from_the_sessions_database() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  write_session(
    &sessions_db,
    "session-1",
    "request-new",
    1_783_987_200_000,
    "openai",
    "gpt-test",
  );
  write_session(
    &sessions_db,
    "session-1",
    "request-old",
    1_783_900_800_000,
    "openai",
    "gpt-test",
  );
  write_session(
    &sessions_db,
    "session-2",
    "request-latest",
    1_784_073_600_000,
    "zai",
    "glm-test",
  );
  Connection::open(&sessions_db)
    .unwrap()
    .execute(
      "INSERT INTO sessions (id, first_seen_ts, last_seen_ts, source) VALUES (?1, ?2, ?3, ?4)",
      params!["", 1_784_160_000_000_i64, 1_784_160_000_000_i64, "header"],
    )
    .unwrap();

  let sessions = list_sessions_from_db(&sessions_db, None).unwrap();
  assert_eq!(sessions.len(), 2);
  assert_eq!(sessions[0].session_id, "session-2");
  let session = sessions
    .iter()
    .find(|session| session.session_id == "session-1")
    .unwrap();
  assert_eq!(session.first_ts, 1_783_987_200_000);
  assert_eq!(session.last_ts, 1_783_987_200_000);
  assert_eq!(session.request_count, 2);
  assert_eq!(session.source.as_deref(), Some("header"));
  assert_eq!(session.last_request_day, "2026-07-14");
  assert_eq!(session.last_request_id, "request-new");
  assert_eq!(session.endpoint.as_deref(), Some("responses"));
  assert_eq!(session.status, Some(200));
  assert_eq!(session.provider_id.as_deref(), Some("openai"));

  let limited = list_sessions_from_db(&sessions_db, Some(1)).unwrap();
  assert_eq!(limited.len(), 1);
  assert_eq!(limited[0].session_id, "session-2");
}

#[test]
fn missing_sessions_database_is_empty_without_creating_a_file() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");

  assert!(list_sessions_from_db(&sessions_db, None).unwrap().is_empty());
  assert!(!sessions_db.exists());
}

#[test]
fn legacy_sessions_database_is_not_migrated_for_viewing() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let conn = Connection::open(&sessions_db).unwrap();
  conn
    .execute_batch(include_str!("../../../schemas/snapshot/sessions/v0.0.0.sql"))
    .unwrap();
  conn
    .execute_batch(
      "CREATE TABLE schema_migrations (
         version INTEGER PRIMARY KEY,
         name TEXT NOT NULL,
         applied_ts INTEGER NOT NULL
       );
       INSERT INTO schema_migrations (version, name, applied_ts) VALUES (1, 'initial', 0);",
    )
    .unwrap();
  drop(conn);

  assert!(matches!(
    list_sessions_from_db(&sessions_db, None),
    Err(crate::Error::UnsupportedSessionSchema { version: 1 })
  ));
  assert!(matches!(
    get_session_from_db(&sessions_db, "legacy", None),
    Err(crate::Error::UnsupportedSessionSchema { version: 1 })
  ));
  assert!(matches!(
    get_session_node_from_db(&sessions_db, "legacy", "legacy-node"),
    Err(crate::Error::UnsupportedSessionSchema { version: 1 })
  ));

  let conn = Connection::open(sessions_db).unwrap();
  let version: i64 = conn
    .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row.get(0))
    .unwrap();
  assert_eq!(version, 1);
  let tree_table_count: i64 = conn
    .query_row(
      "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'session_nodes'",
      [],
      |row| row.get(0),
    )
    .unwrap();
  assert_eq!(tree_table_count, 0);
}

#[test]
fn reads_tree_sessions_without_requiring_session_views() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let conn = Connection::open(&sessions_db).unwrap();
  conn
    .execute_batch(include_str!("../../../schemas/snapshot/sessions/v0.0.0.sql"))
    .unwrap();
  conn
    .execute_batch(include_str!("../../../schemas/migrations/sessions/0002_tree_nodes.sql"))
    .unwrap();
  conn
    .execute_batch(
      "CREATE TABLE schema_migrations (
         version INTEGER PRIMARY KEY,
         name TEXT NOT NULL,
         applied_ts INTEGER NOT NULL
       );
       INSERT INTO schema_migrations (version, name, applied_ts) VALUES
         (1, 'initial', 0),
         (2, 'tree_nodes', 0);",
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO sessions (id, first_seen_ts, last_seen_ts, source, account_id, provider_id, model)
       VALUES ('session-v2', 1783987200000, 1784073600000, 'header', 'account-v2', 'openai', 'gpt-v2')",
      [],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO session_nodes (
         id, session_id, parent_id, request_id, ts, endpoint, status, account_id, provider_id, model,
         reduction_kind, parent_source, common_prefix_messages, request_message_count, response_message_count
       ) VALUES (
         'node-v2', 'session-v2', NULL, 'request-v2', 1784073600000, 'responses', 201,
         'account-v2', 'openai', 'gpt-v2', 'root_snapshot', 'none', 0, 1, 0
       )",
      [],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO session_heads (session_id, node_id, updated_ts)
       VALUES ('session-v2', 'node-v2', 1784073600000)",
      [],
    )
    .unwrap();
  drop(conn);

  let sessions = list_sessions_from_db(&sessions_db, None).unwrap();
  assert_eq!(sessions.len(), 1);
  assert_eq!(sessions[0].session_id, "session-v2");
  assert_eq!(sessions[0].request_count, 1);
  assert_eq!(sessions[0].last_request_id, "request-v2");
  assert_eq!(sessions[0].status, Some(201));

  let detail = get_session_from_db(&sessions_db, "session-v2", None).unwrap().unwrap();
  assert_eq!(detail.head_node_id.as_deref(), Some("node-v2"));
  assert_eq!(detail.nodes.len(), 1);
  assert_eq!(detail.nodes[0].node_id, "node-v2");
  assert!(detail.nodes[0].is_head);

  let node = get_session_node_from_db(&sessions_db, "session-v2", "node-v2")
    .unwrap()
    .unwrap();
  assert_eq!(node.node.node_id, "node-v2");
  assert!(node.request_messages.is_empty());
}

#[test]
fn stored_session_detail_is_missing_without_creating_a_database() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");

  assert!(get_session_from_db(&sessions_db, "missing", None).unwrap().is_none());
  assert!(get_session_node_from_db(&sessions_db, "missing", "missing-node")
    .unwrap()
    .is_none());
  assert!(!sessions_db.exists());

  write_session(
    &sessions_db,
    "stored",
    "stored-node",
    1_800_000_000_000,
    "openai",
    "gpt-test",
  );
  assert!(get_session_from_db(&sessions_db, "missing", None).unwrap().is_none());
  assert!(get_session_node_from_db(&sessions_db, "stored", "missing-node")
    .unwrap()
    .is_none());
}

#[test]
fn stored_session_detail_keeps_the_latest_bounded_nodes_in_chronological_order() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  write_session(
    &sessions_db,
    "stored",
    "node-1",
    1_800_000_001_000,
    "openai",
    "gpt-test",
  );
  write_session(
    &sessions_db,
    "stored",
    "node-2",
    1_800_000_002_000,
    "openai",
    "gpt-test",
  );
  write_session(
    &sessions_db,
    "stored",
    "node-3",
    1_800_000_003_000,
    "openai",
    "gpt-test",
  );

  let detail = get_session_from_db(&sessions_db, "stored", Some(2)).unwrap().unwrap();
  assert!(detail.nodes_truncated);
  assert_eq!(detail.session.request_count, 3);
  assert_eq!(detail.head_node_id.as_deref(), Some("node-3"));
  assert_eq!(
    detail
      .nodes
      .iter()
      .map(|node| node.node_id.as_str())
      .collect::<Vec<_>>(),
    ["node-2", "node-3"]
  );
  assert!(!detail.nodes[0].is_head);
  assert!(detail.nodes[1].is_head);
}

#[test]
fn stored_session_detail_always_includes_the_head_when_timestamps_tie() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  write_session(
    &sessions_db,
    "stored",
    "node-z",
    1_800_000_001_000,
    "openai",
    "gpt-test",
  );
  write_session(
    &sessions_db,
    "stored",
    "node-a",
    1_800_000_001_000,
    "openai",
    "gpt-test",
  );

  let head_only = get_session_from_db(&sessions_db, "stored", Some(1)).unwrap().unwrap();
  assert!(head_only.nodes_truncated);
  assert_eq!(head_only.nodes.len(), 1);
  assert_eq!(head_only.head_node_id.as_deref(), Some("node-a"));
  assert_eq!(head_only.nodes[0].node_id, "node-a");
  assert!(head_only.nodes[0].is_head);

  let tied_nodes = get_session_from_db(&sessions_db, "stored", Some(2)).unwrap().unwrap();
  assert_eq!(
    tied_nodes
      .nodes
      .iter()
      .map(|node| node.node_id.as_str())
      .collect::<Vec<_>>(),
    ["node-a", "node-z"]
  );
}

#[test]
fn stored_sessions_normalize_epoch_seconds_before_sorting_and_serializing() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  write_session(
    &sessions_db,
    "milliseconds",
    "milliseconds-node",
    1_800_000_000_000,
    "openai",
    "gpt-ms",
  );
  write_session(
    &sessions_db,
    "seconds",
    "seconds-node",
    1_900_000_000,
    "openai",
    "gpt-seconds",
  );

  let sessions = list_sessions_from_db(&sessions_db, None).unwrap();
  assert_eq!(sessions[0].session_id, "seconds");
  assert_eq!(sessions[0].first_ts, 1_900_000_000_000);
  assert_eq!(sessions[0].last_ts, 1_900_000_000_000);

  let detail = get_session_from_db(&sessions_db, "seconds", None).unwrap().unwrap();
  assert_eq!(detail.nodes[0].ts, 1_900_000_000_000);
}

#[test]
fn stored_node_returns_input_delta_and_tags_part_content() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  sessions
    .record_tree(&semantic_record(
      "semantic",
      "root",
      1_800_000_001_000,
      vec![message("user", vec![part("text", b"hello")])],
      vec![],
    ))
    .unwrap();
  sessions
    .record_tree(&semantic_record(
      "semantic",
      "child",
      1_800_000_002_000,
      vec![
        message("user", vec![part("text", b"hello")]),
        message("user", vec![part("json", br#"{"tool":"search"}"#)]),
      ],
      vec![message(
        "assistant",
        vec![
          part("text", b"done"),
          part("json", br#"{"ok":true}"#),
          part("binary", &[0xff, 0x00]),
        ],
      )],
    ))
    .unwrap();
  drop(sessions);

  let detail = get_session_node_from_db(&sessions_db, "semantic", "child")
    .unwrap()
    .unwrap();
  assert_eq!(detail.node.parent_node_id.as_deref(), Some("root"));
  assert_eq!(detail.node.reduction_kind, "message_tree");
  assert_eq!(detail.node.common_prefix_messages, 1);
  assert_eq!(detail.node.request_message_count, 1);
  assert_eq!(detail.node.message_id.as_ref().unwrap().len(), 64);
  assert_eq!(detail.node.input_message_count, 2);
  assert_eq!(detail.node.output_message_count, 1);
  assert_eq!(detail.request_messages.len(), 1);
  assert!(matches!(
    detail.request_messages[0].parts[0].content,
    SessionPartContent::Json { ref value } if value["tool"] == "search"
  ));
  assert_eq!(detail.response_messages.len(), 1);
  assert!(matches!(
    detail.response_messages[0].parts[1].content,
    SessionPartContent::Json { ref value } if value["ok"] == true
  ));
  assert!(matches!(
    detail.response_messages[0].parts[2].content,
    SessionPartContent::Binary { byte_length: 2 }
  ));
  assert_eq!(detail.request_messages[0].parts_total, 1);
  assert_eq!(detail.response_messages[0].parts_total, 3);
  assert_eq!(detail.truncation.request_messages.messages_total, 1);
  assert_eq!(detail.truncation.request_messages.messages_returned, 1);
  assert_eq!(detail.truncation.request_messages.messages_omitted_before, 0);
  assert_eq!(detail.truncation.response_messages.messages_total, 1);
  assert_eq!(detail.truncation.parts_total, 4);
  assert_eq!(detail.truncation.parts_returned, 4);
  assert_eq!(detail.truncation.parts_omitted, 0);
  assert_eq!(detail.truncation.content_bytes_total, 34);
  assert_eq!(detail.truncation.content_bytes_returned, 32);
  assert_eq!(detail.truncation.content_parts_truncated, 0);
  assert_eq!(detail.truncation.binary_parts_elided, 1);
  let json = serde_json::to_value(&detail).unwrap();
  assert_eq!(json["request_messages"][0]["parts"][0]["content"]["encoding"], "json");
  assert_eq!(
    json["response_messages"][0]["parts"][2]["content"]["encoding"],
    "binary"
  );
  assert_eq!(json["response_messages"][0]["parts"][2]["byte_length"], 2);
  assert_eq!(json["truncation"]["parts_omitted"], 0);
}

#[test]
fn stored_node_returns_full_input_from_an_independent_branch() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  sessions
    .record_tree(&semantic_record(
      "conflict",
      "root",
      1_800_000_001_000,
      vec![message("user", vec![part("text", b"root")])],
      vec![],
    ))
    .unwrap();
  sessions
    .record_tree(&semantic_record(
      "conflict",
      "suffix",
      1_800_000_002_000,
      vec![
        message("user", vec![part("text", b"root")]),
        message("user", vec![part("text", b"suffix")]),
      ],
      vec![],
    ))
    .unwrap();
  sessions
    .record_tree(&semantic_record(
      "conflict",
      "replacement",
      1_800_000_003_000,
      vec![message("user", vec![part("text", b"replacement")])],
      vec![],
    ))
    .unwrap();
  drop(sessions);

  let detail = get_session_node_from_db(&sessions_db, "conflict", "replacement")
    .unwrap()
    .unwrap();
  assert_eq!(detail.node.parent_node_id, None);
  assert_eq!(detail.node.reduction_kind, "message_tree");
  assert_eq!(detail.node.common_prefix_messages, 0);
  assert_eq!(detail.node.request_message_count, 1);
  assert_eq!(detail.node.input_message_count, 1);
  assert_eq!(detail.node.output_message_count, 0);
  assert_eq!(detail.request_messages.len(), 1);
  assert!(matches!(
    detail.request_messages[0].parts[0].content,
    SessionPartContent::Text {
      ref value,
      truncated: false
    } if value == "replacement"
  ));
  assert_eq!(detail.truncation.request_messages.messages_total, 1);
}

#[test]
fn stored_node_does_not_treat_cross_session_storage_reuse_as_ancestry() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  let shared = message("user", vec![part("text", b"shared")]);
  sessions
    .record_tree(&semantic_record(
      "first-session",
      "first-node",
      1_800_000_001_000,
      vec![shared.clone()],
      vec![],
    ))
    .unwrap();
  sessions
    .record_tree(&semantic_record(
      "second-session",
      "second-node",
      1_800_000_002_000,
      vec![shared, message("user", vec![part("text", b"new")])],
      vec![],
    ))
    .unwrap();
  drop(sessions);

  let raw_counts = Connection::open(&sessions_db)
    .unwrap()
    .query_row(
      "SELECT common_prefix_messages, request_message_count
       FROM session_nodes
       WHERE id = 'second-node'",
      [],
      |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
    )
    .unwrap();
  assert_eq!(raw_counts, (1, 1));

  let detail = get_session_node_from_db(&sessions_db, "second-session", "second-node")
    .unwrap()
    .unwrap();
  assert_eq!(detail.node.parent_node_id, None);
  assert_eq!(detail.node.common_prefix_messages, 0);
  assert_eq!(detail.node.request_message_count, 2);
  assert_eq!(detail.request_messages.len(), 2);
  assert_eq!(detail.truncation.request_messages.messages_total, 2);
}

#[test]
fn stored_node_returns_empty_input_delta_when_node_reuses_parent_prefix() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  sessions
    .record_tree(&semantic_record(
      "unchanged",
      "root",
      1_800_000_001_000,
      vec![message("user", vec![part("text", b"hello")])],
      vec![],
    ))
    .unwrap();
  sessions
    .record_tree(&semantic_record(
      "unchanged",
      "child",
      1_800_000_002_000,
      vec![message("user", vec![part("text", b"hello")])],
      vec![message("assistant", vec![part("text", b"done")])],
    ))
    .unwrap();
  drop(sessions);

  let detail = get_session_node_from_db(&sessions_db, "unchanged", "child")
    .unwrap()
    .unwrap();
  assert_eq!(detail.node.parent_node_id.as_deref(), Some("root"));
  assert_eq!(detail.node.reduction_kind, "message_tree");
  assert_eq!(detail.node.common_prefix_messages, 1);
  assert_eq!(detail.node.request_message_count, 0);
  assert_eq!(detail.node.input_message_count, 1);
  assert_eq!(detail.node.output_message_count, 1);
  assert!(detail.request_messages.is_empty());
  assert_eq!(detail.truncation.request_messages.messages_total, 0);
  assert_eq!(detail.truncation.request_messages.messages_returned, 0);
  assert_eq!(detail.response_messages.len(), 1);
}

#[test]
fn stored_session_derives_nearest_input_ancestor_when_outputs_are_reencoded() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  let shared = vec![message("user", vec![part("text", b"task")])];
  sessions
    .record_tree(&semantic_record(
      "reencoded",
      "root",
      1_800_000_001_000,
      shared.clone(),
      vec![],
    ))
    .unwrap();

  let mut first_input = shared.clone();
  first_input.extend([
    message("reasoning", vec![part("reasoning", b"first thought")]),
    message("function_call", vec![part("function_call", b"first call")]),
    message(
      "function_call_output",
      vec![part("function_call_output", b"first result")],
    ),
  ]);
  sessions
    .record_tree(&semantic_record(
      "reencoded",
      "first",
      1_800_000_002_000,
      first_input.clone(),
      vec![message("assistant", vec![part("text", b"flattened first output")])],
    ))
    .unwrap();

  let mut second_input = first_input;
  second_input.extend([
    message("reasoning", vec![part("reasoning", b"second thought")]),
    message("function_call", vec![part("function_call", b"second call")]),
    message(
      "function_call_output",
      vec![part("function_call_output", b"second result")],
    ),
  ]);
  sessions
    .record_tree(&semantic_record(
      "reencoded",
      "second",
      1_800_000_003_000,
      second_input,
      vec![message("assistant", vec![part("text", b"flattened second output")])],
    ))
    .unwrap();
  drop(sessions);

  let conn = Connection::open(&sessions_db).unwrap();
  let stored_parent: Option<String> = conn
    .query_row("SELECT parent_id FROM session_nodes WHERE id = 'second'", [], |row| {
      row.get(0)
    })
    .unwrap();
  assert_eq!(
    stored_parent, None,
    "message-tree observations must not store node lineage"
  );
  drop(conn);

  let session = get_session_from_db(&sessions_db, "reencoded", None).unwrap().unwrap();
  let first = session.nodes.iter().find(|node| node.node_id == "first").unwrap();
  assert_eq!(first.parent_node_id.as_deref(), Some("root"));
  assert_eq!(first.common_prefix_messages, 1);
  assert_eq!(first.request_message_count, 3);

  let second = session.nodes.iter().find(|node| node.node_id == "second").unwrap();
  assert_eq!(second.parent_node_id.as_deref(), Some("first"));
  assert_eq!(second.parent_source, "input_ancestor");
  assert_eq!(second.common_prefix_messages, 4);
  assert_eq!(second.request_message_count, 3);

  let detail = get_session_node_from_db(&sessions_db, "reencoded", "second")
    .unwrap()
    .unwrap();
  assert_eq!(detail.node.parent_node_id.as_deref(), Some("first"));
  assert_eq!(detail.node.common_prefix_messages, 4);
  assert_eq!(detail.node.request_message_count, 3);
  assert_eq!(detail.request_messages.len(), 3);
  assert_eq!(detail.truncation.request_messages.messages_total, 3);
}

#[test]
fn stored_node_rejects_a_cyclic_message_tree() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  sessions
    .record_tree(&semantic_record(
      "cyclic",
      "cyclic-node",
      1_800_000_001_000,
      vec![
        message("user", vec![part("text", b"first")]),
        message("user", vec![part("text", b"second")]),
      ],
      vec![],
    ))
    .unwrap();
  drop(sessions);

  let conn = Connection::open(&sessions_db).unwrap();
  let tip_id = conn
    .query_row(
      "SELECT message_id FROM session_nodes WHERE id = 'cyclic-node'",
      [],
      |row| row.get::<_, Vec<u8>>(0),
    )
    .unwrap();
  let root_id = conn
    .query_row(
      "SELECT parent_id FROM message_tree WHERE id = ?1",
      params![tip_id.as_slice()],
      |row| row.get::<_, Vec<u8>>(0),
    )
    .unwrap();
  conn
    .execute(
      "UPDATE message_tree SET parent_id = ?1 WHERE id = ?2",
      params![tip_id.as_slice(), root_id.as_slice()],
    )
    .unwrap();
  drop(conn);

  assert!(matches!(
    get_session_node_from_db(&sessions_db, "cyclic", "cyclic-node"),
    Err(crate::Error::InvalidMessageTree { .. })
  ));
}

#[test]
fn stored_node_bounds_message_ranges_and_preserves_their_order() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let request_messages = (0..205)
    .map(|index| message(&format!("request-{index}"), vec![]))
    .collect();
  let response_messages = (0..103)
    .map(|index| message(&format!("response-{index}"), vec![]))
    .collect();
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  sessions
    .record_tree(&semantic_record(
      "bounded-messages",
      "node",
      1_800_000_001_000,
      request_messages,
      response_messages,
    ))
    .unwrap();
  drop(sessions);

  let detail = get_session_node_from_db(&sessions_db, "bounded-messages", "node")
    .unwrap()
    .unwrap();
  assert_eq!(detail.request_messages.len(), 200);
  assert_eq!(detail.request_messages.first().unwrap().role, "request-5");
  assert_eq!(detail.request_messages.last().unwrap().role, "request-204");
  assert_eq!(detail.response_messages.len(), 100);
  assert_eq!(detail.response_messages.first().unwrap().role, "response-0");
  assert_eq!(detail.response_messages.last().unwrap().role, "response-99");
  assert_eq!(detail.truncation.request_messages.messages_total, 205);
  assert_eq!(detail.truncation.request_messages.messages_returned, 200);
  assert_eq!(detail.truncation.request_messages.messages_omitted_before, 5);
  assert_eq!(detail.truncation.request_messages.messages_omitted_after, 0);
  assert_eq!(detail.truncation.response_messages.messages_total, 103);
  assert_eq!(detail.truncation.response_messages.messages_returned, 100);
  assert_eq!(detail.truncation.response_messages.messages_omitted_before, 0);
  assert_eq!(detail.truncation.response_messages.messages_omitted_after, 3);
}

#[test]
fn stored_node_bounds_parts_and_reports_exact_totals() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let parts = (0..260).map(|_| part("text", b"x")).collect();
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  sessions
    .record_tree(&semantic_record(
      "bounded-parts",
      "node",
      1_800_000_001_000,
      vec![message("user", parts)],
      vec![],
    ))
    .unwrap();
  drop(sessions);

  let detail = get_session_node_from_db(&sessions_db, "bounded-parts", "node")
    .unwrap()
    .unwrap();
  assert_eq!(detail.request_messages[0].parts_total, 260);
  assert_eq!(detail.request_messages[0].parts.len(), 256);
  assert_eq!(detail.truncation.parts_total, 260);
  assert_eq!(detail.truncation.parts_returned, 256);
  assert_eq!(detail.truncation.parts_omitted, 4);
  assert_eq!(detail.truncation.content_bytes_total, 260);
  assert_eq!(detail.truncation.content_bytes_returned, 256);
}

#[test]
fn stored_node_bounds_part_content_without_splitting_utf8_or_returning_binary() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let mut oversized_text = vec![b'a'; 65_535];
  oversized_text.extend_from_slice("é".as_bytes());
  let oversized_json = format!(r#"{{"data":"{}"}}"#, "x".repeat(70_000));
  let mut binary = vec![0_u8; 1_000_000];
  binary[0] = 0xff;
  let expected_total = oversized_text.len() + oversized_json.len() + binary.len();
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  sessions
    .record_tree(&semantic_record(
      "bounded-content",
      "node",
      1_800_000_001_000,
      vec![message(
        "user",
        vec![
          part("text", &oversized_text),
          part("json", oversized_json.as_bytes()),
          part("image", &binary),
        ],
      )],
      vec![],
    ))
    .unwrap();
  drop(sessions);

  let detail = get_session_node_from_db(&sessions_db, "bounded-content", "node")
    .unwrap()
    .unwrap();
  let parts = &detail.request_messages[0].parts;
  assert_eq!(parts[0].byte_length, oversized_text.len() as u64);
  assert!(matches!(
    parts[0].content,
    SessionPartContent::Text {
      ref value,
      truncated: true
    } if value.len() == 65_535 && value.is_ascii()
  ));
  assert!(matches!(
    parts[1].content,
    SessionPartContent::Omitted {
      original_encoding: SessionPartEncoding::Json,
      reason: SessionPartOmissionReason::PartLimit
    }
  ));
  assert!(matches!(
    parts[2].content,
    SessionPartContent::Binary { byte_length: 1_000_000 }
  ));
  assert_eq!(detail.truncation.content_bytes_total, expected_total as u64);
  assert_eq!(detail.truncation.content_bytes_returned, 65_535);
  assert_eq!(detail.truncation.content_parts_truncated, 2);
  assert_eq!(detail.truncation.binary_parts_elided, 1);
  let json = serde_json::to_string(&detail).unwrap();
  assert!(!json.contains("base64"));
  assert!(json.contains(r#""encoding":"binary","byte_length":1000000"#));
  assert!(json.contains(r#""encoding":"omitted","original_encoding":"json","reason":"part_limit""#));
}

#[test]
fn stored_node_elides_utf8_binary_types_and_embedded_media_payloads() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let utf8_base64 = br#"aGVsbG8gd29ybGQ="#;
  let raw_image_base64 = br#"iVBORw0KGgo="#;
  let image = br#"{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}"#;
  let audio = br#"{"type":"input_audio","input_audio":{"data":"UklGRg==","format":"wav"}}"#;
  let embedded = br#"{"result":{"b64_json":"AQIDBA=="}}"#;
  let encrypted = br#"{"type":"reasoning","encrypted_content":"ZW5jcnlwdGVk"}"#;
  let blob = br#"{"result":{"blob":"YmluYXJ5"}}"#;
  let safe_json = br#"{"data":"ordinary text","url":"https://example.com/result.json"}"#;
  let remote_image = br#"{"type":"input_image","detail":"auto","image_url":"https://example.com/image.png"}"#;
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  sessions
    .record_tree(&semantic_record(
      "binary-content",
      "node",
      1_800_000_001_000,
      vec![message(
        "user",
        vec![
          part("binary", utf8_base64),
          part("image", raw_image_base64),
          part("input_image", image),
          part("input_audio", audio),
          part("tool_result", embedded),
          part("reasoning", encrypted),
          part("tool_result", blob),
          part("tool_result", safe_json),
          part("input_image", remote_image),
        ],
      )],
      vec![],
    ))
    .unwrap();
  drop(sessions);

  let detail = get_session_node_from_db(&sessions_db, "binary-content", "node")
    .unwrap()
    .unwrap();
  let parts = &detail.request_messages[0].parts;
  for (index, expected_length) in [
    (0, utf8_base64.len()),
    (1, raw_image_base64.len()),
    (2, image.len()),
    (3, audio.len()),
    (4, embedded.len()),
    (6, blob.len()),
  ] {
    let part = &parts[index];
    assert_eq!(part.byte_length, expected_length as u64);
    assert!(matches!(
      part.content,
      SessionPartContent::Binary { byte_length } if byte_length == expected_length as u64
    ));
  }
  assert_eq!(parts[5].byte_length, encrypted.len() as u64);
  assert!(matches!(
    parts[5].content,
    SessionPartContent::Encrypted { byte_length } if byte_length == "ZW5jcnlwdGVk".len() as u64
  ));
  let serialized = serde_json::to_string(&detail).unwrap();
  assert!(serialized.contains(r#""encoding":"encrypted","byte_length":12"#));
  assert!(!serialized.contains("ZW5jcnlwdGVk"));
  assert!(matches!(
    parts[7].content,
    SessionPartContent::Json { ref value }
      if value["data"] == "ordinary text" && value["url"] == "https://example.com/result.json"
  ));
  assert!(matches!(
    parts[8].content,
    SessionPartContent::Json { ref value }
      if value["image_url"] == "https://example.com/image.png" && value["detail"] == "auto"
  ));
  assert_eq!(detail.truncation.binary_parts_elided, 7);
  assert_eq!(
    detail.truncation.content_bytes_returned,
    (safe_json.len() + remote_image.len()) as u64
  );
  assert_eq!(detail.truncation.content_parts_truncated, 0);

  let json = serde_json::to_string(&detail).unwrap();
  assert!(!json.contains("aGVsbG8gd29ybGQ="));
  assert!(!json.contains("iVBORw0KGgo="));
  assert!(!json.contains("UklGRg=="));
  assert!(!json.contains("AQIDBA=="));
  assert!(!json.contains("ZW5jcnlwdGVk"));
  assert!(!json.contains("YmluYXJ5"));
  assert!(!json.contains("data:image"));
}

#[test]
fn stored_node_caps_aggregate_inline_content_bytes() {
  let dir = tempdir();
  let sessions_db = dir.join("sessions.db");
  let content = vec![b'x'; 65_536];
  let mut sessions = SessionsDb::open(&sessions_db).unwrap();
  sessions
    .record_tree(&semantic_record(
      "aggregate-content",
      "node",
      1_800_000_001_000,
      vec![message("user", (0..5).map(|_| part("text", &content)).collect())],
      vec![],
    ))
    .unwrap();
  drop(sessions);

  let detail = get_session_node_from_db(&sessions_db, "aggregate-content", "node")
    .unwrap()
    .unwrap();
  assert_eq!(detail.truncation.content_bytes_total, 327_680);
  assert_eq!(detail.truncation.content_bytes_returned, 262_144);
  assert_eq!(detail.truncation.content_parts_truncated, 1);
  assert!(matches!(
    detail.request_messages[0].parts[4].content,
    SessionPartContent::Omitted {
      original_encoding: SessionPartEncoding::Text,
      reason: SessionPartOmissionReason::AggregateLimit
    }
  ));
}

fn semantic_record(
  session_id: &str,
  request_id: &str,
  ts: i64,
  request_messages: Vec<MessageRecord>,
  response_messages: Vec<MessageRecord>,
) -> TreeRequestRecord {
  TreeRequestRecord {
    ts,
    session_id: session_id.to_string(),
    thread_id: None,
    parent_thread_id: None,
    parent_session_id: None,
    request_id: request_id.to_string(),
    endpoint: "responses".to_string(),
    status: Some(200),
    account_id: Some("account-1".to_string()),
    provider_id: Some("openai".to_string()),
    model: Some("gpt-test".to_string()),
    request_messages,
    response_messages,
  }
}

fn message(role: &str, parts: Vec<PartRecord>) -> MessageRecord {
  MessageRecord {
    role: role.to_string(),
    status: None,
    parts,
  }
}

fn part(part_type: &str, content: &[u8]) -> PartRecord {
  PartRecord {
    part_type: part_type.to_string(),
    content: Bytes::copy_from_slice(content),
  }
}

#[test]
fn reads_requests_and_infers_sessions_across_days() {
  let dir = tempdir();
  write_request(
    &dir,
    "2026-07-13",
    "request-old",
    1_784_358_400_000,
    Some("session-1"),
    Some("openai"),
  );
  write_request(
    &dir,
    "2026-07-14",
    "request-new",
    1_784_444_800_000,
    Some("session-1"),
    Some("openai"),
  );
  write_request(
    &dir,
    "2026-07-14",
    "request-other",
    1_784_444_801_000,
    Some("session-2"),
    Some("zai"),
  );

  let page = list_requests(&dir, &RequestListOptions::default()).unwrap();
  assert_eq!(
    page
      .requests
      .iter()
      .map(|request| request.request_id.as_str())
      .collect::<Vec<_>>(),
    ["request-other", "request-new", "request-old"]
  );

  let sessions = list_sessions(&dir, None).unwrap();
  assert_eq!(sessions.len(), 2);
  assert_eq!(sessions[0].session_id, "session-2");
  let session = sessions
    .iter()
    .find(|session| session.session_id == "session-1")
    .unwrap();
  assert_eq!(session.request_count, 2);
  assert!(session.source.is_none());
  assert_eq!(session.last_request_id, "request-new");

  let detail = get_session(&dir, "session-1", None).unwrap().unwrap();
  assert_eq!(detail.requests.len(), 2);
  assert_eq!(detail.requests[0].request_id, "request-old");
  assert_eq!(detail.requests[1].request_id, "request-new");

  let limited_detail = get_session(&dir, "session-1", Some(1)).unwrap().unwrap();
  assert_eq!(limited_detail.session.request_count, 2);
  assert_eq!(limited_detail.requests.len(), 1);
  assert_eq!(limited_detail.requests[0].request_id, "request-new");
}
