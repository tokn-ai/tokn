use crate::requests::open_day_db;

use super::super::{
  list_latest_requests, list_request_url_paths, list_requests, list_sessions, InvalidRequestCursor, RequestCursor,
  RequestListOptions,
};
use super::support::{request_ids, tempdir, write_request};

#[test]
fn request_summaries_read_classification_without_requiring_body_payloads() {
  let dir = tempdir();
  let day = "2026-07-14";
  for request_id in ["compaction", "normal", "unknown-classification", "invalid-params"] {
    write_request(&dir, day, request_id, 1_784_444_800_000, None, Some("openai"));
  }
  let conn = open_day_db(&dir.join(format!("{day}.db"))).unwrap();
  for (request_id, params_json) in [
    (
      "compaction",
      r#"{"stream":false,"request_classification":{"purpose":"compaction","source":"codex_prompt"}}"#,
    ),
    (
      "unknown-classification",
      r#"{"request_classification":{"purpose":"compaction","source":"future_source"}}"#,
    ),
    ("invalid-params", "invalid json"),
  ] {
    conn
      .execute(
        "UPDATE request_metadata SET params_json = ?2 WHERE request_id = ?1",
        [request_id, params_json],
      )
      .unwrap();
  }
  conn
    .execute("UPDATE request_downstream SET inbound_req_body = NULL", [])
    .unwrap();

  let page = list_requests(
    &dir,
    &RequestListOptions {
      day: Some(day.to_string()),
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  assert_eq!(page.requests.len(), 4);
  for request in page.requests {
    if request.request_id == "compaction" {
      assert_eq!(
        serde_json::to_value(request.request_classification).unwrap(),
        serde_json::json!({"purpose": "compaction", "source": "codex_prompt"})
      );
    } else {
      assert!(request.request_classification.is_none());
    }
  }
}

#[test]
fn paginates_a_request_day_without_duplicates_at_equal_timestamps() {
  let dir = tempdir();
  for request_id in ["request-a", "request-b", "request-c", "request-d", "request-e"] {
    write_request(
      &dir,
      "2026-07-14",
      request_id,
      1_784_444_800_000,
      Some("session-1"),
      Some("openai"),
    );
  }

  let mut options = RequestListOptions {
    day: Some("2026-07-14".to_string()),
    limit: Some(2),
    ..RequestListOptions::default()
  };
  let first = list_requests(&dir, &options).unwrap();
  assert_eq!(request_ids(&first.requests), ["request-e", "request-d"]);
  options.cursor = Some(RequestCursor::decode(first.next_cursor.as_deref().unwrap()).unwrap());

  let second = list_requests(&dir, &options).unwrap();
  assert_eq!(request_ids(&second.requests), ["request-c", "request-b"]);
  options.cursor = Some(RequestCursor::decode(second.next_cursor.as_deref().unwrap()).unwrap());

  let third = list_requests(&dir, &options).unwrap();
  assert_eq!(request_ids(&third.requests), ["request-a"]);
  assert!(third.next_cursor.is_none());

  let all_ids = first
    .requests
    .iter()
    .chain(&second.requests)
    .chain(&third.requests)
    .map(|request| request.request_id.as_str())
    .collect::<std::collections::HashSet<_>>();
  assert_eq!(all_ids.len(), 5);
}

#[test]
fn rejects_malformed_request_cursors() {
  for cursor in [
    "",
    "v1.2026-07-14.1784444800000.1",
    "v2.not-a-day.1784444800000.1",
    "v2.2026-07-14.not-a-timestamp.1",
    "v2.2026-07-14.1784444800000.not-a-rowid",
    "v2.2026-07-14.1784444800000",
    "v2.2026-07-14.1784444800000.1.extra",
  ] {
    assert_eq!(RequestCursor::decode(cursor), Err(InvalidRequestCursor));
  }
}

#[test]
fn errors_only_includes_all_split_request_failure_signals() {
  let dir = tempdir();
  for request_id in [
    "healthy",
    "request-error",
    "lifecycle-error",
    "upstream-error",
    "downstream-error",
  ] {
    write_request(
      &dir,
      "2026-07-14",
      request_id,
      1_784_444_800_000,
      Some("session-1"),
      Some("openai"),
    );
  }
  let conn = open_day_db(&dir.join("2026-07-14.db")).unwrap();
  conn
    .execute(
      "UPDATE request_connection SET request_error = 'failed' WHERE request_id = 'request-error'",
      [],
    )
    .unwrap();
  conn
    .execute(
      "UPDATE request_connection SET status = 500 WHERE request_id = 'lifecycle-error'",
      [],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO request_upstream (request_id, outbound_resp_status) VALUES ('upstream-error', 502)",
      [],
    )
    .unwrap();
  conn
    .execute(
      "UPDATE request_downstream SET inbound_resp_status = 404 WHERE request_id = 'downstream-error'",
      [],
    )
    .unwrap();

  let page = list_requests(
    &dir,
    &RequestListOptions {
      day: Some("2026-07-14".to_string()),
      errors_only: true,
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  let ids = request_ids(&page.requests)
    .into_iter()
    .collect::<std::collections::HashSet<_>>();
  assert_eq!(ids.len(), 4);
  assert!(ids.contains("request-error"));
  assert!(ids.contains("lifecycle-error"));
  assert!(ids.contains("upstream-error"));
  assert!(ids.contains("downstream-error"));
  assert!(!ids.contains("healthy"));
}

#[test]
fn exact_status_filter_uses_downstream_then_upstream_then_lifecycle_precedence() {
  let dir = tempdir();
  write_request(
    &dir,
    "2026-07-14",
    "request-status",
    1_784_444_800_000,
    Some("session-1"),
    Some("openai"),
  );
  let conn = open_day_db(&dir.join("2026-07-14.db")).unwrap();
  conn
    .execute(
      "UPDATE request_connection SET status = 500 WHERE request_id = 'request-status'",
      [],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO request_upstream (request_id, outbound_resp_status) VALUES ('request-status', 502)",
      [],
    )
    .unwrap();
  conn
    .execute(
      "UPDATE request_downstream SET inbound_resp_status = 201 WHERE request_id = 'request-status'",
      [],
    )
    .unwrap();

  let matching = list_requests(
    &dir,
    &RequestListOptions {
      day: Some("2026-07-14".to_string()),
      status: Some(201),
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  assert_eq!(request_ids(&matching.requests), ["request-status"]);
  for shadowed_status in [500, 502] {
    let page = list_requests(
      &dir,
      &RequestListOptions {
        day: Some("2026-07-14".to_string()),
        status: Some(shadowed_status),
        ..RequestListOptions::default()
      },
    )
    .unwrap();
    assert!(page.requests.is_empty());
  }
}

#[test]
fn lists_and_filters_normalized_url_paths_across_raw_url_variants() {
  let dir = tempdir();
  for request_id in ["search-relative", "search-absolute", "responses"] {
    write_request(
      &dir,
      "2026-07-14",
      request_id,
      1_784_444_800_000,
      Some("session-1"),
      Some("openai"),
    );
  }
  let conn = open_day_db(&dir.join("2026-07-14.db")).unwrap();
  conn
    .execute(
      "UPDATE request_downstream SET inbound_req_url = ?2 WHERE request_id = ?1",
      ["search-relative", "/backend-api/codex/alpha/search?client_version=1"],
    )
    .unwrap();
  conn
    .execute(
      "UPDATE request_downstream SET inbound_req_url = ?2 WHERE request_id = ?1",
      [
        "search-absolute",
        "https://chatgpt.com/backend-api/codex/alpha/search?client_version=2",
      ],
    )
    .unwrap();

  let paths = list_request_url_paths(&dir, "2026-07-14").unwrap();
  assert_eq!(paths[0].url_path, "/backend-api/codex/alpha/search");
  assert_eq!(paths[0].request_count, 2);
  assert!(paths
    .iter()
    .any(|path| path.url_path == "/v1/responses" && path.request_count == 1));

  let mut options = RequestListOptions {
    day: Some("2026-07-14".to_string()),
    url_path: Some("/backend-api/codex/alpha/search".to_string()),
    limit: Some(1),
    ..RequestListOptions::default()
  };
  let first = list_requests(&dir, &options).unwrap();
  assert_eq!(first.requests.len(), 1);
  assert!(first.next_cursor.is_some());
  options.cursor = Some(RequestCursor::decode(first.next_cursor.as_deref().unwrap()).unwrap());
  let second = list_requests(&dir, &options).unwrap();
  assert_eq!(second.requests.len(), 1);
  assert!(second.next_cursor.is_none());
  let ids = first
    .requests
    .iter()
    .chain(&second.requests)
    .map(|request| request.request_id.as_str())
    .collect::<std::collections::HashSet<_>>();
  assert_eq!(
    ids,
    std::collections::HashSet::from(["search-relative", "search-absolute"])
  );
}

#[test]
fn aggregate_queries_skip_corrupt_request_day_databases() {
  let dir = tempdir();
  write_request(
    &dir,
    "2026-07-14",
    "request-valid",
    1_784_444_800_000,
    Some("session-valid"),
    Some("openai"),
  );
  std::fs::write(dir.join("2026-07-15.db"), b"not a sqlite database").unwrap();

  let page = list_requests(&dir, &RequestListOptions::default()).unwrap();
  assert_eq!(page.requests.len(), 1);
  assert_eq!(page.requests[0].request_id, "request-valid");

  let sessions = list_sessions(&dir, None).unwrap();
  assert_eq!(sessions.len(), 1);
  assert_eq!(sessions[0].session_id, "session-valid");

  assert!(list_requests(
    &dir,
    &RequestListOptions {
      day: Some("2026-07-15".to_string()),
      ..RequestListOptions::default()
    }
  )
  .is_err());
  assert!(super::super::get_request(&dir, "2026-07-15", "missing", None).is_err());
}

#[test]
fn latest_requests_skip_empty_and_unavailable_days() {
  let dir = tempdir();
  write_request(
    &dir,
    "2026-07-14",
    "request-old",
    1_784_444_800_000,
    Some("session-old"),
    Some("openai"),
  );
  write_request(
    &dir,
    "2026-07-14",
    "request-latest",
    1_784_444_801_000,
    Some("session-old"),
    Some("openai"),
  );
  drop(open_day_db(&dir.join("2026-07-15.db")).unwrap());
  std::fs::write(dir.join("2026-07-16.db"), b"not a sqlite database").unwrap();

  let latest = list_latest_requests(&dir, Some(1), None).unwrap();
  assert_eq!(latest.day.as_deref(), Some("2026-07-14"));
  assert_eq!(latest.requests.len(), 1);
  assert_eq!(latest.requests[0].request_id, "request-latest");
  let cursor = RequestCursor::decode(latest.next_cursor.as_deref().unwrap()).unwrap();
  let next = list_latest_requests(&dir, Some(1), Some(cursor)).unwrap();
  assert_eq!(next.day.as_deref(), Some("2026-07-14"));
  assert_eq!(request_ids(&next.requests), ["request-old"]);
  assert!(next.next_cursor.is_none());
}

#[test]
fn lists_requests_from_only_the_selected_day() {
  let dir = tempdir();
  write_request(
    &dir,
    "2026-07-14",
    "request-old",
    1_784_444_800_000,
    Some("session-old"),
    Some("openai"),
  );
  write_request(
    &dir,
    "2026-07-15",
    "request-new",
    1_784_531_200_000,
    Some("session-new"),
    Some("zai"),
  );

  let page = list_requests(
    &dir,
    &RequestListOptions {
      day: Some("2026-07-14".to_string()),
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  assert_eq!(page.requests.len(), 1);
  assert_eq!(page.requests[0].request_id, "request-old");

  let missing_day_page = list_requests(
    &dir,
    &RequestListOptions {
      day: Some("2026-07-13".to_string()),
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  assert!(missing_day_page.requests.is_empty());
}
